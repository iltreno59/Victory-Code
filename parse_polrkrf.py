import os
import time
import sys
import logging
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup


BASE_URL = "https://polkrf.ru"
LIST_URL_TEMPLATE = BASE_URL + "/veterans?page={page_num}"
PLACEHOLDER_URL = "https://polkrf.ru/assets/index/img/veteran_card_placeholder.jpg"
IMAGES_DIR = os.path.join(os.path.dirname(__file__), "images")

# Polite crawling parameters
REQUEST_TIMEOUT_SECONDS = 20
DELAY_BETWEEN_REQUESTS_SECONDS = 1.5
MAX_IMAGES = 1000


# Logger setup
def _init_logger() -> logging.Logger:
    log_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, log_level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("polkrf_scraper")


logger = _init_logger()


def ensure_images_dir_exists() -> None:
    if not os.path.isdir(IMAGES_DIR):
        os.makedirs(IMAGES_DIR, exist_ok=True)
        logger.info("Created images directory at '%s'", IMAGES_DIR)


def get_filename_from_url(url: str) -> str:
    """Derive a safe filename from the URL path.

    If a file with the same name exists, append a numeric suffix to avoid overwrite.
    """
    parsed = urlparse(url)
    base_name = os.path.basename(parsed.path) or "image"
    # Remove query string artifacts
    if "?" in base_name:
        base_name = base_name.split("?", 1)[0]
    # Fallback extension if missing
    root, ext = os.path.splitext(base_name)
    if not ext:
        ext = ".jpg"
    candidate = root + ext
    idx = 1
    while os.path.exists(os.path.join(IMAGES_DIR, candidate)):
        candidate = f"{root}_{idx}{ext}"
        idx += 1
    return candidate


_session: requests.Session | None = None


def _get_session() -> requests.Session:
    global _session
    if _session is not None:
        return _session
    session = requests.Session()
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/127.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ru,en;q=0.9",
        "Connection": "keep-alive",
    }
    session.headers.update(headers)

    # Retries for common transient errors
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    _session = session
    return session


def http_get(url: str) -> requests.Response:
    """HTTP GET with session, retries, basic headers and timeout."""
    session = _get_session()
    try:
        logger.debug("GET %s", url)
        resp = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        if resp.status_code >= 400:
            logger.warning("HTTP %s for %s", resp.status_code, url)
        resp.raise_for_status()
        return resp
    except requests.exceptions.SSLError as e:
        logger.error("SSL error for %s: %s", url, e)
        raise
    except requests.exceptions.ConnectTimeout as e:
        logger.error("Connect timeout for %s: %s", url, e)
        raise
    except requests.exceptions.ReadTimeout as e:
        logger.error("Read timeout for %s: %s", url, e)
        raise
    except requests.exceptions.ConnectionError as e:
        logger.error("Connection error for %s: %s", url, e)
        raise


def fetch_image_urls_from_page(page_num: int) -> list[str]:
    """Return absolute image URLs from a listing page, filtered by selector and excluding placeholder."""
    list_url = LIST_URL_TEMPLATE.format(page_num=page_num)
    logger.info("Fetching page #%s: %s", page_num, list_url)
    resp = http_get(list_url)
    soup = BeautifulSoup(resp.text, "html.parser")
    urls: list[str] = []
    for img in soup.select("img.b-veteran-card__img"):
        src = (img.get("src") or "").strip()
        if not src:
            logger.debug("Skipping image without src on page %s", page_num)
            continue
        abs_url = urljoin(BASE_URL, src)
        if abs_url == PLACEHOLDER_URL:
            logger.debug("Skipping placeholder image: %s", abs_url)
            continue
        urls.append(abs_url)
    logger.info("Found %d candidate images on page %s", len(urls), page_num)
    return urls


def download_image(url: str) -> bool:
    """Download image to images folder. Returns True if saved, False otherwise."""
    try:
        logger.debug("Downloading image: %s", url)
        resp = http_get(url)
    except Exception as e:
        logger.warning("Failed to fetch image %s: %s", url, e)
        return False

    # Basic content-type validation
    content_type = resp.headers.get("Content-Type", "").lower()
    if "image" not in content_type:
        logger.warning("Non-image content-type '%s' for %s", content_type, url)
        return False

    filename = get_filename_from_url(url)
    out_path = os.path.join(IMAGES_DIR, filename)
    try:
        with open(out_path, "wb") as f:
            f.write(resp.content)
        logger.info("Saved image -> %s", out_path)
        return True
    except Exception as e:
        logger.error("Failed to save image %s to %s: %s", url, out_path, e)
        return False


def main() -> None:
    ensure_images_dir_exists()

    downloaded_count = 0
    seen_urls: set[str] = set()
    page_num = 1

    while downloaded_count < MAX_IMAGES:
        try:
            image_urls = fetch_image_urls_from_page(page_num)
        except Exception as e:
            # If a page fails to load or parse, wait and try the next one
            logger.warning("Failed to process page %s: %s", page_num, e)
            time.sleep(DELAY_BETWEEN_REQUESTS_SECONDS)
            page_num += 1
            continue

        # If no images found, still advance to next page to avoid infinite loop
        for img_url in image_urls:
            if downloaded_count >= MAX_IMAGES:
                break
            if img_url in seen_urls:
                logger.debug("Duplicate image URL skipped: %s", img_url)
                continue
            seen_urls.add(img_url)

            if download_image(img_url):
                downloaded_count += 1
                logger.debug("Downloaded count: %d", downloaded_count)
            # Always delay between requests (both page and image fetches)
            time.sleep(DELAY_BETWEEN_REQUESTS_SECONDS)

        # Delay before next page request
        if downloaded_count < MAX_IMAGES:
            time.sleep(DELAY_BETWEEN_REQUESTS_SECONDS)
            page_num += 1
            logger.info(
                "Proceeding to next page: %s (downloaded: %d)",
                page_num,
                downloaded_count,
            )

    # Optional: print a small completion message for CLI usage
    logger.info("Downloaded exactly %d images to '%s'", downloaded_count, IMAGES_DIR)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted by user", file=sys.stderr)
