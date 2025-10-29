import os
import time
import sys
import logging
from urllib.parse import urljoin, urlparse
import csv

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup


BASE_URL = "https://polkrf.ru"
LIST_URL_TEMPLATE = BASE_URL + "/veterans?page={page_num}"
PLACEHOLDER_URL = "https://polkrf.ru/assets/index/img/veteran_card_placeholder.jpg"
IMAGES_DIR = os.path.join(os.path.dirname(__file__), "images")
METADA_FILE = "metadata.csv"

# Polite crawling parameters
REQUEST_TIMEOUT_SECONDS = 20
DELAY_BETWEEN_REQUESTS_SECONDS = 1.5
MAX_IMAGES = 100


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


def extract_card_id(card_url: str) -> str | None:
    """Extract trailing numeric id from card URL like .../name-12345.

    Returns the numeric id as string or None if not found.
    """
    # Drop query/fragment
    parsed = urlparse(card_url)
    path = parsed.path.rstrip("/")
    if not path:
        return None
    last_segment = path.split("/")[-1]
    tail = last_segment.split("-")[-1]
    digits = "".join(ch for ch in tail if ch.isdigit())
    return digits if digits else None


def _clean_text(text: str | None) -> str:
    if not text:
        return ""
    return " ".join(text.split())


def fetch_card_details(card_url: str) -> dict[str, str]:
    """Fetch and parse veteran card details from card_url using provided selectors.

    Returns a dict with keys: card_id, veteran_name, birth_date, birth_place,
    death_date, death_place, operations, biography, rewards.
    """
    details: dict[str, str] = {
        "card_id": extract_card_id(card_url) or "",
        "veteran_name": "",
        "birth_date": "",
        "birth_place": "",
        "death_date": "",
        "death_place": "",
        "operations": "",
        "biography": "",
        "rewards": "",
    }

    try:
        resp = http_get(card_url)
    except Exception as e:
        logger.warning("Failed to fetch card page %s: %s", card_url, e)
        return details

    soup = BeautifulSoup(resp.text, "html.parser")

    # Name from span.b-title-1__inner (concatenate first two children)
    name_span = soup.select_one("span.b-title-1__inner")
    if name_span is not None:
        child_texts = [
            _clean_text(getattr(child, "get_text", lambda *_: "")(" "))
            for child in name_span.children
        ]
        child_texts = [t for t in child_texts if t]
        details["veteran_name"] = " ".join(child_texts[:2])

    # Birth/Death info: label in .b-text-info__name, value in span.b-text-info__text
    for item in soup.select("div.b-text-info.b-text-info-double__item"):
        label_el = item.select_one(".b-text-info__name")
        value_el = item.select_one("span.b-text-info__text")
        label = _clean_text(label_el.get_text(" ")) if label_el else ""
        value = _clean_text(value_el.get_text(" ")) if value_el else ""
        if not label or not value:
            continue
        ll = label.lower()
        if "дата рождения" in ll:
            details["birth_date"] = value
        elif "место рождения" in ll:
            details["birth_place"] = value
        elif "дата смерти" in ll:
            details["death_date"] = value
        elif "место смерти" in ll or "место гибели" in ll:
            details["death_place"] = value

    # Operations list
    ops = [
        _clean_text(a.get_text(" "))
        for a in soup.select("div.b-list.b-operation__list ul li a")
    ]
    ops = [op for op in ops if op]
    if ops:
        details["operations"] = "; ".join(ops)

    # Biography paragraph
    bio_p = soup.select_one("div.b-operation__wrap p")
    if bio_p is not None:
        details["biography"] = _clean_text(bio_p.get_text(" "))

    # Rewards from medals slider container
    rewards_container = soup.select_one(
        "div.b-medals-slider__container.embla__container"
    )
    rewards: list[str] = []
    if rewards_container is not None:
        for child in rewards_container.find_all(recursive=False):
            link = child.find("a")
            if link is not None:
                link_children = [
                    c for c in link.children if getattr(c, "get_text", None)
                ]
                if len(link_children) >= 2:
                    rewards.append(_clean_text(link_children[1].get_text(" ")))
                    continue
                rewards.append(_clean_text(link.get_text(" ")))
                continue
            rewards.append(_clean_text(child.get_text(" ")))
    rewards = [r for r in rewards if r]
    if rewards:
        details["rewards"] = "; ".join(rewards)

    return details


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


def fetch_image_urls_from_page(page_num: int) -> list[tuple[str, str]]:
    """Return (image_url, card_url) pairs from a listing page.

    Filters by selectors and excludes placeholder image entries.
    """
    list_url = LIST_URL_TEMPLATE.format(page_num=page_num)
    logger.info("Fetching page #%s: %s", page_num, list_url)
    resp = http_get(list_url)
    soup = BeautifulSoup(resp.text, "html.parser")
    entries: list[tuple[str, str]] = []

    # Each card is an anchor with class b-veteran-card; inside it there is an image
    for anchor in soup.select("a.b-veteran-card"):
        href = (anchor.get("href") or "").strip()
        if not href:
            continue
        card_url = urljoin(BASE_URL, href)

        img = anchor.select_one("img.b-veteran-card__img")
        if img is None:
            logger.debug(
                "Skipping card without image on page %s (%s)", page_num, card_url
            )
            continue
        src = (img.get("src") or "").strip()
        if not src:
            logger.debug(
                "Skipping image without src on page %s (%s)", page_num, card_url
            )
            continue
        image_url = urljoin(BASE_URL, src)
        if image_url == PLACEHOLDER_URL:
            logger.debug("Skipping placeholder image: %s", image_url)
            continue

        entries.append((image_url, card_url))

    logger.info(
        "Found %d candidate image-card entries on page %s", len(entries), page_num
    )
    return entries


def download_image(url: str, out_basename: str) -> bool:
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

    # Choose extension by content-type
    ext = ".jpg"
    if "png" in content_type:
        ext = ".png"
    elif "webp" in content_type:
        ext = ".webp"
    elif "jpeg" in content_type or "jpg" in content_type:
        ext = ".jpg"

    out_path = os.path.join(IMAGES_DIR, f"{out_basename}{ext}")
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

    # Prepare metadata file header if needed
    try:
        need_header = (
            not os.path.exists(METADA_FILE) or os.path.getsize(METADA_FILE) == 0
        )
        if need_header:
            with open(METADA_FILE, mode="a", newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f, delimiter=",")
                writer.writerow(
                    [
                        "card_id",
                        "veteran_name",
                        "birth_date",
                        "birth_place",
                        "death_date",
                        "death_place",
                        "operations",
                        "biography",
                        "rewards",
                    ]
                )  # initial header
    except Exception as e:
        logger.warning("Could not initialize metadata file '%s': %s", METADA_FILE, e)

    while downloaded_count < MAX_IMAGES:
        try:
            entries = fetch_image_urls_from_page(page_num)
        except Exception as e:
            # If a page fails to load or parse, wait and try the next one
            logger.warning("Failed to process page %s: %s", page_num, e)
            time.sleep(DELAY_BETWEEN_REQUESTS_SECONDS)
            page_num += 1
            continue

        # If no images found, still advance to next page to avoid infinite loop
        for img_url, card_url in entries:
            if downloaded_count >= MAX_IMAGES:
                break
            if img_url in seen_urls:
                logger.debug("Duplicate image URL skipped: %s", img_url)
                continue
            seen_urls.add(img_url)

            card_id = extract_card_id(card_url)
            if not card_id:
                logger.debug("Could not extract id from card URL: %s", card_url)
                time.sleep(DELAY_BETWEEN_REQUESTS_SECONDS)
                continue

            # Parse card details before saving metadata
            details = fetch_card_details(card_url)
            if not details.get("card_id"):
                details["card_id"] = card_id

            if download_image(img_url, card_id):
                downloaded_count += 1
                logger.debug("Downloaded count: %d", downloaded_count)
                # Append metadata row
                try:
                    with open(
                        METADA_FILE, mode="a", newline="", encoding="utf-8-sig"
                    ) as f:
                        writer = csv.writer(f, delimiter=",")
                        writer.writerow(
                            [
                                details.get("card_id", ""),
                                details.get("veteran_name", ""),
                                details.get("birth_date", ""),
                                details.get("birth_place", ""),
                                details.get("death_date", ""),
                                details.get("death_place", ""),
                                details.get("operations", ""),
                                details.get("biography", ""),
                                details.get("rewards", ""),
                            ]
                        )
                except Exception as e:
                    logger.warning("Failed to write metadata for %s: %s", img_url, e)
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
