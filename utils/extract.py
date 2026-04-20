from datetime import datetime
from time import sleep
import pandas as pd
import requests
from bs4 import BeautifulSoup
import logging
from typing import Any
from concurrent.futures import ThreadPoolExecutor, as_completed
logger = logging.getLogger(__name__) 

URL_LINK = "https://fashion-studio.dicoding.dev"
MAX_PAGES = 50
MAX_WORKERS = 10

def get_product_info(product_card: BeautifulSoup) -> dict[str, Any]:
    detail: dict[str, Any] = {
        "Judul": None, "Harga": None, "Peringkat": None,
        "Warna": None, "Ukuran": None, "Gender": None
    }
    try:
        title_tag = product_card.find("h3", class_="product-title")
        if title_tag: detail["Judul"] = title_tag.get_text(strip=True)

        price_container = product_card.find("div", class_="price-container")
        if price_container:
            price_span = price_container.find("span", class_="price")
            if price_span: detail["Harga"] = price_span.get_text(strip=True)
        else:
            price_unit = product_card.find("p", class_="price")
            if price_unit and "Price Unavailable" in price_unit.get_text(strip=True):
                detail["Harga"] = "Price Unavailable"

        product_tag_all = product_card.find_all("p", style="font-size: 14px; color: #777;")
        for product_tag in product_tag_all:
            text = product_tag.get_text(strip=True)
            if text.startswith("Rating:"): detail["Peringkat"] = text
            elif "Colors" in text and not text.startswith("Rating:"): detail["Warna"] = text
            elif text.startswith("Size:"): detail["Ukuran"] = text
            elif text.startswith("Gender:"): detail["Gender"] = text
        
        if not detail["Peringkat"]:
            product_no_rating = product_card.find("p", string=lambda t: t and "Not Rated" in t)
            if product_no_rating: detail["Peringkat"] = product_no_rating.get_text(strip=True)

    except AttributeError as e:
        logger.error("Kesalahan parsing kartu produk: %s - Kartu: %s", e, product_card.prettify()[:200])
    return detail

def extract_data():
    product_data: list[dict[str, Any]] = []
    timestamp_now = datetime.now().isoformat()

    def scrape_page(page_num):
        url = URL_LINK if page_num == 1 else f"{URL_LINK}/page{page_num}"
        logger.info("Scraping halaman %d: %s", page_num, url)
        page_items = []
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error("Gagal mengambil halaman %d: %s", page_num, e)
            return []
        soup = BeautifulSoup(resp.content, 'html.parser')
        grid = soup.find('div', id="collectionList")
        if not grid:
            logger.info("Tidak ada grid produk ditemukan di halaman %d", page_num)
            return []
        cards = grid.find_all("div", class_="collection-card")
        if not cards:
            logger.warning("Tidak ada kartu produk ditemukan di halaman %d", page_num)
            return []
        for card in cards:
            info = get_product_info(card)
            if info.get("Judul"):
                info["timestamp"] = timestamp_now
                page_items.append(info)
        logger.info("Halaman %d selesai: %d produk", page_num, len(page_items))
        return page_items

    # Process pages in batches of MAX_WORKERS
    for batch_start in range(1, MAX_PAGES + 1, MAX_WORKERS):
        batch_end = min(batch_start + MAX_WORKERS - 1, MAX_PAGES)
        batch_pages = list(range(batch_start, batch_end + 1))
        logger.info("=== Memulai batch halaman %d-%d ===", batch_start, batch_end)
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(scrape_page, p): p for p in batch_pages}
            for future in as_completed(futures):
                page = futures[future]
                try:
                    product_data.extend(future.result())
                except Exception as e:
                    logger.error("Error tak terduga halaman %d: %s", page, e)
        
        logger.info("=== Batch halaman %d-%d selesai. Total produk terkumpul: %d ===\n", batch_start, batch_end, len(product_data))
        sleep(0.5)

    df = pd.DataFrame(product_data)
    df.to_csv('products.csv', index=False)
    logger.info("Ekstraksi data selesai dan disimpan di products.csv")
    return df

if __name__ == '__main__':
    logger.info("Memulai proses ekstraksi (tes modul)...")
    raw_df = extract_data()
    logger.info("Ekstraksi selesai (tes modul). Total produk diekstrak: %d", len(raw_df))
    if not raw_df.empty:
        logger.info("Contoh data yang diekstrak (tes modul):")
        logger.info(raw_df.head())
        logger.info("\nTipe data dari data yang diekstrak (tes modul):")
        raw_df.info(buf=logger.getLogger().handlers[0].stream)
    else:
        logger.warning("Tidak ada data yang diekstrak (tes modul).")