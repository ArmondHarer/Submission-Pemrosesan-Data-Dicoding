import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from bs4 import BeautifulSoup
import logging
import pytest

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.extract import get_product_info, extract_data, URL_LINK, MAX_PAGES
import requests

VALID_PRODUCT_CARD_HTML = """<div class="collection-card"><div class="product-details"><h3 class="product-title">Kaos Keren</h3><div class="price-container"><span class="price">$120.50</span></div><p style="font-size: 14px; color: #777;">Rating: ⭐ 4.5 / 5</p><p style="font-size: 14px; color: #777;">3 Colors</p><p style="font-size: 14px; color: #777;">Size: L</p><p style="font-size: 14px; color: #777;">Gender: Men</p></div></div>"""
PRICE_UNAVAILABLE_PRODUCT_CARD_HTML = """<div class="collection-card"><div class="product-details"><h3 class="product-title">Celana Spesial</h3><p class="price">Price Unavailable</p><p style="font-size: 14px; color: #777;">Rating: ⭐ 3.0 / 5</p><p style="font-size: 14px; color: #777;">2 Colors</p><p style="font-size: 14px; color: #777;">Size: M</p><p style="font-size: 14px; color: #777;">Gender: Unisex</p></div></div>"""
INVALID_RATING_PRODUCT_CARD_HTML = """<div class="collection-card"><div class="product-details"><h3 class="product-title">Jaket Lama</h3><div class="price-container"><span class="price">$75.00</span></div><p style="font-size: 14px; color: #777;">Rating: ⭐ Invalid Rating / 5</p><p style="font-size: 14px; color: #777;">1 Color</p><p style="font-size: 14px; color: #777;">Size: S</p><p style="font-size: 14px; color: #777;">Gender: Women</p></div></div>"""
NO_RATING_PRODUCT_CARD_HTML = """<div class="collection-card"><div class="product-details"><h3 class="product-title">Barang Misterius</h3><div class="price-container"><span class="price">$99.00</span></div><p style="font-size: 14px; color: #777;">Rating: Not Rated</p><p style="font-size: 14px; color: #777;">5 Colors</p><p style="font-size: 14px; color: #777;">Size: XL</p><p style="font-size: 14px; color: #777;">Gender: Men</p></div></div>"""

def test_get_valid_product_info():
    soup = BeautifulSoup(VALID_PRODUCT_CARD_HTML, "html.parser")
    detail = get_product_info(soup.find("div", class_="collection-card"))
    assert detail["Judul"] == "Kaos Keren"
    assert detail["Harga"] == "$120.50"
    assert detail["Peringkat"] == "Rating: ⭐ 4.5 / 5"
    assert detail["Warna"] == "3 Colors"
    assert detail["Ukuran"] == "Size: L"
    assert detail["Gender"] == "Gender: Men"

def test_get_product_info_price_unavailable():
    soup = BeautifulSoup(PRICE_UNAVAILABLE_PRODUCT_CARD_HTML, "html.parser")
    detail = get_product_info(soup.find("div", class_="collection-card"))
    assert detail["Harga"] == "Price Unavailable"

def test_get_product_info_invalid_rating():
    soup = BeautifulSoup(INVALID_RATING_PRODUCT_CARD_HTML, "html.parser")
    detail = get_product_info(soup.find("div", class_="collection-card"))
    assert detail["Peringkat"] == "Rating: ⭐ Invalid Rating / 5"

def test_get_product_info_no_rating():
    soup = BeautifulSoup(NO_RATING_PRODUCT_CARD_HTML, "html.parser")
    detail = get_product_info(soup.find("div", class_="collection-card"))
    assert detail["Peringkat"] == "Rating: Not Rated"

@pytest.fixture(autouse=True)
def force_single_worker(monkeypatch):
    """Force sequential scraping during tests."""
    monkeypatch.setattr("utils.extract.MAX_WORKERS", 1)

@patch('utils.extract.requests.get')
def test_extract_data_success(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = f"<html><body><div id='collectionList'>{VALID_PRODUCT_CARD_HTML}</div></body></html>".encode('utf-8')
    mock_get.return_value = mock_response

    with patch('utils.extract.MAX_PAGES', 1):
        df = extract_data()

    assert not df.empty
    assert len(df) == 1
    assert df.iloc[0]["Judul"] == "Kaos Keren"
    assert "timestamp" in df.columns
    mock_get.assert_called_once_with(f"{URL_LINK}", timeout=10)

@patch('utils.extract.requests.get', side_effect=requests.exceptions.RequestException("Network error"))
def test_extract_data_exception_request(mock_get, caplog):
    mock_get.side_effect = requests.exceptions.RequestException("Kesalahan jaringan tes")
    with patch('utils.extract.MAX_PAGES', 1):
        df = extract_data()
    assert df.empty
    assert "Gagal mengambil halaman" in caplog.text
    assert "Kesalahan jaringan tes" in caplog.text


@patch('utils.extract.requests.get')
def test_extract_no_product_grid(mock_get_func, caplog):
    caplog.set_level(logging.INFO)

    mock_respons = MagicMock()
    mock_respons.status_code = 200
    mock_respons.content = "<html><body>No grid</body></html>".encode('utf-8')
    mock_get_func.return_value = mock_respons
    with patch('utils.extract.MAX_PAGES', 1):
        df = extract_data()
    assert df.empty
    assert "Tidak ada grid produk ditemukan di halaman 1" in caplog.text

@patch('utils.extract.requests.get')
def test_extract_data_iterate_pages(mock_get_func):
    mock_res1 = MagicMock()
    mock_res1.status_code = 200
    mock_res1.content = f"<html><body><div id='collectionList'>{VALID_PRODUCT_CARD_HTML}</div></body></html>".encode('utf-8')

    mock_res2 = MagicMock()
    mock_res2.status_code = 200
    mock_res2.content = f"<html><body><div id='collectionList'>{PRICE_UNAVAILABLE_PRODUCT_CARD_HTML}</div></body></html>".encode('utf-8')

    mock_get_func.side_effect = [mock_res1, mock_res2]
    with patch('utils.extract.MAX_PAGES', 2):
        df = extract_data()
    assert len(df) == 2
    assert mock_get_func.call_count == 2
    mock_get_func.assert_any_call(f"{URL_LINK}", timeout=10)
    mock_get_func.assert_any_call(f"{URL_LINK}/page2", timeout=10)


