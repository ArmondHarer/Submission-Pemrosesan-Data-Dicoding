import pytest
import pandas as pd
import numpy as np
from datetime import datetime

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.transform import (
    transform_price, transform_rating, transform_colors,
    transform_text_column, transform_data, USD_TO_IDR
)

@pytest.fixture
def df_raw_sample():
    data = {
        'Judul': ['Kaos A', 'Unknown Product', 'Hoodie B'],
        'Harga': ['$10.00', '$5.00', '$49.88'],
        'Peringkat': ['Rating: ⭐ 4.5 / 5', 'Rating: ⭐ Invalid Rating / 5', 'Rating: ⭐ 4.8 / 5'],
        'Warna': ['3 Colors', '5 Colors', '3 Colors'],
        'Ukuran': ['Size: M', 'Size: S', 'Size: L'],
        'Gender': ['Gender: Men', 'Gender: Women', 'Gender: Unisex'],
        'timestamp': [datetime.now().isoformat()] * 3
    }
    return pd.DataFrame(data)

def test_valid_price_transform():
    assert transform_price("$10.00") == 10.00 * USD_TO_IDR

def test_price_unavailable_transform():
    assert pd.isna(transform_price("Price Unavailable"))

def test_valid_rating_transform():
    assert transform_rating("Rating: ⭐ 4.5 / 5") == 4.5

def test_invalid_rating_transform():
    assert pd.isna(transform_rating("Rating: ⭐ Invalid Rating / 5"))

def test_valid_colors_transform():
    assert transform_colors("3 Colors") == 3

def test_valid_text_column_transform():
    assert transform_text_column("Size: M", "Size: ") == "M"

def test_full_transform_pipeline(df_raw_sample):
    df_result = transform_data(df_raw_sample.copy())
    assert len(df_result) == 2
    
    result_title = df_result['Judul'].tolist()
    assert 'Kaos A' in result_title
    assert 'Hoodie B' in result_title
    
    assert df_result['Harga'].dtype == float
    assert df_result['Peringkat'].dtype == float

def test_empty_dataframe_transform():
    df_empty = pd.DataFrame()
    df_result = transform_data(df_empty)
    assert df_result.empty

def test_missing_columns(df_raw_sample):
    df_missing = df_raw_sample.drop(columns=['Harga', 'Peringkat'])
    df_result = transform_data(df_missing.copy())
    assert 'Harga' not in df_result.columns
    assert 'Peringkat' not in df_result.columns