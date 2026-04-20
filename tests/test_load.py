import pytest
import pandas as pd
from unittest.mock import patch, mock_open
from datetime import datetime

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.load import load_data

@pytest.fixture
def df_sample_transform(): 
    data = {
        'Judul': ['Kaos Bersih', 'Hoodie Super'], 'Harga': [160000.0, 800000.0],
        'Peringkat': [4.5, 4.9], 'Warna': [3, 5], 'Ukuran': ['M', 'XL'],
        'Gender': ['Pria', 'Unisex'], 'timestamp': [datetime.now().isoformat()] * 2
    }
    return pd.DataFrame(data)

def test_load_data_successfully(df_sample_transform):

    with patch('pandas.DataFrame.to_csv') as mock_df_to_csv:
        result = load_data(df_sample_transform, "dummy_path.csv")
        assert result is True

        mock_df_to_csv.assert_called_once_with(
            "dummy_path.csv", 
            index=False,
            encoding='utf-8'
        )

def test_load_data_empty_dataframe(caplog):
    df_empty = pd.DataFrame()
    path_test_empty = "test_empty.csv"
    result = load_data(df_empty, path_test_empty)
    assert result is False
    assert "DataFrame kosong." in caplog.text
    with patch('pandas.DataFrame.to_csv') as mock_to_csv_check:
        load_data(df_empty, path_test_empty)
        mock_to_csv_check.assert_not_called()

@patch('pandas.DataFrame.to_csv', side_effect=IOError("Disk penuh"))
def test_load_data_io_error(mock_to_csv_error, df_sample_transform, caplog):
    result = load_data(df_sample_transform, "error_io.csv")
    assert result is False
    assert "Gagal menulis ke file CSV" in caplog.text

@patch('pandas.DataFrame.to_csv', side_effect=Exception("Kesalahan pandas"))
def test_load_data_unexpected_error(mock_to_csv_unexpected, df_sample_transform, caplog):
    result = load_data(df_sample_transform, "error_pandas.csv")
    assert result is False
    assert "Kesalahan tak terduga saat memuat data ke" in caplog.text