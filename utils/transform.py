import pandas as pd
import numpy as np
import re
import logging
from datetime import datetime
from typing import Any

import io
logger = logging.getLogger(__name__)

USD_TO_IDR = 16000

def transform_price(price_text: str | None) -> float | None:
    try:
        if pd.isna(price_text) or price_text == "Price Unavailable":
            return np.nan
        price_usd = float(str(price_text).replace('$', '').replace(',', ''))
        return price_usd * USD_TO_IDR
    except ValueError as e:
        logger.warning("Tidak dapat mengkonversi harga: %s. Kesalahan: %s. Mengembalikan NaN.", price_text, e)
        return np.nan
    except Exception as e:
        logger.error("Kesalahan tak terduga saat mengkonversi harga: %s. Kesalahan: %s. Mengembalikan NaN.", price_text, e)
        return np.nan

def transform_rating(rating_text: str | None) -> float | None:
    try:
        if pd.isna(rating_text) or "Invalid Rating" in str(rating_text) or "Not Rated" in str(rating_text):
            return np.nan
        match = re.search(r'(\d+\.?\d*)', str(rating_text))
        if match:
            return float(match.group(1))
        return np.nan
    except ValueError as e:
        logger.warning("Tidak dapat mengkonversi peringkat: %s. Kesalahan: %s. Mengembalikan NaN.", rating_text, e)
        return np.nan
    except Exception as e:
        logger.error("Kesalahan tak terduga saat mengkonversi peringkat: %s. Kesalahan: %s. Mengembalikan NaN.", rating_text, e)
        return np.nan

def transform_colors(colors_text: str | None) -> int | None:
    try:
        if pd.isna(colors_text):
            return np.nan
        match = re.search(r'(\d+)\s*Colors', str(colors_text))
        if match:
            return int(match.group(1))
        return np.nan
    except ValueError as e:
        logger.warning("Tidak dapat mengkonversi warna: %s. Kesalahan: %s. Mengembalikan NaN.", colors_text, e)
        return np.nan
    except Exception as e:
        logger.error("Kesalahan tak terduga saat mengkonversi warna: %s. Kesalahan: %s. Mengembalikan NaN.", colors_text, e)
        return np.nan

def transform_text_column(text: str | None, prefix_to_remove: str) -> str | None:
    try:
        if pd.isna(text):
            return np.nan
        cleaned_text = str(text).replace(prefix_to_remove, '').strip()
        return cleaned_text if cleaned_text else np.nan
    except Exception as e:
        logger.error("Kesalahan tak terduga saat transformasi kolom teks '%s' dengan awalan '%s'. Kesalahan: %s. Mengembalikan NaN.", text, prefix_to_remove, e)
        return np.nan
    
def transform_data(df_input):
    if df_input.empty:
        logger.warning("DataFrame input kosong. Tidak ada data untuk ditransformasi.")
        return df_input
    logger.info("Memulai transformasi data...")
    df_result = df_input.copy()

    try:
        if 'Judul' in df_result.columns:
            df_result['Judul'] = df_result['Judul'].replace('Unknown Product', np.nan)
        else: logger.warning("Kolom 'Judul' tidak ditemukan untuk transformasi.")
    except Exception as e: logger.error(f"Kesalahan saat transformasi 'Judul': {e}")

    transform_columns = {
        'Harga': transform_price,
        'Peringkat': transform_rating,
        'Warna': transform_colors,
        'Ukuran': lambda x: transform_text_column(x, "Size: "),
        'Gender': lambda x: transform_text_column(x, "Gender: ")
    }

    for col, transform_func in transform_columns.items():
        if col in df_result.columns:
            try:
                df_result[col] = df_result[col].apply(transform_func)
            except Exception as e:
                logger.error(f"Kesalahan saat transformasi kolom '{col}': {e}")
        else:
            logger.warning(f"Kolom '{col}' tidak ditemukan untuk transformasi.")

    initial_row_count = len(df_result)
    df_result.dropna(inplace=True)
    df_result.drop_duplicates(inplace=True)
    final_row_count = len(df_result)
    logger.info(f"Transformasi data selesai. Jumlah baris sebelum transformasi: {initial_row_count}, sesudah transformasi: {final_row_count}")

    try:
        if not df_result.empty:
            column_datatype = {
                'Harga': float, 'Peringkat': float, 'Warna': int,
                'Judul': str, 'Ukuran': str, 'Gender': str,
                'timestamp': str
            }
            for column, datatype in column_datatype.items():
                if column in df_result.columns:
                    if column == 'timestamp':
                        df_result[column] = pd.to_datetime(df_result[column], errors='coerce').astype(str)
                    else:
                        df_result[column] = df_result[column].astype(datatype)
        else:
            logger.warning("DataFrame hasil transformasi kosong setelah menghapus nilai NaN. Tidak ada tipe data yang akan diatur.")
    except Exception as e:
        logger.error(f"Kesalahan tak terduga saat mengatur tipe data pada DataFrame hasil transformasi: {e}")
    except KeyError as e:
        logger.error(f"KeyError saat mengatur tipe data pada DataFrame hasil transformasi: {e}")
    return df_result

if __name__ == '__main__':
    
    sample_data= {
        'Judul': ['Kaos Merah', 'Unknown Product', 'Hoodie Biru'], 'Harga': ['$10.00', '$5.00', '$49.88'],
        'Peringkat': ['Rating: ⭐ 4.5 / 5', 'Rating: ⭐ Invalid Rating / 5', 'Rating: ⭐ 4.8 / 5'],
        'Warna': ['3 Colors', '5 Colors', '3 Colors'], 'Ukuran': ['Size: M', 'Size: S', 'Size: L'],
        'Gender': ['Gender: Men', 'Gender: Women', 'Gender: Unisex'],
        'timestamp': [datetime.now().isoformat()] * 3
    }
    df_raw_sample = pd.DataFrame(sample_data)
    logger.info("Data sampel sebelum transformasi (tes modul):\n%s", df_raw_sample)
    df_transformed_sample = transform_data(df_raw_sample.copy())
    logger.info("Data sampel setelah transformasi (tes modul):\n%s", df_transformed_sample)
    if not df_transformed_sample.empty:
        logger.info("\nTipe data dari data sampel setelah transformasi (tes modul):")
        buffer = io.StringIO()
        df_transformed_sample.info(buf=buffer)
        logger.info("\n" + buffer.getvalue())