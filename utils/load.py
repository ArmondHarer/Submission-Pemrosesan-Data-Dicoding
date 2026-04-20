import pandas as pd
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

PATH_CSV_OUTPUT = "products.csv"

def load_data(df_input: pd.DataFrame, path_csv: Path = PATH_CSV_OUTPUT) -> bool:
    if df_input.empty:
        logger.warning("DataFrame kosong. Tidak ada data yang akan dimuat ke %s", path_csv)
        return False
    
    try:
        df_input.to_csv(path_csv, index=False, encoding='utf-8')
        logger.info("Data berhasil dimuat ke %s", path_csv)
        return True
    except IOError as e:
        logger.error("Gagal menulis ke file CSV %s: %s", path_csv, e)
        return False
    except Exception as e:
        logger.error("Kesalahan tak terduga saat memuat data ke %s: %s", path_csv, e)
        return False
    
if __name__ == '__main__':
    data_transform_sample = {
        'Judul': ['Kaos Alpha', 'Hoodie Beta'], 'Harga': [160000.0, 794008.0],
        'Peringkat': [4.5, 4.8], 'Warna': [3, 3], 'Ukuran': ['M', 'L'],
        'Gender': ['Pria', 'Wanita']
    }
    df_sample = pd.DataFrame(data_transform_sample)
    logger.info("Data sampel hasil transformasi untuk dimuat (tes modul):\n%s", df_sample)

    path_csv_tes = "tes_produk_output.csv"
    if load_data(df_sample, path_csv=path_csv_tes):
        logger.info(f"Contoh pemuatan CSV berhasil. Data disimpan ke {path_csv_tes} (tes modul).")
    else:
        logger.error("Contoh pemuatan CSV gagal (tes modul).") 