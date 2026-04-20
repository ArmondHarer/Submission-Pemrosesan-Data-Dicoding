import logging
from logging.handlers import RotatingFileHandler

from dotenv.main import logger
from utils.extract import extract_data
from utils.transform import transform_data  
from utils.load import load_data

import io

def setup_logging():
    log_format = '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
    formatter = logging.Formatter(log_format)
    
    # Console handler
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    
    # Rotating file handler (5 MB max, keep 3 backups)
    file_handler = RotatingFileHandler('logs.txt', maxBytes=5_000_000, backupCount=3)
    file_handler.setFormatter(formatter)
    
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(console)
    root_logger.addHandler(file_handler)
    logging.info("Logging test: If you see this, logging works!")

def main_pipeline():
    logger = logging.getLogger(__name__)
    logger.info("Memulai Pipeline ETL...")

    logger.info("--- Tahap 1: Mengekstrak Data ---")
    try:
        df_raw = extract_data()
        if df_raw.empty:
            logger.error("Ekstraksi menghasilkan DataFrame kosong. Menghentikan pipeline.")
            return
        logger.info(f"Berhasil mengekstrak {len(df_raw)} catatan mentah.")
    except Exception as e:
        logger.critical(f"Kesalahan kritis saat ekstraksi data: {e}", exc_info=True)
        return

    logger.info("--- Tahap 2: Mentransformasi Data ---")
    try:
        df_transformed = transform_data(df_raw)
        if df_transformed.empty:
            logger.warning("Transformasi menghasilkan DataFrame kosong. Tidak ada data untuk dimuat.")
        else:
            logger.info(f"Berhasil mentransformasi data. DataFrame hasil memiliki {len(df_transformed)} catatan.")
            logger.info("Contoh data hasil transformasi:")
            logger.info(df_transformed.head())
            logger.info("\nTipe data dari data hasil transformasi:")
            buffer = io.StringIO()
            df_transformed.info(buf=buffer)
            logger.info("\n" + buffer.getvalue())
    except Exception as e:
        logger.critical(f"Kesalahan kritis saat transformasi data: {e}", exc_info=True)
        return

    logger.info("--- Tahap 3: Memuat Data ---")
    if not df_transformed.empty:
        try:
            if load_data(df_transformed, "products.csv"):
                logger.info("Data berhasil dimuat.")
            else:
                logger.error("Gagal memuat data.")
        except Exception as e:
            logger.critical(f"Kesalahan kritis saat memuat data: {e}", exc_info=True)
    else: 
        logger.warning("DataFrame hasil transformasi kosong. Tidak ada data yang akan dimuat.")

    
    logger.info("Pipeline ETL selesai.")

if __name__ == '__main__':
    setup_logging()
    main_pipeline()