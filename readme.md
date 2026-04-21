# PEMDA ETL Pipeline

An **Extract, Transform, Load (ETL) pipeline** for scraping, cleaning, and processing fashion product data from the Fashion Studio website. Built with Python, featuring comprehensive unit tests and professional error handling for the final project of the **Belajar Fundamental Pemrosesan Data** course at Dicoding

---

## 🔍 Overview

This project implements a complete ETL pipeline that:

1. **Extracts** 1000+ fashion products from 50 pages of [fashion-studio.dicoding.dev](https://fashion-studio.dicoding.dev)
2. **Transforms** raw data by cleaning, validating, and converting currencies (USD to IDR, assuming fixed rate of Rp16.000 IDR per $1 USD)
3. **Loads** processed data into a CSV file ready for analysis

The pipeline also features **19 unit tests**, comprehensive **error handling**, and detailed **logging** throughout all stages.

---

## 🚀 Quick Start

### Prerequisites
- Python 3.8 or higher
- pip (Python package manager)

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/YOUR_USERNAME/Submission-Pemrosesan-Data-Dicoding.git
cd submission-pemda
```

2. **Create virtual environment**
```bash
# Windows
python -m venv venv
venv\Scripts\activate

# macOS/Linux
python3 -m venv venv
source venv/bin/activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

### Running the Pipeline

```bash
# Run complete ETL pipeline
python main.py

# Output: produk.csv (cleaned product data)
```

**Expected Output:**
```
2026-04-15 16:33:45 - INFO - Memulai Pipeline ETL...
--- Tahap 1: Mengekstrak Data ---
Scraping data pada halaman: https://fashion-studio.dicoding.dev?page=1
...
--- Tahap 2: Mentransformasi Data ---
Memulai transformasi data...
...
--- Tahap 3: Memuat Data ---
Data berhasil dimuat ke produk.csv
```

---

## 📁 Project Structure

```
submission-pemda/
│
├── utils/                          # Core ETL modules
│   ├── __init__.py
│   ├── extract.py                 # Web scraping (ambil_konten, ambil_detail_produk, ekstrak_data)
│   ├── transform.py               # Data cleaning (transform_price, transform_rating, etc)
│   └── load.py                    # CSV saving (load_data)
│
├── tests/                          # Unit tests
│   ├── __init__.py
│   ├── test_extract.py            # 8 tests for extraction
│   ├── test_transform.py          # 9 tests for transformation
│   └── test_load.py               # 4 tests for loading
│
├── main.py                        # Pipeline orchestration
├── requirements.txt               # Project dependencies
├── README.md                      # This file
└── produk.csv                     # Output file (generated)
```

---

## 📖 Usage

### Run Full Pipeline

```bash
python main.py
```

Executes all three stages and creates `produk.csv` with processed data.

### Run Individual Modules

```bash
# Test extraction
python -m utils.extract

# Test transformation
python -m utils.transform

# Test loading
python -m utils.load
```

### Run Unit Tests

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_extract.py -v

# Run with coverage report
pytest tests/ --cov=utils --cov-report=term-missing

# Generate HTML coverage report
pytest tests/ --cov=utils --cov-report=html
# Open: htmlcov/index.html
```

---

## 🔄 Data Pipeline

### Flow Diagram

```
┌─────────────────────────────────────┐
│   Fashion Studio Website            │
│ https://fashion-studio.dicoding.dev │
└────────────────┬────────────────────┘
                 │
                 ▼
        ┌────────────────┐
        │    EXTRACT     │
        ├────────────────┤
        │ 50 pages       │
        │ 1000 products  │
        │ 6 fields each  │
        └────────┬───────┘
                 │
                 ▼
        ┌────────────────┐
        │  TRANSFORM     │
        ├────────────────┤
        │ Convert USD→IDR│
        │ Clean invalid  │
        │ Remove nulls   │
        │ Validate types │
        └────────┬───────┘
                 │
                 ▼
        ┌────────────────┐
        │      LOAD      │
        ├────────────────┤
        │ Save to CSV    │
        │ UTF-8 encoding │
        │ Data verified  │
        └────────┬───────┘
                 │
                 ▼
        ┌─────────────────┐
        │  produk.csv     │
        │ ~950 clean rows │
        └─────────────────┘
```

### Data Fields

Each product contains:

| Field | Type | Example | Notes |
|-------|------|---------|-------|
| `Judul` | String | "T-Shirt Keren" | Product name |
| `Harga` | Float | 1600000.0 | Price in IDR |
| `Peringkat` | Float | 4.5 | Rating out of 5 |
| `Warna` | Integer | 3 | Number of color options |
| `Ukuran` | String | "M" | Size (S, M, L, XL) |
| `Gender` | String | "Men" | Target gender |
| `timestamp` | String | "2026-04-15T16:33:45" | ISO format timestamp |

---

## 🧪 Testing

### Test Coverage

**19 comprehensive tests** covering:

#### Extract Tests (8 tests)
- ✅ Valid product extraction
- ✅ Price unavailable handling
- ✅ Invalid rating handling
- ✅ Missing rating handling
- ✅ Successful data extraction
- ✅ Network error handling
- ✅ Missing grid handling
- ✅ Multiple page iteration

#### Transform Tests (9 tests)
- ✅ Valid price transformation
- ✅ Price unavailable handling
- ✅ Valid rating extraction
- ✅ Invalid rating handling
- ✅ Color count extraction
- ✅ Text column cleaning
- ✅ Full transformation pipeline
- ✅ Empty dataframe handling
- ✅ Missing column handling

#### Load Tests (4 tests)
- ✅ Successful CSV loading
- ✅ Empty dataframe handling
- ✅ IO error handling
- ✅ Unexpected error handling

### Test Results

```
===================== 19 passed in 1.23s =====================
tests/test_extract.py::test_get_valid_product_info PASSED
tests/test_extract.py::test_get_product_info_price_unavailable PASSED
tests/test_extract.py::test_get_product_info_invalid_rating PASSED
tests/test_extract.py::test_get_product_info_no_rating PASSED
tests/test_extract.py::test_extract_data_success PASSED
tests/test_extract.py::test_extract_data_exception_request PASSED
tests/test_extract.py::test_ekstrak_data_tidak_ada_grid_produk PASSED
tests/test_extract.py::test_ekstrak_data_iterasi_halaman PASSED
tests/test_transform.py::test_valid_price_transform PASSED
tests/test_transform.py::test_price_unavailable_transform PASSED
tests/test_transform.py::test_valid_rating_transform PASSED
tests/test_transform.py::test_invalid_rating_transform PASSED
tests/test_transform.py::test_valid_colors_transform PASSED
tests/test_transform.py::test_valid_text_column_transform PASSED
tests/test_transform.py::test_full_transform_pipeline PASSED
tests/test_transform.py::test_empty_dataframe_transform PASSED
tests/test_transform.py::test_missing_columns PASSED
tests/test_load.py::test_load_data_successfully PASSED
tests/test_load.py::test_load_data_empty_dataframe PASSED

Coverage: 92% (270/293 statements)
```

---

## 💻 Technical Details

### Technologies Used

- **Language**: Python 3.8+
- **Web Scraping**: BeautifulSoup4
- **Data Processing**: Pandas
- **HTTP Requests**: Requests
- **Testing**: Pytest
- **Mocking**: unittest.mock

### Dependencies

```
requests~=2.32          # HTTP requests
beautifulsoup4~=4.12    # HTML parsing
pandas~=2.2             # Data manipulation
pytest~=7.0             # Testing framework
pytest-cov~=6.0         # Coverage reporting
```

See `requirements.txt` for complete list.

---

## 📧 Contact & Support

- 📌 Found an issue? Create an [Issue](https://github.com/ArmondHarer/submission-pemrosesan-data-dicoding/issues)
- 💡 Have a suggestion? Start a [Discussion](https://github.com/ArmondHarer/submission-pemrosesan-data-dicoding/discussions)
- ⭐ Found it useful? Give it a star!

---

## 🙏 Acknowledgments

- [Fashion Studio API](https://fashion-studio.dicoding.dev) for the data source
- [Dicoding Academy](https://www.dicoding.com/) for the project requirements and learning opportunity
- Python community for excellent libraries

---

Last Updated: April 2026 | Version: 1.0.0
