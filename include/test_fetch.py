# test_fetch.py
from datetime import date
from fetch_ptax import fetch_ptax

print(fetch_ptax(date(2025,1,27), date(2025,1,29)).head())
