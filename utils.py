# utils.py
import logging
from datetime import datetime

def safe_get(data, key, default=None):
    try:
        for k in key.split('.'):
            data = data[k]
        return data if data is not None else default
    except (KeyError, TypeError):
        return default

def format_date(value, full_date=False):
    if not value or value in [-1, 'None', '']: return '-'
    try:
        value = str(value)
        if len(value) == 8:
            dt = datetime.strptime(value, '%Y%m%d')
            return dt.strftime('%Y-%m-%d') if full_date else dt.strftime('%Y')
        return value if full_date else value[:4]
    except ValueError:
        return value