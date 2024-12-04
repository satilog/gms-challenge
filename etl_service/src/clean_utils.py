import re

import pandas as pd
from dateutil import parser


def preprocess_dates(date):
    if pd.isna(date):
        return date
    if not isinstance(date, str):
        date = str(date)
    date = date.replace("/", "-")
    date = re.sub(r"\b(\d{1})-(\d{1})-(\d{4})\b", r"0\1-0\2-\3", date)
    date = re.sub(r"\b(\d{1})-(\d{2})-(\d{4})\b", r"0\1-\2-\3", date)
    date = re.sub(r"\b(\d{2})-(\d{1})-(\d{4})\b", r"\1-0\2-\3", date)
    return date


def parse_mixed_dates(date):
    try:
        return parser.parse(date, dayfirst=True)
    except Exception:
        return pd.NA


def clean_gender(gender):
    if str(gender).lower() in ["female", "male"]:
        return gender
    return pd.NA


def create_prefix_matcher(valid_values):
    def get_prefix_overlap(value, valid_value):
        value = value.lower()
        valid_value = valid_value.lower()
        overlap = 0
        for v_char, r_char in zip(value, valid_value):
            if v_char == r_char:
                overlap += 1
            else:
                break
        return overlap

    def clean_field_by_prefix(value):
        value = value.strip()
        best_match = value
        max_overlap = 0
        for valid_value in valid_values:
            overlap = get_prefix_overlap(value, valid_value)
            if overlap > max_overlap:
                max_overlap = overlap
                best_match = valid_value
        return best_match

    return clean_field_by_prefix
