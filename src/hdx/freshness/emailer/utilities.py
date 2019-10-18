# -*- coding: utf-8 -*-
"""
Utilities
---------

Utility functions
"""
from datetime import datetime


def get_dataset_dates(dataset):
    dataset_date = dataset['dataset_date']
    if not dataset_date:
        return None, None
    if '-' in dataset_date:
        date_start, date_end = dataset_date.split('-')
    else:
        date_start = date_end = dataset_date
    return datetime.strptime(date_start, '%m/%d/%Y'), datetime.strptime(date_end, '%m/%d/%Y')
