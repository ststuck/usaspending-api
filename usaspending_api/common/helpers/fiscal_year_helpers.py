from typing import Tuple, Optional, List
from fiscalyear import FiscalDate, FiscalDateTime, FiscalQuarter
from datetime import datetime


def create_fiscal_year_list(start_year: int = 2000, end_year: Optional[int] = None) -> List[int]:
    """
    return the list of fiscal year as integers
        start_year: int default 2000 FY to start at (inclusive)
        end_year: int default None: FY to end at (exclusive)
            if no end_date is provided, use the current FY
    """
    if end_year is None:
        # to return the current FY, we add 1 here for the range generator below
        end_year = FiscalDate.today().next_fiscal_year.fiscal_year

    if start_year is None or start_year >= end_year:
        raise Exception("Invalid start_year and end_year values")

    return [year for year in range(start_year, end_year)]


def convert_fiscal_quarter_to_dates(fiscal_year: int, fiscal_quarter: int) -> Tuple[datetime, datetime]:
    """Return the start and end dates of a given FY and quarter"""
    fiscal_dates = FiscalQuarter(fiscal_year, fiscal_quarter)
    return fiscal_dates.start, fiscal_dates.end


def previous_fiscal_quarter(fiscal_year: int, fiscal_quarter: int) -> Tuple[int, int]:
    """ Simple helper function to return the previous quarter"""
    if fiscal_quarter == 1:
        return fiscal_year - 1, 4
    return fiscal_year, fiscal_quarter - 1


def fiscal_year_and_quarter_from_datetime(py_dt: datetime) -> Tuple[int, int]:
    fiscal_date = FiscalDateTime(py_dt.year, py_dt.month, py_dt.day, py_dt.hour, py_dt.minute, py_dt.second)
    return fiscal_date.fiscal_year, fiscal_date.quarter
