import datetime

starting_year = 2020
current_year = datetime.datetime.now().year
YEARS = [y for y in range(starting_year, current_year + 1)]


class APILimitReached(Exception):
    pass
