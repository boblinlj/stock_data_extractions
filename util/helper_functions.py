import datetime


def dedup_list(ls: list):
    return list(dict.fromkeys(ls))


def returnNotMatches(a, b):
    return [x for x in a if x not in b]


def regulartime_to_unix(date):
    return int( (date - datetime.date(1970, 1, 1) ).total_seconds())


def unix_to_regulartime(unix: int):
    return datetime.datetime.utcfromtimestamp(unix).strftime('%Y-%m-%d')
