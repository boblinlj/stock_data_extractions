def convert_str_to_num(str_v: str):
    if not (isinstance(str_v, str)):
        data = str_v
    elif 'K' in str_v:
        try:
            data = int(float(str_v.split('K')[0]) * 1000)
        except:
            data = str_v
    elif 'M' in str_v:
        try:
            data = int(float(str_v.split('M')[0]) * 1000000)
        except:
            data = str_v
    elif 'B' in str_v:
        try:
            data = int(float(str_v.split('B')[0]) * 1000000000)
        except:
            data = str_v
    elif 'T' in str_v:
        try:
            data = int(float(str_v.split('B')[0]) * 1000000000000)
        except:
            data = str_v
    elif '%' in str_v:
        try:
            data = float(str_v.split('%')[0]) / 100
        except:
            data = str_v
    elif ',' in str_v:
        try:
            data = str_v.replace(',', '')
        except:
            data = str_v
    elif '-' == str_v:
        data = None
    else:
        data = str_v

    return data
