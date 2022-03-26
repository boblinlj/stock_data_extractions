from modules.extract_yahoo_profile import ExtractProfile

obj = ExtractProfile('YAHOO_SCREENER', updated_dt='2022-03-21', proxy=True)
obj.extract_the_entire_population()
