from mysql import connector
from configs import database_configs_prod as dbconfig
from modules import extract_yahoo_financials
import datetime


mydb = connector.connect(
    host=dbconfig.MYSQL_HOST,
    port=dbconfig.MYSQL_PORT,
    user=dbconfig.MYSQL_USER,
    password=dbconfig.MYSQL_PASSWORD,
    database=dbconfig.MYSQL_DATABASE
)

mycursor = mydb.cursor()

mycursor.execute("select distinct ticker from yahoo_quarterly_fundamental where currencyCode is null order by 1")

myresult = mycursor.fetchall()

spider = extract_yahoo_financials.YahooFinancial(datetime.datetime.today().date() - datetime.timedelta(days=-3),
                                                 targeted_pop='YAHOO_STOCK_ALL',
                                                 batch=True,
                                                 loggerFileName=None)


for stock in myresult[:]:

    try:
        df_12m, df_3m, df_ttm = spider._extract_each_stock(stock[0])
        if df_3m.empty:
            continue
    except Exception as e:
        print(e)
        continue

    currency = df_3m.iloc[0]['currencyCode']

    sql = f"UPDATE yahoo_quarterly_fundamental set currencyCode = '{currency}' where ticker = '{stock[0]}'"
    mycursor.execute(sql)
    mydb.commit()
    print(mycursor.rowcount, "record(s) affected yahoo_quarterly_fundamental")

    sql = f"UPDATE yahoo_annual_fundamental set currencyCode = '{currency}' where ticker = '{stock[0]}'"
    mycursor.execute(sql)
    mydb.commit()
    print(mycursor.rowcount, "record(s) affected yahoo_annual_fundamental")

    sql = f"UPDATE yahoo_trailing_fundamental set currencyCode = '{currency}' where ticker = '{stock[0]}'"
    mycursor.execute(sql)
    mydb.commit()
    print(mycursor.rowcount, "record(s) affected yahoo_trailing_fundamental")

