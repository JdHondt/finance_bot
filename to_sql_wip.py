import yfinance as yf
import pandas as pd
import mysql.connector
import re
from multiprocessing import Pool, Manager


def fmt4sql(val):
    if type(val) == str:
        out = re.sub(r"[ .:\/]", "_", str(val))
        return f"'{out}'"
    elif val is None:
        return "NULL"
    else:
        return str(val)

def process_row(row):
    tickname = row.Ticker
    
    obj = yf.Ticker(tickname)
    info = obj.info

    print(f"{tickname} ticker done!")
    
    if info["quoteType"] == "EQUITY":

        allcols = ["52WeekChange", "SandP52WeekChange", "address1", "algorithm", "annualHoldingsTurnover", "annualReportExpenseRatio", "ask", "askSize", "averageDailyVolume10Day", "averageVolume", "averageVolume10days", "beta", "beta3Year", "bid", "bidSize", "bookValue", "category", "circulatingSupply", "city", "companyOfficers", "country", "currency", "currentPrice", "currentRatio", "dateShortInterest", "dayHigh", "dayLow", "debtToEquity", "dividendRate", "dividendYield", "earningsGrowth", "earningsQuarterlyGrowth", "ebitda", "ebitdaMargins", "enterpriseToEbitda", "enterpriseToRevenue", "enterpriseValue", "exDividendDate", "exchange", "exchangeTimezoneName", "exchangeTimezoneShortName", "expireDate", "fiftyDayAverage", "fiftyTwoWeekHigh", "fiftyTwoWeekLow", "financialCurrency", "fiveYearAverageReturn", "fiveYearAvgDividendYield", "floatShares", "forwardEps", "forwardPE", "freeCashflow", "fromCurrency", "fullTimeEmployees", "fundFamily", "fundInceptionDate", "gmtOffSetMilliseconds", "grossMargins", "grossProfits", "heldPercentInsiders", "heldPercentInstitutions", "impliedSharesOutstanding", "industry", "isEsgPopulated", "lastCapGain", "lastDividendDate", "lastDividendValue", "lastFiscalYearEnd", "lastMarket", "lastSplitDate", "lastSplitFactor", "legalType", "logo_url", "longBusinessSummary", "longName", "market", "marketCap", "maxAge", "maxSupply", "messageBoardId", "morningStarOverallRating", "morningStarRiskRating", "mostRecentQuarter", "navPrice", "netIncomeToCommon", "nextFiscalYearEnd", "numberOfAnalystOpinions", "open", "openInterest", "operatingCashflow", "operatingMargins", "payoutRatio", "pegRatio", "phone", "preMarketPrice", "previousClose", "priceHint", "priceToBook", "priceToSalesTrailing12Months", "profitMargins", "quickRatio", "quoteType", "recommendationKey", "recommendationMean", "regularMarketDayHigh", "regularMarketDayLow", "regularMarketOpen", "regularMarketPreviousClose", "regularMarketPrice", "regularMarketVolume", "returnOnAssets", "returnOnEquity", "revenueGrowth", "revenuePerShare", "revenueQuarterlyGrowth", "sector", "sharesOutstanding", "sharesPercentSharesOut", "sharesShort", "sharesShortPreviousMonthDate", "sharesShortPriorMonth", "shortName", "shortPercentOfFloat", "shortRatio", "startDate", "state", "strikePrice", "symbol", "targetHighPrice", "targetLowPrice", "targetMeanPrice", "targetMedianPrice", "threeYearAverageReturn", "toCurrency", "totalAssets", "totalCash", "totalCashPerShare", "totalDebt", "totalRevenue", "tradeable", "trailingAnnualDividendRate", "trailingAnnualDividendYield", "trailingEps", "trailingPE", "twoHundredDayAverage", "volume", "volume24Hr", "volumeAllCurrencies", "website", "yield", "ytdReturn", "zip", "name"]

        cols = [(k,v) for k,v in info.items() 
                if not k in ["website", "logo_url", "longBusinessSummary", "companyOfficers"] and k in allcols]

        print("in if-statement")

        cols.append(("name", row.Name))

        return tuple(fmt4sql(x[1]) for x in cols)

        # sql = "INSERT INTO stock ({0}) VALUES ({1})".format(
        # ", ".join([x[0] for x in cols]),
        # ", ".join([fmt4sql(x[1]) for x in cols])
        # )

        # print(f"{tickname} trying to execute!")

        # try:
        #     mycursor.execute(sql)

        #     mydb.commit()

        #     print(mycursor.rowcount, "record inserted.")
        # except Exception as e:
            # print(e)

    else:
        print("not a stock!", info["quoteType"])


if __name__ == "__main__":
    tickdf = pd.read_csv("data/stocks.csv", error_bad_lines=False, delimiter=";")

    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="jens",
    database="finance",
    auth_plugin='mysql_native_password'
    )

    mycursor = mydb.cursor()

    

    rows = [row for idx, row in tickdf.head(100).iterrows()]

    out = []
    for row in rows:
        out.append(process_row(row))

    sql = "INSERT INTO stock ({0}) VALUES {1}".format(
        ", ".join([x[0] for x in cols]),
        ", ".join(out)
        )


    # pool = Pool()
    # print("started pool!")
    # out = pool.map(process_row, rows)
    # pool.close()
    # pool.join()

    mydb.commit()