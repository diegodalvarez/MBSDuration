# -*- coding: utf-8 -*-
"""
Created on Wed Aug 23 08:36:29 2023

@author: Diego
"""


import pdblp
import datetime as dt

start_date = dt.date(year = 1980, month = 1, day = 1)
end_date = dt.date.today()

end_date_input  = end_date.strftime("%Y%m%d")
start_date_input = start_date.strftime("%Y%m%d")

tickers = [
    ".MOVEMBS G Index", ".30CC105 G Index", ".MBSMODEL G Index",
    "MTGEFNCL Index", "MOVE Index", "USGG10YR Index", "USGG2YR Index",
    ".HOMESUP G Index"]

con = pdblp.BCon(debug = False, port = 8194, timeout = 5_000)
con.start()

df_tmp = (con.bdh(
    tickers = tickers,
    flds = ["PX_LAST"],
    start_date = start_date_input,
    end_date = end_date_input).
    reset_index().
    melt(id_vars = "date"))

(df_tmp.to_parquet(
    path = "mtge.parquet",
    engine = "pyarrow"))

start_date = dt.date(year = 1980, month = 1, day = 1)
end_date = dt.date.today()

tickers = [
    "LUMSTRUU Index", "LUMSER Index", "LUMSOAS Index", "LUMSSTAT Index",
    "LUMSYW Index", "LD19TRUU Index", "LD10OAS Index"]

con = pdblp.BCon(debug = False, port = 8194, timeout = 5_000)
con.start()

df_tmp = (con.bdh(
    tickers = tickers,
    flds = ["PX_LAST"],
    start_date = start_date_input,
    end_date = end_date_input).
    reset_index().
    melt(id_vars = "date"))

(df_tmp.to_parquet(
    path = "mbs.parquet",
    engine = "pyarrow"))

end_date_input  = end_date.strftime("%Y%m%d")
start_date_input = start_date.strftime("%Y%m%d")

tickers = ["LUMSMD Index"]

con = pdblp.BCon(debug = False, port = 8194, timeout = 5_000)
con.start()

df_tmp = (con.bdh(
    tickers = tickers,
    flds = ["PX_LAST"],
    start_date = start_date_input,
    end_date = end_date_input).
    reset_index().
    melt(id_vars = "date"))

(df_tmp.to_parquet(
    path = "mbs_duration.parquet",
    engine = "pyarrow"))