import yfinance as yf
import pandas as pd
import mysql.connector
import re
from multiprocessing import Pool, Manager
import json
import tqdm
import numpy as np


def process_row(tickname):
    
    obj = yf.Ticker(tickname)
    info = obj.info

    # print(f"{tickname} ticker done!")
    
    if "quoteType" in info.keys() and info["quoteType"] == "EQUITY":
        return info
    else:
        pass
        # print("not a stock!", info["quoteType"])


if __name__ == "__main__":
    tickdf = pd.read_csv("data/stocks.csv", error_bad_lines=False, delimiter=";")

    rows = tickdf.Ticker.tolist()

    # out = []
    # for row in rows:
        # out.append(process_row(row))

    with Pool() as pool:
        print("started pool!")      

        file_chunks = np.array_split(rows, 100)
        
        for bid in range(82,len(file_chunks)):
            chunk = file_chunks[bid]

            print(f"starting on chunk {bid}")

            out = []
            for x in tqdm.tqdm(pool.imap_unordered(process_row, chunk), total=len(chunk)):
                if not x is None:
                    out.append(x)

            # pool.join()

            # Save to json
            with open(f'raw_out/raw{bid}.json', 'w') as f:
                json.dump(out[1:], f)
        