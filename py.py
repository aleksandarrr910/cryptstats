import os
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from projectModels import CryptoCurrency, ConnectionSession

import pandas as pd
import requests


CMC_API_LISTINGS = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
CMC_API_KEY = "d874c6adf5c841e59c152ff39d1e088d"

BINANCE_API_KLINES = "https://api.binance.com/api/v3/klines"

DATA_DIR = "data"
TOP_LIMIT = 1000


def create_data_dir():
    """
    Креира data/ directiry доколку не постои
    """
    os.makedirs(DATA_DIR, exist_ok=True)


def build_klines_dataframe(raw_klines):
    """
    Од Binance klines прави DataFrame
    со колони: date, open, high, low, close, volume, market_cap
    """
    if not raw_klines:
        return pd.DataFrame(
            columns=["date", "open", "high", "low", "close", "volume", "market_cap"]
        )

    df = pd.DataFrame(
        raw_klines,
        columns=[
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_asset_volume",
            "number_of_trades",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
            "ignore"
        ]
    )

    df["date"] = pd.to_datetime(df["open_time"], unit="ms").dt.date
    df = df[["date", "open", "high", "low", "close", "volume", "quote_asset_volume"]]
    df = df.rename(columns={"quote_asset_volume": "market_cap"})
    return df

#-----------------------------------------------------
#  Filter 1

def fetch_top_coins(limit=TOP_LIMIT):
    """
    Filter 1:
    Зема top 1000 coins oд CoinMarketCap
    Ги екстрахира нивните симболи
    """

    session = requests.Session()
    session.headers.update({"X-CMC_PRO_API_KEY": CMC_API_KEY})

    params = {"limit": limit}
    response = session.get(CMC_API_LISTINGS, params=params)
    payload = response.json()
    data = payload.get("data", [])

    coins = [entry["symbol"] for entry in data]

    print(f"Filter 1: Loaded {len(coins)} coins")
    return coins


#-----------------------------------------------------
#  Filter 2 – функционална верзија

# def check_existing_data(coin_symbols):
#     """
#     Filter 2:
#     За секој coin проверува дали постои CSV
#     Враќа низа од торки со symbol и last_date
#     """
#     create_data_dir()
#     results = []
#
#     for symbol in coin_symbols:
#         path = os.path.join(DATA_DIR, f"{symbol}.csv")
#
#         # Доколку не постои CSV за coin-от, сигналирај на Filter 3 да земе целосна историска дата
#         if not os.path.exists(path):
#             results.append((symbol, None))
#             continue
#
#         try:
#             df = pd.read_csv(path, parse_dates=["date"])
#
#             # Доколку постои НО е празна, истото како погоре ако не постои
#             if df.empty:
#                 results.append((symbol, None))
#             else:
#                 # Најди го најскориот ден
#                 last_date = df["date"].max().date()
#                 results.append((symbol, last_date))
#         except Exception:
#             results.append((symbol, None))
#
#     print("Filter 2: Checked existing CSV files")
#     return results

from sqlalchemy import func
from projectModels import ConnectionSession, CryptoCurrency

def check_existing_data(coin_symbols):
    """
    Filter 2 (DB верзија):
    За секој coin ја проверува последната dateCoin во базата.
    Враќа листа: [(symbol, last_date_or_None), ...]
    """
    results = []
    session = ConnectionSession()

    try:
        for symbol in coin_symbols:
            last_date = (
                session.query(func.max(CryptoCurrency.dateCoin))
                .filter(CryptoCurrency.coinSymbol == symbol)
                .scalar()
            )
            results.append((symbol, last_date))
    finally:
        session.close()

    print("Filter 2: Checked existing data in DB")
    return results



#-----------------------------------------------------
#  Filter 3

# def fetch_binance_data(coins_with_dates):
#     """
#     Filter 3:
#     За секој пар (coin, last_date) превзема историја од Binance
#     користејќи 10 threads за побрзо и поефикасно извршување
#     """
#     session = requests.Session()
#     output = []
#
#     exchange_info = session.get("https://api.binance.com/api/v3/exchangeInfo").json()
#
#     PREFERRED_QUOTES = ["USDT", "USDC", "BUSD", "FDUSD", "BTC", "ETH", "BNB"]
#
#
#
#     def find_valid_trading_pair(symbol):
#         """
#         Ги наоѓаме сите парови што Binance ги поддржува за тој симбол, пр. BTCUSDT, BTCUSDC etc. во all_pairs
#         и потоа тој симбол го спојуваме со секој QUOTE од преферираните и
#         проверуваме дали постои во all_pairs
#         Доколку да, врати го
#         Доколку не, нема QUOTE и игнорирај го
#         """
#         all_pairs = {item["symbol"] for item in exchange_info["symbols"]}
#
#         for quote in PREFERRED_QUOTES:
#             pair = f"{symbol}{quote}"
#             if pair in all_pairs:
#                 return pair
#
#         return None
#
#
#
#     def fetch_coin(symbol, last_date):
#         """
#         Ги делиме тие 10 години на секции од по 1000 денови за да избегнеме RateLimit
#         И ги земаме податоците за секој ден од денес до пред 10 години (или до дента за која што има најрано податоци)
#         """
#
#         # Повикуваме проверка и наоѓање за парот/quote-от
#         print(f"Filter 3: Fetching data for {symbol}")
#         trading_pair = find_valid_trading_pair(symbol)
#         if trading_pair is None:
#             print(f"  Skipped {symbol}: no valid trading pair on Binance")
#             return symbol, pd.DataFrame()
#         print(f"  Using trading pair: {trading_pair}")
#
#         # Денешен/краен датум и почетен/пред 10 години датум
#         today = datetime.utcnow().date()
#         start_date = today - timedelta(days=3650) if last_date is None else last_date + timedelta(days=1)
#         end_date = today
#         all_chunks = []
#
#         while start_date <= end_date:
#             chunk_end = min(start_date + timedelta(days=999), end_date)
#             start_ms = int(datetime.combine(start_date, datetime.min.time()).timestamp() * 1000)
#             end_ms = int(datetime.combine(chunk_end, datetime.min.time()).timestamp() * 1000)
#
#             params = {
#                 "symbol": trading_pair,
#                 "interval": "1d",
#                 "startTime": start_ms,
#                 "endTime": end_ms,
#                 "limit": 1000
#             }
#
#             resp = session.get(BINANCE_API_KLINES, params=params, timeout=30)
#             raw = resp.json()
#             df_chunk = build_klines_dataframe(raw)
#
#             # земаме 24 часовни податоци за секој coin
#             try:
#                 ticker = session.get(f"https://api.binance.com/api/v3/ticker/24hr", params={"symbol": trading_pair}, timeout=10).json()
#                 df_chunk["last_price"] = float(ticker["lastPrice"])
#                 df_chunk["high_24h"] = float(ticker["highPrice"])
#                 df_chunk["low_24h"] = float(ticker["lowPrice"])
#                 df_chunk["volume_24h"] = float(ticker["volume"])
#             except Exception as e:
#                 df_chunk["last_price"] = float("nan")
#                 df_chunk["high_24h"] = float("nan")
#                 df_chunk["low_24h"] = float("nan")
#                 df_chunk["volume_24h"] = float("nan")
#                 print(f"  Warning: Could not fetch 24h stats for {symbol}: {e}")
#
#             all_chunks.append(df_chunk)
#             start_date = chunk_end + timedelta(days=1)
#
#         full_df = pd.concat(all_chunks, ignore_index=True) if all_chunks else pd.DataFrame(
#             columns=["date","open","high","low","close","volume","market_cap","last_price","high_24h","low_24h","volume_24h"]
#         )
#         return symbol, full_df
#
#     max_workers = 10
#     futures = []
#     with ThreadPoolExecutor(max_workers=max_workers) as executor:
#         for symbol, last_date in coins_with_dates:
#             futures.append(executor.submit(fetch_coin, symbol, last_date))
#         for future in as_completed(futures):
#             output.append(future.result())
#
#     print("Filter 3: Binance data fetched (threaded)")
#     return dict(output)

def fetch_binance_data(coins_with_dates):
    """
    Filter 3:
    За секој coin превзема историја од Binance.
    >>> ВАЖНО: игнорираме last_date и СЕКОГАШ влечеме фиксно YEARS_BACK години наназад.
    """
    session = requests.Session()
    output = []

    exchange_info = session.get("https://api.binance.com/api/v3/exchangeInfo").json()

    PREFERRED_QUOTES = ["USDT", "USDC", "BUSD", "FDUSD", "BTC", "ETH", "BNB"]
    YEARS_BACK = 10  # колку години наназад сакаш

    def find_valid_trading_pair(symbol):

        all_pairs = {item["symbol"] for item in exchange_info["symbols"]}

        for quote in PREFERRED_QUOTES:
            pair = f"{symbol}{quote}"
            if pair in all_pairs:
                return pair

        return None

    def fetch_coin(symbol, last_date):

        print(f"Filter 3: Fetching data for {symbol}")
        trading_pair = find_valid_trading_pair(symbol)
        if trading_pair is None:
            print(f"  Skipped {symbol}: no valid trading pair on Binance")
            return symbol, pd.DataFrame()
        print(f"  Using trading pair: {trading_pair}")

        # Денешен/краен датум и почетен/пред YEARS_BACK години датум
        today = datetime.utcnow().date()
        start_date = today - timedelta(days=365 * YEARS_BACK)
        end_date = today
        print(f"  Date range for {symbol}: {start_date} -> {end_date}")

        all_chunks = []

        while start_date <= end_date:
            chunk_end = min(start_date + timedelta(days=999), end_date)
            start_ms = int(datetime.combine(start_date, datetime.min.time()).timestamp() * 1000)
            end_ms = int(datetime.combine(chunk_end, datetime.min.time()).timestamp() * 1000)

            params = {
                "symbol": trading_pair,
                "interval": "1d",
                "startTime": start_ms,
                "endTime": end_ms,
                "limit": 1000
            }

            resp = session.get(BINANCE_API_KLINES, params=params, timeout=30)
            raw = resp.json()
            df_chunk = build_klines_dataframe(raw)

            # земаме 24 часовни податоци за секој coin
            try:
                ticker = session.get(
                    "https://api.binance.com/api/v3/ticker/24hr",
                    params={"symbol": trading_pair},
                    timeout=10
                ).json()
                df_chunk["last_price"] = float(ticker["lastPrice"])
                df_chunk["high_24h"] = float(ticker["highPrice"])
                df_chunk["low_24h"] = float(ticker["lowPrice"])
                df_chunk["volume_24h"] = float(ticker["volume"])
            except Exception as e:
                df_chunk["last_price"] = float("nan")
                df_chunk["high_24h"] = float("nan")
                df_chunk["low_24h"] = float("nan")
                df_chunk["volume_24h"] = float("nan")
                print(f"  Warning: Could not fetch 24h stats for {symbol}: {e}")

            all_chunks.append(df_chunk)
            start_date = chunk_end + timedelta(days=1)

        full_df = pd.concat(all_chunks, ignore_index=True) if all_chunks else pd.DataFrame(
            columns=[
                "date", "open", "high", "low", "close", "volume", "market_cap",
                "last_price", "high_24h", "low_24h", "volume_24h"
            ]
        )
        print(f"  Total rows for {symbol}: {len(full_df)}")
        return symbol, full_df

    max_workers = 10
    futures = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for symbol, last_date in coins_with_dates:
            futures.append(executor.submit(fetch_coin, symbol, last_date))
        for future in as_completed(futures):
            output.append(future.result())

    print("Filter 3: Binance data fetched (threaded)")
    return dict(output)



# def save_to_csv(coin_data):
#     """
#     Ги спојува новите податоци со постоечките CSV и ги запишува.
#     """
#     create_data_dir()
#
#     numeric_cols = ["open", "high", "low", "close", "volume", "market_cap"]
#
#     for symbol, df in coin_data.items():
#
#         # Скокнува coins со празни и невалидни dataframes
#         if df is None or df.empty or not all(col in df.columns for col in numeric_cols):
#             print(f"Filter 4: Skipped saving {symbol} (no valid data)")
#             continue
#
#
#         path = os.path.join(DATA_DIR, f"{symbol}.csv")
#
#         if os.path.exists(path):
#             old_df = pd.read_csv(path)
#             combined = pd.concat([old_df, df], ignore_index=True)
#         else:
#             combined = df
#             print(f"Filter 4: Saved {len(combined)} rows for {symbol}")
#
#         for col in numeric_cols:
#             if col in combined.columns:
#                 combined[col] = combined[col].astype(float).round(2)
#
#         combined.to_csv(path, index=False)
#
#
# def save_combined_csv(all_data):
#     """
#     Ги спојуваме сите CSV за сите coins во еден голем CSV
#     """
#     combined_list = []
#
#     for coin, df in all_data.items():
#         if df is None or df.empty:
#             continue
#         df = df.copy()
#         df["coin"] = coin
#         combined_list.append(df)
#
#     if combined_list:
#         combined_df = pd.concat(combined_list, ignore_index=True)
#         combined_df.to_csv("combined_data.csv", index=False)
#         print(f"Saved combined_data.csv with {len(combined_df)} rows")
#     else:
#         print("No data available to create combined CSV.")
#


def save_to_db(coin_data):

    database = ConnectionSession()
    numeric_cols = ["open", "high", "low", "close", "volume", "market_cap"]

    try:
        for symbol, df in coin_data.items():
            if df is None or df.empty:
                print(f"Filter 4: Skipped {symbol} (empty dataframe)")
                continue

            missing = [c for c in numeric_cols if c not in df.columns]
            if missing:
                print(f"Filter 4: Skipped {symbol} (missing columns: {missing})")
                continue

            df_clean = df.copy()

            # броевите -> float + заокружување
            for col in numeric_cols:
                df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce").round(2)

            print(f"Filter 4: Preparing {len(df_clean)} rows for {symbol}...")

            records_to_insert = []
            for row in df_clean.itertuples(index=False):
                # row.date веќе е datetime.date (го правиме во build_klines_dataframe)
                record = CryptoCurrency(
                    coinSymbol=symbol,
                    high=float(row.high),
                    low=float(row.low),
                    # daily close цена – ќе ја користиме за графикот
                    closeTime=float(row.close),
                    # volume од DF го ставаме како quoteVolume во базата
                    quoteVolume=float(row.volume),
                    # market_cap од DF го ставаме во coinMarketCap
                    coinMarketCap=float(row.market_cap),
                    # датумот го ставаме во dateCoin (SQLAlchemy Date)
                    dateCoin=row.date
                )

                records_to_insert.append(record)

            if records_to_insert:
                database.add_all(records_to_insert)
                print(f"Filter 4: Added {len(records_to_insert)} rows for {symbol}")
            else:
                print(f"Filter 4: Nothing to insert for {symbol}")

        database.commit()
        print("Filter 4: All data saved to DB")

    except Exception as e:
        database.rollback()
        print(f"Filter 4: Error while saving to DB: {e}")
        raise
    finally:
        database.close()

    return coin_data






def main():
    start_time = time.time()

    coins = fetch_top_coins()
    coins_with_dates = check_existing_data(coins)
    fresh_data = fetch_binance_data(coins_with_dates)
    save_to_db(fresh_data)


    end_time = time.time()
    elapsed = end_time - start_time

    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)

    print(f"\nTotal runtime: {minutes} minutes {seconds} seconds")

if __name__ == "__main__":
    main()
