import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta

API_KEY = "2597f84529bce37c53b227e21ba471abb6d58a6e5090ed4f3993be77313729e4"
# cryptocompare api key 超过了就再注册一个免费版

def fetch_markets_data(api_key: str) -> pd.DataFrame:
    '''Download all market name list from cryptocompare API

     Args:
        api_key(str): API key for authentication

     Return:
        market name list dataframe
        
     Usage:
        fetch_markets_data(api_key)
     
    '''
    url_markets = "https://data-api.cryptocompare.com/spot/v1/markets"
    headers = {"Content-type": "application/json", "authorization": f"Apikey {api_key}"}
    
    response = requests.get(url_markets, headers=headers)
    
    if response.status_code == 200:
        data = response.json().get("Data", {})
        markets = [{"market": market, **details} for market, details in data.items()]
        markets_df = pd.DataFrame(markets)
        
        print(f"✅ Successfully fetched {len(markets_df)} markets.")
        return markets_df
    else:
        print(f"❌ API request failed: {response.status_code}, {response.text}")
        return None
    

def fetch_market_instruments(api_key: str, selected_markets: list, save_dir: str) -> pd.DataFrame:
    '''Download instrument list for selected market list

     Args:
         api_key(str): API key for authentication
         selected_markets(list): List of markets to fetch instruments for, case-sensitive
         save_dir(str): Directory to save the result CSV file

     Return:
         DataFrame containing instruments data

     Usage:
        selected_markets = ["binance", "coinbase", "kraken"]
        save_directory = "./result"
        fetch_market_instruments(api_key, selected_markets, save_dir)
    '''
    url_instruments = "https://data-api.cryptocompare.com/spot/v1/markets/instruments"
    headers = {"Content-type": "application/json", "authorization": f"Apikey {api_key}"}
    
    all_instruments = []

    for market in selected_markets:
        params = {"market": market}
        response = requests.get(url_instruments, headers=headers, params=params)
        
        if response.status_code == 200:
            json_response = response.json()
            market_data = json_response.get("Data", {}).get(market, {})
            instruments_data = market_data.get("instruments", {})

            # All instruments for the current market
            for instrument_name, instrument_details in instruments_data.items():
                all_instruments.append({
                    "market": market,
                    "instrument": instrument_name,
                    "base_asset": instrument_details.get("INSTRUMENT", "N/A"),
                    "status": instrument_details.get("INSTRUMENT_STATUS", "UNKNOWN")
                })

            print(f"✅ {market}: Retrieved {len(instruments_data)} trading pairs.")
        else:
            print(f"❌ API request failed: {response.status_code}, {response.text}")

        # avoid rate limit
        time.sleep(0.5)

    instruments_df = pd.DataFrame(all_instruments)

    os.makedirs(save_dir, exist_ok=True)
    csv_filepath = os.path.join(save_dir, "crypto_instruments.csv")
    instruments_df.to_csv(csv_filepath, index=False, encoding="utf-8")

    print(f"✅ Trading pair data has been saved to {csv_filepath}.")

    return instruments_df


def load_instruments(filepath: str) -> pd.DataFrame:
    '''Load instrument data from CSV file, containing all the markets and instruments

     Args:
         filepath(str): Path to the CSV file

     Return:
         DataFrame containing instrument data
    '''
    instruments_df = pd.read_csv(filepath)
    
    # instruments end with USDT, USD, USDC, KRW 
    filtered_instruments_df = instruments_df[
        instruments_df["instrument"].str.endswith(("-USDT", "-USD", "-USDC", "-KRW"))
    ]
    
    print(f"✅ Loaded {len(filtered_instruments_df)} filtered trading pairs from CSV.")
    return filtered_instruments_df


def fetch_spot_volume_data(
    api_key: str,
    instruments: pd.DataFrame,
    save_dir: str,
    limit: int = 2000,
    aggregate: int = 1,
    fill: str = "true",
    apply_mapping: str = "true"
    ):
    '''Download spot trading volume data for selected instruments

     Args:
         api_key(str): API key for authentication
         instruments(pd.DataFrame): DataFrame containing market and instruments
         save_dir(str): Directory to save the spot volume data
         limit(int): Number of data points to fetch per instrument (default: 2000)
         aggregate(int): Time interval in hours for aggregation (default: 1)
         fill(str): Whether to fill missing data points (default: 'true')
         apply_mapping(str): Whether to apply asset mapping (default: 'true')

     Return:
         None (CSV files will be saved)
         
     Usage:
         instruments_df = load_instruments_from_csv("xxx.csv")
         fetch_spot_volume_data(API_KEY, instruments_df, save_directory)

    '''
    os.makedirs(save_dir, exist_ok=True)

    # all the exchanges
    for market in instruments["market"].unique():
        trade_data = []

        #n instruments in the current market
        market_instruments_df = instruments[instruments["market"] == market]

        for _, row in market_instruments_df.iterrows():
            instrument = row["instrument"]

            params = {
                "market": market,
                "instrument": instrument,
                "limit": limit,  
                "aggregate": aggregate,  
                "fill": fill,
                "apply_mapping": apply_mapping,
                "response_format": "JSON",
            }

            response = requests.get("https://data-api.cryptocompare.com/spot/v1/historical/hours", 
                                    headers={"Content-type": "application/json", "authorization": f"Apikey {api_key}"}, 
                                    params=params)

            if response.status_code == 200:
                json_response = response.json()
                if "Data" in json_response and isinstance(json_response["Data"], list) and len(json_response["Data"]) > 0:
                    for entry in json_response["Data"]:
                        trade_data.append({
                            # all the data
                            "market": market,
                            "instrument": instrument,
                            "base_asset": entry["BASE"],
                            "quote_asset": entry["QUOTE"],
                            "time": pd.to_datetime(entry["TIMESTAMP"], unit="s"),
                            "open": entry["OPEN"],
                            "high": entry["HIGH"],
                            "low": entry["LOW"],
                            "close": entry["CLOSE"],
                            "total_trades": entry["TOTAL_TRADES"],
                            "total_trades_buy": entry["TOTAL_TRADES_BUY"],
                            "total_trades_sell": entry["TOTAL_TRADES_SELL"],
                            "volume": entry["VOLUME"],
                            "quote_volume": entry["QUOTE_VOLUME"],
                            "volume_buy": entry["VOLUME_BUY"],
                            "quote_volume_buy": entry["QUOTE_VOLUME_BUY"],
                            "volume_sell": entry["VOLUME_SELL"],
                            "quote_volume_sell": entry["QUOTE_VOLUME_SELL"],
                        })
                    print(f"✅ Data fetched: {market} - {instrument} ({len(json_response['Data'])} hours)")
                else:
                    print(f"❌ No data for {market} - {instrument}")
            else:
                print(f"❌ API request failed for {market} - {instrument}, Status Code: {response.status_code}")

            time.sleep(0.5)

        if trade_data:
            spot_volume_df = pd.DataFrame(trade_data)
            csv_filepath = os.path.join(save_dir, f"{market}_spot_volume_{limit}h.csv")
            spot_volume_df.to_csv(csv_filepath, index=False, encoding="utf-8")
            print(f"📁 Data for {market} saved to {csv_filepath}")


def process_spot_volume_data(df: pd.DataFrame, time_interval: str = "1D", group_by_instrument: bool = True) -> tuple:
    '''Process spot trading volume data with aggregation

     Args:
         df(pd.DataFrame): DataFrame containing spot volume data
         time_interval(str): Aggregation time interval (default: '1D' for daily).
                            Examples: '4h' (4 hours), '12h' (12 hours), '1D' (daily)
         group_by_instrument(bool): Whether to aggregate per instrument (default: False)

     Return:
         Tuple (processed DataFrame, aggregation description)
         
     Usage:
         df = pd.read_csv("Binance_spot_volume_1000h.csv")
         df_4h, desc_4h = process_spot_volume_data(df, time_interval="4h", group_by_instrument=True)
         print(desc_4h)
         print(df_4h.head())
    '''
    if "time" not in df.columns:
        raise ValueError("❌ The DataFrame must contain a 'time' column.")

    df.set_index("time", inplace=True)

    agg_methods = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
        "quote_volume": "sum"
    }
    if group_by_instrument:
        grouped_df = df.groupby("instrument").resample(time_interval).agg(agg_methods)
        grouped_df = grouped_df.reset_index()
        # 去除 instrument 列中的前缀，在这个使用过程中输出会给instrument加上1000前缀，原因是instrument
        # 在groupby的时候被当做了index，会被转化为string类型等格式变化造成。
        grouped_df["instrument"] = grouped_df["instrument"].str.replace("1000", "")
        desc = f"Data aggregated by instrument and resampled with {time_interval} interval."
    else:
        grouped_df = df.resample(time_interval).agg(agg_methods).reset_index()
        desc = f"Data resampled with {time_interval} interval (aggregated across all instruments)."
    
    return grouped_df, desc
 
            
markets_df = fetch_markets_data(API_KEY)
print(markets_df.head())

selected_markets = ["binance", "coinbase", "kraken"]
save_directory = "./result"

instruments_df = fetch_market_instruments(API_KEY, selected_markets, save_directory)
print(instruments_df.head())

csv_filepath = os.path.join(save_directory, "crypto_instruments.csv")
filtered_instruments_df = load_instruments(csv_filepath)
print(filtered_instruments_df.head())

fetch_spot_volume_data(
    api_key=API_KEY,
    instruments=filtered_instruments_df,
    save_dir=save_directory,
    limit=2000,
    aggregate=1,
    fill="true",
    apply_mapping="true"
)
print("✅ Spot volume data fetching completed!")