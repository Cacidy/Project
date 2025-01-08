import os
import logging
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from etherscan_functions import get_erc20_transfers, get_block_numbers_by_date

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()


def fetch_erc20_transfers(address: str, start_block: int, end_block: int, offset: int = 1000) -> pd.DataFrame:
    """
    Fetch ERC20 token transfer data and process it into a DataFrame.
    :param address: Contract address you decide to fetch the data from.
    :param start_block: Start block number.
    :param end_block: End block number.
    :param offset: Number of transactions to fetch per API call.
    """
    all_transfers = []
    page = 1

    logger.info(f"Fetching ERC20 transfers from block {start_block} to {end_block}...")

    while True:
        try:
            transfers = get_erc20_transfers(
                address=address, startblock=start_block, endblock=end_block, page=page, offset=offset
            )

            if transfers is None:
                logger.warning("API returned None. Exiting...")
                break
            if isinstance(transfers, list) and len(transfers) == 0:
                logger.info("Empty list received. Exiting...")
                break
            if isinstance(transfers, pd.DataFrame) and transfers.empty:
                logger.info("Empty DataFrame received. Exiting...")
                break

            if isinstance(transfers, list):
                transfers_df = pd.DataFrame(transfers)
            elif isinstance(transfers, pd.DataFrame):
                transfers_df = transfers
            else:
                logger.warning(f"Unexpected data format: {type(transfers)}. Exiting...")
                break

            if transfers_df.empty:
                logger.info("Empty DataFrame received. Exiting...")
                break

            all_transfers.extend(transfers_df.to_dict(orient='records'))
            logger.info(f"Page {page}: Retrieved {len(transfers_df)} transactions.")
            page += 1

        except Exception as e:
            logger.warning(f"API request failed on page {page}: {e}")
            break

    final_df = pd.DataFrame(all_transfers)
    logger.info(f"Final dataset contains {len(final_df)} unique transactions.")
    return final_df


def process_and_save_transfers(transfers_df: pd.DataFrame, output_file: str) -> None:
    """
    Process ERC20 transfers DataFrame and save to a CSV file.
    :param transfers_df: DataFrame containing ERC20 transfer data.
    :param output_file: Path to save the processed data.
    """
    if not transfers_df.empty:
        transfers_df['dateTime'] = pd.to_datetime(
            pd.to_numeric(transfers_df['timeStamp'], errors='coerce'), unit='s', utc=True
        ).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        transfers_df.insert(
            transfers_df.columns.get_loc('value') + 1,
            'ActualValue',
            pd.to_numeric(transfers_df['value'], errors='coerce') / 
            (10 ** pd.to_numeric(transfers_df['tokenDecimal'], errors='coerce'))
        )
        
        cols = ['dateTime'] + [col for col in transfers_df.columns if col != 'dateTime']
        transfers_df = transfers_df[cols]
        
        output_file = os.path.join(output_file, 'erc20_transfers.csv')
        transfers_df.to_csv(output_file, index=False)
        logger.info(f"Data successfully saved to {output_file}")
    else:
        logger.warning("No valid transfers to save. DataFrame is empty.")


def fetch_and_save_erc20_transfers(
    address: str,
    start_date: str,
    end_date: str,
    output_file: str,
    offset: int = 1000
) -> pd.DataFrame:
    """
    Fetch block numbers, retrieve ERC20 transfers, and save the processed data in one step.
    combing the abover 2 functions
    :param address: Ethereum address to fetch data for.
    :param start_date: Start date in 'YYYY-MM-DD HH:MM' format (local time).
    :param end_date: End date in 'YYYY-MM-DD HH:MM' format (local time).
    :param output_file: Path to save the processed data.
    :return: A Pandas DataFrame containing the processed transfer data.

    # if you wanna use this function in another script, you can import it like this:

    # from data_fetch_function import fetch_and_save_erc20_transfers

    # ADDRESS = "0x5be9a4959308A0D0c7bC0870E319314d8D957dBB"
    # START_DATE = "2024-12-10 08:00"
    # END_DATE = "2024-12-12 08:00"
    # OUTPUT_FILE = "C:/Users/YuweiCao/Documents/GitHub/Project/etherscan/result"
    # API_KEY = "VQAIR728IM4Z8RZKPYBR4ESM5I3WBZK2C1"
    # transfers_df = fetch_and_save_erc20_transfers(
    #         address=ADDRESS,
    #         start_date=START_DATE,
    #         end_date=END_DATE,
    #         output_file=OUTPUT_FILE
    #     )
    """
    try:
        # Step 1: Get block numbers for the given date range
        start_block, end_block = get_block_numbers_by_date(
            start_date=start_date,
            end_date=end_date,
            include_all=False
        )
        logger.info(f"Start block: {start_block}, End block: {end_block}")

        # Step 2: Fetch ERC20 transfers
        transfers_df = fetch_erc20_transfers(
            address=address,
            start_block=start_block,
            end_block=end_block,
            offset=offset
        )

        # Step 3: Save processed transfers
        process_and_save_transfers(transfers_df, output_file)
        logger.info(f"Transfers saved to {output_file}")

        # Check if DataFrame is empty
        if transfers_df.empty:
            transfers_df['dateTime'] = pd.to_datetime(transfers_df['dateTime'])
            transfers_df = transfers_df.sort_values(by=['dateTime', 'hash']).reset_index(drop=True)
            # organize data
            logger.info("No transfers found for the given address and date range.")
            return transfers_df
        else :
            return transfers_df

    except Exception as e:
        logger.error(f"An error occurred during fetch and save: {e}")
        raise


def save_partial_transfers(transfers_df: pd.DataFrame, output_file: str) -> None:
    """
    Save partial ERC20 transfers DataFrame to a CSV file.
    :param transfers_df: DataFrame containing ERC20 transfer data.
    :param output_file: Path to save the processed data.
    """
    columns_to_keep = [
    'dateTime', 'blockNumber', 'timeStamp', 'hash', 'from', 'to',
    'value', 'ActualValue', 'tokenName', 'tokenSymbol'
    ]
    if not transfers_df.empty:
        filtered_transaction_data = transfers_df[columns_to_keep]
        output_file = 'filtered_transaction_data.csv'
        filtered_transaction_data.to_csv(output_file, index=False)


def highlight_three_records(grouped_df: pd.core.groupby.DataFrameGroupBy, data: pd.DataFrame) -> pd.DataFrame:
    """
    Highlight and remove transactions with 3 records in the same hash.
    : param grouped_df: Grouped DataFrame by transaction hash.
    : param data: Original DataFrame containing transaction data.
    """
    matched_hashes = []

    for tx_hash, group in grouped_df:
        if len(group) == 3:
            print(f"⚠️ High Alert: Transaction Hash {tx_hash} contains 3 records:")
            print(group)
            matched_hashes.append(tx_hash)

    # i want to delete the abnormal data
    remaining_data = data[~data['hash'].isin(matched_hashes)].reset_index(drop=True)
    return remaining_data


def process_duplicate_hashes(duplicate_hashes: pd.DataFrame, address: str, base_tokens: set) -> pd.DataFrame:
    """
    Process duplicate transaction hashes to identify BUY/SELL actions.
    : param duplicate_hashes: DataFrame containing duplicate transaction hashes.
    : param address: Ethereum address to filter transactions for.
    : param base_tokens: Set of base tokens to filter transactions for.
    """
    output_records = []
    for hash_val, group in duplicate_hashes.groupby('hash'):
        base_tokens_group = group[group['tokenSymbol'].isin(base_tokens)]
        other_tokens_group = group[~group['tokenSymbol'].isin(base_tokens)]

        if not base_tokens_group.empty and not other_tokens_group.empty:
            transaction_type = "SELL" if base_tokens_group['to'].iloc[0] == address else "BUY"
            base_token_value = base_tokens_group['ActualValue'].sum()
            base_token_symbol = base_tokens_group['tokenSymbol'].iloc[0]

            other_token_value = other_tokens_group['ActualValue'].sum()
            other_token_symbol = other_tokens_group['tokenSymbol'].iloc[0]

            average_price = base_token_value / other_token_value if other_token_value > 0 else 0
            price_unit = f"{base_token_symbol}/{other_token_symbol}"
            record = (
                f"{group['timeStamp'].iloc[0]} W {transaction_type} {other_token_value} {other_token_symbol} "
                f"of {base_token_value} {base_token_symbol} at avg price {average_price:.6f} "
                f"(at {group['dateTime'].iloc[0]})"
            )

            output_records.append({
                "formatted_record": record,
                "timeStamp": group['timeStamp'].iloc[0],
                "dateTime": group['dateTime'].iloc[0],
                "hash": group['hash'].iloc[0],
                "transaction_type": transaction_type,
                "base_token_volume": base_token_value,
                "other_token_volume": other_token_value,
                "average_price": average_price,
                "token_symbol": other_token_symbol,
                "price_unit": price_unit
            })
            
    return pd.DataFrame(output_records)


from typing import Tuple

def find_matched_transactions(transaction_data: pd.DataFrame, address: str, base_tokens: set) -> Tuple[pd.DataFrame, list]:
    """
    Find and process matched BUY/SELL transactions.
    : param transaction_data: DataFrame containing transaction data.
    : param address: Ethereum address to filter transactions for.
    : param base_tokens: Set of base tokens to filter transactions for.
    """
    matched_records = []
    matched_indices = []
    skip_next = False

    for i in range(len(transaction_data) - 1):
        if skip_next:
            skip_next = False
            continue

        current_row = transaction_data.iloc[i]
        next_row = transaction_data.iloc[i + 1]

        if ((current_row['to'] == address and next_row['from'] == address) or
            (current_row['from'] == address and next_row['to'] == address)):
            if ((current_row['tokenSymbol'] in base_tokens or next_row['tokenSymbol'] in base_tokens) and
                not (current_row['tokenSymbol'] in base_tokens and next_row['tokenSymbol'] in base_tokens)):
                transaction_type = "\'SELL\'" if current_row['to'] == address else "\'BUY\'"
                base_token = current_row if current_row['tokenSymbol'] in base_tokens else next_row
                other_token = next_row if base_token is current_row else current_row

                base_token_value = base_token['ActualValue']
                base_token_symbol = base_token['tokenSymbol']
                other_token_value = other_token['ActualValue']
                other_token_symbol = other_token['tokenSymbol']

                average_price = base_token_value / other_token_value if other_token_value > 0 else 0
                price_unit = f"{base_token_symbol}/{other_token_symbol}"

                record = {
                    "formatted_record": (
                        f"{current_row['timeStamp']} W {transaction_type} {other_token_value} {other_token_symbol} "
                        f"of {base_token_value} {base_token_symbol} at avg price {average_price:.6f} "
                        f"(at {current_row['dateTime']})"
                    ),
                    "dateTime": current_row['dateTime'],
                    "timeStamp": current_row['timeStamp'],
                    "hash": current_row['hash'],
                    "transaction_type": transaction_type,
                    "base_token_volume": base_token_value,
                    "other_token_volume": other_token_value,
                    "average_price": average_price,
                    "token_symbol": other_token_symbol,
                    "price_unit": price_unit
                }

                matched_records.append(record)
                matched_indices.extend([i, i + 1])
                skip_next = True

    return pd.DataFrame(matched_records), matched_indices


def find_single_transactions(transaction_data: pd.DataFrame, address: str, base_tokens: set) -> pd.DataFrame:
    """
    Process single BUY/SELL transactions.
    : param transaction_data: DataFrame containing transaction data.
    : param address: Ethereum address to filter transactions for.
    : param base_tokens: Set of base tokens to filter transactions for.
    """
    single_records = []
    for _, row in transaction_data.iterrows():
        if row['tokenSymbol'] in base_tokens:
            transaction_type = "single SELL" if row['from'] == address else "single BUY"
        else:
            transaction_type = "single BUY" if row['from'] == address else "single SELL"

        record = {
            "formatted_record": (
                f"{row['timeStamp']} W {transaction_type} {row['ActualValue']} {row['tokenSymbol']} "
                f"(at {row['dateTime']})"
            ),
            "dateTime": row['dateTime'],
            "timeStamp": row['timeStamp'],
            "hash": row['hash'],
            "transaction_type": transaction_type
        }

        single_records.append(record)
    return pd.DataFrame(single_records)


def process_transactions(transaction_data: pd.DataFrame, output_file: str, address: str, base_tokens: set) -> pd.DataFrame:
    """
    Process transaction data and save the final result to a CSV file.
    : param transaction_data: DataFrame containing transaction data.
    : param output_file: Path to save the processed data.
    : param address: Ethereum address to filter transactions for.
    : param base_tokens: Set of base tokens to filter transactions for.
    """
    address = address.lower()
    save_partial_transfers(transaction_data, output_file)
    transaction_data['dateTime'] = pd.to_datetime(transaction_data['dateTime'])
    transaction_data = transaction_data.sort_values(by=['dateTime', 'hash']).reset_index(drop=True)

    duplicate_hashes = transaction_data[transaction_data.duplicated(subset=['hash'], keep=False)]

    # step 1: find if there are 3 records in the same hash, delete them if true.
    three_record_hashes = highlight_three_records(duplicate_hashes.groupby('hash'), transaction_data)
    duplicate_hashes = duplicate_hashes[~duplicate_hashes['hash'].isin(three_record_hashes)]

    filtered_data_1 = transaction_data[~transaction_data['hash'].isin(duplicate_hashes['hash'])].reset_index(drop=True)
    # step 2: find the type one BUY/SELL
    output_df_1 = process_duplicate_hashes(duplicate_hashes, address, base_tokens)
    print("Formatted Transactions: part 1")
    if not output_df_1.empty:
        output_df_1 = output_df_1.sort_values(by='dateTime').reset_index(drop=True)
        # for record in output_df_1['formatted_record']:
        #     print(record)

    # step 3: Find 'BUY/SELL' transactions
    output_df_2, matched_indices = find_matched_transactions(filtered_data_1, address, base_tokens)
    filtered_data_2 = filtered_data_1.drop(index=matched_indices).reset_index(drop=True)
    combined_df = pd.concat([output_df_1, output_df_2], ignore_index=True)
    print("Formatted Transactions: part 2")
    if not combined_df.empty:
        combined_df = combined_df.sort_values(by='dateTime').reset_index(drop=True)
        # for record in combined_df['formatted_record']:
        #     print(record)

    # step 4: the rest are single transactions
    output_df_3 = find_single_transactions(filtered_data_2, address, base_tokens)

    # Combine all results
    final_combined_df = pd.concat([output_df_1, output_df_2, output_df_3], ignore_index=True)
    if not final_combined_df.empty:
        final_combined_df = final_combined_df.sort_values(by='dateTime').reset_index(drop=True)
        # final_combined_df.to_csv(output_file, index=False)
        print("Formatted Transactions: final part")
        # for record in final_combined_df['formatted_record']:
        #     print(record)
    
    return output_df_1, combined_df, final_combined_df


def calculate_pnl(record_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate PnL for BTC transactions based on BTC_record_df.

    :param btc_record_df: DataFrame containing BTC transaction records.
    :return: DataFrame with calculated PnL and summary of Realised and Unrealised PnL.
    """
    
    # Define current price as the price of the last row
    current_price = record_df.iloc[-1]['average_price']
    
    results = []
    total_pnl = 0

    for _, row in record_df.iterrows():
        transaction_type = row['transaction_type']
        amount = row['other_token_volume']
        price = row['average_price']

        # Calculate PnL for each row based on the difference from the current price
        pnl = (current_price - price) * amount
        
        # For SELL transactions, PnL is negative because it's reducing the position
        if transaction_type == "SELL":
            pnl *= -1

        # Ignore the last row (current price reference)
        if row.name == record_df.index[-1]:
            pnl = 0

        total_pnl += pnl

        # Append the result for this transaction
        results.append({
            "Date": row['dateTime'],
            "timeStamp": row['timeStamp'],
            "Type": transaction_type,
            "amount": amount,
            "price": price,
            "pnl": pnl
        })

    # Convert results to a DataFrame
    pnl_df = pd.DataFrame(results)

    return pnl_df


def get_transfer_account_counts(transfers_df, top_accounts_df):
    """

    """

    from_address_counts = transfers_df["from"].value_counts().reset_index()
    from_address_counts.columns = ["address", "from_count"]

    to_address_counts = transfers_df["to"].value_counts().reset_index()
    to_address_counts.columns = ["address", "to_count"]

    address_counts = pd.merge(
        from_address_counts, to_address_counts, on="address", how="outer"
    ).fillna(0)
    address_counts["from_count"] = address_counts["from_count"].astype(int)
    address_counts["to_count"] = address_counts["to_count"].astype(int)

    address_counts["total_value_count"] = (
        address_counts["from_value"] + address_counts["to_value"]
    )
    
    top_addresses_set = set(top_accounts_df["Address"])
    address_counts["is_in_top_accounts"] = address_counts["address"].apply(
        lambda x: x in top_addresses_set
    )
    
    return address_counts


def get_public_name_tag(address, name_tag_cache):
    """
    
    """
    if address in name_tag_cache:
        return name_tag_cache[address]
    
    url = f"https://etherscan.io/address/{address}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    response = requests.get(url, headers=headers)
    # print(response.text[:500])
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        title = soup.title.string
        if title:
            name_tag = title.split('|')[0].strip()
            name_tag = name_tag.split('\n')[0].strip()
            name_tag_cache[address] = name_tag
            return name_tag
        else:
            name_tag_cache[address] = "Unknown"
            return "Unknown"
    else:
        name_tag_cache[address] = f"Error fetching data: {response.status_code}"
        return f"Error fetching data: {response.status_code}"


def add_name_tags(address_counts):
    """

    """
    name_tag_cache = {}
    tqdm.pandas()  # 进度条初始化
    address_counts["name_tag"] = address_counts["address"].progress_apply(
        lambda x: get_public_name_tag(x, name_tag_cache)
    )
    address_counts["name_tag"] = address_counts["name_tag"].apply(
        lambda x: "Address" if "Address" in str(x) else x
    )
    return address_counts


def process_and_save_address(transfers_df, top_accounts_df, output_file):
    """
    
    """
    address_counts = get_transfer_account_counts(transfers_df, top_accounts_df)
    address_counts = add_name_tags(address_counts)
    
    address_counts.to_csv(output_file, index=False)
    print(f"Data has been save to {output_file}")