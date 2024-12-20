import os
import logging
import pandas as pd
from etherscan_functions import get_erc20_transfers, get_block_numbers_by_date

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()


def fetch_erc20_transfers(address: str, start_block: int, end_block: int, offset: int = 100) -> pd.DataFrame:
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
    """
    if not transfers_df.empty:
        transfers_df['dateTime'] = pd.to_datetime(
            pd.to_numeric(transfers_df['timeStamp'], errors='coerce'), unit='s', utc=True
        ).dt.strftime('%Y-%m-%d %H:%M:%S')
        cols = ['dateTime'] + [col for col in transfers_df.columns if col != 'dateTime']
        transfers_df = transfers_df[cols]
        output_file = os.path.join(output_file, 'erc20_transfers.csv')
        # transfers_df.to_csv(output_file, index=False)
        logger.info(f"Data successfully saved to {output_file}")
    else:
        logger.warning("No valid transfers to save. DataFrame is empty.")


def fetch_and_save_erc20_transfers(
    address: str,
    start_date: str,
    end_date: str,
    output_file: str
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
            end_block=end_block
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
    'value', 'tokenName', 'tokenSymbol'
    ]
    if not transfers_df.empty:
        filtered_transaction_data = transfers_df[columns_to_keep]
        output_file = 'filtered_transaction_data.csv'
        filtered_transaction_data.to_csv(output_file, index=False)


def highlight_three_records(grouped_df, data):
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
    """
    output_records = []
    for hash_val, group in duplicate_hashes.groupby('hash'):
        base_tokens_group = group[group['tokenSymbol'].isin(base_tokens)]
        other_tokens_group = group[~group['tokenSymbol'].isin(base_tokens)]

        if not base_tokens_group.empty and not other_tokens_group.empty:
            transaction_type = "SELL" if base_tokens_group['to'].iloc[0] == address else "BUY"
            base_token_info = f"{base_tokens_group['value'].sum()} {base_tokens_group['tokenSymbol'].iloc[0]}"
            other_token_info = f"{other_tokens_group['value'].sum()} {other_tokens_group['tokenSymbol'].iloc[0]}"
            record = (
                f"{group['timeStamp'].iloc[0]} W {transaction_type} {other_token_info} of {base_token_info} "
                f"(at {group['dateTime'].iloc[0]})"
            )
            output_records.append({
                "formatted_record": record,
                "timeStamp": group['timeStamp'].iloc[0],
                "dateTime": group['dateTime'].iloc[0]
            })
    return pd.DataFrame(output_records)


def find_matched_transactions(transaction_data: pd.DataFrame, address: str, base_tokens: set) -> (pd.DataFrame, list):
    """
    Find and process matched BUY/SELL transactions.
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

                base_token_info = f"{base_token['value']} {base_token['tokenSymbol']}"
                other_token_info = f"{other_token['value']} {other_token['tokenSymbol']}"

                record = (
                    f"{current_row['timeStamp']} W {transaction_type} {other_token_info} of {base_token_info} "
                    f"(at {current_row['dateTime']})"
                )
                matched_records.append({"formatted_record": record, "dateTime": current_row['dateTime']})
                matched_indices.extend([i, i + 1])
                skip_next = True

    return pd.DataFrame(matched_records), matched_indices


def find_single_transactions(transaction_data: pd.DataFrame, address: str, base_tokens: set) -> pd.DataFrame:
    """
    Process single BUY/SELL transactions.
    """
    single_records = []
    for _, row in transaction_data.iterrows():
        if row['tokenSymbol'] in base_tokens:
            transaction_type = "single SELL" if row['from'] == address else "single BUY"
        else:
            transaction_type = "single BUY" if row['from'] == address else "single SELL"

        record = f"{row['timeStamp']} W {transaction_type} {row['value']} {row['tokenSymbol']} (at {row['dateTime']})"
        single_records.append({
            "formatted_record": record,
            "dateTime": row['dateTime'],
            "timeStamp": row['timeStamp'],
            "hash": row['hash']
        })
    return pd.DataFrame(single_records)


def process_transactions(transaction_data: pd.DataFrame, output_file: str, address: str, base_tokens: set):
    """
    Process transaction data and save the final result to a CSV file.
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
        for record in output_df_1['formatted_record']:
            print(record)

    # step 3: Find 'BUY/SELL' transactions
    output_df_2, matched_indices = find_matched_transactions(filtered_data_1, address, base_tokens)
    filtered_data_2 = filtered_data_1.drop(index=matched_indices).reset_index(drop=True)
    combined_df = pd.concat([output_df_1, output_df_2], ignore_index=True)
    print("Formatted Transactions: part 2")
    if not combined_df.empty:
        combined_df = combined_df.sort_values(by='dateTime').reset_index(drop=True)
        for record in combined_df['formatted_record']:
            print(record)

    # step 4: the rest are single transactions
    output_df_3 = find_single_transactions(filtered_data_2, address, base_tokens)

    # Combine all results
    final_combined_df = pd.concat([output_df_1, output_df_2, output_df_3], ignore_index=True)
    if not final_combined_df.empty:
        final_combined_df = final_combined_df.sort_values(by='dateTime').reset_index(drop=True)
        # final_combined_df.to_csv(output_file, index=False)
        print("Formatted Transactions: final part")
        for record in final_combined_df['formatted_record']:
            print(record)
