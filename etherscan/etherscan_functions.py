import requests
import pytz
import json
import os
import pandas as pd
from datetime import datetime
import pandas as pd
from typing import List, Dict, Any

API_KEY = "VQAIR728IM4Z8RZKPYBR4ESM5I3WBZK2C1"
BASE_URL = "https://api.etherscan.io/v2/api"

# for more chain id, visit https://docs.etherscan.io/etherscan-v2/getting-started/supported-chains
# for more endpoints, visit https://forms.blockscan.com/public/grid/3E9QiN00NLhCQVibiP3Z-Bpqhmd7zGXsgapEKJupxiI
# 封装为函数是因为url以及response这种变量命很容易重复，导致代码不易维护
# ******************************************** Useful part *******************************************************
def get_erc20_transfers(
    address: str,
    contract_address: str = None,
    startblock: int = 0,
    endblock: int = 99999999,
    page: int = 1,
    offset: int = 1000,
    sort: str = "asc",
    chain_id: int = 1
) -> List[Dict[str, Any]]:
    """
    Fetch the list of ERC-20 token transfer events for a specific address.
    ERC-20 transfers from an address, specify the address parameter
    ERC-20 transfers from a contract address, specify the contract address parameter
    ERC-20 transfers from an address filtered by a token contract, specify both address and contract address parameters.
    
    :param address: the string representing the address to check for balance
    :param contract_address: Optional. the string representing the token contract address to check for balance
    :param startblock: the integer block number to start searching for transactions
    :param endblock: the integer block number to stop searching for transactions
    :param page: the integer page number, if pagination is enabled
    :param offset: the number of transactions displayed per page.
    :param sort: sorting preference, use 'asc' to sort by ascending and 'desc' to sort by descending
    :param chain_id: The blockchain network ID. Default is 1 (Ethereum Mainnet).
    :return: A list of dictionaries containing the ERC-20 token transfer events.
    """
    url = (
        f"{BASE_URL}?chainid={chain_id}&module=account&action=tokentx"
        f"&address={address}&startblock={startblock}&endblock={endblock}"
        f"&page={page}&offset={offset}&sort={sort}&apikey={API_KEY}"
    )
    
    if contract_address:
        url += f"&contractaddress={contract_address}"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        # print(data)
        if data.get("status") == "1":
            return pd.DataFrame(data.get("result", []))
        else:
            raise ValueError(f"Request failed: {data.get('message')}")
    else:
        raise ConnectionError(f"Request failed, code: {response.status_code}")


def get_block_number_by_timestamp(timestamp: int, closest: str = "before", chain_id: int = 1) -> int:
    """
    Returns the block number that was mined at a certain timestamp.

    :param timestamp: The integer representing the Unix timestamp in seconds.
    :param closest: the closest available block to the provided timestamp, either before or after.
    :param chain_id: The blockchain network ID. Default is 1 (Ethereum Mainnet).
    :return: The block number closest to the provided timestamp.
    """
    url = f"{BASE_URL}?chainid={chain_id}&module=block&action=getblocknobytime&timestamp={timestamp}&closest={closest}&apikey={API_KEY}"
    
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data.get("status") == "1":
            return int(data.get("result"))
        else:
            raise ValueError(f"Request failed: {data.get('message')}")
    else:
        raise ConnectionError(f"Request failed, code: {response.status_code}")


def get_block_numbers_by_date(
    start_date: str,
    end_date: str,
    closest: str = "before",
    include_all: bool = False,
) -> List[int]:
    """
    Returns block numbers for a specified date range.

    :param start_date: Start date in 'YYYY-MM-DD' format.
    :param end_date: End date in 'YYYY-MM-DD' format.
    :param closest: 'before' or 'after', to find the closest block.
    :param include_all: If True, returns all block numbers in the range. 
                        If False, only returns the start and end block numbers.
    :return: A list of block numbers. 
             If include_all is True, includes all blocks in the range. 
             Otherwise, includes only the start and end block numbers.

    :raises ValueError: If input dates are invalid or API responses indicate failure.
    :raises ConnectionError: If the HTTP request fails.
    """
    # Convert input time (Singapore Time) to UTC
    singapore_tz = pytz.timezone("Asia/Singapore")
    utc_tz = pytz.utc

    try:
        # Parse input dates and localize to Singapore timezone
        start_dt_sgt = singapore_tz.localize(datetime.strptime(start_date, "%Y-%m-%d %H:%M"))
        end_dt_sgt = singapore_tz.localize(datetime.strptime(end_date, "%Y-%m-%d %H:%M"))
    except ValueError:
        raise ValueError("Dates must be in 'YYYY-MM-DD HH:MM' format.")

    # Convert Singapore time to UTC
    start_timestamp = int(start_dt_sgt.astimezone(utc_tz).timestamp())
    end_timestamp = int(end_dt_sgt.astimezone(utc_tz).timestamp())

    if start_timestamp >= end_timestamp:
        raise ValueError("Start date must be earlier than end date.")

    # Fetch block numbers using timestamps
    start_block = get_block_number_by_timestamp(timestamp=start_timestamp, closest="before")
    end_block = get_block_number_by_timestamp(timestamp=end_timestamp, closest="after")
    # combine the use of 'before' and 'after' to get a wider range of blocks


    # Return range or just start and end block numbers
    if include_all:
        return list(range(start_block, end_block + 1))
    return [start_block, end_block]


# ******************************************** Accounts *******************************************************


def get_ether_balance(address: str, chain_id: int = 1, tag: str = "latest") -> float:
    """
    Get Ether Balance for a Single Address(余额)
    
    :param address: Ethereum address
    :param chain_id: chain name, default Ethereum Mainnet (1)
    :param tag: the string pre-defined block parameter, either earliest, pending or latest
    :return: balance(ETH)
    """
    url = (f"{BASE_URL}?chainid={chain_id}"
        f"&module=account"
        f"&action=balance"
        f"&address={address}"
        f"&tag={tag}"
        f"&apikey={API_KEY}")
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data.get("status") == "1":
            balance_wei = int(data.get("result", 0))
            return balance_wei / 10**18  # wei to ETH
        else:
            raise ValueError(f"request failed: {data.get('message')}")
    else:
        raise ConnectionError(f"request failed, code: {response.status_code}")
    

def get_multi_address_balance(addresses: List[str], chain_id: int = 1, tag: str = "latest") -> Dict[str, float]:
    """
    Get Ether balance for multiple addresses.
    
    :param addresses: the strings representing the addresses to check for balance, separated by ',' up to 20 addresses per call
    :param chain_id: Blockchain network ID, default is 1 (Ethereum Mainnet)
    :param tag: the integer pre-defined block parameter, either earliest, pending or latest
    :return: Dictionary with addresses and their balances in ETH
    """
    if not addresses:
        raise ValueError("Address list cannot be empty")
    
    address_param = ",".join(addresses)
    
    url = (f'{BASE_URL}?chainid={chain_id}'
           f'&module=account'
           f'&action=balancemulti&address={address_param}'
           f'&tag={tag}'
           f'&apikey={API_KEY}')
    
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        if data.get("status") == "1":
            results = data.get("result", [])
            return {item["account"]: int(item["balance"]) / 10**18 for item in results}
            # wei to ETH
        else:
            raise ValueError(f"Request failed: {data.get('message')}")
    else:
        raise ConnectionError(f"Request failed, code: {response.status_code}")
    

def get_normal_transactions(
    address: str,
    startblock: int = 0,
    endblock: int = 99999999,
    page: int = 1,
    offset: int = 10,
    sort: str = "asc",
    chain_id: int = 1
) -> List[Dict[str, Any]]:
    """
    Returns the list of transactions performed by an address, with optional pagination.
    
    :param address: the string representing the addresses to check for balance
    :param startblock: the integer block number to start searching for transactions
    :param endblock: the integer block number to stop searching for transactions
    :param page: the integer page number, if pagination is enabled
    :param offset: the number of transactions displayed per page
    :param sort: the sorting preference, use 'asc to sort' by ascending and 'desc' to sort by descending 
                Tip: Specify a smaller startblock and endblock range for faster search results.
    :param chain_id: chain name, default Ethereum Mainnet (1)
    :return: the list of transactions
    """
    url = (
        f"{BASE_URL}?chainid={chain_id}&module=account&action=txlist"
        f"&address={address}&startblock={startblock}&endblock={endblock}"
        f"&page={page}&offset={offset}&sort={sort}&apikey={API_KEY}"
    )

    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        if data.get("status") == "1":
            return data.get("result", [])
        else:
            raise ValueError(f"Request failed: {data.get('message')}")
    else:
        raise ConnectionError(f"Request failed, code: {response.status_code}")
    
    
def get_internal_transactions(
    address: str,
    startblock: int = 0,
    endblock: int = 99999999,
    page: int = 1,
    offset: int = 10,
    sort: str = "asc",
    chain_id: int = 1
) -> List[Dict[str, Any]]:
    """
    Returns the list of internal transactions performed by an address, with optional pagination.
    
    :param address: the string representing the addresses to check for balance
    :param startblock: the integer block number to start searching for transactions
    :param endblock: the integer block number to stop searching for transactions
    :param page: he integer page number, if pagination is enabled
    :param offset: the number of transactions displayed per page
    :param sort: the sorting preference, use 'asc' to sort by ascending and 'desc' to sort by descending
    :param chain_id: chain name, default Ethereum Mainnet (1)
    :return: the list of internal transactions
    """
    url = (
        f"{BASE_URL}?chainid={chain_id}&module=account&action=txlistinternal"
        f"&address={address}&startblock={startblock}&endblock={endblock}"
        f"&page={page}&offset={offset}&sort={sort}&apikey={API_KEY}"
    )

    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        if data.get("status") == "1":
            return data.get("result", [])
        else:
            raise ValueError(f"Request failed: {data.get('message')}")
    else:
        raise ConnectionError(f"Request failed, code: {response.status_code}")


# Get 'Internal Transactions' by Transaction Hash
# Get "Internal Transactions" by Block Range
# Get a list of 'ERC721 - Token Transfer Events' by Address
# Get a list of 'ERC1155 - Token Transfer Events' by Address
# Get list of Blocks Validated by Address
# Get Beacon Chain Withdrawals by Address and Block Range
# Pro: Get Historical Ether Balance for a Single Address By BlockNo 

# ******************************************** Contracts *******************************************************



def get_contract_abi(contract_address: str, chain_id: int = 1) -> Dict[str, Any]:
    """
    Fetch the ABI (Application Binary Interface) of a verified smart contract.
    
    :param contract_address: the contract address that has a verified source code
    :param chain_id: The blockchain network ID. Default is 1 (Ethereum Mainnet).
    :return: The ABI of the contract as a JSON object.
    """
    url = f"{BASE_URL}?chainid={chain_id}&module=contract&action=getabi&address={contract_address}&apikey={API_KEY}"
    
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        if data.get("status") == "1":
            return data.get("result")  # Return the ABI (JSON string or list)
        else:
            raise ValueError(f"Request failed: {data.get('message')}")
    else:
        raise ConnectionError(f"Request failed, code: {response.status_code}")
    

def get_contract_source_code(contract_address: str, chain_id: int = 1) -> Dict[str, Any]:
    """
    Fetch the Solidity source code of a verified smart contract.
    
    :param contract_address: The contract address that has a verified source code
    :param chain_id: The blockchain network ID. Default is 1 (Ethereum Mainnet).
    :return: The source code of the contract as a JSON object.
    """
    # Construct the request URL
    url = f"{BASE_URL}?chainid={chain_id}&module=contract&action=getsourcecode&address={contract_address}&apikey={API_KEY}"
    
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        if data.get("status") == "1":
            return data.get("result", [])
        else:
            raise ValueError(f"Request failed: {data.get('message')}")
    else:
        raise ConnectionError(f"Request failed, code: {response.status_code}")

    
# Get Contract Creator and Creation Tx Hash
# Check Source Code Verification Status
# Verify Proxy Contract

# ******************************************** Transactions *******************************************************

def check_contract_execution_status(txhash: str, chain_id: int = 1) -> Dict[str, Any]:
    """
    Returns the status code of a contract execution.
    
    :param txhash: the string representing the transaction hash to check the execution status
    :param chain_id: The blockchain network ID. Default is 1 (Ethereum Mainnet).
    :return: The execution status as a dictionary.
    """
    # Construct the request URL
    url = f"{BASE_URL}?chainid={chain_id}&module=transaction&action=getstatus&txhash={txhash}&apikey={API_KEY}"
    
    # Send the GET request
    response = requests.get(url)
    
    # Handle response
    if response.status_code == 200:
        data = response.json()
        if data.get("status") == "1":
            return data.get("result", {})
        else:
            raise ValueError(f"Request failed: {data.get('message')}")
    else:
        raise ConnectionError(f"Request failed, code: {response.status_code}")
    

def check_transaction_receipt_status(txhash: str, chain_id: int = 1) -> Dict[str, Any]:
    """
    Returns the status code of a transaction execution.
    
    :param txhash: the string representing the transaction hash to check the execution status
    :param chain_id: The blockchain network ID. Default is 1 (Ethereum Mainnet).
    :return: The receipt status as a dictionary.
    """
    # Construct the request URL
    url = f"{BASE_URL}?chainid={chain_id}&module=transaction&action=gettxreceiptstatus&txhash={txhash}&apikey={API_KEY}"
    
    # Send the GET request
    response = requests.get(url)
    
    # Handle response
    if response.status_code == 200:
        data = response.json()
        if data.get("status") == "1":
            return data.get("result", {})
        else:
            raise ValueError(f"Request failed: {data.get('message')}")
    else:
        raise ConnectionError(f"Request failed, code: {response.status_code}")
    
# ******************************************** Blocks *******************************************************

def get_block_and_uncle_rewards(block_number: int, chain_id: int = 1) -> Dict[str, Any]:
    """
    Returns the block reward and 'Uncle' block rewards.

    :param block_number: the integer block number to check block rewards for eg. 12697906
    :param chain_id: The blockchain network ID. Default is 1 (Ethereum Mainnet).
    :return: The rewards as a dictionary.
    """
    url = f"{BASE_URL}?chainid={chain_id}&module=block&action=getblockreward&blockno={block_number}&apikey={API_KEY}"
    response = requests.get(url)
    
    # Handle response
    if response.status_code == 200:
        data = response.json()
        if data.get("status") == "1":
            return data.get("result", {})
        else:
            raise ValueError(f"Request failed: {data.get('message')}")
    else:
        raise ConnectionError(f"Request failed, code: {response.status_code}")

# Get Estimated Block Countdown Time by BlockNo
# Pro: Get Daily Average Block Size 
# Pro: Get Daily Block Count and Rewards 
# Pro: Get Daily Block Rewards
# Pro: Get Daily Average Time for A Block to be Included in the Ethereum Blockchain
# Pro: Get Daily Uncle Block Count and Rewards 

# ******************************************** Logs *******************************************************
def get_logs(address: str, from_block: int, to_block: int, page: int = 1, offset: int = 1000, chain_id: int = 1) -> List[Dict[str, Any]]:
    """
    Fetch logs for a specific contract address within a block range.

    :param address: the string representing the address to check for logs
    :param from_block: the integer block number to start searching for logs eg. 12878196
    :param to_block: the integer block number to stop searching for logs eg. 12879196
    :param page: the integer page number, if pagination is enabled
    :param offset: the number of transactions displayed per page limited to 1000 records per query, 
                    use the page parameter for subsequent records
    :param chain_id: The blockchain network ID. Default is 1 (Ethereum Mainnet).
    :return: A list of logs as dictionaries.
    """
    # Construct the request URL
    url = (f"{BASE_URL}?chainid={chain_id}&module=logs&action=getLogs"
           f"&address={address}&fromBlock={from_block}&toBlock={to_block}"
           f"&page={page}&offset={offset}&apikey={API_KEY}")
    
    # Send the GET request
    response = requests.get(url)
    
    # Handle the response
    if response.status_code == 200:
        data = response.json()
        file_name = os.path.join(r"./result", "logs.json")
        if data.get("status") == "1":
            file_name = os.path.join(r"./result", "logs.json")
            with open(file_name, "w", encoding="utf-8") as file:
                json.dump(data, file, indent=4, ensure_ascii=False)
            return data.get("result", [])
        else:
            raise ValueError(f"Request failed: {data.get('message')}")
    else:
        raise ConnectionError(f"Request failed, code: {response.status_code}")


def get_logs_by_topics(from_block: int, to_block: int, topic0: str, topic1: str, topic0_1_opr: str = "and", page: int = 1, offset: int = 1000, chain_id: int = 1) -> List[Dict[str, Any]]:
    """
    Fetch logs within a block range filtered by topics.

    :param from_block: The starting block number (inclusive).
    :param to_block: The ending block number (inclusive).
    :param topic: the topic numbers to search for limited totopic0, topic1, topic2, topic3
    :param topicOperator: the topic operator when multiple topic combinations are used limited to and or or
    :param page: The page number for pagination. Default is 1.
    :param offset: the number of transactions displayed per page limited to 1000 records per query, use the page parameter for subsequent records
    :param chain_id: The blockchain network ID. Default is 1 (Ethereum Mainnet).
    :return: A list of logs as dictionaries.
    """
    # Construct the request URL
    url = (f"{BASE_URL}?chainid={chain_id}&module=logs&action=getLogs"
           f"&fromBlock={from_block}&toBlock={to_block}&topic0={topic0}"
           f"&topic0_1_opr={topic0_1_opr}&topic1={topic1}&page={page}"
           f"&offset={offset}&apikey={API_KEY}")
    
    # Send the GET request
    response = requests.get(url)
    
    # Handle the response
    if response.status_code == 200:
        data = response.json()
        if data.get("status") == "1":
            return data.get("result", [])
        else:
            raise ValueError(f"Request failed: {data.get('message')}")
    else:
        raise ConnectionError(f"Request failed, code: {response.status_code}")


def get_logs_with_address_and_topics(
    from_block: int,
    to_block: int,
    address: str,
    topic0: str,
    topic1: str,
    topic0_1_opr: str = "and",
    page: int = 1,
    offset: int = 1000,
    chain_id: int = 1
) -> List[Dict[str, Any]]:
    """
    Fetch logs for a specific contract address within a block range filtered by topics.

    :param from_block: The starting block number (inclusive).
    :param to_block: The ending block number (inclusive).
    :param address: the string representing the address to check for logs
    :param topic: the topic numbers to search for limited totopic0, topic1, topic2, topic3
    :param topicOperator: the topic operator when multiple topic combinations are used limited to 'and' or 'or'
    :param page: The page number for pagination. Default is 1.
    :param offset: the number of transactions displayed per page limited to 1000 records per query,
                    use the page parameter for subsequent records
    :param chain_id: The blockchain network ID. Default is 1 (Ethereum Mainnet).
    :return: A list of logs as dictionaries.
    """
    # Construct the request URL
    url = (
        f"{BASE_URL}?chainid={chain_id}&module=logs&action=getLogs"
        f"&fromBlock={from_block}&toBlock={to_block}&address={address}"
        f"&topic0={topic0}&topic0_1_opr={topic0_1_opr}&topic1={topic1}"
        f"&page={page}&offset={offset}&apikey={API_KEY}"
    )
    
    # Send the GET request
    response = requests.get(url)
    
    # Handle the response
    if response.status_code == 200:
        data = response.json()
        if data.get("status") == "1":
            return data.get("result", [])
        else:
            raise ValueError(f"Request failed: {data.get('message')}")
    else:
        raise ConnectionError(f"Request failed, code: {response.status_code}")

# ******************************************** Geth/Parity Proxy *******************************************************

# eth_blockNumber
# eth_getBlockByNumber
# eth_getUncleByBlockNumberAndIndex
# eth_getBlockTransactionCountByNumber

def get_transaction_by_hash(tx_hash: str) -> Dict[str, Any]:
    """
    Returns the information about a transaction requested by transaction hash.

    :param tx_hash: The string representing the hash of the transaction.
    :return: A dictionary containing transaction details.
    """
    url = f"{BASE_URL}?module=proxy&action=eth_getTransactionByHash&txhash={tx_hash}&apikey={API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if "result" in data:
            return data["result"]
        else:
            raise ValueError(f"Request failed: {data.get('message')}")
    else:
        raise ConnectionError(f"Request failed, code: {response.status_code}")

# eth_getTransactionByBlockNumberAndIndex

def get_transaction_count(address: str, chain_id: int = 1, tag: str = "latest") -> int:
    """
    Returns the number of transactions performed by an address.

    :param chain_id: The blockchain network ID. Default is 1 (Ethereum Mainnet).
    :param address: The Ethereum address.
    :param tag: The block tag ("latest", "earliest", or block number). Default is "latest".
    :return: The transaction count (nonce).
    """
    url = f"{BASE_URL}?chainid={chain_id}&module=proxy&action=eth_getTransactionCount&address={address}&tag={tag}&apikey={API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if "result" in data:
            return int(data["result"], 16)  # Convert hex to int
        else:
            raise ValueError(f"Request failed: {data.get('message')}")
    else:
        raise ConnectionError(f"Request failed, code: {response.status_code}")

# eth_sendRawTransaction

def get_transaction_receipt(tx_hash: str) -> Dict[str, Any]:
    """
    Returns the receipt of a transaction by transaction hash.

    :param tx_hash: the string representing the hash of the transaction.
    :return: A dictionary containing the transaction receipt.
    """
    url = f"{BASE_URL}?module=proxy&action=eth_getTransactionReceipt&txhash={tx_hash}&apikey={API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if "result" in data:
            return data["result"]
        else:
            raise ValueError(f"Request failed: {data.get('message')}")
    else:
        raise ConnectionError(f"Request failed, code: {response.status_code}")


def eth_call(data: str, to_address: str, chain_id: int = 1, tag: str = "latest") -> str:
    """
    Executes a new message call immediately without creating a transaction on the block chain.

    :param data: the hash of the method signature and encoded parameters. Usually ABI encoded.
    :param to_address: the string representing the address to interact with.
    :param tag: The string pre-defined block parameter, either earliest, pending or latest
    :return: The result of the call as a hex string.
    """
    url = f"{BASE_URL}?chainid={chain_id}&module=proxy&action=eth_call&to={to_address}&data={data}&tag={tag}&apikey={API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if "result" in data:
            return data["result"]
        else:
            raise ValueError(f"Request failed: {data.get('message')}")
    else:
        raise ConnectionError(f"Request failed, code: {response.status_code}")
    

def get_code(address: str, chain_id: int = 1, tag: str = "latest") -> str:
    """
    Returns code at a given address.

    :param address: the string representing the address to get code.
    :param tag: the string pre-defined block parameter, either earliest, pending or latest.
    :return: The code as a hex string.
    """
    url = f"{BASE_URL}?chainid={chain_id}&module=proxy&action=eth_getCode&address={address}&tag={tag}&apikey={API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if "result" in data:
            return data["result"]
        else:
            raise ValueError(f"Request failed: {data.get('message')}")
    else:
        raise ConnectionError(f"Request failed, code: {response.status_code}")


# eth_getStorageAt
# eth_gasPrice
# eth_estimateGas

# ******************************************** Tokens *******************************************************

# ******************************************** Gas Tracker *******************************************************

# ******************************************** Stats *******************************************************

# ******************************************** Chain Specific *******************************************************

