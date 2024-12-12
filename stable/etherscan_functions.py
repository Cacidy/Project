import requests
import json
from typing import List, Dict, Any

API_KEY = "VQAIR728IM4Z8RZKPYBR4ESM5I3WBZK2C1"
BASE_URL = "https://api.etherscan.io/v2/api"
# for chain id, visit https://docs.etherscan.io/etherscan-v2/getting-started/supported-chains
# for more endpoints, visit https://forms.blockscan.com/public/grid/3E9QiN00NLhCQVibiP3Z-Bpqhmd7zGXsgapEKJupxiI
# 封装为函数是因为url以及response这种变量命很容易重复，导致代码不易维护


def get_ether_balance(address: str, chain_id: int = 1, tag: str = "latest") -> float:
    """
    获取以太坊地址的余额
    :param address: 要查询的目标地址
    :param chain_id: 查询的链，默认为 Ethereum Mainnet (1)
    :param tag: 区块标签，默认是 "latest"
    :return: 地址余额 (ETH)
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
    :param api_key: Etherscan API key
    :param base_url: Etherscan API base URL
    :param addresses: List of Ethereum addresses (as a list)
    :param chain_id: Blockchain network ID, default is 1 (Ethereum Mainnet)
    :param tag: Block tag to query, default is "latest"
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
    获取指定地址的普通交易记录
    :param address: 要查询的地址
    :param startblock: 起始区块号，默认值为 0
    :param endblock: 结束区块号，默认值为 99999999
    :param page: 页码，默认值为 1
    :param offset: 每页显示的交易数量，默认值为 10
    :param sort: 排序方式，"asc" 升序，"desc" 降序，默认值为 "asc"
    :param chain_id: 区块链网络 ID，默认值为 1（以太坊主网）
    :return: 包含交易记录的列表
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