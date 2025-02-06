import pandas as pd
import time
import requests
from tqdm import tqdm
from bs4 import BeautifulSoup
'''
这个文件用于存储top list，直接运行就可以得到result里面的top_accounts.csv，存储用于以后参照
'''

OUTPUT_FILE = "C:/Users/YuweiCao/Documents/GitHub/Project/Project/etherscan/result"
# storing the last 10,000 top accounts, as a list for further processing
# if we alr have one dont run it again! takes a long time
def get_top_accounts(page=1):
    """
    get top accounts data from etherscan.io, store them as a list
    """
    url = f"https://etherscan.io/accounts?p={page}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table", class_="table")
        rows = table.find_all("tr")[1:]

        accounts = []
        for row in rows:
            cols = row.find_all("td")
            address_tag = cols[1].find("a")  # 定位到地址的 <a> 标签

            # 提取完整地址
            full_address = address_tag["href"].split("/")[-1]  # 从 href 提取完整地址
            name_tag = cols[2].text.strip()  # 提取 Name Tag
            balance = cols[3].text.strip()
            percentage = cols[4].text.strip()
            txn_count = cols[5].text.strip()

            accounts.append({
                "Rank": cols[0].text.strip(),
                "Address": full_address,
                "Name Tag": name_tag,
                "Balance": balance,
                "Percentage": percentage,
                "Txn Count": txn_count,
            })
        return accounts
    else:
        print(f"Failed to fetch page {page}, status code: {response.status_code}")
        return []

def scrape_top_accounts(max_pages=5):
    """
    scrape top accounts data from etherscan.io, store them as a list
    """
    all_accounts = []
    for page in range(1, max_pages + 1):
        print(f"Scraping page {page}...")
        accounts = get_top_accounts(page)
        all_accounts.extend(accounts)
        time.sleep(2)  # 避免请求过于频繁
    return all_accounts


# Storing the last 10,000 top accounts only
top_accounts = scrape_top_accounts(max_pages=400)

top_accounts_df = pd.DataFrame(top_accounts)

top_accounts_df["Name Tag"] = top_accounts_df["Name Tag"].fillna("Unknown Whale")
top_accounts_df.to_csv(OUTPUT_FILE + "/top_accounts.csv", index=False)

print(f"Top account list has been filled and saved to {OUTPUT_FILE}")