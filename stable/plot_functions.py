import os
import pandas as pd 
import matplotlib.pyplot as plt
import requests


def return_list():
    """
    Fetch data from DeFiLlama API and return the top 10 chains by TVL.
    从DeFiLlama API获取数据并返回TVL前10的链。
    
    """
    url = 'https://api.llama.fi/v2/chains'
    response = requests.get(url)
    if response.status_code == 200: # data was fetched successfully
        data = response.json()
        df = pd.DataFrame(data)
    else:
        raise Exception('Error fetching data from DeFiLlama API')

    dex_all_chains_url = "https://api.llama.fi/overview/dexs?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyVolume"
    dex_all_chains_response = requests.get(dex_all_chains_url)

    dex_all_chains_data = dex_all_chains_response.json()
    dex_all_chains = set(dex_all_chains_data.get('allChains', []))


    fees_all_chains_url = "https://api.llama.fi/overview/fees?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyFees"
    fees_all_chains_response = requests.get(fees_all_chains_url)

    fees_all_chains_data = fees_all_chains_response.json()
    fees_all_chains = set(fees_all_chains_data.get('allChains', []))

    common_chains = dex_all_chains.intersection(fees_all_chains) # get the common chains between the two sets

    df = df[['name', 'tvl']]
    name_list = df[df['name'].isin(common_chains)]

    # Sort the data to get the top 10 chains by TVL
    name_list = name_list.sort_values(by='tvl', ascending=False).head(11)
    print(name_list)
    return name_list


def plot_chains(data_dict, metric, mode='combined', selected_chain=None):
    """
    plot data for different chains.(combnied or single)
    这个函数是用来对数据进行可视化的，可以选择不同的模式，不同的模式下会有不同的输出。

    Parameters:
    data_dict (dict): dict of dataframes, key is chain name, value is dataframe.
    metric (str): metric type（'fee'、'tvl'、'volume'）.
    mode (str): 'combined' means plot data for same metric，'single' means plot data for different chains, different metric.
    selected_chain (str): used under only 'single' mode, specify the chain name.
    """
    
    # print(plt.style.available)

    if mode == 'combined':
        plt.figure(figsize=(14, 7))
        
        for chain_name, df in data_dict.items():
            plt.plot(df['date'], df['value'], label=chain_name)

        plt.xlabel('Date')
        plt.ylabel(f'{metric.capitalize()} Value')
        plt.title(f'{metric.capitalize()} Over Time for Different Chains')
        plt.xticks(rotation=45)
        plt.grid(True)
        plt.legend(loc='best')
        plt.tight_layout()
        
        output_folder = './results/combined_charts'
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        # save the plot
        
        plt.savefig(os.path.join(output_folder,f'combined_{metric}_chart.png'))
        print(f"combined {metric} has been saved as combined_{metric}_chart.png")
        plt.show()
        plt.close()
        
    elif mode == 'single':
        if selected_chain is None:
            raise ValueError("In 'single' mode, you must specify 'selected_chain'.")
        
        if selected_chain not in data_dict:
            raise ValueError(f"Chain '{selected_chain}' not found in data.")

        df = data_dict[selected_chain]

        plt.style.use('seaborn-v0_8')

        plt.figure(figsize=(14, 7))

        plt.plot(df['date'], df['value'], color='royalblue', linewidth=2.5, label=f'{selected_chain} {metric}')
    
        plt.xlabel('Date', fontsize=14, fontweight='bold')
        plt.ylabel(f'{metric.capitalize()} Value', fontsize=14, fontweight='bold')
        plt.title(f'{selected_chain} {metric.capitalize()} Over Time', fontsize=16, fontweight='bold')
        plt.xticks(rotation=45, fontsize=12)
        plt.yticks(fontsize=12)
        plt.grid(visible=True, linestyle='--', linewidth=0.5)
        plt.legend(loc='best', fontsize=12)
        plt.tight_layout()

        output_folder = './results/output_single_charts'
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        
        plt.savefig(os.path.join(output_folder, f'{selected_chain}_{metric}_single_chart.png'))
        print(f"{selected_chain} {metric} has been saved as {selected_chain}_{metric}_single_chart.png")
        plt.show()
        plt.close()
        
        
def plot_chain_zscore(data_dict_all, chain_name, metric, window_size=30, threshold=2):
    """
    compute Z score for a single metric of a single chain and plot the anomaly detection chart.
    单独一个链的单个指标的Zscore以及异常图表绘制。就一条线。

    Parameters:
    - data_dict_all (dict): all data of 150 days.
    - chain_name (str): name of the chain
    - metric (str): metric type（'fee'、'tvl'、'volume'）.
    - window_size (int): size of the rolling window, default 30.
    - threshold (int): z score threshold, default 2.
    """
    df = data_dict_all[metric][chain_name].copy()

    df['rolling_mean'] = df['value'].rolling(window=window_size).mean()
    df['rolling_std'] = df['value'].rolling(window=window_size).std()

    df['z_score'] = (df['value'] - df['rolling_mean']) / df['rolling_std']

    df['is_anomaly'] = df['z_score'].abs() > threshold

    df_plot = df.tail(365*2)

    plt.style.use('seaborn-v0_8')
    plt.figure(figsize=(14, 7))

    plt.plot(df_plot['date'], df_plot['value'], color='royalblue', linewidth=2.5, label='Value')

    plt.plot(df_plot['date'], df_plot['rolling_mean'], color='orange', linestyle='--', linewidth=2, label=f'{window_size}-day Rolling Mean')

    anomalies = df_plot[df_plot['is_anomaly']]
    plt.scatter(anomalies['date'], anomalies['value'], color='red', s=60, edgecolor='black', label='Anomalies', zorder=5)

    plt.xlabel('Date', fontsize=14, fontweight='bold')
    plt.ylabel(f'{metric.capitalize()} Value', fontsize=14, fontweight='bold')
    plt.title(f'{chain_name} {metric.capitalize()} Over Time with Z-Score Anomalies (Last 120 Days)', fontsize=18, fontweight='bold')
    plt.xticks(rotation=45, fontsize=12)
    plt.yticks(fontsize=12)
    plt.grid(visible=True, linestyle='--', linewidth=0.5)
    plt.legend(loc='best', fontsize=12)
    plt.tight_layout()
    
    output_folder = './results/z_score_charts'
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        
    plt.savefig(os.path.join(output_folder, f'{chain_name}_{metric}_Z.png'))
    print(f"{chain_name} {metric} has been saved {chain_name}_{metric}_Z.png")
    plt.show()
    plt.close()