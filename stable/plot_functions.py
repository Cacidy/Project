import os
import pandas as pd 
import matplotlib.pyplot as plt
import requests
import matplotx as mplx

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

    name_list = name_list.sort_values(by='tvl', ascending=False).head(11)
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
        
        # plt.savefig(os.path.join(output_folder,f'combined_{metric}_chart.png'))
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
        
        # plt.savefig(os.path.join(output_folder, f'{selected_chain}_{metric}_single_chart.png'))
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
        
    # plt.savefig(os.path.join(output_folder, f'{chain_name}_{metric}_Z.png'))
    print(f"{chain_name} {metric} has been saved {chain_name}_{metric}_Z.png")
    plt.show()
    plt.close()
    
    
def plot_tvl_zscore(data_dict_all, chain_name, window_size=30, threshold=2):
    """
    Compute Z score for a single metric of a single chain and plot the anomaly detection chart.
    单独一个链的tvl'(tvl/price)Zscore以及异常图表绘制。因为其实我们不关心tvl的变化。
    函数绘制结果包括tvl'、rolling mean、price、anomalies。共三条线。
    这个函数绘制了近两年的数据。注意输入数据应该是365*2+window_size的长度。
    Parameters:
    - data_dict_all (dict): all data of 150 days.
    - chain_name (str): name of the chain
    - metric (str): metric type ('fee', 'tvl', 'volume').
    - window_size (int): size of the rolling window, default 30.
    - threshold (int): z score threshold, default 2， we can also change it into 3.
    - include_price (bool): whether to include price data in the plot, default False.
    """
    
    # Ensure required data exists
    if 'tvl' not in data_dict_all or 'price' not in data_dict_all:
        print("Missing 'tvl' or 'price' data in the dataset.")
        return

    # Merge TVL and Price data
    tvl_df = data_dict_all['tvl'][chain_name].copy().tail(365*2+window_size)
    price_df = data_dict_all['price'][chain_name].copy().tail(365*2+window_size)
    merged_df = pd.merge(tvl_df, price_df, on='date', suffixes=('_tvl', '_price'))

    # Compute tvl'
    # merged_df['tvl_prime'] = merged_df['value_tvl'] / merged_df['value_price'] / 1e6
    merged_df['tvl_prime'] = merged_df['value_tvl'] / merged_df['value_price']


    # Compute rolling statistics for tvl'
    merged_df['rolling_mean'] = merged_df['tvl_prime'].rolling(window=window_size).mean()
    merged_df['rolling_std'] = merged_df['tvl_prime'].rolling(window=window_size).std()
    merged_df['Z_score'] = (merged_df['tvl_prime'] - merged_df['rolling_mean']) / merged_df['rolling_std']
    merged_df['is_anomaly'] = merged_df['Z_score'].abs() > threshold

    # Select data for the last 365*2 days
    df_plot = merged_df.tail(365*2)

    # Plot the results
    plt.style.use('seaborn-v0_8')
    fig, ax1 = plt.subplots(figsize=(14, 7))

    ax1.plot(df_plot['date'], df_plot['tvl_prime'], color='royalblue', linewidth=2.5, label="TVL'")
    ax1.plot(df_plot['date'], df_plot['rolling_mean'], color='orange', linestyle='--', linewidth=2, label=f'{window_size}-day Rolling Mean')

    # Mark anomalies
    anomalies = df_plot[df_plot['is_anomaly']]
    ax1.scatter(anomalies['date'], anomalies['tvl_prime'], color='red', s=60, edgecolor='black', label='Anomalies', zorder=5)

    # Add secondary y-axis for price
    ax2 = ax1.twinx()
    ax2.plot(df_plot['date'], df_plot['value_price'], color='green', linestyle='-', linewidth=2, label='Price')
    ax2.set_ylabel('Price', fontsize=14, fontweight='bold', color='green')
    ax2.tick_params(axis='y', labelsize=12, colors='green')

    # Axis labels and formatting
    ax1.set_xlabel('Date', fontsize=14, fontweight='bold')
    ax1.set_ylabel("TVL'/Price (in B)", fontsize=14, fontweight='bold')
    ax1.tick_params(axis='y', labelsize=12)
    ax1.tick_params(axis='x', rotation=45, labelsize=12)
    ax1.grid(visible=True, linestyle='--', linewidth=0.5)
    ax1.legend(loc='upper left', fontsize=12)
    # Format the left y-axis to show values with 'B'
    # ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:.0f}m'))
    
    ax2.legend(loc='upper right', fontsize=12)

    # Title
    plt.title(f"{chain_name} TVL' Over Time with Z-Score Anomalies (Last 2 years)", fontsize=18, fontweight='bold')
    plt.tight_layout()

    output_folder = './results/z_score_charts'
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    # plt.savefig(os.path.join(output_folder, f'{chain_name}_tvl\'.png'))
    print(f"{chain_name} TVL' Z-Score chart with Price has been saved ({chain_name}_tvl_prime_zscore.png)")
    plt.show()
    plt.close()
    
    
def plot_three_metrics(data_dict_all, chain_name, window_size=30, threshold=2):
    """
    Plot three metrics (fee, TVL'/Price, and Price) for a given chain on a single figure.
    The x-axis (date) is shared across all three subplots.

    Parameters:
    - data_dict_all (dict): Dictionary containing all data.
    - chain_name (str): The name of the chain to plot.
    - window_size (int): The window size for the rolling mean and standard deviation.
    - threshold (int): The Z-score threshold for anomaly detection.
    """
    # Check if all required data exists
    if 'fee' not in data_dict_all or 'tvl' not in data_dict_all or 'price' not in data_dict_all:
        print("Missing one or more required datasets (fee, tvl, price) in data_dict_all.")
        return

    if chain_name not in data_dict_all['fee'] or chain_name not in data_dict_all['tvl'] or chain_name not in data_dict_all['price']:
        print(f"Data for chain {chain_name} is missing in one or more datasets.")
        return

    # Prepare data
    fee_df = data_dict_all['fee'][chain_name].copy().tail(365*2+window_size)
    tvl_df = data_dict_all['tvl'][chain_name].copy().tail(365*2+window_size)
    price_df = data_dict_all['price'][chain_name].copy().tail(365*2+window_size)
    
    # rename the value column to avoid confusion
    fee_df.rename(columns={'value': 'value_fee'}, inplace=True)
    tvl_df.rename(columns={'value': 'value_tvl'}, inplace=True)
    price_df.rename(columns={'value': 'value_price'}, inplace=True)


    # Merge all data on the 'date' column
    merged_df = fee_df.merge(tvl_df, on='date', suffixes=('_fee', '_tvl'))
    merged_df = merged_df.merge(price_df, on='date')

    # Compute TVL'/Price
    merged_df['tvl_prime'] = merged_df['value_tvl'] / merged_df['value_price']

    # Compute rolling statistics for tvl'
    merged_df['rolling_mean'] = merged_df['tvl_prime'].rolling(window=window_size).mean()
    merged_df['rolling_std'] = merged_df['tvl_prime'].rolling(window=window_size).std()
    merged_df['Z_score'] = (merged_df['tvl_prime'] - merged_df['rolling_mean']) / merged_df['rolling_std']
    merged_df['is_anomaly'] = merged_df['Z_score'].abs() > threshold

    # Plot
    plt.style.use('seaborn-v0_8')
    fig, axes = plt.subplots(3, 1, figsize=(12, 12), sharex=True, gridspec_kw={'hspace': 0.15})

    # Plot Fee
    axes[0].plot(merged_df['date'], merged_df['value_fee'], color='blue', linewidth=1.5)
    axes[0].set_title(f'Fee Over Time', loc='left', fontsize=14, fontweight='bold')
    axes[0].set_ylabel('Fee', fontsize=12)
    axes[0].grid(visible=True, linestyle='--', linewidth=0.5)

    # Plot TVL' with anomalies
    axes[1].plot(merged_df['date'], merged_df['tvl_prime'], color='orange', linewidth=1.5, label="TVL'")
    axes[1].plot(merged_df['date'], merged_df['rolling_mean'], color='purple', linestyle='--', linewidth=1.5, label=f'{window_size}-day Rolling Mean')

    # Highlight anomalies
    anomalies = merged_df[merged_df['is_anomaly']]
    axes[1].scatter(anomalies['date'], anomalies['tvl_prime'], color='red', s=30, edgecolor='black', label='Anomalies', zorder=5)

    axes[1].set_title(f"TVL' Over Time with Anomalies", loc='left', fontsize=14, fontweight='bold')
    axes[1].set_ylabel("TVL'", fontsize=12)
    axes[1].legend(loc='upper left', fontsize=10)
    axes[1].grid(visible=True, linestyle='--', linewidth=0.5)

    # Plot Price
    axes[2].plot(merged_df['date'], merged_df['value_price'], color='green', linewidth=1.5)
    axes[2].set_title(f'Price Over Time', loc='left', fontsize=14, fontweight='bold')
    axes[2].set_xlabel('Date', fontsize=12)
    axes[2].set_ylabel('Price', fontsize=12)
    axes[2].grid(visible=True, linestyle='--', linewidth=0.5)

    # Format x-axis
    for ax in axes:
        ax.tick_params(axis='x', rotation=45, labelsize=10)
        ax.tick_params(axis='y', labelsize=10)

    plt.tight_layout()
    plt.show()