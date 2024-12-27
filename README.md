<!--
 * @Author: caoyuwei e1349149@u.nus.edu
 * @Date: 2024-12-27 14:45:04
 * @LastEditors: caoyuwei e1349149@u.nus.edu
 * @LastEditTime: 2024-12-27 17:40:45
 * @FilePath: \Project\README.md
-->
# 项目名称

三个小小项目，主要目的是：实现一些数据可视化的dashboard demo；静态数据对链上数据处理；ethersacan api使用以及实现记账簿功能

## 项目结构

Project  
├── dashboard/  
│   ├── dashboard.ipynb  
│   ├── defiLlamaAPI.py  
│   └── demo.ipynb  
├── etherscan/  
│   ├── result/  
│   │   └── filtered_transaction_data.csv  
│   ├── cal_PnL.ipynb  
│   ├── data_fetch.ipynb  
│   ├── data_process.ipynb  
│   ├── etherscan_example.ipynb  
│   ├── etherscan_functions.py  
│   ├── fetch_process_function.py  
│   └── moralis.ipynb  
├── stable_dashboard/  
│   ├── data/  
│   │   └── results/  
│   │       ├── data_fetch.py  
│   │       ├── plot_functions.py  
│   └── __init__.py  
├── README.md  

## 代码细节

### dashboard  

* dashboard.ipynb 是dashboard的一个初步实现，可以实现一部分的交互展示数据，在tvl，volume和fee中选择展示。使用DeFiLlama API获取数据，展示的时间范围是当天以及120天前之间的数据。
* defiLlamaAPI.py [作者源码在这里]([URL](https://github.com/AdamGetbags/defiLlamaAPI) "源码") 对defiLlama api使用的例子，可以作为参考。
* demo.ipynb 一个用来debug的测试文件，可以观察api获取的数据，用这个发现了bitcoin数据不完整的问题。

### stable_dashboard

* data 存储静态数据，包括fee,price,tvl和volume
* result 分为combined_charts和z_score_charts，分别存储了tvl top10的fee、tvl、volume合并图；以及zscore可视化异常点的一些测试图。
* data_fetch.ipynb 获取数据的代码，运行可以自动更新data存储为到今天为止共两年的数据（包括fee,price,tvl和volume），代码中的Notice有获取数据的注意要点（Bitcoin有缺失，Polygon和Tron是使用cryptocompare api单独获取的）。获取链的范围是按照当前tvl top 10的链。
* plot_functions.py 绘制图像时需要的函数集合。
* plot_chains.ipynb 最终可视化代码，`data_dict_all` 存储所有从本地读取的数据，直接给函数即可  
`plot_three_metrics_all(data_dict_all, chain_name='Bitcoin', window_size=60, threshold=2)`  
其中`chain_name`是指定想要可视化链的名字，`window_size`以及`threshold`是zscore计算的相关参数。调用这个函数可以看到三个图合并，如：  
![Bitcoin](C:\Users\YuweiCao\Documents\GitHub\Project\Project\stable_dashboard\results\z_score_charts\Bitcoin_3.png "Bitcoin")  
`plot_metric_chart(data_dict_all, 'Bitcoin', 'price')`  
其中`chain_name`是指定想要可视化链的名字，`price`是 the metric to plot (fee, tvl, price)。可视化单个元素，便于观察。主要是为了观察Bitcoin具体price判断是否有问题。如：  
![Bitcoin](C:\Users\YuweiCao\Documents\GitHub\Project\Project\stable_dashboard\results\z_score_charts\bitcoin_price.png "Bitcoin")  

### etherscan

* result 存储交易数据。all_erc20_transfers.csv是trump对应地址所有数据；erc20_transfers.csv是每次运行代码时存储的结果；filtered_transaction_data.csv是对数据的列进行了一些选取方便比对数据，也涵盖所有的交易；from_to_address_count.csv是对于交易地址的计数。
* moralis.ipynb 对moralis api使用的几个例子，测试了一下api获取数据的限制。
* etherscan_functions.py 和 etherscan_examples.ipynb 分别是对etherscan api的基础函数封装以及基础调用示例。优先使用的函数放在etherscan_functions.py的前面。
* data_fetch.ipynb和data_process.ipynb是获取（所有的）交易数据以及处理的一个流程。其中处理包括：
  * 计算除以TokenDecimal计算ActualValue。
  * 判断是否有三条同hash数据，判断是否为一二三类交易。
    * 一二三类交易定义为：BUY/SELL:同hash一进一出，其中之一为`BASE_TOKENS`；"BUY"/"SELL":不同hash相邻一进一出，token其中之一为`BASE_TOKENS`；single BUY/SELL第三类交易定义为其余单独的进/出。
* fetch_process_function.py 存储data_fetch.ipynb和data_process.ipynb中的函数，便于后面可以给参数直接调用。
* cal_PnL.ipynb 调用接口，提供`ADDRESS,START_DATE,END_DATE,OUTPUT_FILE,API_KEY,BASE_TOKENS`参数，`fetch_and_save_erc20_transfers`函数可以获取保存数据；`process_transactions`判断交易类型；并且进行初步PnL计算（还有问题）；最后一段代码对交易地址进行计数。
  * 调用逻辑是cal_PnL.ipynb <-- fetch_process_function.py <-- etherscan_functions.py

## 存在问题

1.对于交易类型的判断还有不少问题，第一类型没问题，但是二三有问题。这就导致计算pnl的时候只有第一类是正确的，一旦加上第二类就会有错误。  
2.交易地址判断。交易地址如何判断是不是交易所呢，频繁交易地址目前只有三个（包括自己的）。