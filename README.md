# CTA_fundamental_strategy
This markdown file is the framework to demonstrate the research process of a fundamental strategy in China commodities. It also shows part of the backtesting results in the metal sector, including copper, aluminium, zinc and nickle.
## Table of Contents
- [I. Overview of Data](#1)
- [II. Backtesting Philosophy](#2)
- [III. Backtesting Results](#3)
- [IV. Return Models](#4)
- [V. Risk Models](#5)
- [VI. Future Work](#6)

<span id="1"></span>
## I. Overview of Data
The data source comes from a database merchant called Mysteel. We selected fundamental data on daily and weekly frequency, which have more than five years‘ historical data. The raw data can be classified in four kinds:
- **Price**: including future prices, spot prices, cost of goods, profits of goods
- **Supply**: including the productivity effenciency, supply quantity
- **Demand**: including the trading volume
- **Inventory**: including the social inventory and exchange‘s warehouse inventory

<span id="2"></span>
## II. Backtesting Philosophy
### Step 1: Data Preparation
  - **Check the data quality**: 
    - Detect the outliers
    - Check the delay frequency of the publish time for the historical data
  - **Data cleaning**:
    - Remove the outliers
    - Trace the real publish time
### Step 2: Construct the Return Models for each commodity
- **Construct Factors**: 
  - POP(period over period)
  - YOY(year over year)
  - Momentum
  - Z-score, Quantile 
- **Calculate Factor Returns** in the historical window from 2015 to 2020.
- **Explore effective factors** that is relative to the daily return of commodities from both the macro-economics and quantitative prospective. 
- **Form the factor infrastructure as follows**. There are three layers in the factor architecture. 
  - The top layer is the style of fundamental data, in the aspects of prices, supply, demand and inventory.
  - The middle layer is the data subjects for each style.
  - The bottom layer is the derived factors of data subjects which are effective in our analysis.

![image](https://github.com/jxin2618/CTA_fundamental_strategy/blob/main/figures/Fundamental_data_infrastructure.png)

- **Factor Combination** by equal-weighting in each layer. Generate signals to predict daily return for each commodity.

### Step 3: Construct the Risk Model for the portfolio
We used Target Volatility Strategy to tune weights on each commodity. The strategy works as follows:
1. First consider Target Volatility for Single commodity. If we set a fixed target volatility for each commodity by leveraging, then the weight of i-th commodity would be $$ w_{t+1}^i = \frac{\sigma_{tgt}}{N\sigma_t^i}$$
2. Second consider the correlation among commodities. The volatility of the portfolio is 
$$\sigma_p = \sqrt{\sum_{i=1}^Nw_i^2\sigma_i^2 + 2\sum_{i=1}^{N}\sum_{j=i+1}^Nw_iw_j\sigma_i\sigma_j\rho_{ij}}$$
3. If the weights of all commodities are adjusted to target volatility, the volatility of the portfolio is 
$$ \sigma_P=\frac{\sigma_{tgt}}{N}\sqrt{N+2\sum_{i=1}^N\sum_{j=i+1}^N\rho_{ij}} $$
4. The average of correlation is:
$$ \bar{\rho}= \frac{2\sum^N_{i=1}\sum^N_{j=i+1}\rho_{ij}}{N(N-1)}$$
5. The equation of volatility becomes:
$$ \sigma_P = \sigma_{tgt}\sqrt{\frac{1+(N-1)\bar{\rho}}{N}} $$
It shows that the diversification makes the volatility of portfolio lower than the volatility of single commodity.
6. Adjust the target volatility of single commodity according to the correlation factors:
$$ \sigma_{tgt} = \sigma_{P, tgt}\sqrt{\frac{N}{1+(N-1)\bar{\rho}}} =  \sigma_{P, tgt} \times CF(\bar{\rho})$$
7. Tune the weight of each commodity by 
$$ w_{t+1}^i = \frac{\sigma_{P,tgt}}{N\sigma_t^i}\sqrt{\frac{N}{1+(N-1)\bar{\rho}}} = \frac{\sigma_{P,tgt}}{N\sigma^i_t}\cdot CF(\bar{\rho}) $$


<span id="3"></span>
## III. Backtesting Results
This section lists some interesting backtesting and analytical results of copper.
### Overview of Copper's supply and demand

![image](https://github.com/jxin2618/CTA_fundamental_strategy/blob/main/figures/cu产业链.png)

### Overview of Copper's fundamental data

![image](https://github.com/jxin2618/CTA_fundamental_strategy/blob/main/figures/cu_fundamental_data.png)

### Results
- **Inventory**
  - Exchanges' Warehouse Inventory
 
  ![image](https://github.com/jxin2618/CTA_fundamental_strategy/blob/main/figures/CU_LME.png)![image](https://github.com/jxin2618/CTA_fundamental_strategy/blob/main/figures/CU_SHFE.png)
   
  ![image](https://github.com/jxin2618/CTA_fundamental_strategy/blob/main/figures/cu_shfe_backtest.png)
   
  - Social Inventory
  
  ![image](https://github.com/jxin2618/CTA_fundamental_strategy/blob/main/figures/sh保税区.png)

  ![image](https://github.com/jxin2618/CTA_fundamental_strategy/blob/main/figures/sh_free_trade_zone_backtest.png)
  
  
### Demand

<span id="4"></span>
## IV. Return Models
### Performance of Single Commodity
1. Copper

 ![image](https://github.com/jxin2618/CTA_fundamental_strategy/blob/main/figures/CU_category.png)
 
 The correlation between the styles of factors is as follows:

|  | price | inventory|
| ------ | ------ | ------ |
| price| 1 | |
| inventory | 0.236 | 1 |


2. Aluminium

![image](https://github.com/jxin2618/CTA_fundamental_strategy/blob/main/figures/AL_category.png)

The correlation among the styles of factors is as follows:

|  | price | inventory| demand |
| ------ | ------ | ------ | ------ |
| price| 1 | | |
| inventory | 0.254 | 1 | |
| demand | 0.026 | 0.097 | 1 |

3. Zinc

![image](https://github.com/jxin2618/CTA_fundamental_strategy/blob/main/figures/ZN_category.png)

The correlation among the styles of factors is as follows:

|  | price | inventory| demand |
| ------ | ------ | ------ | ------ |
| price| 1 | | |
| inventory | -0.036 | 1 | |
| demand | 0.181 | 0.032 | 1 |

4. Nickle

![image](https://github.com/jxin2618/CTA_fundamental_strategy/blob/main/figures/NI_category.png)

The correlation between the styles of factors is as follows:

|  | price | inventory| 
| ------ | ------ | ------ | 
| price| 1 | | 
| inventory | 0.010 | 1 | 


<span id="5"></span>
## V. Risk Models
### Performance of Portfolio
- Performance of each commodity: before target volatility strategy

![image](https://github.com/jxin2618/CTA_fundamental_strategy/blob/main/figures/options_nav.png)

- Performance of each commodity: after target volatility strategy

![image](https://github.com/jxin2618/CTA_fundamental_strategy/blob/main/figures/options_nav_after_tvs_0.05.png)

- Performance of strategy

![image](https://github.com/jxin2618/CTA_fundamental_strategy/blob/main/figures/after_tvs_5pct.png)

- Historical Performance

![image](https://github.com/jxin2618/CTA_fundamental_strategy/blob/main/figures/hist_performance.png)


- Seasonality of the portfolio

![image](https://github.com/jxin2618/CTA_fundamental_strategy/blob/main/figures/seasonal_effect_0.05.png)

<span id="6"></span>
## VI. Future Work
1. Weighting method
2. Crosssectionally
