# CTA_fundamental_strategy
This markdown file is the framework to demonstrate the research process of a fundamental strategy in China commodities. It also shows part of the backtesting results in the metal sector, including copper, aluminium, zinc and nickle.
## Overview of Data
The data source comes from a database merchant called Mysteel. We selected fundamental data on daily and weekly frequency, which have more than five years historical data. The raw data can be classified in four kinds:
- Price: including future prices, spot prices, cost of goods, profits of goods
- Supply: including the productivity effenciency, supply quantity
- Demand: including the trading volume
- Inventory: including the social inventory and exchange‘s warehouse inventory

## Backtesting Philosophy
### Step 1: Data Preparation
  - Check the data quality: 
    - Detect the outliers
    - Check the delay frequency of the historical data
  - Data cleaning:
    - Remove the outliers
    - Trace the real publish time
### Step 2: Construct the Return Models for each commodity
- Construct Factors:

![image](https://github.com/jxin2618/CTA_fundamental_strategy/blob/main/figures/Fundamental_data_infrastructure.png)


### Step 3: Construct the Risk Model for the portfolio

## Backtesting Results
This section lists some interesting backtesting and analytical results of copper.
### Overview of Copper's supply and demand

![image](https://github.com/jxin2618/CTA_fundamental_strategy/blob/main/figures/cu产业链.png)

### Overview of Copper's fundamental data

![image](https://github.com/jxin2618/CTA_fundamental_strategy/blob/main/figures/cu_fundamental_data.png)

### The 
### Price
### Inventory
### Demand

## Return Models
### Performance of Single Commodity
1. Copper

![image](https://github.com/jxin2618/CTA_fundamental_strategy/blob/main/figures/CU_category.png)

2. Aluminium

![image](https://github.com/jxin2618/CTA_fundamental_strategy/blob/main/figures/AL_category.png)

3. Zinc

![image](https://github.com/jxin2618/CTA_fundamental_strategy/blob/main/figures/ZN_category.png)

4. Nickle

![image](https://github.com/jxin2618/CTA_fundamental_strategy/blob/main/figures/NI_category.png)

## Risk Models
### Performance of Portfolio
- performance of each commodity: before target volatility strategy

![image](https://github.com/jxin2618/CTA_fundamental_strategy/blob/main/figures/options_nav.png)

- performance of each commodity: after target volatility strategy

![image](https://github.com/jxin2618/CTA_fundamental_strategy/blob/main/figures/options_nav_after_tvs_0.05.png)

- performance of strategy

![image](https://github.com/jxin2618/CTA_fundamental_strategy/blob/main/figures/after_tvs_5pct.png)

- Seasonality of the portfolio

![image](https://github.com/jxin2618/CTA_fundamental_strategy/blob/main/figures/seasonal_effect_0.05.png)

## Future Work
1. Weighting method
2. Crosssectionally
