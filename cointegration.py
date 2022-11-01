import pandas as pd
from statsmodels.tsa.stattools import coint


# get data from sql database
returns = pd.read_sql('price_table', 'sqlite:///price_data.db')


# Pairs in blacklist will not be tested, low volume tickers added
blacklist = ['BUSD', 'AUCTION', 'STMX', 'BAND', 'TOMO', 'FLM', 'SPELL', 'JASMY', 'RAYUSDT', 'FLOW', 'ROSE', 'XEM',
             'OCEAN', 'FILUSDT', 'HOTUSDT', 'SFPUSDT', 'NKNUSDT']
for i in blacklist:
    for j in returns.columns:
        if i in j:
            returns = returns.drop(j, axis=1)


# find cointegrated pairs
def cointegrated_pairs(data):
    """
    Test all pairs of dataframe for cointegration

    :param data: dataframe with symbols as column name and price data (float) as rows

    :return: pairs_list: list of pairs that are cointegrated
             Eg: [('BTCUSDT', 'XMRUSDT), ('ETHUSDT', 'DOTUSDT'), ('BTCUSDT', 'XRPUSDT')]
    """

    n = data.shape[1]

    keys = data.keys()

    pairs_list = []

    # test pair for cointegration with all pairs after them
    for i in range(n):
        for j in range(i + 1, n):

            s1 = data[keys[i]]
            s2 = data[keys[j]]

            # cointegration test
            score, pvalue, crit_dict = coint(s1, s2)

            # store pairs if p-value < 0.05
            if pvalue < 0.05:
                pairs_list.append((keys[i], keys[j]))

    return pairs_list


pairs = cointegrated_pairs(returns)

print(pairs)
