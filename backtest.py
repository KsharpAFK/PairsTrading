import pandas as pd
import statsmodels.api as sm
from statsmodels.regression.rolling import RollingOLS
from multiprocessing import Process
from cointegration import pairs

returns = pd.read_sql('price_table', 'sqlite:///price_data.db')


def split_list(pairlist):
    """
    Split a list into four parts

    :param pairlist: list of n elements
    :return: list1, list2, list3, list4

    Usage: pair_list_1, pair_list_2, pair_list_3, pair_list_4 = split_list(pairs)
    """

    n = int(len(pairlist) // 4)

    return pairlist[0:n], pairlist[n:(n * 2)], pairlist[(n * 2):(n * 3)], pairlist[(n * 3):]


def backtest_calc(df, symbol_list):
    pnl = 0
    pair1_entry = 0
    pair2_entry = 0
    level = 0
    for index in range(1, df.shape[0]):

        pair1_current = float(df[symbol_list[0]][index])
        pair2_current = float(df[symbol_list[1]][index])

        # zscore = 0 , close all pos
        if df['zscore'][index - 1] >= 0 >= df['zscore'][index] \
                or df['zscore'][index - 1] <= 0 <= df['zscore'][index] \
                or index == df.shape[1] - 1:

            if level < 0:  # close short pair1, close long pair2
                pnl += (pair2_current - pair2_entry) / pair2_entry \
                       - (pair1_current - pair1_entry) / pair1_entry
                level = 0
                pair1_entry = 0
                pair2_entry = 0

            elif level > 0:  # close long pair1, close short pair2
                pnl += (pair1_current - pair1_entry) / pair1_entry \
                       - (pair2_current - pair2_entry) / pair2_entry
                level = 0
                pair1_entry = 0
                pair2_entry = 0

            else:
                continue

        # short pair1, long pair2
        # zscore <= -3
        if df['zscore'][index - 1] >= -3 >= df['zscore'][index] and level != -3:
            pair1_entry = (pair1_current + pair1_entry) / 2
            pair2_entry = (pair2_current + pair2_entry) / 2
            level = -3

        # long btc, short xmr
        # zscore >= 3
        if df['zscore'][index - 1] <= 3 <= df['zscore'][index] and level != 3:
            pair1_entry = (pair1_current + pair1_entry) / 2
            pair2_entry = (pair2_current + pair2_entry) / 2
            level = 3

    return pnl * 100


def backtest(pair_list):
    """
    Backtest pairs trading strategy on list of pairs. Store results in a csv file
    :param pair_list: list of pairs which are cointegrated
    :return: df_pnl: dataframe of two columns -> pairs, returns
    """
    backtest_result = []
    for tup in pair_list:
        symbol_list = list(tup)
        price_data = returns[symbol_list].astype(float)

        s1 = price_data[symbol_list[0]]
        s2 = price_data[symbol_list[1]]

        # calculate spread

        s1 = sm.add_constant(s1)
        results = RollingOLS(s2, s1, window=50).fit()
        s1 = s1[symbol_list[0]]
        b = results.params[symbol_list[0]]
        spread = s2 - b * s1

        price_data['spread'] = spread
        price_data['spread'] = price_data['spread'].fillna(0)

        spread_mavg1 = spread.rolling(1).mean()  # 1 MA of spread
        spread_mavg30 = spread.rolling(50).mean()  # 50 MA
        std_50 = spread.rolling(50).std()  # rolling 50 standard deviation

        # Compute z score
        zscore_50_1 = (spread_mavg1 - spread_mavg30) / std_50

        price_data['zscore'] = zscore_50_1
        price_data['zscore'] = price_data['zscore'].fillna(0)

        pnl = backtest_calc(price_data, symbol_list)
        temp_list = (symbol_list, pnl)
        backtest_result.append(temp_list)

    df_pnl = pd.DataFrame(backtest_result, columns=['pairs', 'returns'])

    return df_pnl


# Divide pair list in four parts for multiprocessing
pairs_1, pairs_2, pairs_3, pairs_4 = split_list(pairs)


def process_1():

    pnl_1 = backtest(pairs_1)
    pnl_1.to_csv('pairs_pnl_1.csv')


def process_2():

    pnl_2 = backtest(pairs_2)
    pnl_2.to_csv('pairs_pnl_2.csv')


def process_3():

    pnl_3 = backtest(pairs_3)
    pnl_3.to_csv('pairs_pnl_3.csv')


def process_4():

    pnl_4 = backtest(pairs_4)
    pnl_4.to_csv('pairs_pnl_4.csv')


if __name__ == '__main__':

    p1 = Process(target=process_1)
    p1.start()
    p2 = Process(target=process_2)
    p2.start()
    p3 = Process(target=process_3)
    p3.start()
    p4 = Process(target=process_4)
    p4.start()

    p1.join()
    p2.join()
    p3.join()
    p4.join()
