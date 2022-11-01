import asyncio
import pandas as pd
from binance import AsyncClient, BinanceSocketManager
import sqlalchemy
from config import api_key, api_secret


# create sqlite database
engine = sqlalchemy.create_engine("sqlite:///price_data.db")


async def main():

    client = await AsyncClient.create(api_key=api_key, api_secret=api_secret)
    bm = BinanceSocketManager(client)

    # start any sockets here
    price_socket = bm.all_mark_price_socket()

    # receive messages
    async with price_socket as mpsm:
        while True:
            res = await mpsm.recv()

            price = [sub['p'] for sub in res['data']]  # tick data
            symbol_list = [sub['s'] for sub in res['data']]  # tickers

            pricedf = pd.DataFrame(price).T
            pricedf.columns = symbol_list

            # store value in database
            pricedf.to_sql('price_table', engine, if_exists='append', index=False)


if __name__ == "__main__":

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
