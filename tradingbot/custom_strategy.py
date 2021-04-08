import logging
import sys

import pandas as pd

from tradingbot.ordermanager import OrderManager


class CustomOrderManager(OrderManager):
    """A sample order manager for implementing your own custom strategy"""

    async def place_orders(self):
        # implement your custom strategy here
        order_qty_dollar = 100
        threshold = 1000  # need to find an optimal value
        depth = 0.05  # need to find an optimal value

        market_depth = self.binance_futures.depth
        bid = map(lambda x: (float(x[0]), x[1]), sorted(filter(lambda x: x[1] > 0, market_depth.items()), key=lambda x: -float(x[0])))
        ask = map(lambda x: (float(x[0]), x[1]), sorted(filter(lambda x: x[1] < 0, market_depth.items()), key=lambda x: float(x[0])))
        bid = pd.DataFrame(bid, columns=['price', 'size'])
        ask = pd.DataFrame(ask, columns=['price', 'size'])
        mid = (bid['price'][0] + ask['price'][0]) / 2.0

        buy = bid[bid['price'] > mid * (1 - depth)]['size'].sum()
        sell = ask[ask['price'] > mid * (1 + depth)]['size'].sum()
        alpha = buy - sell

        buy_orders = []
        sell_orders = []

        try:
            order_qty = order_qty_dollar / float(self.binance_futures.last_price)

            if alpha > threshold and not self.long_position_limit_exceeded():
                buy_orders.append({'price': bid['price'][0], 'quantity': order_qty, 'side': "Buy"})
            if alpha < -threshold and not self.short_position_limit_exceeded():
                sell_orders.append({'price': ask['price'][0], 'quantity': order_qty, 'side': "Sell"})

            await self.converge_orders(buy_orders, sell_orders)
        except ZeroDivisionError:
            # Skip until receiving the last price in WS.
            pass
        except:
            logging.warning('Order error.', exc_info=True)


def run():
    order_manager = CustomOrderManager()

    # Try/except just keeps ctrl-c from printing an ugly stacktrace
    try:
        order_manager.run_loop()
    except (KeyboardInterrupt, SystemExit):
        sys.exit()

run()
