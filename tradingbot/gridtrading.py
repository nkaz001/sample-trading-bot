import logging
import sys

import pandas as pd
from numpy import floor, ceil

from tradingbot.ordermanager import OrderManager


class CustomOrderManager(OrderManager):
    """A sample order manager for implementing your own custom strategy"""

    async def place_orders(self):
        # implement your custom strategy here
        order_qty_dollar = 50
        half_spread = 3.588029293964708
        price_range = 158.10344098886486
        grid_num = 20  # 60
        tick_size = 0.1
        tick_ub = 100000
        order_interval = 2 * price_range / grid_num
        interval_tick = int(round(order_interval / tick_size))
        max_position = 5000

        market_depth = self.binance_futures.depth
        bid = map(lambda x: (float(x[0]), x[1]), sorted(filter(lambda x: x[1] > 0, market_depth.items()), key=lambda x: -float(x[0])))
        ask = map(lambda x: (float(x[0]), x[1]), sorted(filter(lambda x: x[1] < 0, market_depth.items()), key=lambda x: float(x[0])))
        bid = pd.DataFrame(bid, columns=['price', 'size'])
        ask = pd.DataFrame(ask, columns=['price', 'size'])
        if len(bid) == 0 or len(ask) == 0:
            return
        mid = (bid['price'][0] + ask['price'][0]) / 2.0

        bid_order_begin = min(mid - half_spread, bid['price'][0])
        ask_order_begin = max(mid + half_spread, ask['price'][0])
        lb_price = mid - price_range
        ub_price = mid + price_range

        x = float(self.binance_futures.running_qty) * mid / max_position

        if x <= 1:
            max_bid_order_tick = int(round(bid_order_begin / tick_size))
            max_bid_order_tick = int(floor(max_bid_order_tick / interval_tick))
            min_bid_order_tick = int(floor(lb_price / tick_size / interval_tick))
        else:
            # Cancel all bid orders if the position exceeds the maximum position.
            min_bid_order_tick = max_bid_order_tick = 0
        if x >= -1:
            min_ask_order_tick = int(round(ask_order_begin / tick_size))
            min_ask_order_tick = int(ceil(min_ask_order_tick / interval_tick))
            max_ask_order_tick = int(ceil(ub_price / tick_size / interval_tick))
        else:
            # Cancel all ask orders if the position exceeds the maximum position.
            min_ask_order_tick = max_ask_order_tick = tick_ub - 1

        buy_orders = []
        sell_orders = []
        for tick in range(max_bid_order_tick, min_bid_order_tick, -1):
            bid_price_tick = tick * interval_tick
            bid_price = bid_price_tick * tick_size
            order_qty = round(order_qty_dollar / bid_price, 3)
            buy_orders.append({'price': '%1.f' % bid_price, 'quantity': order_qty, 'side': "Buy"})
        for tick in range(min_ask_order_tick, max_ask_order_tick, 1):
            ask_price_tick = tick * interval_tick
            ask_price = ask_price_tick * tick_size
            order_qty = round(order_qty_dollar / ask_price, 3)
            sell_orders.append({'price': '%1.f' % ask_price, 'quantity': order_qty, 'side': "Sell"})

        try:
            logging.info('mid=%.1f, running_qty%%=%f, buy_orders=%s, sell_orders=%s', mid, x, buy_orders, sell_orders)
            await self.converge_orders(buy_orders, sell_orders)
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
