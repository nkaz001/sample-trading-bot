import asyncio
import logging
import math
import os
import signal
import sys
from os.path import getmtime

from tradingbot import settings
from tradingbot.binancefutures import BinanceFutures

# Used for reloading the bot - saves modified times of key files
import os
watched_files_mtimes = [(f, getmtime(f)) for f in settings.WATCHED_FILES]


def round_down(n, decimals=0):
    multiplier = 10 ** decimals
    return math.floor(n * multiplier) / multiplier


class OrderManager:

    async def place_orders(self):
        pass

    def restart(self):
        logging.info("Restarting the tradingbot...")
        os.execv(sys.executable, [sys.executable] + sys.argv)

    async def converge_orders(self, buy_orders, sell_orders):
        """Converge the orders we currently have in the book with what we want to be in the book.
           This involves amending any open orders and creating new ones if any have filled completely.
           We start from the closest orders outward."""

        # existing_orders = await self.binance_futures.open_orders()
        # assert len(existing_orders) == len(self.binance_futures.open_orders_ws)
        # matched = 0
        # for order in existing_orders:
        #     for ws_order in self.binance_futures.open_orders_ws.values():
        #         if order['clientOrderId'] == ws_order['clientOrderId']:
        #             matched += 1
        #             break
        # assert matched == len(existing_orders)
        existing_orders = self.binance_futures.open_orders_ws.values()

        for order in buy_orders:
            order['price'] = str(order['price'])
            order['quantity'] = str(round_down(order['quantity'], 3))
        for order in sell_orders:
            order['price'] = str(order['price'])
            order['quantity'] = str(round_down(order['quantity'], 3))

        # Check all existing orders and match them up with what we want to place.
        # If there's an open one, we might be able to amend it to fit what we want.
        for order in existing_orders:
            order["keep"] = False
            if order['side'] == 'BUY':
                for i, buy_order in enumerate(buy_orders):
                    if buy_order['price'] == order['price'] and abs((float(buy_order['price']) / float(order['price'])) - 1) <= settings.RELIST_INTERVAL:
                        # and buy_order['quantity'] == order['origQty']
                        order["keep"] = True
                        del buy_orders[i]
                        break
            else:
                for i, sell_order in enumerate(sell_orders):
                    if sell_order['price'] == order['price'] and abs((float(sell_order['price']) / float(order['price'])) - 1) <= settings.RELIST_INTERVAL:
                        # and sell_order['quantity'] == order['origQty']
                        order["keep"] = True
                        del sell_orders[i]
                        break
        to_cancel = [order for order in existing_orders if not order.get("keep")]
        to_create = [{'price': order['price'], 'quantity': order['quantity'], 'side': 'BUY'} for order in buy_orders] + \
            [{'price': order['price'], 'quantity': order['quantity'], 'side': 'SELL'} for order in sell_orders]

        # Could happen if we exceed a delta limit
        if len(to_cancel) > 0:
            logging.info("Canceling %d orders:" % (len(to_cancel)))
            for order in reversed(to_cancel):
                logging.info("%4s %s @ %s" % (order['side'], order['origQty'], order['price']))
            cancel_task = []
            while to_cancel:
                to_cancel_bulk = []
                while to_cancel and len(to_cancel_bulk) < 5:
                    to_cancel_bulk.append(to_cancel.pop(0))
                cancel_task.append(self.binance_futures.cancel_bulk_orders([x['orderId'] for x in to_cancel_bulk]))
            response = await asyncio.gather(*cancel_task)
            logging.debug(response)

        if len(to_create) > 0:
            logging.info("Creating %d orders:" % (len(to_create)))
            for order in reversed(to_create):
                logging.info("%4s %s @ %s" % (order['side'], order['quantity'], order['price']))
            create_task = []
            while to_create:
                to_create_bulk = []
                while to_create and len(to_create_bulk) < 5:
                    to_create_bulk.append(to_create.pop(0))
                create_task.append(self.binance_futures.create_bulk_orders(to_create_bulk))
            response = await asyncio.gather(*create_task)
            logging.debug(response)

    ###
    # Position Limits
    ###

    def short_position_limit_exceeded(self):
        """Returns True if the short position limit is exceeded"""
        if not settings.CHECK_POSITION_LIMITS:
            return False
        position = float(self.binance_futures.running_qty)
        return position <= settings.MIN_POSITION / float(self.binance_futures.last_price)

    def long_position_limit_exceeded(self):
        """Returns True if the long position limit is exceeded"""
        if not settings.CHECK_POSITION_LIMITS:
            return False
        position = float(self.binance_futures.running_qty)
        return position >= settings.MAX_POSITION / float(self.binance_futures.last_price)

    ###
    # Running
    ###

    def check_file_change(self):
        """Restart if any files we're watching have changed."""
        for f, mtime in watched_files_mtimes:
            if getmtime(f) > mtime:
                self.restart()

    def run_loop(self):
        logging.basicConfig(level=settings.LOG_LEVEL)
        ioloop = asyncio.get_event_loop()

        try:
            self.binance_futures = BinanceFutures(settings.API_KEY, settings.API_SECRET, settings.SYMBOL, settings.TESTNET)
            self.run = True

            async def start():
                asyncio.create_task(self.binance_futures.connect())
                while self.run:
                    # sys.stdout.write("-----\n")
                    # sys.stdout.flush()
                    # self.check_file_change()

                    await asyncio.sleep(settings.LOOP_INTERVAL)
                    await self.place_orders()

            async def stop():
                await self.binance_futures.close()
                self.run = False
                ioloop.stop()

            def signal_handler():
                ioloop.create_task(stop())

            ioloop.add_signal_handler(signal.SIGTERM, signal_handler)
            ioloop.add_signal_handler(signal.SIGINT, signal_handler)
            ioloop.run_until_complete(start())
        except RuntimeError as e:
            if e.args[0] != 'Event loop stopped before Future completed.':
                raise e
        finally:
            ioloop.close()
