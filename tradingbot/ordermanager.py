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

        to_create = []
        to_cancel = []
        buys_matched = 0
        sells_matched = 0
        existing_orders = await self.binance_futures.open_orders()

        for order in buy_orders:
            order['price'] = str(order['price'])
            order['quantity'] = str(round_down(order['quantity'], 3))
        for order in sell_orders:
            order['price'] = str(order['price'])
            order['quantity'] = str(round_down(order['quantity'], 3))

        # Check all existing orders and match them up with what we want to place.
        # If there's an open one, we might be able to amend it to fit what we want.
        for order in existing_orders:
            try:
                if order['side'] == 'BUY':
                    desired_order = buy_orders[buys_matched]
                    buys_matched += 1
                else:
                    desired_order = sell_orders[sells_matched]
                    sells_matched += 1

                # Found an existing order. Do we need to amend it?
                if desired_order['quantity'] != order['origQty'] or (
                        # If price has changed, and the change is more than our RELIST_INTERVAL, amend.
                        desired_order['price'] != order['price'] and
                        abs((float(desired_order['price']) / float(order['price'])) - 1) > settings.RELIST_INTERVAL):
                    to_cancel.append(order)
                    to_create.append({'price': desired_order['price'], 'quantity': desired_order['quantity'], 'side': order['side']})
            except IndexError:
                # Will throw if there isn't a desired order to match. In that case, cancel it.
                to_cancel.append(order)

        while buys_matched < len(buy_orders):
            to_create.append(buy_orders[buys_matched])
            buys_matched += 1

        while sells_matched < len(sell_orders):
            to_create.append(sell_orders[sells_matched])
            sells_matched += 1

        if len(to_create) > 0:
            logging.info("Creating %d orders:" % (len(to_create)))
            for order in reversed(to_create):
                logging.info("%4s %s @ %s" % (order['side'], order['quantity'], order['price']))
            response = await self.binance_futures.create_bulk_orders(to_create)
            logging.debug(response)

        # Could happen if we exceed a delta limit
        if len(to_cancel) > 0:
            logging.info("Canceling %d orders:" % (len(to_cancel)))
            for order in reversed(to_cancel):
                logging.info("%4s %s @ %s" % (order['side'], order['origQty'], order['price']))
            response = await self.binance_futures.cancel_bulk_orders([x['orderId'] for x in to_cancel])
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
                    sys.stdout.write("-----\n")
                    sys.stdout.flush()

                    # self.check_file_change()

                    await asyncio.sleep(settings.LOOP_INTERVAL)
                    await self.place_orders()

            async def stop():
                self.run = False
                await self.binance_futures.close()
                ioloop.stop()

            def signal_handler():
                ioloop.create_task(stop())

            try:
                ioloop.add_signal_handler(signal.SIGTERM, signal_handler)
                ioloop.add_signal_handler(signal.SIGINT, signal_handler)
            except NotImplementedError:
                # add_signal_handler supports only Linux-compatible OS.
                pass
            ioloop.run_until_complete(start())
        except RuntimeError as e:
            if e.args[0] != 'Event loop stopped before Future completed.':
                raise e
        finally:
            ioloop.close()
