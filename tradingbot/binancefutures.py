import asyncio
import base64
import hashlib
import hmac
import json
import logging
import time
import urllib.parse
import uuid

import aiohttp
from aiohttp import ClientSession, WSMsgType
from yarl import URL


class BinanceFutures:
    def __init__(self, api_key, api_secret, symbol='btcusdt', testnet=True, orderIDPrefix='bot_bf_', postOnly=False, timeout=10):
        self.api_key = api_key
        self.api_secret = api_secret
        self.symbol = symbol
        self.client = aiohttp.ClientSession(headers={ 'Content-Type': 'application/json' })
        self.closed = False
        self.depth = {}
        self.pending_messages = None
        self.prev_u = None
        self.testnet = testnet
        self.timeout = timeout
        self.postOnly = postOnly
        self.orderIDPrefix = orderIDPrefix
        self.last_price = 0
        self.open_orders_ws = {}

    def open_orders_active(self):
        return {order_id: order for order_id, order in self.open_orders_ws.items() if order['status'] in ['PENDING_NEW', 'NEW', 'PARTIALLY_FILLED']}

    async def __on_message(self, message):
        message = json.loads(message)
        logging.debug(message)
        stream = message['stream']
        if self.listen_key == stream:
            data = message['data']
            e = data['e']
            if e == 'listenKeyExpired':
                logging.warning('Listen key is expired.')
                await self.ws.close()
            elif e == 'ACCOUNT_UPDATE':
                account = data['a']
                positions = account['P']
                for position in positions:
                    position_side = position['ps']
                    if 'BOTH' == position_side:
                        if position['s'].upper() == self.symbol.upper():
                            self.running_qty = position['pa']
            elif e == 'ORDER_TRADE_UPDATE':
                timestamp = data['E']
                order = data['o']
                order_ = {
                    'symbol': order['s'],
                    'clientOrderId': order['c'],
                    'side': order['S'],
                    'origQty': order['q'],
                    'price': order['p'],
                    'status': order['X'],
                    'orderId': order['i'],
                    'executedQty': order['l'],
                    'cumQty': order['z'],
                    'updateTime': order['T']
                }
                existing_order = self.open_orders_ws.setdefault(order['c'], order_)
                if 'updateTime' not in existing_order or existing_order['updateTime'] < order_['updateTime']:
                    existing_order.update(order_)
                now = time.time()
                for order_id, order in list(self.open_orders_ws.items()):
                    if order['status'] not in ['PENDING_NEW', 'NEW', 'PARTIALLY_FILLED'] \
                            and order['updateTime'] < now - 300:
                        del self.open_orders_ws[order_id]
        elif stream == '%s@depth@0ms' % self.symbol:
            data = message['data']
            u = data['u']
            pu = data['pu']
            if self.prev_u is None or pu != self.prev_u:
                if self.pending_messages is None:
                    logging.warning('Mismatch on the book. prev_update_id=%s, pu=%s' % (self.prev_u, pu))
                    asyncio.create_task(self.__get_marketdepth_snapshot())
                    self.pending_messages = []
                self.pending_messages.append(data)
                return
            for price, qty in data['b']:
                if qty == '0':
                    del self.depth[price]
                else:
                    self.depth[price] = float(qty)
            for price, qty in data['a']:
                if qty == '0':
                    del self.depth[price]
                else:
                    self.depth[price] = -float(qty)
            self.prev_u = u
        elif stream == '%s@aggTrade' % self.symbol:
            data = message['data']
            price = data['p']
            qty = data['q']
            self.last_price = price
            self.last_qty = qty

    async def __keep_alive(self):
        while not self.closed:
            try:
                await asyncio.sleep(5)
                await self.keepalive_user_data_stream()
                await self.ws.pong()
            except:
                pass

    async def __curl_binancefutures(self, path, query=None, timeout=None, verb=None, rethrow_errors=True, max_retries=None):
        if timeout is None:
            timeout = self.timeout

        # Default to POST if data is attached, GET otherwise
        if not verb:
            verb = 'POST' if query else 'GET'

        # By default don't retry POST or PUT. Retrying GET/DELETE is okay because they are idempotent.
        # In the future we could allow retrying PUT, so long as 'leavesQty' is not used (not idempotent),
        # or you could change the clOrdID (set {"clOrdID": "new", "origClOrdID": "old"}) so that an amend
        # can't erroneously be applied twice.
        if max_retries is None:
            max_retries = 0 if verb in ['POST', 'PUT'] else 3

        if query is None:
            query = {}
        query['timestamp'] = str(int(time.time() * 1000) - 1000)
        query = urllib.parse.urlencode(query)
        query = query.replace('%27', '%22')
        signature = hmac.new(self.api_secret.encode('utf-8'), query.encode('utf-8'), hashlib.sha256).hexdigest()

        def exit_or_throw(e):
            if rethrow_errors:
                raise e
            else:
                exit(1)

        def retry():
            self.retries += 1
            if self.retries > max_retries:
                raise Exception("Max retries on %s (%s) hit, raising." % (path, json.dumps(query or '')))
            return self.__curl_binancefutures(path, query, timeout, verb, rethrow_errors, max_retries)

        # Make the request
        try:
            if self.testnet:
                url = URL('https://testnet.binancefuture.com/fapi%s?%s&signature=%s' % (path, query, signature), encoded=True)
            else:
                url = URL('https://fapi.binance.com/fapi%s?%s&signature=%s' % (path, query, signature), encoded=True)
            logging.debug("sending req to %s: %s" % (url, json.dumps(query or query or '')))
            response = await self.client.request(verb, url, headers={'X-MBX-APIKEY': self.api_key}, timeout=timeout)
            # Make non-200s throw
            response.raise_for_status()

        except aiohttp.ClientResponseError as e:
            # 401 - Auth error. This is fatal.
            if e.status == 401:
                logging.error("API Key or Secret incorrect, please check and restart.")
                logging.error("Error: " + e.message)
                if query:
                    logging.error(query)
                # Always exit, even if rethrow_errors, because this is fatal
                exit(1)

            # 404, can be thrown if order canceled or does not exist.
            # elif e.status == 404:
            #     if verb == 'DELETE':
            #         logging.error("Order not found: %s" % query['orderId'])
            #         return
            #     logging.error("Unable to contact the Binance Futures API (404). " + "Request: %s \n %s" % (url, json.dumps(query)))
            #     exit_or_throw(e)

            # 429, ratelimit; cancel orders & wait until X-RateLimit-Reset
            elif e.status == 429:
                logging.error("Ratelimited on current request. Sleeping, then trying again. Try fewer " + "Request: %s \n %s" % (url, json.dumps(query)))
                logging.warning("Canceling all known orders in the meantime.")

                await self.cancel_all_orders()

                #logging.error("Your ratelimit will reset at %s. Sleeping for %d seconds." % (reset_str, to_sleep))
                to_sleep = 5
                logging.error("Sleeping for %d seconds." % (to_sleep))
                time.sleep(to_sleep)

                # Retry the request.
                return await retry()

            elif e.status == 502:
                logging.warning("Unable to contact the Binance Futures API (502), retrying. " + "Request: %s \n %s" % (url, json.dumps(query)))
                await asyncio.sleep(3)
                return await retry()

            # 503 - Binance Futures temporary downtime, likely due to a deploy. Try again
            elif e.status == 503:
                logging.warning("Unable to contact the Binance Futures API (503), retrying. " + "Request: %s \n %s" % (url, json.dumps(query)))
                await asyncio.sleep(3)
                return await retry()

            elif e.status == 400:
                pass
            # If we haven't returned or re-raised yet, we get here.
            logging.error("Unhandled Error: %s: %s" % (e, e.message))
            logging.error("Endpoint was: %s %s: %s" % (verb, path, json.dumps(query)))
            exit_or_throw(e)

        except asyncio.TimeoutError as e:
            # Timeout, re-run this request
            logging.warning("Timed out on request: %s (%s), retrying..." % (path, json.dumps(query or '')))
            return await retry()

        except aiohttp.ClientConnectionError as e:
            logging.warning("Unable to contact the Binance Futures API (%s). Please check the URL. Retrying. Request: %s \n %s" % (e, url, json.dumps(query)))
            await asyncio.sleep(1)
            return await retry()

        # Reset retry counter on success
        self.retries = 0
        return await response.json()

    async def get_symbol_info(self, symbol):
        resp = await self.__curl_binancefutures(verb='GET', path='/v1/exchangeInfo')
        for x in resp['symbols']:
            if x['symbol'].upper() == symbol.upper():
                return x

    async def create_orders(self, order):
        """Create a single order."""
        order['newClientOrderId'] = self.orderIDPrefix + base64.b64encode(uuid.uuid4().bytes).decode('utf8').replace('+', '').replace('/', '').rstrip('=\n')
        order['symbol'] = self.symbol
        order['side'] = order['side'].upper()
        order['type'] = 'LIMIT'
        if self.postOnly:
            order['timeInForce'] = 'GTX'
        else:
            order['timeInForce'] = 'GTC'
        pending_order = order.copy()
        pending_order['status'] = 'PENDING_NEW'
        pending_order['clientOrderId'] = order['newClientOrderId']
        self.open_orders_ws[order['newClientOrderId']] = pending_order
        try:
            resp = await self.__curl_binancefutures(verb='POST', path='/v1/order', query=order,
                                                    max_retries=0)
            order_id = resp['clientOrderId']
            order = self.open_orders_ws[order_id]
            if 'updateTime' not in order or order['updateTime'] < resp['updateTime']:
                order.update(resp)
            return resp
        except (aiohttp.ClientResponseError, aiohttp.ClientConnectionError, asyncio.TimeoutError):
            order = self.open_orders_ws.get(pending_order['newClientOrderId'])
            if order is not None and order['status'] == 'PENDING_NEW':
                del self.open_orders_ws[pending_order['newClientOrderId']]
            raise

    async def create_bulk_orders(self, orders):
        """Create multiple orders."""
        if len(orders) > 5:
            raise Exception('The number of orders cannot exceed 5.')
        pending_orders = []
        for order in orders:
            order['newClientOrderId'] = self.orderIDPrefix + base64.b64encode(uuid.uuid4().bytes).decode('utf8').replace('+', '').replace('/', '').rstrip('=\n')
            order['symbol'] = self.symbol
            order['side'] = order['side'].upper()
            order['type'] = 'LIMIT'
            if self.postOnly:
                order['timeInForce'] = 'GTX'
            else:
                order['timeInForce'] = 'GTC'
            pending_order = order.copy()
            pending_order['status'] = 'PENDING_NEW'
            pending_order['clientOrderId'] = order['newClientOrderId']
            self.open_orders_ws[order['newClientOrderId']] = pending_order
            pending_orders.append(pending_order)
        try:
            resp = await self.__curl_binancefutures(verb='POST', path='/v1/batchOrders', query={'batchOrders': orders},
                                                    max_retries=0)
            for item in resp:
                order_id = item['clientOrderId']
                order = self.open_orders_ws[order_id]
                if 'updateTime' not in order or order['updateTime'] < item['updateTime']:
                    order.update(item)
            return resp
        except (aiohttp.ClientResponseError, aiohttp.ClientConnectionError, asyncio.TimeoutError):
            for pending_order in pending_orders:
                order = self.open_orders_ws.get(pending_order['newClientOrderId'])
                if order is not None and order['status'] == 'PENDING_NEW':
                    del self.open_orders_ws[pending_order['newClientOrderId']]
            raise

    async def cancel_bulk_orders(self, origClientOrderIdList):
        if len(origClientOrderIdList) > 10:
            raise Exception('The number of orders cannot exceed 10.')
        resp = await self.__curl_binancefutures(verb='DELETE', path='/v1/batchOrders',
                                                query={'symbol': self.symbol, 'origClientOrderIdList': origClientOrderIdList},
                                                max_retries=0)
        for item in resp:
            if 'code' in item:
                if item['code'] != -2011:
                    logging.warning('cancel_bulk_orders: error response=%s' % str(item))
                continue
            order_id = item['clientOrderId']
            order = self.open_orders_ws[order_id]
            if 'updateTime' not in order or order['updateTime'] < item['updateTime']:
                order.update(item)
        return resp

    async def cancel_all_orders(self):
        resp = await self.__curl_binancefutures(verb='DELETE', path='/v1/allOpenOrders', query={'symbol': self.symbol})
        # if resp['code'] == '200':
        #     now = time.time()
        #     for order_id, order in self.open_orders_ws.items():
        #         if order['status'] != 'PENDING_NEW':
        #             order['updateTime'] = now
        #             order['status'] = 'CANCEL_ALL'
        return resp

    async def open_orders(self):
        return await self.__curl_binancefutures(verb='GET', path='/v1/openOrders')

    async def open_position(self):
        response = await self.__curl_binancefutures(verb='GET', path='/v2/positionRisk')
        for position in response:
            side = position['positionSide']
            if 'BOTH' == side:
                if position['symbol'].upper() == self.symbol.upper():
                    return position['positionAmt']
        return '0'

    async def open_user_data_stream(self):
        response = await self.__curl_binancefutures(verb='POST', path='/v1/listenKey')
        return response["listenKey"]

    async def keepalive_user_data_stream(self):
        return await self.__curl_binancefutures(verb='PUT', path='/v1/listenKey')

    async def connect(self):
        try:
            await self.cancel_all_orders()
            self.running_qty = await self.open_position()
            self.listen_key = await self.open_user_data_stream()
            if self.testnet:
                url = 'wss://stream.binancefuture.com/stream?streams=%s/%s/%s' % (self.listen_key, '%s@depth@0ms' % self.symbol, '%s@aggTrade' % self.symbol)
            else:
                url = 'wss://fstream.binance.com/stream?streams=%s/%s/%s' % (self.listen_key, '%s@depth@0ms' % self.symbol, '%s@aggTrade' % self.symbol)
            async with ClientSession() as session:
                async with session.ws_connect(url) as ws:
                    logging.info('WS Connected.')
                    self.ws = ws
                    self.keep_alive = asyncio.create_task(self.__keep_alive())
                    async for msg in ws:
                        if msg.type == WSMsgType.TEXT:
                            await self.__on_message(msg.data)
                        elif msg.type == WSMsgType.BINARY:
                            pass
                        elif msg.type == WSMsgType.PING:
                            await self.ws.pong()
                        elif msg.type == WSMsgType.PONG:
                            await self.ws.ping()
                        elif msg.type == WSMsgType.ERROR:
                            exc = ws.exception()
                            raise exc if exc is not None else Exception
        except:
            logging.exception('WS Error')
        finally:
            logging.info('WS Disconnected.')
            self.keep_alive.cancel()
            await self.keep_alive
            self.ws = None
            self.depth.clear()
            if not self.closed:
                await asyncio.sleep(1)
                asyncio.create_task(self.connect())

    async def close(self):
        await self.cancel_all_orders()
        self.closed = True
        if self.ws is not None:
            await self.ws.close()
        await self.client.close()
        await asyncio.sleep(1)

    async def __get_marketdepth_snapshot(self):
        data = await self.__curl_binancefutures(verb='GET', path='/v1/depth', query={'symbol': self.symbol, 'limit': 1000})
        l_bid, _ = data['bids'][-1]
        h_ask, _ = data['asks'][-1]
        self.depth = {price: qty for price, qty in self.depth.items() if (price < l_bid and qty > 0) or (price > h_ask and qty < 0)}
        for price, qty in data['bids']:
            self.depth[price] = float(qty)
        for price, qty in data['asks']:
            self.depth[price] = -float(qty)
        lastUpdateId = data['lastUpdateId']
        self.prev_u = None
        # Process the pending messages.
        while self.prev_u is None:
            while self.pending_messages:
                item = self.pending_messages.pop(0)
                u = item['u']
                U = item['U']
                pu = item['pu']
                # https://binance-docs.github.io/apidocs/futures/en/#how-to-manage-a-local-order-book-correctly
                # The first processed event should have U <= lastUpdateId AND u >= lastUpdateId
                if (u < lastUpdateId or U > lastUpdateId) and self.prev_u is None:
                    continue
                if self.prev_u is not None and pu != self.prev_u:
                    logging.warning('UpdateId does not match. symbol=%s, prev_update_id=%d, pu=%d' % (self.symbol, self.prev_u, pu))
                for price, qty in item['b']:
                    if qty != '0':
                        self.depth[price] = float(qty)
                for price, qty in item['a']:
                    if qty != '0':
                        self.depth[price] = -float(qty)
                self.prev_u = u
            if self.prev_u is None:
                await asyncio.sleep(0.5)
        self.pending_messages = None
        logging.warning('The book is initialized. symbol=%s, prev_update_id=%d' % (self.symbol, self.prev_u))
