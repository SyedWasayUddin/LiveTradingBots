import json
import ccxt
import time
import pandas as pd
from typing import Any, Optional, Dict, List


class BitgetFutures:
    def __init__(self, api_setup: Optional[Dict[str, Any]] = None) -> None:
        if api_setup is None:
            api_setup = {}

        self.session = ccxt.bitget({
            "apiKey": api_setup.get("apiKey"),
            "secret": api_setup.get("secret"),
            "password": api_setup.get("password"),
            "enableRateLimit": True,
            "options": {
                "defaultType": "swap",  # Required for futures
                "headers": {
                    "PAPTRADING": "1"   # ✅ Tells Bitget to use demo environment
                }
            }
        })

        print("✅ CCXT Bitget configured for DEMO environment.")
        print(f"🔗 API Type: {self.session.options.get('defaultType')}")
        self.markets = self.session.load_markets()
        print("✅ All market symbols:", list(self.markets.keys()))
        


    def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        return self.session.fetch_ticker(symbol)

    def fetch_min_amount_tradable(self, symbol: str) -> float:
        return self.markets[symbol]['limits']['amount']['min']

    def amount_to_precision(self, symbol: str, amount: float) -> str:
        return self.session.amount_to_precision(symbol, amount)

    def price_to_precision(self, symbol: str, price: float) -> str:
        return self.session.price_to_precision(symbol, price)

    def fetch_balance(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self.session.fetch_balance(params or {})

    def fetch_order(self, id: str, symbol: str) -> Dict[str, Any]:
        market = self.session.market(symbol)
        return self.session.fetch_order(id, market['id'])

    def fetch_open_orders(self, symbol: str) -> List[Dict[str, Any]]:
        market = self.session.market(symbol)
        product_type = market['info'].get('productType', 'umcbl')
        return self.session.private_mix_get_v2_mix_order_orders_pending({
            'symbol': market['id'],
            'productType': product_type
    })

    def fetch_open_trigger_orders(self, symbol: str) -> List[Dict[str, Any]]:
        market = self.session.market(symbol)
        return self.session.fetch_open_orders(market['id'], params={"stop": True})

    def fetch_closed_trigger_orders(self, symbol: str) -> List[Dict[str, Any]]:
        market = self.session.market(symbol)
        return self.session.fetch_closed_orders(market['id'], params={"stop": True})

    def cancel_order(self, id: str, symbol: str) -> Dict[str, Any]:
        market = self.session.market(symbol)
        return self.session.cancel_order(id, market['id'])

    def cancel_trigger_order(self, id: str, symbol: str) -> Dict[str, Any]:
        market = self.session.market(symbol)
        return self.session.cancel_order(id, market['id'], params={"stop": True})

    def fetch_open_positions(self, symbol: str) -> List[Dict[str, Any]]:
        market = self.session.market(symbol)
        positions = self.session.fetch_positions([market['id']])
        return [pos for pos in positions if float(pos.get('contracts', 0)) > 0]

    def flash_close_position(self, symbol: str, side: Optional[str] = None) -> Dict[str, Any]:
        market = self.session.market(symbol)
        return self.session.close_position(market['id'], side=side)

    def set_margin_mode(self, symbol: str, margin_mode: str = 'isolated') -> None:
        market = self.session.market(symbol)
        self.session.set_margin_mode(margin_mode, market['id'])

    def set_leverage(self, symbol: str, margin_mode: str = 'isolated', leverage: int = 1) -> None:
        market = self.session.market(symbol)
        if margin_mode == 'isolated':
            for side in ['long', 'short']:
                self.session.set_leverage(leverage, market['id'], params={'holdSide': side})
        else:
            self.session.set_leverage(leverage, market['id'])

    def fetch_recent_ohlcv(self, symbol: str, timeframe: str, limit: int = 1000) -> pd.DataFrame:
        bitget_fetch_limit = 200
        ms_map = {
            '1m': 60000, '5m': 300000, '15m': 900000, '30m': 1800000,
            '1h': 3600000, '2h': 7200000, '4h': 14400000, '1d': 86400000,
        }

        end_ts = int(time.time() * 1000)
        start_ts = end_ts - (limit * ms_map[timeframe])
        current_ts = start_ts
        ohlcv_data = []

        while current_ts < end_ts:
            req_end = min(current_ts + bitget_fetch_limit * ms_map[timeframe], end_ts)
            fetched = self.session.fetch_ohlcv(symbol, timeframe, params={
                "startTime": str(current_ts),
                "endTime": str(req_end),
                "limit": bitget_fetch_limit,
            })
            ohlcv_data.extend(fetched)
            current_ts += bitget_fetch_limit * ms_map[timeframe] + 1

        df = pd.DataFrame(ohlcv_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        df.sort_index(inplace=True)
        return df

    def place_market_order(self, symbol: str, side: str, amount: float, reduce: bool = False) -> Dict[str, Any]:
        market = self.session.market(symbol)
        params = {'reduceOnly': reduce}
        amount = self.amount_to_precision(symbol, amount)
        return self.session.create_order(market['id'], 'market', side, amount, params=params)

    def place_limit_order(self, symbol: str, side: str, amount: float, price: float, reduce: bool = False) -> Dict[str, Any]:
        market = self.session.market(symbol)
        params = {'reduceOnly': reduce}
        amount = self.amount_to_precision(symbol, amount)
        price = self.price_to_precision(symbol, price)
        return self.session.create_order(market['id'], 'limit', side, amount, price, params=params)

    def place_trigger_market_order(self, symbol: str, side: str, amount: float, trigger_price: float, reduce: bool = False) -> Optional[Dict[str, Any]]:
        try:
            market = self.session.market(symbol)
            amount = self.amount_to_precision(symbol, amount)
            trigger_price = self.price_to_precision(symbol, trigger_price)
            params = {'reduceOnly': reduce, 'triggerPrice': trigger_price}
            return self.session.create_order(market['id'], 'market', side, amount, params=params)
        except Exception as e:
            print(e)
            return None

    def place_trigger_limit_order(self, symbol: str, side: str, amount: float, trigger_price: float, price: float, reduce: bool = False) -> Optional[Dict[str, Any]]:
        try:
            market = self.session.market(symbol)
            amount = self.amount_to_precision(symbol, amount)
            trigger_price = self.price_to_precision(symbol, trigger_price)
            price = self.price_to_precision(symbol, price)
            params = {'reduceOnly': reduce, 'triggerPrice': trigger_price}
            return self.session.create_order(market['id'], 'limit', side, amount, price, params=params)
        except Exception as e:
            print(e)
            return None
