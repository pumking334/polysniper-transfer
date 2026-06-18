#!/usr/bin/env python3
"""
Order bridge for polysniper. One JSON line out.

Verbs:
  buy_fok / buy_limit / sell_fak / sell_limit / status / cancel / balance / heartbeat / order_fills

This keeps the same live interface you pasted, adds buy_limit for partial entry fills,
and heartbeat so resting orders are not auto-cancelled by session expiry.
"""

import json
import os
import sys
import time
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

BUILDER_CODE = "0xb61cf3ff36101da7ea9bbf7a58f23f9f76466acb99c0cd44c3d274fe1340e434"
UNIT = 1_000_000.0


def out(d):
    print(json.dumps(d))
    sys.exit(0)


def order_status_str(r):
    if not isinstance(r, dict):
        return ""
    return str(r.get("status") or r.get("orderStatus") or "").lower()


def make_client():
    from py_clob_client_v2 import ApiCreds, ClobClient, SignatureTypeV2

    creds = ApiCreds(
        api_key=os.getenv("POLY_API_KEY"),
        api_secret=os.getenv("POLY_API_SECRET"),
        api_passphrase=os.getenv("POLY_API_PASSPHRASE"),
    )
    return ClobClient(
        host="https://clob.polymarket.com",
        chain_id=137,
        key=os.getenv("PRIVATE_KEY"),
        creds=creds,
        signature_type=SignatureTypeV2.POLY_1271,
        funder=os.getenv("DEPOSIT_WALLET_ADDRESS"),
    )


def token_balance(client, token):
    from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType

    try:
        b = client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token)
        )
        return float(b.get("balance", 0) or 0) / UNIT
    except Exception:
        return None


def tick_and_neg(client, token):
    try:
        tick = str(client.get_tick_size(token))
    except Exception:
        tick = "0.01"
    try:
        neg = client.get_neg_risk(token)
    except Exception:
        neg = False
    return tick, neg


def round_to_tick(price, tick):
    dec = len(tick.rstrip("0").split(".")[-1]) if "." in tick else 0
    return round(price, max(dec, 2))


def place(client, token, side_const, price, shares, otype, tick, neg):
    from py_clob_client_v2 import OrderArgs, OrderType, PartialCreateOrderOptions

    return client.create_and_post_order(
        OrderArgs(
            token_id=token,
            price=price,
            size=float(shares),
            side=side_const,
            builder_code=BUILDER_CODE,
        ),
        options=PartialCreateOrderOptions(tick_size=tick, neg_risk=neg),
        order_type=getattr(OrderType, otype),
    )


def parse_num(v):
    try:
        return float(v)
    except Exception:
        return 0.0


def extract_trade_rows(payload):
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("data", "trades", "items", "results"):
            val = payload.get(key)
            if isinstance(val, list):
                return val
    return []


def trade_matches_order(trade, order_id):
    if not isinstance(trade, dict):
        return False

    if str(trade.get("taker_order_id") or trade.get("takerOrderId") or "") == order_id:
        return True

    maker_orders = trade.get("maker_orders") or trade.get("makerOrders") or []
    if isinstance(maker_orders, list):
        for mo in maker_orders:
            if isinstance(mo, dict):
                for key in ("order_id", "orderID", "id", "hash"):
                    if str(mo.get(key) or "") == order_id:
                        return True
            elif str(mo) == order_id:
                return True
    return False


def collect_order_fills(client, order_id, token_id, fallback_price=0.0, polls=8, sleep_s=0.25):
    trade_ids = set()
    total_size = 0.0
    total_notional = 0.0

    try:
        from py_clob_client_v2 import TradeParams
    except Exception:
        TradeParams = None

    for _ in range(max(1, polls)):
        rows = []
        try:
            if TradeParams is not None:
                rows = extract_trade_rows(client.get_trades(TradeParams(asset_id=token_id)))
            else:
                rows = extract_trade_rows(client.get_trades())
        except Exception:
            try:
                rows = extract_trade_rows(client.get_trades())
            except Exception:
                rows = []

        for tr in rows:
            if not trade_matches_order(tr, order_id):
                continue
            tid = str(tr.get("id") or tr.get("trade_id") or tr.get("match_time") or len(trade_ids))
            if tid in trade_ids:
                continue
            trade_ids.add(tid)
            size = parse_num(tr.get("size") or tr.get("matchedAmount") or tr.get("amount"))
            price = parse_num(tr.get("price"))
            if size > 0 and price > 0:
                total_size += size
                total_notional += size * price

        if total_size > 0:
            break
        time.sleep(sleep_s)

    avg_price = (total_notional / total_size) if total_size > 0 else fallback_price

    try:
        od = client.get_order(order_id)
        order_matched = parse_num(
            od.get("size_matched")
            or od.get("sizeMatched")
            or od.get("matchedAmount")
            or od.get("sizeMatchedAmount")
        )
        if order_matched > total_size:
            total_size = order_matched
            if avg_price <= 0:
                avg_price = fallback_price
    except Exception:
        pass

    return round(total_size, 6), round(avg_price, 6), len(trade_ids)


def main():
    if len(sys.argv) < 2:
        out({"ok": False, "error": "no verb"})

    verb = sys.argv[1]

    try:
        from py_clob_client_v2.order_builder.constants import BUY, SELL

        client = make_client()

        if verb == "balance":
            token = sys.argv[2]
            out({"ok": True, "token": token, "shares": token_balance(client, token)})

        if verb in ("buy_fok", "sell_fak"):
            token, price, shares = sys.argv[2], float(sys.argv[3]), float(sys.argv[4])
            tick, neg = tick_and_neg(client, token)
            is_buy = verb == "buy_fok"
            side = BUY if is_buy else SELL
            op = round_to_tick(min(price + 0.03, 0.97) if is_buy else max(price - 0.02, 0.01), tick)
            otype = "FOK" if is_buy else "FAK"

            bal_before = token_balance(client, token)
            r = place(client, token, side, op, shares, otype, tick, neg)
            oid = r.get("orderID", r.get("orderId", ""))

            filled = 0.0
            confirmed = False
            for _ in range(20):
                time.sleep(0.5)
                bal_after = token_balance(client, token)
                if bal_before is not None and bal_after is not None:
                    delta = (bal_after - bal_before) if is_buy else (bal_before - bal_after)
                    if delta > 0.000001:
                        filled = round(delta, 4)
                        confirmed = True
                        break

            assume = False
            if not confirmed and oid:
                filled = float(shares)
                assume = True

            avg_fill_price = 0.0
            if oid:
                tf, ap, tc = collect_order_fills(client, oid, token, fallback_price=op, polls=4, sleep_s=0.25)
                if tf > 0:
                    filled = max(filled, tf)
                    avg_fill_price = ap
                else:
                    tc = 0
            else:
                tc = 0

            out(
                {
                    "ok": filled > 0,
                    "filled": filled,
                    "avg_fill_price": avg_fill_price or op,
                    "trade_count": tc,
                    "assume_filled": assume,
                    "confirmed": confirmed,
                    "order_id": oid,
                    "status": order_status_str(r) or "done",
                    "price": op,
                    "requested": shares,
                }
            )

        elif verb == "buy_limit":
            token, price, shares = sys.argv[2], float(sys.argv[3]), float(sys.argv[4])
            tick, neg = tick_and_neg(client, token)
            op = round_to_tick(price, tick)
            r = place(client, token, BUY, op, shares, "GTC", tick, neg)
            oid = r.get("orderID", r.get("orderId", ""))
            out({"ok": bool(oid), "order_id": oid, "status": order_status_str(r) or "live", "price": op, "requested": shares})

        elif verb == "sell_limit":
            token, price, shares = sys.argv[2], float(sys.argv[3]), float(sys.argv[4])
            tick, neg = tick_and_neg(client, token)
            op = round_to_tick(price, tick)
            r = place(client, token, SELL, op, shares, "GTC", tick, neg)
            oid = r.get("orderID", r.get("orderId", ""))
            out({"ok": bool(oid), "order_id": oid, "status": order_status_str(r) or "live", "price": op})

        elif verb == "status":
            token = sys.argv[2]
            out({"ok": True, "token": token, "shares": token_balance(client, token)})

        elif verb == "cancel":
            ids = [x for x in sys.argv[2].split(",") if x]
            try:
                res = client.cancel_orders(ids)
            except Exception:
                res = {}
                for i in ids:
                    try:
                        res[i] = client.cancel_order(i)
                    except Exception as e:
                        res[i] = str(e)
            out({"ok": True, "cancelled": ids, "_raw": res if isinstance(res, dict) else str(res)})

        elif verb == "order_fills":
            order_id = sys.argv[2]
            token_id = sys.argv[3]
            fallback_price = float(sys.argv[4]) if len(sys.argv) > 4 else 0.0
            filled, avg_fill_price, trade_count = collect_order_fills(client, order_id, token_id, fallback_price=fallback_price, polls=8, sleep_s=0.25)
            out({
                "ok": filled > 0,
                "order_id": order_id,
                "token": token_id,
                "filled": filled,
                "avg_fill_price": avg_fill_price,
                "trade_count": trade_count,
            })

        elif verb == "heartbeat":
            heartbeat_id = sys.argv[2] if len(sys.argv) > 2 else ""
            try:
                resp = client.post_heartbeat(heartbeat_id)
                out({"ok": True, "heartbeat_id": resp.get("heartbeat_id", ""), "_raw": resp})
            except Exception as e:
                # best-effort resync with empty heartbeat id
                try:
                    resp = client.post_heartbeat("")
                    out({"ok": True, "heartbeat_id": resp.get("heartbeat_id", ""), "resynced": True, "_raw": resp, "prev_error": str(e)})
                except Exception as e2:
                    out({"ok": False, "error": str(e), "resync_error": str(e2)})

        else:
            out({"ok": False, "error": f"unknown verb {verb}"})

    except Exception as e:
        out({"ok": False, "status": "error", "filled": 0.0, "error": str(e)})


if __name__ == "__main__":
    main()
