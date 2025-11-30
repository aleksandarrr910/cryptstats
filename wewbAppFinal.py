from collections import OrderedDict
from datetime import date, timedelta
from sqlalchemy import func
from flask import Flask, render_template, request, redirect, url_for
from projectModels import ConnectionSession, CryptoCurrency
import requests
from datetime import datetime, timedelta

from flask import Flask, render_template, request, redirect, url_for
from collections import OrderedDict
from sqlalchemy import func

from projectModels import ConnectionSession, CryptoCurrency

webApp = Flask(__name__)

BINANCE_TICKER_URL = "https://api.binance.com/api/v3/ticker/24hr"
PREFERRED_QUOTES = ["USDT", "FDUSD", "BUSD", "USDC"]


def get_trading_pair(symbol: str) -> str | None:
    """
    Наједноставно: пробуваме со неколку quote валути.
    Пр. BTC -> BTCUSDT, BTCFDUSD...
    """
    for quote in PREFERRED_QUOTES:
        pair = f"{symbol.upper()}{quote}"
        try:
            r = requests.get(BINANCE_TICKER_URL, params={"symbol": pair}, timeout=4)
            if r.status_code == 200:
                # ако е OK, ова е валидниот пар
                return pair
        except Exception:
            continue
    return None


def get_realtime_info(symbol: str) -> dict | None:
    """
    Земаме 24h статистики од Binance:
    lastPrice, priceChangePercent, highPrice, lowPrice, volume...
    """
    pair = get_trading_pair(symbol)
    if pair is None:
        return None

    r = requests.get(BINANCE_TICKER_URL, params={"symbol": pair}, timeout=4)
    if r.status_code != 200:
        return None

    data = r.json()
    return {
        "pair": pair,
        "last_price": float(data["lastPrice"]),
        "change_percent": float(data["priceChangePercent"]),
        "high_24h": float(data["highPrice"]),
        "low_24h": float(data["lowPrice"]),
        "volume_24h": float(data["volume"]),
    }


@webApp.route("/")
def Home():
    """
    Home:
    - вади листа на различни coin симболи од базата
    - ја рендерира Home.html
    """
    session = ConnectionSession()
    try:
        rows = session.query(CryptoCurrency.coinSymbol).distinct().all()
    finally:
        session.close()

    symbols = [r[0] for r in rows if r[0] is not None]

    return render_template("Home.html", symbols=symbols)


@webApp.route("/coin")
def coinCrypto():
    """
    /coin?symbol=BTC

    - го чита симболот од query string
    - ги зема сите записи за тој симбол, подредени по датум
    - за секоја година го задржува ПОСЛЕДНИОТ coinMarketCap
      (последниот ден во годината што го имаме во базата)
    - враќа најмногу 10 ПОсЛЕДНИ години
    - испраќа chart_rows = [{year: ..., close: ...}, ...] кон CryptoCoin.html
    """
    symbol = request.args.get("symbol", type=str)

    if not symbol:
        return redirect(url_for("Home"))

    session = ConnectionSession()
    try:
        # земаме дата + market cap за дадениот симбол, сортирано по датум
        rows = (
            session.query(CryptoCurrency.dateCoin, CryptoCurrency.coinMarketCap)
            .filter(CryptoCurrency.coinSymbol == symbol)
            .filter(CryptoCurrency.dateCoin.isnot(None))
            .filter(CryptoCurrency.coinMarketCap.isnot(None))
            .order_by(CryptoCurrency.dateCoin.asc())
            .all()
        )

        if not rows:
            session.close()
            return f"No info for the coin: {symbol}", 404

        # year -> последна вредност во таа година (пошто сме порастечки по датум)
        year_to_value = {}
        for date_val, mcap_val in rows:
            year = date_val.year
            year_to_value[year] = float(mcap_val)

        if not year_to_value:
            session.close()
            return f"No yearly data for the coin: {symbol}", 404

        # ги сортираме годините и ги земаме само последните 10
        years_sorted = sorted(year_to_value.keys())
        last_years = years_sorted[-10:]

        chart_rows = [
            {"year": year, "close": year_to_value[year]}
            for year in last_years
        ]

    finally:
        session.close()

    return render_template(
        "CryptoCoin.html",
        symbol=symbol,
        chart_rows=chart_rows
    )


@webApp.route("/compare", methods=["GET"])
def compare_coins():
    session = ConnectionSession()
    try:
        # 1) сите симболи за dropdown листата
        rows = (
            session.query(CryptoCurrency.coinSymbol)
            .distinct()
            .order_by(CryptoCurrency.coinSymbol.asc())
            .all()
        )
        all_symbols = [r[0] for r in rows if r[0] is not None]

        # 2) симболи што ги избрал корисникот од <select multiple>
        raw_list = request.args.getlist("symbols")
        if raw_list:
            symbols = [s.strip().upper() for s in raw_list if s.strip()]
        else:
            # fallback ако некој рачно прати ?symbols=BTC,ETH
            symbols_param = request.args.get("symbols", "").strip()
            if symbols_param:
                symbols = [s.strip().upper() for s in symbols_param.split(",") if s.strip()]
            else:
                symbols = []

        series = {}
        rows = []  # за debug
        if symbols:
            end_date = datetime.utcnow().date()
            start_date = end_date - timedelta(days=30)

            # 3) ги вадиме market-cap вредностите за последни 30 дена
            rows = (
                session.query(
                    CryptoCurrency.coinSymbol,
                    CryptoCurrency.dateCoin,
                    CryptoCurrency.coinMarketCap,
                )
                .filter(CryptoCurrency.coinSymbol.in_(symbols))
                .filter(CryptoCurrency.dateCoin >= start_date)
                .filter(CryptoCurrency.dateCoin <= end_date)
                .filter(CryptoCurrency.coinMarketCap.isnot(None))
                .order_by(CryptoCurrency.coinSymbol, CryptoCurrency.dateCoin)
                .all()
            )

            # series["BTC"] = [{date: "...", value: ...}, ...]
            series = {sym: [] for sym in symbols}
            for sym, date_val, mcap_val in rows:
                if date_val is None or mcap_val is None:
                    continue
                series[sym].append({
                    "date": date_val.isoformat(),
                    "value": float(mcap_val),
                })

        # мал debug да видиш што се случува во конзола
        print("[COMPARE] selected symbols:", symbols)
        print("[COMPARE] rows from DB:", len(rows))
        print("[COMPARE] series sizes:",
              {k: len(v) for k, v in series.items()})

    finally:
        session.close()

    return render_template(
        "Compare.html",
        all_symbols=all_symbols,
        selected_symbols=symbols,
        series=series,
    )


@webApp.route("/stats", methods=["GET"])
def stats_overview():
    """
    FR5: Преглед на целосни статистики за избран coin:
    high, low, close, volume, market-cap (за последниот ден во базата).
    """

    session = ConnectionSession()
    symbol = request.args.get("symbol", type=str)

    try:
        # сите достапни симболи
        rows = (
            session.query(CryptoCurrency.coinSymbol)
            .distinct()
            .order_by(CryptoCurrency.coinSymbol.asc())
            .all()
        )
        all_symbols = [r[0] for r in rows if r[0] is not None]

        stats = None
        if symbol:
            symbol = symbol.upper()
            # најнов ден за конкретен coin
            last_row = (
                session.query(CryptoCurrency)
                .filter(CryptoCurrency.coinSymbol == symbol)
                .filter(CryptoCurrency.dateCoin.isnot(None))
                .order_by(CryptoCurrency.dateCoin.desc())
                .first()
            )

            if last_row:
                stats = {
                    "symbol": symbol,
                    "date": last_row.dateCoin,
                    "high": float(last_row.high) if last_row.high is not None else None,
                    "low": float(last_row.low) if last_row.low is not None else None,
                    "close": float(last_row.closeTime) if last_row.closeTime is not None else None,
                    "volume": float(last_row.quoteVolume) if last_row.quoteVolume is not None else None,
                    "market_cap": float(last_row.coinMarketCap) if last_row.coinMarketCap is not None else None,
                }

        # DEBUG
        print(f"[STATS] symbol={symbol}, has_stats={stats is not None}")

    finally:
        session.close()

    return render_template(
        "Stats.html",
        all_symbols=all_symbols,
        selected_symbol=symbol,
        stats=stats,
    )

@webApp.route("/coins")
def coins_overview():
    """
    Посебна страница:
    - ги листа сите coins од базата
    - за секој покажува последен market cap и промена vs претходниот ден
    - се користи за мали dashboards
    """
    session = ConnectionSession()
    try:
        # сите уникатни симболи
        rows = (
            session.query(CryptoCurrency.coinSymbol)
            .distinct()
            .order_by(CryptoCurrency.coinSymbol.asc())
            .all()
        )
        symbols = [r[0] for r in rows if r[0] is not None]

        coin_cards = []

        for sym in symbols:
            # последни 2 записа (за да сметаме промена)
            last_two = (
                session.query(CryptoCurrency.dateCoin, CryptoCurrency.coinMarketCap)
                .filter(CryptoCurrency.coinSymbol == sym)
                .filter(CryptoCurrency.dateCoin.isnot(None))
                .filter(CryptoCurrency.coinMarketCap.isnot(None))
                .order_by(CryptoCurrency.dateCoin.desc())
                .limit(2)
                .all()
            )

            if not last_two:
                continue

            last_date, last_cap = last_two[0]
            last_cap = float(last_cap)

            change_percent = None
            direction = "flat"  # default

            if len(last_two) > 1:
                _, prev_cap = last_two[1]
                prev_cap = float(prev_cap)
                if prev_cap != 0:
                    change_percent = ((last_cap - prev_cap) / prev_cap) * 100.0

            if change_percent is not None:
                if change_percent > 0:
                    direction = "up"
                elif change_percent < 0:
                    direction = "down"
                else:
                    direction = "flat"

            coin_cards.append({
                "symbol": sym,
                "last_date": last_date,
                "last_cap": last_cap,
                "change_percent": change_percent,
                "direction": direction,
            })

        # опционално: можеш да ги сортираш по market cap или по име
        # coin_cards.sort(key=lambda c: c["symbol"])

    finally:
        session.close()

    return render_template("Coins.html", coin_cards=coin_cards)



if __name__ == "__main__":
    webApp.run(debug=True)
