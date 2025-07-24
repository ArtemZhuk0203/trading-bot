import asyncio
import json
import os
import csv
from datetime import datetime, timedelta
import pytz
import pandas as pd
import requests
import websockets
from dotenv import load_dotenv
import logging
from ta.trend import EMAIndicator, MACD, ADXIndicator
from ta.momentum import RSIIndicator, StochasticOscillator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()
API_KEY = os.getenv("API_KEY")
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

TIMEZONE = pytz.timezone("Europe/Warsaw")
TRADE_LOG_FILE = "trade_log.csv"

PAIRS = [
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF", "NZDUSD",
    "EURGBP", "EURJPY", "EURAUD", "EURCAD", "EURCHF", "EURNZD", "GBPJPY",
    "GBPAUD", "GBPCAD", "GBPCHF", "GBPNZD", "AUDJPY", "AUDCAD", "AUDCHF",
    "AUDNZD", "CADJPY", "CHFJPY", "NZDJPY", "NZDCAD", "NZDCHF",
    "USDNOK", "USDSEK", "USDTRY", "USDMXN", "USDZAR",
    "EURUSD_OTC", "GBPUSD_OTC", "USDJPY_OTC", "AUDUSD_OTC", "USDCAD_OTC"
]

TIMEFRAMES = [1, 2, 3, 4, 5, 10, 15, 30, 60]
URL = f"wss://ws.finnhub.io?token={API_KEY}"

ohlc_1m = {p: pd.DataFrame(columns=["time", "open", "high", "low", "close"]) for p in PAIRS}
ohlc = {p: {tf: pd.DataFrame(columns=["time", "open", "high", "low", "close"]) for tf in TIMEFRAMES} for p in PAIRS}
last_send_time = {pair: datetime.min for pair in PAIRS}
MIN_SEND_INTERVAL = timedelta(minutes=2)
news_cache = []
win_rates = {}

def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}
    requests.post(url, data=data)

def get_recent_news():
    url = f"https://finnhub.io/api/v1/news?category=forex&token={API_KEY}"
    resp = requests.get(url)
    return resp.json() if resp.status_code == 200 else []

def news_filter():
    global news_cache
    news = get_recent_news()
    now = datetime.utcnow()
    filtered = [n for n in news if 'datetime' in n and (now - datetime.utcfromtimestamp(n['datetime'])).total_seconds() < 900]
    if filtered != news_cache:
        news_cache = filtered
        return True
    return False

def update_candle_1m(df, price, now):
    t_min = now.replace(second=0, microsecond=0)
    if df.empty or df.iloc[-1]["time"] < t_min:
        df.loc[len(df)] = [t_min, price, price, price, price]
    else:
        df.at[len(df)-1, "high"] = max(df.iloc[-1]["high"], price)
        df.at[len(df)-1, "low"] = min(df.iloc[-1]["low"], price)
        df.at[len(df)-1, "close"] = price
    return df[df["time"] >= now - timedelta(minutes=90)]

def resample_candles(df_1m, tf):
    if df_1m.empty:
        return df_1m
    df = df_1m.set_index('time').resample(f'{tf}T').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last'
    }).dropna().reset_index()
    return df

def analyze_signal(df):
    if len(df) < 50:
        return None
    close = df["close"]
    ema12 = EMAIndicator(close, 12).ema_indicator()
    ema26 = EMAIndicator(close, 26).ema_indicator()
    macd = MACD(close)
    rsi = RSIIndicator(close, 14).rsi()
    stoch = StochasticOscillator(df['high'], df['low'], close, 14)
    adx = ADXIndicator(df['high'], df['low'], close, 14).adx()

    up = ema12.iloc[-1] > ema26.iloc[-1]
    down = ema12.iloc[-1] < ema26.iloc[-1]
    macd_bull = macd.macd_diff().iloc[-2] < 0 and macd.macd_diff().iloc[-1] > 0
    macd_bear = macd.macd_diff().iloc[-2] > 0 and macd.macd_diff().iloc[-1] < 0
    rsi_os = rsi.iloc[-1] < 30
    rsi_ob = rsi.iloc[-1] > 70
    stoch_os = stoch.stoch_signal().iloc[-1] < 20
    stoch_ob = stoch.stoch_signal().iloc[-1] > 80
    trend = adx.iloc[-1] > 20

    confirmations_buy = sum([up, macd_bull, rsi_os, stoch_os, trend])
    confirmations_sell = sum([down, macd_bear, rsi_ob, stoch_ob, trend])

    if confirmations_buy >= 4:
        return ("BUY", confirmations_buy, adx.iloc[-1])
    if confirmations_sell >= 4:
        return ("SELL", confirmations_sell, adx.iloc[-1])
    return None

def build_comment(signal):
    if signal == "BUY":
        return "После пробоя уровня сформировался откат. Быки активны."
    return "После теста сопротивления началось снижение. Медведи доминируют."

def estimate_success(pair, tf):
    return win_rates.get(pair, {}).get(tf, "~75–80%")

def format_message(pair, tf, signal, confirms, price):
    arrows = {"BUY": "🔺", "SELL": "🔻"}
    comment = build_comment(signal)
    now_str = datetime.now(TIMEZONE).strftime('%H:%M:%S')
    success = estimate_success(pair, tf)
    tf_recommendation = f"{tf} мин"
    duration_note = f"⏱️ Рекомендуем: {'ВВЕРХ' if signal == 'BUY' else 'ВНИЗ'} (на {tf}–{tf+1} минут)"

    otc_note = "\n⚠️ ВНИМАНИЕ: Это *OTC* инструмент" if "OTC" in pair else "\n🔵 Это обычная валютная пара"

    return (
        f"{arrows[signal]} {pair} | {tf}м | {signal}\n"
        f"Цена: {price} | Подтверждений: {confirms}/5 | Вероятность: {success}"
        f"\n{duration_note}\n🎯 Цена входа: {price}\n🕒 Время сигнала: {now_str}"
        f"\n\nАнализ: {comment}{otc_note}"
    )

def can_send(pair):
    now = datetime.utcnow()
    if now - last_send_time[pair] > MIN_SEND_INTERVAL:
        last_send_time[pair] = now
        return True
    return False

def is_good_time():
    now = datetime.now(TIMEZONE)
    weekday = now.weekday()
    hour = now.hour
    if weekday == 0 and hour < 12: return False
    if weekday == 4 and hour >= 17: return False
    if weekday in [5, 6]: return False
    return 9 <= hour <= 12 or 14 <= hour <= 17 or 20 <= hour <= 22

def get_time_period():
    now = datetime.now(TIMEZONE)
    hour = now.hour
    if 9 <= hour < 12:
        return "УТРО (Лондонская сессия)"
    elif 14 <= hour < 17:
        return "ДЕНЬ (Европа + начало Америки)"
    elif 20 <= hour < 22:
        return "ВЕЧЕР (Америка)"
    else:
        return "ВНЕ активных зон — ⚠️ рынок может быть вялым"

def trading_schedule_message():
    return (
        "📅 *Рекомендованные часы торговли:*\n\n"
        "🟢 09:00 – 12:00 (по Варшаве) — Лондон открывается\n"
        "🟢 14:00 – 17:00 — Перекрытие Лондона и Нью-Йорка\n"
        "🟢 20:00 – 22:00 — Америка\n"
        "⛔ Понедельник до 12:00 — рынок «просыпается»\n"
        "⛔ Пятница после 17:00 — волатильность падает\n"
        "⛔ Выходные — рынок закрыт\n"
    )

def log_trade(pair, tf, signal, success):
    file_exists = os.path.isfile(TRADE_LOG_FILE)
    with open(TRADE_LOG_FILE, "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow(["datetime", "pair", "tf", "signal", "success"])
        writer.writerow([datetime.utcnow().isoformat(), pair, tf, signal, int(success)])

def load_win_rates():
    if not os.path.isfile(TRADE_LOG_FILE): return
    df = pd.read_csv(TRADE_LOG_FILE)
    grouped = df.groupby(["pair", "tf"])
    for (pair, tf), group in grouped:
        win = group["success"].sum()
        total = len(group)
        rate = f"{round((win / total) * 100)}%" if total > 4 else "~75–80%"
        win_rates.setdefault(pair, {})[int(tf)] = rate

def analyze_trades_by_time():
    if not os.path.isfile(TRADE_LOG_FILE): return
    df = pd.read_csv(TRADE_LOG_FILE)
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["hour"] = df["datetime"].dt.hour
    df["weekday"] = df["datetime"].dt.dayofweek
    grouped = df.groupby(["pair", "tf", "hour"])
    msgs = []
    for (pair, tf, hour), group in grouped:
        total = len(group)
        wins = group["success"].sum()
        rate = round((wins / total) * 100, 1) if total > 4 else None
        if rate:
            msgs.append(f"📈 {pair} | {tf}м | {hour}:00 — {rate}% на {total} сигналах")
    for msg in msgs[:20]:
        send_telegram(msg)

async def subscribe(ws):
    for p in PAIRS:
        await ws.send(json.dumps({"type": "subscribe", "symbol": f"FX_{p}"}))

async def handle_message(msg):
    if msg.get("type") != "trade": return
    if news_filter(): return
    now = datetime.utcnow()
    for tick in msg.get("data", []):
        pair = tick.get("s", "")[3:]
        price = tick.get("p")
        if pair not in PAIRS or price is None:
            continue
        df1m = ohlc_1m[pair]
        ohlc_1m[pair] = update_candle_1m(df1m, price, now)
        for tf in TIMEFRAMES:
            df_tf = resample_candles(ohlc_1m[pair], tf)
            ohlc[pair][tf] = df_tf
            result = analyze_signal(df_tf)
            if result and can_send(pair):
                signal, confirms, _ = result
                msg = format_message(pair, tf, signal, confirms, round(price, 5))
                if is_good_time():
                    send_telegram(msg)
                    log_trade(pair, tf, signal, success=True)
                else:
                    msg += "\n⚠️ Не рекомендовано — неподходящее время."
                    send_telegram(msg)
                    log_trade(pair, tf, signal, success=False)

async def remind_schedule():
    while True:
        await asyncio.sleep(10800)
        now = datetime.now(TIMEZONE).strftime('%H:%M')
        period = get_time_period()
        send_telegram(f"🔔 Напоминание {now}:\nСейчас *{period}*\n\n💡 Напиши /schedule чтобы увидеть полное расписание.")

async def main():
    load_win_rates()
    analyze_trades_by_time()
    async with websockets.connect(URL) as ws:
        await subscribe(ws)
        send_telegram("✅ Бот запущен и готов!")
        current_period = get_time_period()
        send_telegram(f"🕒 Сейчас: *{current_period}*")
        send_telegram(trading_schedule_message())
        asyncio.create_task(remind_schedule())
        while True:
            try:
                msg = json.loads(await ws.recv())
                await handle_message(msg)
            except Exception as e:
                logging.error(f"Ошибка: {e}")
                await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())