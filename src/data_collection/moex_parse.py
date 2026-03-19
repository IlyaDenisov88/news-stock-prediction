import time
from pathlib import Path

import pandas as pd
import requests

MOEX_BASE_URL = "https://iss.moex.com/iss"

TIMEFRAME_TO_INTERVAL = {
    "1m": 1,
    "10m": 10,
    "1h": 60,
    "1d": 24,
    "1w": 7,
    "1mo": 31,
}

MOEX_CANDLE_COLUMNS = [
    "open",
    "close",
    "high",
    "low",
    "value",
    "volume",
    "begin",
    "end",
]


def build_moex_candles_url(
    security: str,
    engine: str = "stock",
    market: str = "shares",
    board: str = "tqbr",
) -> str:
    return (
        f"{MOEX_BASE_URL}/engines/{engine}/markets/{market}/"
        f"boards/{board}/securities/{security}/candles.json"
    )


def validate_timeframe(timeframe: str) -> int:
    if timeframe not in TIMEFRAME_TO_INTERVAL:
        available = ", ".join(TIMEFRAME_TO_INTERVAL.keys())
        raise ValueError(
            f"Неизвестный timeframe: '{timeframe}'. Доступные значения: {available}"
        )
    return TIMEFRAME_TO_INTERVAL[timeframe]


def fetch_moex_candles_chunk(
    url: str,
    interval: int,
    date_from: str,
    date_till: str,
    start: int,
    timeout: int = 30,
) -> list[list]:
    params = {
        "interval": interval,
        "from": date_from,
        "till": date_till,
        "start": start,
    }

    response = requests.get(url, params=params, timeout=timeout)
    response.raise_for_status()

    data = response.json()
    return data["candles"]["data"]


def load_moex_candles(
    security: str,
    date_from: str,
    date_till: str,
    timeframe: str = "1d",
    *,
    board: str = "tqbr",
    market: str = "shares",
    engine: str = "stock",
    sleep_seconds: float = 0.2,
    chunk_size: int = 500,
) -> list[list]:
    interval = validate_timeframe(timeframe)
    url = build_moex_candles_url(
        security=security.upper(),
        engine=engine,
        market=market,
        board=board,
    )

    all_rows: list[list] = []
    start = 0

    while True:
        rows = fetch_moex_candles_chunk(
            url=url,
            interval=interval,
            date_from=date_from,
            date_till=date_till,
            start=start,
        )

        if not rows:
            break

        all_rows.extend(rows)
        print(f"[{security.upper()} | {timeframe}] загружено строк: {len(all_rows)}")

        start += chunk_size
        time.sleep(sleep_seconds)

    return all_rows


def candles_to_dataframe(
    rows: list[list],
    security: str,
    timeframe: str,
) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=MOEX_CANDLE_COLUMNS)

    if df.empty:
        return pd.DataFrame(
            columns=[
                "security",
                "timeframe",
                "datetime",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "value",
            ]
        )

    df["begin"] = pd.to_datetime(df["begin"])
    df["end"] = pd.to_datetime(df["end"])

    df["security"] = security.upper()
    df["timeframe"] = timeframe

    df = df.rename(columns={"begin": "datetime"})

    df = df[
        [
            "security",
            "timeframe",
            "datetime",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "value",
        ]
    ].sort_values("datetime").reset_index(drop=True)

    return df


def build_output_path(
    security: str,
    timeframe: str,
    date_from: str,
    date_till: str,
    base_dir: str = "data/stocks",
) -> Path:
    return Path(base_dir) / f"{security.lower()}_{timeframe}_{date_from}_{date_till}.csv"


def save_dataframe(df: pd.DataFrame, output_path: str | Path) -> None:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")


def download_ticker_history(
    security: str,
    date_from: str,
    date_till: str,
    timeframe: str,
    *,
    board: str = "tqbr",
    market: str = "shares",
    engine: str = "stock",
    base_dir: str = "data/stocks",
) -> pd.DataFrame:
    rows = load_moex_candles(
        security=security,
        date_from=date_from,
        date_till=date_till,
        timeframe=timeframe,
        board=board,
        market=market,
        engine=engine,
    )

    df = candles_to_dataframe(
        rows=rows,
        security=security,
        timeframe=timeframe,
    )

    output_path = build_output_path(
        security=security,
        timeframe=timeframe,
        date_from=date_from,
        date_till=date_till,
        base_dir=base_dir,
    )

    save_dataframe(df, output_path)

    print(f"[OK] {security.upper()}: сохранено {len(df)} строк в {output_path}")
    return df


if __name__ == "__main__":
    TOP50_MOEX_TICKERS = [
    # банки / финансы
    "SBER", "VTBR", "T", "MOEX",

    # нефть и газ
    "GAZP", "LKOH", "ROSN", "NVTK", "SIBN", "TATN", "SNGS", "SNGSP",

    # металлургия / сырье
    "GMKN", "RUAL", "MAGN", "CHMF", "ALRS", "PLZL", "PHOR", "UGLD",

    # ритейл / потреб сектор
    "MGNT", "X5", "OZON", "BELU",

    # IT / технологии
    "YDEX", "VKCO", "POSI", "ASTR", "IVAT",

    # транспорт / логистика
    "FLOT", "AFLT",

    # энергетика
    "IRAO", "HYDR", "FEES",

    # девелоперы
    "PIKK", "LSRG", "ETLN",

    # холдинги / прочее
    "AFKS", "ENPG",

    # доп ликвидные (расширение)
    "RNFT", "EUTR", "BAZA", "TRNFP", "MTSS",
    "RTKM", "RTKMP", "SMLT", "HEAD", "FIXP",
    "CIAN", "SELG", "KZOSP"]


    DATE_FROM = "2024-01-01"
    DATE_TILL = "2026-03-18"
    TIMEFRAME = "1h"

    all_dfs = []  

    for ticker in TOP50_MOEX_TICKERS:
        try:
            df = download_ticker_history(
                security=ticker,
                date_from=DATE_FROM,
                date_till=DATE_TILL,
                timeframe=TIMEFRAME,
            )
            all_dfs.append(df)
        except Exception as e:
            print(f"[ERROR] {ticker}: {e}")

    # --- объединяем ---
    if all_dfs:
        full_df = pd.concat(all_dfs, ignore_index=True)

        output_path = f"data/stocks/all_{TIMEFRAME}_{DATE_FROM}_{DATE_TILL}.csv"
        save_dataframe(full_df, output_path)

        print(f"\n[FINAL] общий датасет: {len(full_df)} строк")
        print(f"[FINAL] сохранено в {output_path}")