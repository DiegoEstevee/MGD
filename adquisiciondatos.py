from TradingviewData import TradingViewData, Interval
import os
import pandas as pd

request = TradingViewData()

nifty_data = request.get_hist(
    symbol='LINKUSD',
    exchange='BINANCE',
    interval=Interval.daily,
    n_bars=1500
)

df = nifty_data.copy().reset_index()

df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
df = df.dropna(subset=["datetime"])

df["year"] = df["datetime"].dt.year
df["month"] = df["datetime"].dt.month
df["day"] = df["datetime"].dt.day

base_out = "output_to_upload/crypto=chainlink/exchange=binance/dataset=nifty_data"
os.makedirs(base_out, exist_ok=True)

df = df.sort_values("datetime")

for (y, m), chunk in df.groupby(["year", "month"]):
    out_dir = os.path.join(
        base_out,
        f"year={y}",
        f"month={m:02d}"
    )
    os.makedirs(out_dir, exist_ok=True)

    out_file = os.path.join(out_dir, "data.csv")

    chunk.drop(columns=["datetime", "year", "month"]).to_csv(out_file, index=False)

print("Listo.")
