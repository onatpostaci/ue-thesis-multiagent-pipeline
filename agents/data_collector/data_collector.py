import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import time
import os

class LoanDataCSVAgent:
    def __init__(self, path):
        self.path = path

    def collect(self):
        start = datetime.now()
        print(f"[LoanDataCSVAgent] START at {start.time()}")
        time.sleep(1)
        df = pd.read_csv(self.path)
        end = datetime.now()
        print(f"[LoanDataCSVAgent] END at {end.time()} — Duration: {end - start}")
        return df


class BankDataCSVAgent:
    def __init__(self, path):
        self.path = path

    def collect(self):
        start = datetime.now()
        print(f"[BankDataCSVAgent] START at {start.time()}")
        time.sleep(1)
        df = pd.read_csv(self.path, sep=";")
        end = datetime.now()
        print(f"[BankDataCSVAgent] END at {end.time()} — Duration: {end - start}")
        return df


class WorldBankAPIAgent:
    def __init__(self, country="PT", indicators=None):
        self.country = country
        self.indicators = indicators or ["FP.CPI.TOTL", "NY.GDP.PCAP.CD", "SL.UEM.TOTL.ZS"]
        self.base_url = "https://api.worldbank.org/v2/country/{country}/indicator/{indicator}?format=json&per_page=1000"

    def collect(self):
        from datetime import datetime
        import time
        import requests
        import pandas as pd

        start = datetime.now()
        print(f"[WorldBankAPIAgent] START at {start.time()}")
        time.sleep(1)

        merged = None
        for indicator in self.indicators:
            url = self.base_url.format(country=self.country, indicator=indicator)
            response = requests.get(url)
            try:
                records = response.json()[1]
                df = pd.DataFrame(records)[["date", "value"]].dropna()
                df["date"] = df["date"].astype(int)
                df = df.rename(columns={"value": indicator})
                df = df.sort_values("date")
                if merged is None:
                    merged = df
                else:
                    merged = pd.merge(merged, df, on="date", how="outer")
            except Exception as e:
                print(f"[WorldBankAPIAgent] Failed to fetch {indicator}: {e}")

        merged = merged.sort_values("date").reset_index(drop=True)
        end = datetime.now()
        print(f"[WorldBankAPIAgent] END at {end.time()} — Duration: {end - start}, Rows: {len(merged)}")
        return merged


class DataCollectorAgent:
    def __init__(self, sources):
        self.sources = sources

    def collect_all(self):
        start_total = datetime.now()
        print(f"\n[DataCollectorAgent] COLLECT ALL START at {start_total.time()}")

        results = {}
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(agent.collect): name for name, agent in self.sources.items()}
            for future in as_completed(futures):
                name = futures[future]
                try:
                    results[name] = future.result()
                    print(f"[{name}] Data collected: {len(results[name])} rows.")
                except Exception as e:
                    print(f"[{name}] Collection failed: {e}")

        end_total = datetime.now()
        print(f"[DataCollectorAgent] COLLECT ALL END at {end_total.time()} — Total Duration: {end_total - start_total}\n")
        return results