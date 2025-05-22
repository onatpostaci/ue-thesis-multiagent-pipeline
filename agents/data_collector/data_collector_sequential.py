class DataCollectorSequentialAgent:
    def __init__(self, sources):
        self.sources = sources

    def collect_all(self):
        from datetime import datetime
        start_total = datetime.now()
        print(f"\n[DataCollectorAgent] SEQUENTIAL COLLECT START at {start_total.time()}")

        results = {}
        for name, agent in self.sources.items():
            try:
                print(f"\n>>> Running {name} sequentially...")
                start = datetime.now()
                df = agent.collect()
                end = datetime.now()
                duration = end - start
                results[name] = df
                print(f"[{name}] Finished in {duration}. Rows: {len(df)}")
            except Exception as e:
                print(f"[{name}] Collection failed: {e}")

        end_total = datetime.now()
        print(f"\n[DataCollectorAgent] SEQUENTIAL COLLECT END at {end_total.time()} — Total Duration: {end_total - start_total}\n")
        return results