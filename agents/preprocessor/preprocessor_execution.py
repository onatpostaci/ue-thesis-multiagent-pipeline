import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from agents.preprocessor.preprocessor import PreprocessorAgent
from time import perf_counter

def enrich_bank_with_macro(bank_df: pd.DataFrame, wb_df: pd.DataFrame) -> pd.DataFrame:
    month_map = {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                 'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12}
    bank_df["month_num"] = bank_df["month"].str.lower().map(month_map)
    bank_df["year"] = 2008

    enriched_df = pd.merge(bank_df, wb_df, left_on="year", right_on="date", how="left")
    enriched_df = enriched_df.drop(columns=["date", "year", "month_num"])

    enriched_df = enriched_df.dropna()  # Drop any records missing macro indicators
    return enriched_df

def run_parallel_preprocessing(df):
    preprocessor = PreprocessorAgent()

    tasks = {
        "bank_data": lambda: preprocessor.preprocess_bank_data(df["bank_data"]),
        "loan_data": lambda: preprocessor.preprocess_loan_data(df["loan_data"]),
        "world_bank": lambda: preprocessor.preprocess_worldbank_data(df["world_bank"])
    }

    results = {}
    start_time = perf_counter()
    print("\n[Preprocessing] Parallel execution starting...")

    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(func): name for name, func in tasks.items()}
        for future in as_completed(futures):
            name = futures[future]
            try:
                results[name] = future.result()
                print(f"[{name}] ✅ Preprocessing completed. Rows: {len(results[name])}")
            except Exception as e:
                print(f"[{name}] ❌ Preprocessing failed: {e}")

    end_time = perf_counter()
    print(f"\n[Preprocessing] Total execution time: {end_time - start_time:.3f} seconds\n")
    return results

def run_sequential_preprocessing(df):
    preprocessor = PreprocessorAgent()

    tasks = {
        "bank_data": lambda: preprocessor.preprocess_bank_data(df["bank_data"]),
        "loan_data": lambda: preprocessor.preprocess_loan_data(df["loan_data"]),
        "world_bank": lambda: preprocessor.preprocess_worldbank_data(df["world_bank"])
    }

    results = {}
    start_time = perf_counter()
    print("\n[Preprocessing] Sequential execution starting...")

    for name, func in tasks.items():
        try:
            result = func()
            results[name] = result
            print(f"[{name}] ✅ Preprocessing completed. Rows: {len(result)}")
        except Exception as e:
            print(f"[{name}] ❌ Preprocessing failed: {e}")

    end_time = perf_counter()
    print(f"\n[Preprocessing] Total execution time: {end_time - start_time:.3f} seconds\n")
    return results