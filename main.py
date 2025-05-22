from agents.data_collector.data_collector import (
    LoanDataCSVAgent,
    BankDataCSVAgent,
    WorldBankAPIAgent,
    DataCollectorAgent
)
from agents.preprocessor.preprocessor_execution import (
    run_parallel_preprocessing,
    run_sequential_preprocessing,
    enrich_bank_with_macro
)

def main():
    sources = {
        "loan_data": LoanDataCSVAgent("data/loan_data.csv"),
        "bank_data": BankDataCSVAgent("data/bank_data.csv"),
        "world_bank": WorldBankAPIAgent(indicators=[
            "FP.CPI.TOTL", 
            "NY.GDP.PCAP.CD"
        ])
    }

    collector = DataCollectorAgent(sources)
    dataframes = collector.collect_all()

    # Enrich and replace bank_data
    dataframes["bank_data"] = enrich_bank_with_macro(
        bank_df=dataframes["bank_data"],
        wb_df=dataframes["world_bank"]
    )

    # Preprocess


    processed = run_parallel_preprocessing(dataframes)  # or run_parallel_preprocessing(dataframes)

    print(processed)

    print("\n[✅ Final Output Shapes]")
    for name, df in processed.items():
        print(f"{name}: {df.shape}")

if __name__ == "__main__":
    main()