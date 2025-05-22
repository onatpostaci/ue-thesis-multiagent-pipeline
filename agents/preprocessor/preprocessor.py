import pandas as pd
from sklearn.preprocessing import StandardScaler

class PreprocessorAgent:
    def __init__(self, name="PreprocessorAgent"):
        self.name = name
        self.scaler = StandardScaler()

    def preprocess_bank_data(self, df: pd.DataFrame) -> pd.DataFrame:
        print(f"[{self.name}] Preprocessing BANK data...")

        df["y"] = df["y"].map({"yes": 1, "no": 0})

        binary_columns = ["default", "housing", "loan"]
        for col in binary_columns:
            df[col] = df[col].map({"yes": 1, "no": 0})

        df = pd.get_dummies(df, columns=[
            "job", "marital", "education", "contact", "month", "poutcome"
        ], drop_first=True)

        num_cols = ["age", "balance", "day", "duration", "campaign", "pdays", "previous"]
        df[num_cols] = self.scaler.fit_transform(df[num_cols])

        df = df.dropna()  # Final NaN cleanup after transformation

        print(f"[{self.name}] BANK data preprocessing complete. Shape: {df.shape}")
        return df

    def preprocess_loan_data(self, df: pd.DataFrame) -> pd.DataFrame:
        print(f"[{self.name}] Preprocessing LOAN data...")

        df = df.drop(columns=["LoanID"])
        for col in ["HasMortgage", "HasDependents", "HasCoSigner"]:
            df[col] = df[col].map({"yes": 1, "no": 0})

        df = pd.get_dummies(df, columns=[
            "Education", "EmploymentType", "MaritalStatus", "LoanPurpose"
        ], drop_first=True)

        numeric_cols = [
            "Age", "Income", "LoanAmount", "CreditScore",
            "MonthsEmployed", "NumCreditLines",
            "InterestRate", "LoanTerm", "DTIRatio"
        ]
        df[numeric_cols] = self.scaler.fit_transform(df[numeric_cols])

    
        print(f"[{self.name}] LOAN data preprocessing complete. Shape: {df.shape}")
        return df

    def preprocess_worldbank_data(self, df: pd.DataFrame) -> pd.DataFrame:
        print(f"[{self.name}] Preprocessing WORLD BANK data...")

        if df.empty or "date" not in df.columns:
            print(f"[{self.name}] Empty or invalid World Bank dataframe.")
            return df

        df = df.dropna(how='all', subset=df.columns.difference(['date']))
        df["date"] = df["date"].astype(int)

        # Normalize all indicator columns
        indicator_cols = df.columns.difference(["date"])
        df[indicator_cols] = self.scaler.fit_transform(df[indicator_cols])

        df = df.dropna()
        print(f"[{self.name}] WORLD BANK data preprocessing complete. Shape: {df.shape}")
        return df