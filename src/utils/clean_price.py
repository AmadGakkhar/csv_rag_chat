import pandas as pd
import re


def clean_price_column(df):

    # Normalize column names
    df.columns = df.columns.str.strip().str.lower()

    # Identify the price column
    price_column = None
    for col in df.columns:
        if "price" in col:
            price_column = col
            break

    if price_column is None:
        raise ValueError("No price column found in the CSV file.")

    # Clean the price column
    def clean_price(value):
        if pd.isna(value) or value in [
            "N/A",
            "Call for price",
            "Call For Price",
            "call for price",
            "FREE",
            "Free",
            "free",
            "N/A",
            "n/a",
            "N/a",
            "N/A",
            "N/a",
        ]:
            return None
        value = re.sub(r"[^\d.]", "", str(value))
        try:
            return float(value)
        except ValueError:
            return None

    df[price_column] = df[price_column].apply(clean_price)

    return df


# Example usage
# cleaned_df = clean_price_column('/path/to/your/csvfile.csv')
# cleaned_df.to_csv('/path/to/your/cleaned_csvfile.csv', index=False)
