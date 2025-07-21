import pandas as pd

df = pd.read_csv("boa_data_2025_26.csv")

print("Columns:", df.columns)
print("Sample rows:")
print(df.head())

scottish_keywords = ["Beinn", "Dumfries", "Scottish", "Isle", "Cairn"]

def is_scottish(name):
    return any(keyword.lower() in str(name).lower() for keyword in scottish_keywords)

df_scotland = df[df["Generator_Full_Name"].apply(is_scottish)]

print(f"Total rows in dataset: {len(df)}")
print(f"Rows filtered for Scotland: {len(df_scotland)}")

df_scotland.to_csv("boa_data_scotland.csv", index=False)
