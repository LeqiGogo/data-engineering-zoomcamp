import sys
import pandas as pd


print("arguments", sys.argv) # sys.argv: pass the parameter entered in the bash command to the py

day = int(sys.argv[1]) # day = the 1st parameter entered
print(f"Running pipeline for day {day}")



df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
print(df.head())

df.to_parquet(f"output_day_{sys.argv[1]}.parquet")

