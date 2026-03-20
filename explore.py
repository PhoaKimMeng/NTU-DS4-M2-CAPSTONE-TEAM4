import os
import pandas as pd

path = "/home/hoa_im_eng/NTU-DS4-M2-CAPSTONE-TEAM4-1/downloaded_data(backup)"
files = [f for f in os.listdir(path) if f.endswith(".csv")]

for f in files:
    df = pd.read_csv(os.path.join(path, f), nrows=0)
    print(f"--- {f} ---")
    print(df.columns.tolist())
    print("\n")
