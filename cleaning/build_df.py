#%% Import packages
import pandas as pd
import time
import functions
import geobr
from shapely.geometry import box
import numpy as np

#%% Set years to query
years = range(2021, 2022)
munis = geobr.read_municipality(year=2020)

muni_codes = munis['code_muni'].unique()
# For now, subset to a few munis
muni_codes = muni_codes[:20]
len(muni_codes)

#%% Get list of municipalities
import pandas as pd
import numpy as np

def build_flood_dataframe(muni_codes, start_year, end_year, start_month, end_month):
    records = []

    for code in muni_codes:
        print(f"\n Processing municipality {code}")

        try:
            xx_clipped = functions.query_gfm(code, start_year, end_year, start_month, end_month)

            if xx_clipped is None:
                continue

            flood_map = functions.get_max_flood_extent(xx_clipped)

            valid_pixels = np.count_nonzero((flood_map.values != 255))
            flooded_pixels = np.count_nonzero((flood_map.values == 1))

            flood_ratio = flooded_pixels / valid_pixels if valid_pixels > 0 else np.nan

            records.append({
                "muni_code": code,
                "start_year": start_year,
                "end_year": end_year,
                "start_month": start_month,
                "end_month": end_month,
                "flooded_pixels": flooded_pixels,
                "valid_pixels": valid_pixels,
                "flood_ratio": flood_ratio
            })

        except Exception as e:
            print(f"Error for muni {code}: {e}")
            continue

    df = pd.DataFrame(records)
    return df

flood_df = build_flood_dataframe(
    muni_codes=muni_codes,
    start_year=2022,
    end_year=2022,
    start_month=9,
    end_month=9
)

flood_df
# %%
