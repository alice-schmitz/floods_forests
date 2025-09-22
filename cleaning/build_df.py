#%% Import packages
import pandas as pd
import time
import functions
import geobr

#%% Set years to query
years = range(2021, 2022)

#%% Get list of municipalities
munis = geobr.read_municipality(year=2020)
muni_codes = munis['code_muni'].unique()
# For now, subset to a few munis
muni_codes = muni_codes[:20] 

#%% Build panel dataframe

results = []

for year in years:
    for muni_code in muni_codes:
        try:
            print(f"Processing {muni_code} for {year}...")

            # Query and process
            xx = functions.query_gfm(muni_code, year, year, 1, 12)
            flood_extent = functions.get_max_flood_extent(xx)
            flooded_pixels = int((flood_extent == 1).sum().item())

            results.append({
                "code_muni": muni_code,
                "year": year,
                "flooded_pixels": flooded_pixels
            })

        except Exception as e:
            print(f"Skipping {muni_code}, {year} due to error: {e}")
            continue

        time.sleep(1)

    # Convert to dataframe and append to disk after each year
    df_year = pd.DataFrame(results)
    df_year.to_csv("flooded_pixels_panel.csv", mode="a", header=(year == years[0]), index=False)
    results = []  # reset for next year



