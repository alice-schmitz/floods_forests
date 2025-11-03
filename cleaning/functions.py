import geobr
from odc import stac as odc_stac
from pystac_client import Client
import pyproj
import xarray as xr
import rioxarray


# retrieve items from GFM catalog that are located within municipality boundaries
def query_gfm(muni_code, start_year, end_year, start_month, end_month):
    # Read in municipality shapefile
    muni = geobr.read_municipality(code_muni=muni_code, year=2020)
    bbox_muni = muni.total_bounds

    # Define area of interest
    minx, miny, maxx, maxy = bbox_muni
    aoi = {
        "type": "Polygon",
        "coordinates": [[
            [minx, maxy],  # top-left
            [maxx, maxy],  # top-right
            [maxx, miny],  # bottom-right
            [minx, miny],  # bottom-left
            [minx, maxy]   # close the polygon
        ]]
    }

    # set up STAC API search
    catalog = Client.open("https://stac.eodc.eu/api/v1")

    items = []
    for y in range(start_year, end_year + 1):
        for m in range(start_month, end_month + 1):
            end_day = ndays_thismonth(y, m)
            date_range = f"{y}-{str(m).zfill(2)}-01/{y}-{str(m).zfill(2)}-{end_day}"

            print(f"Searching {date_range}...")
            sub_items = search_and_retrieve(catalog, aoi, date_range)

            if len(sub_items) > 0:
                items.extend(sub_items)

    if len(items) == 0:
        print("No items found for this muni/year range")
        return None

    # Create xarray from items
    crs = pyproj.CRS.from_wkt(items[0].properties["proj:wkt2"])
    xx = odc_stac.load(
        items,
        bbox=bbox_muni,
        crs=crs,
        bands=["ensemble_flood_extent"],
        dtype="uint8",
        chunks={"x": 2000, "y": 2000, "time": -1},
        resolution=20
    )

    # Cast municipality shapefile to same CRS as GFM data
    muni = muni.to_crs(xx.rio.crs)

    # Clip GFM data to municipality boundaries
    xx_clipped = xx.rio.clip(muni.geometry.values, muni.crs, drop=True)

    return xx_clipped


def search_and_retrieve(cat, aoi, daterange):
    search = cat.search(
        collections="GFM",
        intersects=aoi,
        datetime=daterange
    )
    items = search.item_collection()
    print("   Found", len(items), "items.")
    return items


def ndays_thismonth(year, month):
    if month in [1, 3, 5, 7, 8, 10, 12]:
        return 31
    elif month == 2 and year % 4 == 0:
        return 29
    elif month == 2:
        return 28
    else:
        return 30


# calculate the max flood extent (binary map of flooded pixels)
def get_max_flood_extent(xx):
    xx = xx.ensemble_flood_extent

    # Filter the data to exclude values of 255 (nodata) and 0 (no-flood), then sum along time
    result = xx.where((xx != 255) & (xx != 0)).sum(dim="time")

    # Convert the result to binary (1 if flooded at least once)
    result = xr.where(result > 0, 1, 0).astype("uint8")

    computed_result = result.compute(sync=True)

    # Save result as GeoTIFF for inspection
    computed_result.rio.to_raster("max_flood.tif", compress="LZW")

    return computed_result
