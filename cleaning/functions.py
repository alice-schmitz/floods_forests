# functions.py: functions for GFM processing

import geobr
from odc import stac as odc_stac
from pystac_client import Client
import pyproj
import xarray as xr
import rioxarray
from shapely.geometry import Polygon

# retrieve items from GFM catalog that are located within municipality boundaries
# and within the specified date range
# save as xarray, clip the data to municipality boundaries
def query_gfm(muni_code,start_year,end_year,start_month,end_month):

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

    # Get bounding box from AOI
    #polygon = Polygon(aoi['coordinates'][0])

    #set up to search the STAC API
    catalog = Client.open("https://stac.eodc.eu/api/v1")

    items = [] 
    
    # Search iteratively to avoid too large a cache 
    for y in range(start_year,end_year+1):
        for m in range(start_month,end_month+1):
            end_day = ndays_thismonth(y, m)
            date_range = f"{y}-{str(m).zfill(2)}-01/{y}-{str(m).zfill(2)}-{end_day}"
            
            print(f"Searching {date_range}...")
            sub_items = search_and_retrieve(catalog, aoi, date_range)
            if len(sub_items) > 0:
              items.extend(items)  # append to list
              #save_as_xarray(items, bbox_muni, f"{y}_{m}")

    # Create xarray from items
    crs = pyproj.CRS.from_wkt(items[0].properties["proj:wkt2"])
    xx = odc_stac.load(
        items, 
        bbox=bbox_muni,
        crs=crs,
        bands=["ensemble_flood_extent"],
        dtype="uint8",
        chunks={"x": 2000, "y": 2000, "time": -1}, 
        resolution=20)
    
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

    # Retrieve all found items
    items = search.item_collection()
    print("We found", len(items), "items, that match our filter criteria.")
    return items

def save_as_xarray(items, poly, name):
    crs = pyproj.CRS.from_wkt(items[0].properties["proj:wkt2"])

    # Load asset data into xarray using odc-stac
    xx = odc_stac.load(
        items,
        bbox=poly,
        crs=crs,
        bands=["ensemble_flood_extent"],
        dtype="uint8",
        chunks={"x": 2000, "y": 2000, "time": -1}, 
        resolution=20)

    # Overwrite a single temporary file instead of saving many
    xx.to_zarr("GFM_temp.zarr", mode="w")


def ndays_thismonth(year, month):
    if month in [1, 3, 5, 7, 8, 10, 12]:
        end_day = 31
    elif month == 2 and year % 4 == 0:
        end_day = 29
    elif month == 2:
        end_day = 28 
    else:
        end_day = 30

    return end_day

#subset to tiles (time slices) with non-missing data
def keep_non_missing(xx):
    flood = xx["ensemble_flood_extent"]
    valid_mask = flood != 255

    # for each time slice, check if there are any pixels with non-missing data (!=255)
    has_valid_data = valid_mask.any(dim=["x", "y"])

    # filter the xarray to only include time slices with valid data
    xx_filtered = xx.sel(time=has_valid_data)

    return xx_filtered

# calculate the total number of pixels which flooded
# at least once during the time period
def count_flooded_pixels(xx):
    # max over all time slices
    flood = xx["ensemble_flood_extent"]

    # Create Boolean mask: True where flooded (==1), False elsewhere
    was_flooded = (flood == 1)

    # Collapse over time: True if flooded in any time slice
    flood_composite = was_flooded.any(dim="time").astype("uint8")

    flood_composite.rio.to_raster("flood_composite.tif")

    # Count flooded pixels (1s in the composite)
    total_flooded_pixels = int(flood_composite.sum().compute())

    return total_flooded_pixels

def get_max_flood_extent(xx):
    xx = xx.ensemble_flood_extent

    # Filter the data to exclude values of 255 (nodata) and 0 (no-flood), then sum
    # along the "time" dimension 
    result = xx.where((xx != 255) & (xx != 0)).sum(dim="time")

    # Convert the result to binary (1 where the sum is greater than 0, otherwise 0)
    # and set the data type to uint8 
    result = xr.where(result > 0, 1, 0).astype("uint8")

    # Compute the result
    computed_result = result.compute(sync=True)

    # Save the computed result to a GeoTIFF file with LZW compression
    computed_result.rio.to_raster("max_flood.tif", compress="LZW")

    return computed_result