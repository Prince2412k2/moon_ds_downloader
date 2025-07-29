import math
import os
import requests
from itertools import product

# Constants
MOON_TREK_BASE = (
    "https://trek.nasa.gov/moon/TrekWS/rest/transform/latlon/subset/stream/tiff"
)
WAC_SRC = "LRO_WAC_Mosaic_Global_303ppd"
SLDEM_SRC = "LRO_LOLAKaguya_DEM_60N60S_512ppd"
DEG_PER_KM = 1 / 30.3  # 303 pixels per degree ≈ 100m/pixel


# Define the bounding box function
def make_bbox(lat, lon, size_km):
    delta = (size_km / 2) * DEG_PER_KM
    return {
        "ulx": lon - delta,
        "uly": lat + delta,
        "lrx": lon + delta,
        "lry": lat - delta,
    }


os.makedirs(os.path.join("moon_dataset", "wac"), exist_ok=True)
os.makedirs(os.path.join("moon_dataset", "dem"), exist_ok=True)


# Define the download function
def download_patch(lat, lon, size_km, output_dir, prefix):
    os.makedirs(output_dir, exist_ok=True)
    bbox = make_bbox(lat, lon, size_km)

    results = {}
    for src, label in [(WAC_SRC, "wac"), (SLDEM_SRC, "dem")]:
        url = (
            f"{MOON_TREK_BASE}?src={src}"
            f"&ulx={bbox['ulx']}&uly={bbox['uly']}"
            f"&lrx={bbox['lrx']}&lry={bbox['lry']}"
        )
        filename = os.path.join(output_dir, label, f"{prefix}_{label}.tif")
        try:
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            with open(filename, "wb") as f:
                f.write(r.content)
            results[label] = filename
        except Exception as e:
            results[label] = f"Error: {e}"
    return results


# Generate lat/lon grid
def generate_lat_lon_grid(start_lat, end_lat, start_lon, end_lon, step_deg):
    return [
        (lat, lon)
        for lat, lon in product(
            frange(start_lat, end_lat, step_deg), frange(start_lon, end_lon, step_deg)
        )
    ]


# Float range helper
def frange(start, stop, step):
    while start < stop:
        yield round(start, 6)
        start += step


def main():
    # Directory to store dataset
    output_dir = os.path.realpath("moon_dataset")
    os.makedirs(output_dir, exist_ok=True)

    # Define the dataset region and grid
    start_lat, end_lat = -2.0, 2.0  # small test range; you can expand later
    start_lon, end_lon = -2.0, 2.0
    step_deg = 1.0  # 1° step = ~30 km
    patch_size_km = 25.6  # same as earlier: ~256 px at 100m/px

    # Generate lat/lon grid
    grid_points = generate_lat_lon_grid(
        start_lat, end_lat, start_lon, end_lon, step_deg
    )

    # Download loop
    download_results = []
    for i, (lat, lon) in enumerate(grid_points):
        prefix = f"patch_{lat}_{lon}"
        print(
            f"📦 Downloading patch {i + 1}/{len(grid_points)} at lat={lat}, lon={lon}"
        )
        result = download_patch(lat, lon, patch_size_km, output_dir, prefix)
        download_results.append((lat, lon, result))

    print("DONE")  # Show a few download results
