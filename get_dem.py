import os
import subprocess
from tqdm import tqdm
import requests


def download_file_with_progress(url, output_path):
    response = requests.get(url, stream=True)
    response.raise_for_status()

    total_size = int(response.headers.get("content-length", 0))
    block_size = 1024  # 1 Kibibyte

    with (
        open(output_path, "wb") as file,
        tqdm(
            desc=output_path,
            total=total_size,
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
        ) as bar,
    ):
        for data in response.iter_content(block_size):
            file.write(data)
            bar.update(len(data))


MOON_TREK_BASE = (
    "https://trek.nasa.gov/moon/TrekWS/rest/transform/latlon/subset/stream/tiff"
)
SLDEM_SRC = "LRO_LOLAKaguya_DEM_60N60S_512ppd"


def download_patch(west, north, east, south, out_dir="dem"):
    """Download a Moon SLDEM patch with visible curl progress bar."""
    os.makedirs(out_dir, exist_ok=True)

    filename = os.path.join(out_dir, f"{west}_{north}_{east}_{south}.tif")
    url = f"{MOON_TREK_BASE}?src={SLDEM_SRC}&ulx={west}&uly={north}&lrx={east}&lry={south}"

    print(f"\nDownloading patch:\n{url}")
    print(f"Saving to: {filename}\n")

    download_file_with_progress(url, filename)


"""download_patch(west, north, east, south)"""
West = -0.1648
East = +0.1648
North = +0.1648
South = -0.1648

download_patch(West, East, North, South)
