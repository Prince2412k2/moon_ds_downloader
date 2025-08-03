import os
import requests
import json
from tqdm import tqdm
import pvl
import pandas as pd

BARE_URL = "https://pds.lroc.asu.edu/data/"
BASE_URL = f"{BARE_URL}/LRO-L-LROC-2-EDR-V1.0/"


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


def get_sldem_image(row, path):
    # Collect all latitudes and longitudes
    lats = [
        row["UPPER_LEFT_LATITUDE"],
        row["UPPER_RIGHT_LATITUDE"],
        row["LOWER_RIGHT_LATITUDE"],
        row["LOWER_LEFT_LATITUDE"],
    ]
    lons = [
        row["UPPER_LEFT_LONGITUDE"],
        row["UPPER_RIGHT_LONGITUDE"],
        row["LOWER_RIGHT_LONGITUDE"],
        row["LOWER_LEFT_LONGITUDE"],
    ]

    ulx = min(lons)  # west
    uly = max(lats)  # north
    lrx = max(lons)  # east
    lry = min(lats)  # south

    base_sldem_url = (
        "https://trek.nasa.gov/moon/TrekWS/rest/transform/latlon/subset/stream/tiff"
    )
    sldem_url = f"{base_sldem_url}?ulx={ulx}&uly={uly}&lrx={lrx}&lry={lry}"
    download_file_with_progress(sldem_url, os.path.join(path, "sldem.tif"))


def process_dataframe(df):
    def is_nac_image(row):
        path = row["FILE_SPECIFICATION_NAME"]
        return "NAC" in path and "MAP" in path

    # Columns to retain
    columns_to_keep = [
        "FILE_SPECIFICATION_NAME",
        "PRODUCT_ID",
        "UPPER_LEFT_LATITUDE",
        "UPPER_LEFT_LONGITUDE",
        "UPPER_RIGHT_LATITUDE",
        "UPPER_RIGHT_LONGITUDE",
        "LOWER_RIGHT_LATITUDE",
        "LOWER_RIGHT_LONGITUDE",
        "LOWER_LEFT_LATITUDE",
        "LOWER_LEFT_LONGITUDE",
    ]

    # Filter rows first
    filtered_rows = df[df.apply(is_nac_image, axis=1)]

    # Select only desired columns
    filtered_df = filtered_rows[columns_to_keep].copy()

    # Remove last 6 characters from path
    filtered_df.loc[:, "FILE_SPECIFICATION_NAME"] = filtered_df[
        "FILE_SPECIFICATION_NAME"
    ].str[:-6]

    # Deduplicate based on cleaned path
    deduplicated_df = filtered_df.drop_duplicates(
        subset="FILE_SPECIFICATION_NAME", keep="first"
    )

    return deduplicated_df


def save_coord_json(
    up_left_lat,
    up_left_longi,
    up_right_lat,
    up_right_longi,
    low_right_lat,
    low_right_longi,
    low_left_lat,
    low_left_longi,
    path,
):
    coordict = {
        "up_left_lat": up_left_lat,
        "up_left_longi": up_left_longi,
        "up_right_lat": up_right_lat,
        "up_right_longi": up_right_longi,
        "low_right_lat": low_right_lat,
        "low_right_longi": low_right_longi,
        "low_left_lat": low_left_lat,
        "low_left_longi": low_left_longi,
    }
    with open(os.path.join(path, "coordinates.json"), "w") as json_file:
        json.dump(coordict, json_file)


def get_headers_lbl(path):
    label = pvl.load(path)
    return [i[1]["NAME"] for i in label["INDEX_TABLE"][6:]]  # pyright: ignore


def get_index_tab(index):
    base_path = os.path.join("DATA", index)
    os.makedirs(base_path, exist_ok=True)
    print(f"Created a folder at {base_path}")

    lbl_path = os.path.join(base_path, "INDEX.LBL")
    tab_path = os.path.join(base_path, "INDEX.TAB")
    download_file_with_progress(f"{BASE_URL}{index}/INDEX/INDEX.LBL", lbl_path)
    print(f"Downloaded {index}/INDEX/INDEX.LBL")

    download_file_with_progress(f"{BASE_URL}/{index}/INDEX/INDEX.TAB", tab_path)
    print(f"Downloaded {index}/INDEX/INDEX.LBL")

    headers = get_headers_lbl(lbl_path)
    df = pd.read_csv(tab_path, sep=",", header=None, names=headers)
    return process_dataframe(df)


def get_stero_pair(id, index, url_path):
    url_path = url_path.strip()
    url = f"{BARE_URL}/{url_path}"
    id = id.strip()
    base_path = os.path.join("DATA", index)
    id_dir_path = os.path.join(base_path, id)
    os.makedirs(id_dir_path, exist_ok=True)
    id_path = os.path.join(id_dir_path, id)
    download_file_with_progress(f"{url}LE.IMG", f"{id_path}LE.IMG")
    download_file_with_progress(f"{url}RE.IMG", f"{id_path}RE.IMG")
    download_file_with_progress(f"{url}LE.xml", f"{id_path}LE.xml")
    download_file_with_progress(f"{url}RE.xml", f"{id_path}RE.xml")
    print(f"Downloaded {id}")
    return id_dir_path


def download_images(index, max_samples=5):
    """index -> LROLRC_0001,LROLRC_0002,LROLRC_0003 ...
    refer -> (https://pds.lroc.asu.edu/data/LRO-L-LROC-2-EDR-V1.0/) for more info
    """

    df = get_index_tab(index)
    for idx, row in enumerate(df.itertuples(index=False)):
        if "NAC" not in row.FILE_SPECIFICATION_NAME:  # pyright: ignore
            continue
        path = get_stero_pair(
            row.PRODUCT_ID[:-2],  # pyright: ignore
            index,
            row.FILE_SPECIFICATION_NAME,  # pyright: ignore
        )
        save_coord_json(
            row.UPPER_LEFT_LATITUDE,  # pyright: ignore
            row.UPPER_LEFT_LONGITUDE,  # pyright: ignore
            row.UPPER_RIGHT_LATITUDE,  # pyright: ignore
            row.UPPER_RIGHT_LONGITUDE,  # pyright: ignore
            row.LOWER_RIGHT_LATITUDE,  # pyright: ignore
            row.LOWER_RIGHT_LONGITUDE,  # pyright: ignore
            row.LOWER_LEFT_LATITUDE,  # pyright: ignore
            row.LOWER_LEFT_LONGITUDE,  # pyright: ignore
            path,
        )  # pyright: ignore
        get_sldem_image(row, path)

        if idx > max_samples - 2:
            print("Max samples reached")
            break


download_images("LROLRC_0001", 2)
