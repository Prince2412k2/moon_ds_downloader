from os import mkdir, path
import os
import requests
from bs4 import BeautifulSoup
import json
from tqdm import tqdm


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


def html_table_to_json_from_url(url):
    response = requests.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table")
    if not table:
        raise ValueError("No table found in the HTML.")

    # Extract headers
    headers = [
        cell.get_text(strip=True) for cell in table.find("tr").find_all(["th", "td"])
    ]

    # Extract rows
    data = []
    for row in table.find_all("tr")[1:]:
        cells = row.find_all(["td", "th"])
        if len(cells) != len(headers):
            continue
        row_data = {
            headers[i]: cells[i].get_text(strip=True) for i in range(len(cells))
        }
        data.append(row_data)

    return data


def get_all_base_ids(index):
    url = f"https://pds.lroc.asu.edu/data/LRO-L-LROC-2-EDR-V1.0/{index}/DATA/MAP/"
    json_data = html_table_to_json_from_url(url)
    os.makedirs("DATA", exist_ok=True)
    return [i["Name"] for i in json_data][1:]


def get_all_img_ids(index: str, base_id: str):
    url = f"https://pds.lroc.asu.edu/data/LRO-L-LROC-2-EDR-V1.0/{index}/DATA/MAP/{base_id}NAC/"
    json_data = html_table_to_json_from_url(url)

    os.makedirs(os.path.join("DATA", base_id), exist_ok=True)
    return [i["Name"][:-6] for i in json_data][1:]


def get_stero_pair(index, base_id, img_id):
    url = f"https://pds.lroc.asu.edu/data/LRO-L-LROC-2-EDR-V1.0/{index}/DATA/MAP/{base_id}NAC/{img_id}"
    img_path = os.path.join("DATA", base_id, img_id)
    os.makedirs(img_path, exist_ok=True)
    download_file_with_progress(f"{url}LE.IMG", f"{img_path}LE.IMG")
    download_file_with_progress(f"{url}RE.IMG", f"{img_path}RE.IMG")
    download_file_with_progress(f"{url}LE.xml", f"{img_path}LE.xml")
    download_file_with_progress(f"{url}RE.xml", f"{img_path}RE.xml")


def download_images(index="LROLRC_0001", max_samples=20):
    base_ids = get_all_base_ids(index)
    max = max_samples
    for base_id in base_ids:
        for img_id in set(get_all_img_ids(index, base_id)):
            if max < 1:
                exit(print("exceeded max samples"))
            get_stero_pair(index, base_id, img_id)
            max -= 1


download_images()
