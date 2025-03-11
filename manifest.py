import os
import zipfile
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from json import loads, dumps

from requests import get

import tables

bungie_url = "https://www.bungie.net"
manifest_url = f"{bungie_url}/Platform/Destiny2/Manifest"
headers = {
    'x-api-key': 'cec16aab4aba4561a504c671c7161670',
}


def write_manifest(manifest_dict: dict, version: str):
    if os.path.exists("manifest.json"):
        with open("manifest.json", "r") as manifest_file:
            dictionary = loads(manifest_file.read())
            old_version = dictionary["Response"]["version"]
        if version == old_version:
            print("Manifest is up-to-date")
            return
    else:
        with open("manifest.json", "w") as manifest_file:
            manifest_file.write(dumps(manifest_dict, indent=4))


def download(url: str, return_type: str = '') -> str | dict | bytes | None:
    with get(f"{bungie_url}{url}", stream=True, allow_redirects=True, headers=headers) as response:
        response.raise_for_status()
        if return_type == "":
            return response.content
        if return_type.lower() == "json":
            return response.json()
        elif return_type.lower() == "text":
            return response.text
        else:
            return response.content


def download_manifest():
    return download("/Platform/Destiny2/Manifest", "json")


def download_database(db_url: str) -> bytes:
    bytes_array = download(db_url)
    return bytes_array


def extract_archive(bytes_array: bytes):
    with zipfile.ZipFile(BytesIO(bytes_array), "r") as archive:
        namelist = archive.namelist()[0]
        with archive.open(namelist, "r") as file:
            data = file.read()
        with open("world_content.db", "wb") as world_content:
            world_content.write(data)


def extract_dburl(manifest_dict: dict, lang: str = "en"):
    return manifest_dict["Response"]["mobileWorldContentPaths"][lang]


def extract_json_world_paths(manifest_dict: dict, lang: str = "en") -> dict:
    if lang not in tables.langs:
        raise Exception(f"Language '{lang}' not found.")
    return manifest_dict["Response"]["jsonWorldComponentContentPaths"][lang]


def extract_json_world_components(manifest_dict: dict, table_name: str = "DestinyInventoryItemDefinition",
                                  lang: str = "en") -> dict:
    if table_name not in tables.tables:
        raise Exception(f"Table '{table_name}' not found.")
    return manifest_dict["Response"]["jsonWorldComponentContentPaths"][lang][table_name]


def download_table(url: str, table_name: str):
    data = download(url, "text")
    with open(f"data/{table_name}.json", "w") as file:
        file.write(data)
    print(f"Table '{table_name}' downloaded.")


def download_json_files(table_names: dict):
    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(download_table, table_names.values(), table_names.keys())


if __name__ == "__main__":
    pass
