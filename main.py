import pathlib

from manifest import download_manifest, write_manifest, extract_dburl, extract_json_world_paths, download_database, \
    extract_archive, download_json_files


def main():
    pathlib.Path("data").mkdir(parents=True, exist_ok=True)

    manifest: dict = download_manifest()
    new_version = manifest["Response"]["version"]
    write_manifest(manifest, new_version)
    db_url = extract_dburl(manifest)
    db_bytes = download_database(db_url)
    extract_archive(db_bytes)
    tables = extract_json_world_paths(manifest)
    download_json_files(tables)


if __name__ == '__main__':
    main()
