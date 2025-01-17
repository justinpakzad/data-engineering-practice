import json
import glob
import csv
import os
from pathlib import Path


def recursive_parser_json(json_data, col_name=""):
    flattened_data = {}
    for key, value in json_data.items():
        extended_col_name = f"{col_name}_{key}" if col_name else key
        if isinstance(value, dict):
            flattened_data.update(recursive_parser_json(value, extended_col_name))
        elif isinstance(value, list):
            for i, val in enumerate(value, start=1):
                flattened_data.update({f"{extended_col_name}_{i}": val})
        else:
            flattened_data[extended_col_name] = value
    return flattened_data


def main():
    for item in glob.glob(f"data/**/*.json", recursive=True):
        with open(item, "r") as f:
            json_data = json.load(f)
        flattened_data = recursive_parser_json(json_data)
        if not os.path.isdir("data/csvs"):
            os.mkdir("data/csvs")
        file_name = f'{item.split("/")[-1].split(".json")[0]}.csv'
        field_names = list(flattened_data.keys())
        with open(f"data/csvs/{file_name}", "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            writer.writeheader()
            writer.writerow(flattened_data)


if __name__ == "__main__":
    main()
