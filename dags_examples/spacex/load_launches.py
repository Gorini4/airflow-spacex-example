import requests
import argparse
import csv
import os
from datetime import datetime


def parse_launch_record(r: dict):
    return [
        r['flight_number'],
        r['mission_name'],
        r['launch_year'],
        r['launch_date_utc'],
        r['rocket']['rocket_id'],
        r['rocket']['rocket_name'],
        r['rocket']['rocket_type'],
        r['launch_site']['site_name_long'],
        r['launch_success'],
        r['launch_failure_details']['reason'] if 'launch_failure_details' in r else '',
    ]

def load_json(launch_year: int, rocket: str):
    url = 'https://api.spacexdata.com/v3/launches/past'
    payload = {'launch_year': launch_year}
    if rocket != '':
        payload['rocket_id'] = rocket
    headers = {}
    response = requests.request('GET', url, headers = headers, params = payload, allow_redirects=False)
    return [parse_launch_record(r) for r in response.json()]

def write_csv(data: tuple, base_dir: str, year: int, rocket: str):
    dir_name = "{}/year={}/rocket={}/".format(
        base_dir,
        year,
        rocket if rocket != '' else 'all'
    )
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    filename = dir_name + 'data.csv'
    with open(filename, mode='w') as launch_file:
        launch_writer = csv.writer(launch_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for r in data:
            launch_writer.writerow(r)

def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Loads data about SpaxeX launches')
    # parser.add_argument('-s', dest='start_date', type=valid_date, required=True, help='filter launches by date (YYYY-MM-DD)')
    # parser.add_argument('-e', dest='end_date', type=valid_date, required=True, help='filter launches by date (YYYY-MM-DD)')
    parser.add_argument('-o', dest='output_dir', type=str, required=True, help='output directory for csv files')
    parser.add_argument('-y', dest='launch_year', type=int, required=True, help='filter by launch year (e.g. 2018)')
    parser.add_argument('-r', dest='rocket', type=str, default='', help='rocket type (e.g. "falcon9")')

    args = parser.parse_args()
    print(args)

    data = load_json(args.launch_year, args.rocket)
    write_csv(data, args.output_dir, args.launch_year, args.rocket)



