from process_data.raw_data_download import download_geo_coord, download_datagouv, download_opendatasoft, download_datagouv_france2017, download_datagouv_france2022, download_opendatasoft_france2017, download_opendatasoft_france2022
from process_data.feed_data_to_postgresql import insert_france2017, insert_france2022
import argparse, os


def extract():
    download_geo_coord()
    download_datagouv()
    download_opendatasoft()



STAGES = {
    "extract": extract,
    "download_geo_coord": download_geo_coord,
    "download_datagouv_france2017": download_datagouv_france2017,
    "download_datagouv_france2022" : download_datagouv_france2022,
    "download_opendatasoft_france2017" : download_opendatasoft_france2017,
    "download_opendatasoft_france2022" : download_opendatasoft_france2022,
    "france2017": insert_france2017,
    "france2022": insert_france2022,
}


parser = argparse.ArgumentParser()
parser.add_argument(
    "--stage",
    choices=["extract", "download_geo_coord", "download_datagouv_france2017", "download_datagouv_france2022", "download_opendatasoft_france2017", "download_opendatasoft_france2022", "france2017", "france2022", "all"],
    required=True,
)

args = parser.parse_args()

if args.stage == "all":
    extract()
    insert_france2017()
    insert_france2022()
else:
    STAGES[args.stage]()
