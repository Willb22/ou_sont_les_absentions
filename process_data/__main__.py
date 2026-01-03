from process_data.raw_data_download import download_geo_coord, download_datagouv, download_opendatasoft
from process_data.feed_data_to_postgresql import insert_france2017, insert_france2022
import argparse

def extract():
    download_geo_coord()
    download_datagouv()
    download_opendatasoft()


STAGES = {
    "extract": extract,
    "france2017": insert_france2017,
    "france2022": insert_france2022,
}


parser = argparse.ArgumentParser()
parser.add_argument(
    "--stage",
    choices=["extract", "france2017", "france2022", "all"],
    required=True,
)

args = parser.parse_args()

if args.stage == "all":
    extract()
    insert_france2017()
    insert_france2022()
else:
    STAGES[args.stage]()
