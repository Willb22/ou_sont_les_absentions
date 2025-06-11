from process_data.raw_data_download import download_geo_coord, download_datagouv, download_opendatasoft
from process_data.feed_data_to_postgresql import insert_france2017, insert_france2022

download_geo_coord()
download_datagouv()
download_opendatasoft()

insert_france2017()
insert_france2022()
