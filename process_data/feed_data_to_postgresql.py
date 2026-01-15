import pandas as pd
import dask.dataframe as dd
from sqlalchemy import MetaData, String, Integer, Float
from sqlalchemy.orm import sessionmaker
import os, sys

def allow_imports():
    current_directory = os.path.dirname(__file__)
    parent_directory = os.path.abspath(os.path.join(current_directory, os.pardir))
    if parent_directory not in sys.path:
        sys.path.append(parent_directory)
allow_imports()
from db_connections import Connectdb, log_memory_after, database_name, table_connection, User_france2017, User_france2022
from config import configurations, logging

current_directory = os.path.dirname(__file__)
project_directory = os.path.abspath(os.path.join(current_directory, os.pardir))

path_geo_coords = f"{project_directory}{configurations['raw_data_sources']['path_geo_coords']}"
path_datagouv_france2017 = f"{project_directory}{configurations['raw_data_sources']['france2017']['path_datagouv_france2017']}"
path_opendatasoft_france2017 = f"{project_directory}{configurations['raw_data_sources']['france2017']['path_opendatasoft_france2017']}"
path_datagouv_france2022 = f"{project_directory}{configurations['raw_data_sources']['france2022']['path_datagouv_france2022']}"
path_opendatasoft_france2022 = f"{project_directory}{configurations['raw_data_sources']['france2022']['path_opendatasoft_france2022']}"

dask_read_block_size = configurations['ram_memory_settings']['dask_read_block_size']
dask_paris_partitions = configurations['ram_memory_settings']['dask_paris_partitions']
dask_partitions_table_insert = configurations['ram_memory_settings']['dask_partitions_table_insert']
insert_rows_per_batch = configurations['ram_memory_settings']['insert_rows_per_batch']
insert_method = configurations['table_insertions']['insertion_method']
orm_multiple_inserts = configurations['table_insertions']['orm_multiple_inserts']
class Table_inserts(Connectdb):
    def __init__(self):
        super().__init__(database_name=database_name, table_connection=table_connection)
        self.table_name = ''
        self.opendatasoft_cols_to_read = list()
        self.opendatasoft_col_types = dict()
        self.path_opendatasoft = ''
        self.dask_read_block_size = None
        self.path_geo_coords = path_geo_coords



    def create_denomination_complete(self, df):
        df['dénomination complète'] = df['Libellé du département'] + ' (' + df['Code du département'] + ')'
        # cols = ['Libellé du département', 'Code du département']
        # df['dénomination complète'] = df[cols].apply(lambda row : '{} ({})'.format(*cols), axis=1)
        return df

    def default_read(self, path_read, path_write):
        if hasattr(self, 'path_opendatasoft'):
            if path_read == self.path_opendatasoft:
                df = pd.read_csv(path_read, sep=';', lineterminator='\r', low_memory=False)
            else:
                df = pd.read_csv(path_read)
        df = df.dropna()
        df.to_csv(path_write)

    def paris_geo_coords(self):
        geo = dd.read_csv(
            self.path_geo_coords,
            dtype="object",
            assume_missing=True,
            blocksize=self.dask_read_block_size
        )
        geo["code_postal"] = geo["code_postal"].fillna("00")
        geo_paris = geo[geo["code_postal"].str.startswith("75")]
        geo_paris["col_merge"] = (
                geo_paris["circonscription_code"].astype(str)
                + "_"
                + geo_paris["code_postal"].str[-2:]
                + geo_paris["code"].astype(str).str[-2:]
        )
        return geo_paris

    def process_raw_opendatasoft(self):
        header_chunk = pd.read_csv(self.path_opendatasoft, index_col=False, nrows=0, sep=';').columns
        all_csv_cols = header_chunk.tolist()
        logging.info(f'header chunk read from {self.path_opendatasoft} are {header_chunk}')
        col_indices_to_read = [all_csv_cols.index(col) for col in self.opendatasoft_cols_to_read]

        df = dd.read_csv(self.path_opendatasoft, sep=';', lineterminator='\r', usecols=col_indices_to_read,
                         blocksize=self.dask_read_block_size,
                         dtype=self.opendatasoft_col_types)
        logging.info(f'After READ file {self.path_opendatasoft} Dataframe partitions is {df.npartitions}')
        df = df.dropna()
        df = df[self.opendatasoft_cols_to_read]
        nested_coordinates_label = self.opendatasoft_cols_to_read[0]
        df['latitude'] = df[nested_coordinates_label].apply(lambda x: float(x.split(',')[0]) if type(x) is str else x,
                                                 meta=df[nested_coordinates_label])
        df['longitude'] = df[nested_coordinates_label].apply(lambda x: float(x.split(',')[1]) if type(x) is str else x,
                                                  meta=df[nested_coordinates_label])
        df = df.drop(nested_coordinates_label, axis=1)
        df['Code du département'] = df['Code du département'].apply(lambda x: str(x)[1:], meta=df[
            'Code du département'])  # truncate unwanted '\n'
        df = self.ammend_pourcentage_abs_col(df)
        return df

    def ammend_pourcentage_abs_col(self, df):
        df['% Abs/Ins'] = df['% Abs/Ins'].apply(lambda x: x.replace(',', '.') if type(x) == str else x,
                                                meta=df['% Abs/Ins'])
        df = df.rename(columns={'% Abs/Ins': 'Pourcentage_Abstentions'})
        return df

    def insert_to_db(self, df):
        conn, cursor = self.connect_driver()
        dbExists = self.check_database_exists(conn, cursor)
        if dbExists is False:
            self.create_db(cursor)
        self.conn_orm, self.db, self.uri = self.connect_orm()
        Session = sessionmaker(autocommit=False, autoflush=False, bind=self.db)
        self.session = Session()


        logging.info(log_memory_after('retrieve dataframe before SQL'))
        metadata_obj = MetaData()
        metadata_obj.create_all(self.db)
        logging.info(log_memory_after('SQL ORM configs '))
        col_types = dict()
        col_types['Code du département'] = String
        col_types['Libellé du département'] = String
        col_types['Libellé de la commune'] = String
        col_types['Inscrits'] = Integer
        col_types['Abstentions'] = Integer
        col_types['Pourcentage_Abstentions'] = Float
        col_types['latitude'] = Float
        col_types['longitude'] = Float
        col_types['dénomination complète'] = String
        col_types['Adresse complète'] = String
        df = df.repartition(npartitions=dask_partitions_table_insert)
        # df = df.persist()  # if on a distributed system

        logging.info(f'After repartition Dataframe partitions is {df.npartitions}')
        logging.info(f'insert_rows_per_batch is {insert_rows_per_batch}')
        logging.info(f'insert_method is {insert_method}')
        logging.info(f'orm_multiple_inserts is {orm_multiple_inserts}')


        if insert_method == 'builtin_pandas':
            for i in range(df.npartitions):
                partition = df.get_partition(i)
                if i == 0:
                    partition.to_sql(self.table_name, uri=self.uri, if_exists='replace', index=False, chunksize = insert_rows_per_batch, method='multi',
                                     dtype=col_types)
                if i > 0:
                    partition.to_sql(self.table_name, uri=self.uri, if_exists='append', index=False, chunksize = insert_rows_per_batch, method='multi',
                                     dtype=col_types)
        else:
            self.define_mapper_france()
            partitions = df.to_delayed()
            logging.info(f'table cols are {self.table_object.c.keys()}')
            for delayed_partition in partitions:
                partition = delayed_partition.compute()  # Now it's a Pandas DataFrame
                data_to_insert = [{key.replace(' ', '_') : val for key, val in row.items()} for row in partition.to_dict(orient='records')]
                #logging.info(f'Data to insert is {data_to_insert}')
                if orm_multiple_inserts:
                    self.conn_orm.execute(self.table_object.insert(), data_to_insert)
                    self.conn_orm.commit()
                else:# Abandon this clause, runtime too high
                    for i, row in enumerate(data_to_insert):
                        row = {key.replace(' ', '_') : val for key, val in row.items()}
                        if i < 3:
                            logging.info(f'row to insert is {row}')
                        ins = self.table_object.insert().values(**row)
                        self.conn_orm.execute(ins)
                        self.conn_orm.commit()
        logging.info(log_memory_after(f'sql insertion {self.table_name} with method {insert_method}'))

                #session.commit() # If using session

class Process_france2017(Table_inserts):
    def __init__(self, path_opendatasoft):
        super().__init__()
        self.path_datagouv_france2017 = path_datagouv_france2017
        self.path_opendatasoft = path_opendatasoft
        self.dask_read_block_size = dask_read_block_size
        self.table_name = 'france_pres_2017'
        self.opendatasoft_col_types = {'Coordonnées': 'object', 'Code du département': 'object', 'Département': 'object',
                      'Libellé de la commune': 'object', 'Abstentions': 'object', 'Inscrits': 'object',
                      '% Abs/Ins': 'object',
                      'lib_du_b_vote': 'object', 'Code Postal': 'float64'} #, # code postal as float to avoid ValueError
        self.opendatasoft_cols_to_read = ['Coordonnées', 'Code du département', 'Département',
                        'Commune', 'Inscrits', 'Abstentions', '% Abs/Ins', 'Adresse', 'Code Postal']
        self.user = User_france2017
        self.table_name = 'france_pres_2017'

    def paris_datagouv(self):
        dict_dtype = {"Code du département": "object", "Code du b.vote": "object", "Code de la circonscription": "object","Libellé du département": "object"}
        df = dd.read_csv(
            self.path_datagouv_france2017,
            sep=";",
            decimal=",",
            index_col=False,
            dtype=dict_dtype,
            assume_missing=True,
            blocksize=self.dask_read_block_size
        )
        logging.info(log_memory_after("read datagouv csv 2017"))
        df_paris = df[df["Code du département"] == "75"]
        logging.info(f'INSIDE paris_datagouv After GET PARIS DATAGOUV 2017 file, df_paris has  {df_paris.shape[0].compute()} ROWS and df_paris Code du département has {len(df_paris["Code du département"])} ROWS')
        df_paris["Code du département"] = df_paris["Code du département"].astype(str)
        df_paris["Code du b.vote"] = df_paris["Code du b.vote"].astype(str)
        df_paris["Code de la circonscription"] = (df_paris["Code de la circonscription"].astype(str).str.zfill(2))
        df_paris["Code du b.vote"] = df_paris["Code du b.vote"].str.zfill(4)
        df_paris["col_merge"] = (
                df_paris["Code du département"]
                + "-"
                + df_paris["Code de la circonscription"]
                + "_"
                + df_paris["Code du b.vote"]
        )
        return df_paris

    def join_for_paris(self):
        df_paris = self.paris_datagouv()
        geo_paris = self.paris_geo_coords()
        df_merged = dd.merge(geo_paris, df_paris, on='col_merge', how="inner")
        return df_merged

    def add_paris(self, df):
        paris_with_coords = self.join_for_paris()
        keep_columns = ['longitude', 'latitude', 'Code du département', 'Libellé du département',
                        'Libellé de la commune', '% Abs/Ins', 'Inscrits', 'Abstentions', 'geo_adresse']
        paris_keep_columns = paris_with_coords[keep_columns]
        renamed_cols = {'geo_adresse': 'Adresse complète'}
        paris_keep_columns = paris_keep_columns.rename(columns=renamed_cols)
        paris_keep_columns['Code du département'] = paris_keep_columns['Code du département'].astype(str)
        paris_keep_columns = self.create_denomination_complete(paris_keep_columns)
        #dask_paris = dd.from_pandas(paris_keep_columns, npartitions=dask_paris_partitions) # Deprecated as use of Dask is now harmonised
        dask_paris = self.ammend_pourcentage_abs_col(paris_keep_columns)
        df = dd.concat([df, dask_paris])
        return df

    def create_adresse_complete(self, df):
        df['Adresse complète'] = df['Adresse'].map(str) + ' ' + df['Libellé de la commune'].map(str) + ' ' + df[
            'Code Postal'].map(int).map(str)
        df = df.drop(['Adresse', 'Code Postal'], axis=1)
        return df

    def harmonize_opendatasoft_columns(self, df):
        renamed_cols = {'Commune': 'Libellé de la commune', 'Département': 'Libellé du département'}
        df = df.rename(columns=renamed_cols)
        df = self.create_denomination_complete(df)
        return df

    def dask_dataframe(self):
        df = self.process_raw_opendatasoft()
        df = self.harmonize_opendatasoft_columns(df)
        df = self.create_adresse_complete(df)
        df = self.add_paris(df)
        return df



class Process_france2022(Table_inserts):
    def __init__(self, path_opendatasoft):
        super().__init__()
        self.path_datagouv_france2022 = path_datagouv_france2022
        self.path_opendatasoft = path_opendatasoft
        self.dask_read_block_size = dask_read_block_size
        self.table_name = 'france_pres_2022'
        self.opendatasoft_col_types = {'location': 'object', 'Code du département': 'object', 'Libellé du département': 'object',
                      'Libellé de la commune': 'object', 'Abstentions': 'object', 'Inscrits': 'object',
                      '% Abs/Ins': 'object',
                      'lib_du_b_vote': 'object'}
        self.opendatasoft_cols_to_read = ['location', 'Code du département', 'Libellé du département',
                        'Libellé de la commune', 'Inscrits', 'Abstentions', '% Abs/Ins', 'lib_du_b_vote']
        self.user = User_france2022
        self.table_name = 'france_pres_2022'
    def paris_datagouv(self):
        all_csv_cols = dd.read_csv(
            self.path_datagouv_france2022,
            sep=';',
            assume_missing=True,
            blocksize=None
        ).columns.tolist()
        logging.info(f'header chunk read from csv file datagouv 2022 are {all_csv_cols}')
        cols_to_read = [
            'Code du département',
            'Libellé du département',
            'Libellé de la commune',
            'Code de la circonscription',
            'Code du b.vote',
            'Inscrits',
            'Abstentions',
            '% Abs/Ins'
        ]
        df = dd.read_csv(
            self.path_datagouv_france2022,
            sep=';',
            decimal=',',
            usecols=cols_to_read,
            index_col=False,
            dtype='object',
            assume_missing=True,
            blocksize=self.dask_read_block_size
        )
        df_paris = df[df["Code du département"] == "75"]
        df_paris['Code du département'] = df_paris['Code du département'].astype(str)
        df_paris['Code de la circonscription'] = (
            df_paris['Code de la circonscription']
            .astype(str)
            .str.zfill(2)
        )
        df_paris['Code du b.vote'] = (
            df_paris['Code du b.vote']
            .astype(str)
            .str.zfill(4)
        )
        df_paris['col_merge'] = (
                df_paris['Code du département']
                + '-'
                + df_paris['Code de la circonscription']
                + '_'
                + df_paris['Code du b.vote']
        )
        return df_paris

    def join_for_paris(self):
        df_paris = self.paris_datagouv()
        geo_paris = self.paris_geo_coords()
        df_merged = dd.merge(
            df_paris,
            geo_paris,
            on='col_merge',
            how='inner',
            #broadcast=True  # safe if geo_paris is smaller
        )
        return df_merged

    def add_paris(self, df):
        paris_with_coords = self.join_for_paris()
        keep_columns = [
            'longitude',
            'latitude',
            'Code du département',
            'Libellé du département',
            'Libellé de la commune',
            '% Abs/Ins',
            'Inscrits',
            'Abstentions',
            'geo_adresse'
        ]
        paris_keep_columns = paris_with_coords[keep_columns]
        paris_keep_columns = paris_keep_columns.rename(columns={'geo_adresse': 'Adresse complète'})
        paris_keep_columns['Code du département'] = (paris_keep_columns['Code du département'].astype(str))
        paris_keep_columns = self.create_denomination_complete(paris_keep_columns)
        paris_keep_columns = self.ammend_pourcentage_abs_col(paris_keep_columns)
        df = dd.concat([df, paris_keep_columns])
        return df

    def create_adresse_complete(self, df):
        df['Adresse complète'] = df['lib_du_b_vote'].map(str) + ' ' + df['Libellé de la commune'].map(str)
        df = df.drop(['lib_du_b_vote'], axis=1)
        return df

    def dask_dataframe(self):
        df = self.process_raw_opendatasoft()
        df = self.create_denomination_complete(df)
        df = self.create_adresse_complete(df)
        df = self.add_paris(df)
        return df

def insert_france2017():
    process_france2017 = Process_france2017(path_opendatasoft_france2017)
    df_france2017 = process_france2017.dask_dataframe()
    process_france2017.insert_to_db(df_france2017)


def insert_france2022():
    process_france2022 = Process_france2022(path_opendatasoft_france2022)
    df_france2022 = process_france2022.dask_dataframe()
    process_france2022.insert_to_db(df_france2022)













