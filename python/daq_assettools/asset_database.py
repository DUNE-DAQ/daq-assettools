#!/usr/bin/env python

import os
import sqlite3
import datetime
from daq_assettools.asset_file import AssetFile
import json


#####################
# file metadata table
#####################
class Database(object):
    table = "dunedaq_assets"
    columns = ['file_id', 'name'  , 'subsystem', 'label',
               'path', 'checksum',  'size',
               'format', 'status', 'description',
               'catalog_ts', 'update_ts','replica_uri']
    columns_type = [ 'INTEGER PRIMARY KEY', 'TEXT NOT NULL', 'TEXT NOT NULL',
                    'TEXT NOT NULL', 'TEXT NOT NULL', 'TEXT', 'INTEGER',
                    'TEXT', 'TEXT', 'TEXT',
                    'TEXT', 'TEXT', 'TEXT' ]
    status = ['valid', 'new version available', 'expired']
    def __init__(self, db_file):
        self.database_file = os.path.abspath(db_file)
        create_new = False
        if not os.path.exists(db_file):
            create_new = True
            if not os.access('/path/to/folder', os.W_OK):
                print(f"ERROR: {db_file} is not writable.")
        self.conn = sqlite3.connect(db_file)
        self.cursor = self.conn.cursor()
        if create_new:
            self.create_table()
        return

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    def get_next_file_id(self):
        next_id = 0
        sql_query = f"SELECT max(file_id) FROM {self.table}"
        max_id = self.query(sql_query)
        if max_id[0][0] is not None:
            next_id = max_id[0][0] +1
        return next_id

    def insert_file(self, src, file_md):
        asset_file = AssetFile(file_md, src)
        asset_file.catalog()
        file_id = self.get_next_file_id()
        asset_file.md['file_id'] = file_id
        row_value = ()
        for i in self.columns:
            if i == 'catalog_ts' or i == 'update_ts':
                if i not in asset_file.md:
                    ivalue = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    asset_file.md[i] = ivalue
            elif  i in asset_file.md:
                ivalue = asset_file.md[i]
            else:
                ivalue = None
            row_value = row_value + (ivalue, )
        self.insert(row_value)
        asset_file.copy_to_hash_dir()
        print(f"INFO: cataloged {asset_file.md['path']}/{asset_file.md['name']}")
        return asset_file

    def retire_files(self, query_dict):
        self.update_files(query_dict, {"status":"expired"})
        return

    def update_file(self, file_md, change_dict):
        new_file_md = file_md
        new_values = []
        change_dict['update_ts'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for i in change_dict:
            if i != 'size':
                new_values.append(f'{i} = "{change_dict[i]}"')
            else:
                new_values.append(f'{i} = {change_dict[i]}')
            new_file_md[i] = change_dict[i]
        # Update file.json
        asset_file = AssetFile(new_file_md)
        asset_file.write_md_json()
        # Update database entry
        new_values_sql = ','.join(new_values)
        sql_update = f'''UPDATE {self.table} SET {new_values_sql} WHERE file_id = {new_file_md['file_id']}'''
        print(f"INFO: updating {file_md['path']}/{file_md['name']}")
        self.cursor.execute(sql_update)
        self.conn.commit()
        return

    def update_files(self, query_dict, change_dict):
        for i in self.get_files(query_dict):
            self.update_file(i, change_dict)
        return

    def get_files(self, query_dict):
        #"select name, path, status, "
        # group, format
        # rows --> file_md
        columns_list = ", ".join(self.columns)
        filters = []
        for i in query_dict:
            filters.append(f"{i} = '{query_dict[i]}'")
        row_filter = " AND ".join(filters)
        sql_query = f'''SELECT {columns_list}
        FROM {self.table}
        WHERE {row_filter};
        '''
        rows = self.query(sql_query)
        files_md = []
        for i in rows:
            files_md.append(dict(zip(tuple(self.columns), i)))
        return files_md

    def create_table(self):
        sql_query = ''' SELECT name FROM sqlite_master WHERE type='table'
        AND name='{}' '''.format(self.table)
        all_rows = self.query(sql_query)
        table_exist = False
        columns = []
        for i in all_rows:
            if self.table in i:
                print(self.table)
                table_exist = True
                break

        if not table_exist:
            for i in range(len(self.columns)):
                columns.append(self.columns[i] + ' ' + self.columns_type[i])
            create_table_string = ''' CREATE TABLE {} ({})'''.format(self.table,
                                                   ", ".join(columns))
            print(create_table_string)
            self.cursor.execute(create_table_string)
            self.conn.commit()
            print("created table")
        else:
            qout = "Error when creating table {} in database ".format(self.table)
            qout += " -- table already exists."
            print(qout)
        return

    def scan_directory(self, path):
        return

    def query(self, query):
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return rows

    def insert(self, row_value):
        self.cursor.execute('''INSERT INTO {}({}) VALUES ({})'''.format(
            self.table, ", ".join(self.columns),
            ", ".join(['?']*len(self.columns))),
            row_value)
        self.conn.commit()
        return

