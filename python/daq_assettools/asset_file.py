#!/usr/bin/env python

import json
import os
import hashlib
import shutil
import datetime
import sys


def calc_file_checksum_md5(file_name):
    hash_md5 = ""
    try:
        with open(file_name, 'rb') as file_to_check:
            # read contents of the file
            data = file_to_check.read()
            # pipe contents of the file through
            hash_md5 = hashlib.md5(data).hexdigest()
    except FileNotFoundError:
        print("ERROR: file does not exist, exit now.")
        sys.exit(1)
    return hash_md5


def get_hash_dir(hash_md5):
    hash_path = '/'.join(hash_md5[0:3])
    return hash_path


class AssetFile(object):
    def __init__(self, md, src=""):
        self.md = md
        if src == "" and "name" in md and "path" in md:
            self.src = os.path.join(md['path'], md['name'])
        else:
            self.src = os.path.abspath(src)
        return

    def catalog(self):
        self.md["checksum"] = calc_file_checksum_md5(self.src)
        self.md["path"] = os.path.join("files", get_hash_dir(self.md["checksum"]))
        self.md["size"] = os.path.getsize(self.src)
        if "replica_uri" not in self.md:
            hostname = os.uname()[1]
            self.md["replica_uri"] = f"{hostname}:{self.src}"
        return


    def copy_to_hash_dir(self):
        src = os.path.abspath(self.src)
        fname = self.md['name']
        dest_path = os.path.join(self.md['path'], fname)
        if os.path.exists(dest_path):
            self.md['name'] = str(datetime.datetime.now().microsecond)[:2] + '_' + fname
            print(f"INFO: A file with name: {fname} already exists at {dest_path}")
            print(f"INFO: Rename {fname} to {self.md['name']}")
            self.copy_to_hash_dir()
        if not os.path.exists(self.md['path']):
            os.makedirs(self.md['path'])
        shutil.copy(src, dest_path)
        self.write_md_json()
        return

    def write_md_json(self):
        md_json_file = os.path.join(self.md['path'], self.md['name'] + '.json')
        with open(md_json_file, 'w') as mf:
            json.dump(self.md, mf, indent=4)
            mf.write('\n')
        return
