from daq_assettools.asset_file import AssetFile
from daq_assettools.asset_database import Database
import json
import os
import shutil

import argparse

def common_parser():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--db-file',
                        default='/cvmfs/dunedaq.opensciencegrid.org/assets/dunedaq-asset-db.sqlite',
                        help="path to database file")
    parser.add_argument('-n', '--name', help="asset name")
    parser.add_argument("--subsystem", choices=['readout', 'trigger'], help="asset subsystem")
    parser.add_argument('-l', '--label', help="asset label")
    parser.add_argument('-f', '--format', choices=['binary', 'text'], help="asset file format")
    parser.add_argument('--status', choices=['valid', 'expired', 'new_version_available'], help="asset file status")
    parser.add_argument('-c', '--checksum', help="MD5 checksum of asset file")
    parser.add_argument('--description', help="description of asset file")
    parser.add_argument('--replica-uri', help="replica URI")
    return parser

def make_qdict(pargs):
    qdict = {}
    if pargs.name is not None:
        qdict['name'] = pargs.name
    if pargs.subsystem is not None:
        qdict['subsystem'] = pargs.subsystem
    if pargs.label is not None:
        qdict['label'] = pargs.label
    if pargs.status is not None:
        qdict['status'] = pargs.status
    if pargs.format is not None:
        qdict['format'] = pargs.format
    if pargs.description is not None:
        qdict['description'] = pargs.description
    if pargs.checksum is not None:
        qdict['checksum'] = pargs.checksum
    if pargs.replica_uri is not None:
        qdict['replica_uri'] = pargs.replica_uri
    return qdict


def get_assets():
    parser = common_parser()
    parser.add_argument('-p', '--print-metadata', action='store_true', help="print full metadata")
    parser.add_argument('--copy-to', help="path to the directory where asset files will be copied to.")
    args = parser.parse_args()

    asset_db = Database(args.db_file)
    qdict = make_qdict(args)
    if len(qdict) == 0:
        print("Error: at least one metadata field is required. Use '-h' for the helper.")
        return
    if args.copy_to is not None:
        if not os.path.isdir(args.copy_to):
            print(f"Error: the destination path '{args.copy_to}' does not exist.")
            return
    files = asset_db.get_files(qdict)
    root_dir = os.path.dirname(asset_db.database_file)
    if len(files) >0:
        print("{:<32} {:<15} {:<15} {:<15} {}".format('checksum', 'subsystem', 'label',
                                                            'status', 'file_path'))
    else:
        print("No asset files matched the query.")
        return
    for i in files:
        print("{:<32} {:<15} {:<15} {:<15} {}/{}/{}".format(i['checksum'], i['subsystem'], i['label'],
                                                     i['status'], root_dir,
                                                     i['path'], i['name']))
        if args.print_metadata:
            print("----------- File metadata ---------------")
            print(json.dumps(i, indent=4))
        if args.copy_to is not None:
            new_filename = os.path.splitext(i['name'])[0] + '-' + i['checksum'][:7] + os.path.splitext(i['name'])[1]
            dst = os.path.join(args.copy_to, new_filename)
            src = f"{root_dir}/{i['path']}/{i['name']}"
            shutil.copy2(src, dst)
            print(f"---- copied to {dst}\n")
    return


def update_assets():
    parser = common_parser()
    parser.add_argument('--json-string', help="json string to be updated in metadata")
    args = parser.parse_args()
    asset_db = Database(args.db_file)
    qdict = make_qdict(args)
    if len(qdict) == 0:
        print("Error: at least one metadata field is required. Use '-h' for the helper.")
        return
    change_dict = json.loads(args.json_string)
    asset_db.update_files(qdict, change_dict)
    return

def retire_assets():
    parser = common_parser()
    args = parser.parse_args()
    asset_db = Database(args.db_file)
    qdict = make_qdict(args)
    if len(qdict) == 0:
        print("Error: at least one metadata field is required. Use '-h' for the helper.")
        return
    asset_db.retire_files(qdict)
    return

def add_assets():
    parser = common_parser()
    parser.add_argument('-s', '--source', help='path to asset file')
    parser.add_argument('--json-file', help='json file containing file metadata')
    args = parser.parse_args()
    asset_db = Database(args.db_file)
    file_md = {}
    qdict = make_qdict(args)
    if args.json_file is not None:
        with open(args.json_file, 'r') as jf:
            file_md = json.load(jf)
    for i in qdict:
        if i == "checksum":
            continue
        file_md[i] = qdict[i]
    asset_db.insert_file(args.source, file_md)
    return
