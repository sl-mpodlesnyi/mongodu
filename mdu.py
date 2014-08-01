#!/usr/bin/env python3
import pymongo
import sys
from argparse import ArgumentParser
from json import loads, dumps

db_fields = ['dataSize', 'storageSize', 'indexSize', 'fileSize']
db_sum_fields = ['indexSize', 'fileSize', 'indexSize']
collection_fields = ['storageSize', 'lastExtentSize', 'totalIndexSize', 'paddingFactor', 'indexSizes']

METRICSYS = {
        'b': 1,
        'K': 1024,
        'M': 1024 ** 2,
        'G': 1024 ** 3,
    }

class MongoDiskUsage():
    ''' Mongo Disk Usage analyzer
    '''
    def __init__(self, dbauth, port, user, password, unit='G', limit=5):
        self._dbauth = dbauth
        self._port = port
        self._user = user
        self._password = password
        self._unit = METRICSYS[unit]
        self._limit = limit

    def __dbstats__(self, db):
        result = {}
        dbstats = db.command('dbstats')
        result['status'] = { i: dbstats[i] for i in db_fields }
        result['collections'], result['duStorageSize'], result['duIndexSize'] = self.__collstatus__(db)
        return result

    def __collstatus__(self, db):
        result = {}
        size = 0
        indexsize = 0
        db.read_preference = pymongo.ReadPreference.SECONDARY
        for collection in db.collection_names():
            collstats = db.command("collstats", collection)
            size += float(collstats['storageSize'])
            indexsize += float(collstats['totalIndexSize'])
            result[collection] = { stats: collstats[stats] for stats in collection_fields }
        return result, size, indexsize

    def get(self, host, dbname=None):
        ''' Run get data
        '''
        conn = pymongo.MongoClient(host, self._port)
        db = conn[self._dbauth]
        if not db.authenticate(self._user, self._password):
            sys.exit("Username/Password incorrect")
        if dbname:
            dbs = [dbname]
        else:
            dbs = conn.database_names()
        result = { db: self.__dbstats__(conn[db]) for db in dbs }
        conn.close()
        return result

    def put(self, data):
        du = {}
        dbsize = sum([ float(data_value['duStorageSize']) for data_key, data_value in data.items()])
        isize = sum([ float(data_value['duIndexSize']) for data_key, data_value in data.items()])
        for data_key, data_value in data.items():
            du[data_key] = {}
            du[data_key]['size'] = round(float(data_value['status']['fileSize'] / self._unit), 2)
            du[data_key]['duStorageSize'] = round(float(data_value['duStorageSize']) / self._unit, 2)
            du[data_key]['duIndexSize'] = round(float(data_value['duIndexSize']) / self._unit, 2)
            du[data_key]['EmptySize'] = round((float(data_value['status']['fileSize']) - float(data_value['status']['storageSize'])) / self._unit, 2)
            du[data_key]['%'] = round((float(data_value['duStorageSize']) / dbsize) * 100, 2)
            du[data_key]['i%'] = round((float(data_value['duIndexSize']) / isize) * 100, 2)
            du[data_key]['collections'] = {}
            du_key_collections = du[data_key]['collections']
            for collections_key, collections_value in data_value['collections'].items():
                size = float(collections_value['storageSize'])
                indexsize = float(collections_value['totalIndexSize'])
                p = (size / dbsize) * 100
                pi = (indexsize / isize) * 100
                padding = float(collections_value['paddingFactor'])
                if padding > 1.5 or p > self._limit or pi > self._limit:
                    du_key_collections[collections_key] = {}
                if padding > 1.5:
                    du_key_collections[collections_key]['padding size'] = round(((size * padding) - size) / self._unit, 2)
                if p > self._limit:
                    du_key_collections[collections_key]['%'] = round(p, 2)
                if pi > self._limit:
                    du_key_collections[collections_key]['i%'] = round(pi, 2)
        return du

if __name__ == '__main__':
    parser = ArgumentParser(prog='mongodu', description='MongoDB disk usage')
    parser.add_argument('--host', '-m', help='MongoDB host')
    parser.add_argument('--file', '-f', help='JSON-file for loads or dumps data')
    parser.add_argument('--port', '-p', default=27017, type=int, help='TCP port')
    parser.add_argument('--db', '-d', default='admin', help='database for auth')
    parser.add_argument('--user', '-u', default='admin', help='user for auth')
    parser.add_argument('--password', '-w', default='admin', help='password for auth')
    parser.add_argument('--limit', '-l', default=5, type=int, help='limit of percantage size to view')
    parser.add_argument('--unit', '-n', default='G', choices=['b', 'K', 'M',
        'G'], help='unit of digital information ')

    args = parser.parse_args()
    mdu = MongoDiskUsage(args.db, args.port, args.user, args.password, args.unit, args.limit)
    dum = { 'sort_keys': True, 'indent': 4, 'separators': (',', ': ')}
    if args.host and args.file:
        with open(args.file, 'w') as f:
            f.write(dumps(mdu.get(args.host), **dum))
    elif args.file:
        with open(args.file, 'r') as f:
            print(dumps(mdu.put(loads(f.read())), **dum))
    elif args.host:
        print(dumps(mdu.put(mdu.get(args.host)), **dum))
    else:
        parser.print_help()
