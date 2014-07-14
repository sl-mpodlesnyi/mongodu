#!/usr/bin/env python3
import pymongo
import sys
from argparse import ArgumentParser
from json import loads, dumps

db_fields = ['dataSize', 'storageSize', 'indexSize', 'fileSize']
db_sum_fields = ['indexSize', 'fileSize', 'indexSize']
collection_fields = ['storageSize', 'lastExtentSize', 'totalIndexSize', 'paddingFactor']
collection_sum_fields = ['lastExtentSize', 'totalIndexSize']

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
        result['collstats'], result['dusize'] = self.__collstatus__(db)
        return result

    def __collstatus__(self, db):
        result = {}
        size = 0
        db.read_preference = pymongo.ReadPreference.SECONDARY
        for collection in db.collection_names():
            collstats = db.command("collstats", collection)
            size += sum([ float(collstats[s]) for s in collection_sum_fields ])
            result[collection] = {}
            for stats in collection_fields:
                result[collection][stats] = collstats[stats]
        return result, size

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
        dbsize = sum([ float(data[i]['dusize']) for i in list(data.keys())])
        for i in list(data.keys()):
            du[i] = {'size': round(float(data[i]['status']['fileSize'] / self._unit), 2), 'du':
                    round(float(data[i]['dusize']) / self._unit, 2), '%': round((float(data[i]['dusize']) / dbsize)
                    * 100, 2), 'collections': {}}
            for j in list(data[i]['collstats'].keys()):
                realc = data[i]['collstats'][j]
                s = sum([ float(realc[c]) for c in collection_sum_fields ])
                p = (s / dbsize) * 100
                padding = float(realc['paddingFactor'])
                if padding > 1.5 or p > self._limit:
                    du[i]['collections'][j] = {}
                if padding > 1.5:
                    du[i]['collections'][j]['padding size'] = (s * padding) - s
                if p > self._limit:
                    du[i]['collections'][j]['%'] = round(p, 2)
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
