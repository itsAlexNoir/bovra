""" database.py

This module contain routines for creating and interacting with a mongodb database.
"""
import pymongo
import logging

log = logging.getLogger(__name__)

def connect_mongo_daemon(host=None, port=None):
    if host is None and port is None:
        client = pymongo.MongoClient()
    else:
        client = pymongo.MongoClient(host, port)
    return client

def get_mongo_database(client, dbname):
    return client[dbname]

def get_mongo_collection(db, collection):
    return db[collection]

def insert_one_document(db, coll, entry):
    mycoll = db[coll]
    return mycoll.insert_one(entry).inserted_id

def insert_many_documents(db, coll, entries):
    mycoll = db[coll]
    return mycoll.insert_many(entries).inserted_ids

