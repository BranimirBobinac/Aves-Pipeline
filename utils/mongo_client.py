

from pymongo import MongoClient
from pymongo.database import Database
from utils.config import MONGO_URI, MONGO_DB


def get_db() -> Database:
    """
    Vrati instancu MongoDB baze podataka.
    PyMongo automatski upravlja connection poolom.
    Ova funkcija je singleton-like - pozovi je svaki put kad trebaš bazu.
    """
    # MongoClient(uri) - kreira konekciju na MongoDB server
    client = MongoClient(MONGO_URI)
    # Pristup bazi podataka po imenu
    return client[MONGO_DB]


def get_collection(collection_name: str):
    """
    Dohvati specifičnu kolekciju (= tablicu) iz baze.
    Kolekcija se automatski kreira ako ne postoji.

    Primjer: get_collection("species") → db.species
    """
    db = get_db()
    return db[collection_name] 
