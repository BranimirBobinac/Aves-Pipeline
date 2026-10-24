

import sys
import requests
from pymongo import UpdateOne
from utils.config import AVES_SPECIES_URL, COLLECTION_SPECIES
from utils.mongo_client import get_collection


def collect_species():
    
    # Dohvati kolekciju "species" iz MongoDB-a
    col = get_collection(COLLECTION_SPECIES)

    # Provjeri postoje li već podaci - ako da, preskoči
    count = col.count_documents({})
    if count > 0:
        print(f"[Korak 1] Taksonomski podaci već postoje ({count} vrsta). Preskačem.")
        return

    print("[Korak 1] Dohvaćam podatke o vrstama ptica s aves.regoch.net...")

    try:
        # HTTP GET zahtjev na API
        response = requests.get(AVES_SPECIES_URL, timeout=30)
        response.raise_for_status() 
        species_data = response.json() 
        print(f"[Korak 1] Primljeno {len(species_data)} vrsta.")

    except requests.RequestException as e:
        print(f"[Korak 1] Greška pri dohvatu: {e}")
        print("[Korak 1] Koristim testne podatke za demonstraciju...")
        # Fallback testni podaci u slučaju nedostupnosti API-ja
        species_data = _get_mock_species()

    operations = []
    for species in species_data:
        taxon_key = species.get("taxonKey") or species.get("key") or species.get("id")
        if not taxon_key:
            continue 

       
        operations.append(
            UpdateOne(
                {"taxon_key": str(taxon_key)},    # Filter - pronađi po ovom polju
                {"$set": {                          # Postavi ova polja
                    "taxon_key": str(taxon_key),
                    "scientific_name": species.get("scientificName", ""),
                    "common_name": species.get("vernacularName") or species.get("canonicalName", ""),
                    "kingdom": species.get("kingdom", "Animalia"),
                    "phylum": species.get("phylum", "Chordata"),
                    "class": species.get("class", "Aves"),
                    "order": species.get("order", ""),
                    "family": species.get("family", ""),
                    "genus": species.get("genus", ""),
                    "species": species.get("species", ""),
                    "raw": species  
                }},
                upsert=True  
            )
        )

    if operations:
        result = col.bulk_write(operations)
        print(f"[Korak 1]  Umetnutih: {result.upserted_count}, Ažuriranih: {result.modified_count}")

   
    col.create_index("taxon_key", unique=True)
    col.create_index("scientific_name") 
    col.create_index("common_name")

    print(f"[Korak 1]  Taksonomska baza popunjena s {col.count_documents({})} vrsta.")


def _get_mock_species() -> list:
    """
    Testni podaci - koristi se kad API nije dostupan.
    Sadržava 15 uobičajenih europskih vrsta ptica.
    """
    return [
        {"taxonKey": "2481139", "scientificName": "Turdus merula",
         "vernacularName": "Blackbird", "order": "Passeriformes", "family": "Turdidae",
         "genus": "Turdus", "species": "T. merula"},
        {"taxonKey": "2481926", "scientificName": "Parus major",
         "vernacularName": "Great Tit", "order": "Passeriformes", "family": "Paridae",
         "genus": "Parus", "species": "P. major"},
        {"taxonKey": "2494043", "scientificName": "Erithacus rubecula",
         "vernacularName": "European Robin", "order": "Passeriformes", "family": "Muscicapidae",
         "genus": "Erithacus", "species": "E. rubecula"},
        {"taxonKey": "2480862", "scientificName": "Fringilla coelebs",
         "vernacularName": "Common Chaffinch", "order": "Passeriformes", "family": "Fringillidae",
         "genus": "Fringilla", "species": "F. coelebs"},
        {"taxonKey": "2481070", "scientificName": "Corvus cornix",
         "vernacularName": "Hooded Crow", "order": "Passeriformes", "family": "Corvidae",
         "genus": "Corvus", "species": "C. cornix"},
        {"taxonKey": "2481864", "scientificName": "Columba palumbus",
         "vernacularName": "Common Wood Pigeon", "order": "Columbiformes", "family": "Columbidae",
         "genus": "Columba", "species": "C. palumbus"},
        {"taxonKey": "2473498", "scientificName": "Ardea cinerea",
         "vernacularName": "Grey Heron", "order": "Pelecaniformes", "family": "Ardeidae",
         "genus": "Ardea", "species": "A. cinerea"},
        {"taxonKey": "5788664", "scientificName": "Apus apus",
         "vernacularName": "Common Swift", "order": "Apodiformes", "family": "Apodidae",
         "genus": "Apus", "species": "A. apus"},
        {"taxonKey": "2480978", "scientificName": "Cyanistes caeruleus",
         "vernacularName": "Blue Tit", "order": "Passeriformes", "family": "Paridae",
         "genus": "Cyanistes", "species": "C. caeruleus"},
        {"taxonKey": "2481887", "scientificName": "Sturnus vulgaris",
         "vernacularName": "Common Starling", "order": "Passeriformes", "family": "Sturnidae",
         "genus": "Sturnus", "species": "S. vulgaris"},
        {"taxonKey": "2473633", "scientificName": "Falco tinnunculus",
         "vernacularName": "Common Kestrel", "order": "Falconiformes", "family": "Falconidae",
         "genus": "Falco", "species": "F. tinnunculus"},
        {"taxonKey": "2481948", "scientificName": "Passer domesticus",
         "vernacularName": "House Sparrow", "order": "Passeriformes", "family": "Passeridae",
         "genus": "Passer", "species": "P. domesticus"},
        {"taxonKey": "2481116", "scientificName": "Hirundo rustica",
         "vernacularName": "Barn Swallow", "order": "Passeriformes", "family": "Hirundinidae",
         "genus": "Hirundo", "species": "H. rustica"},
        {"taxonKey": "9651916", "scientificName": "Luscinia megarhynchos",
         "vernacularName": "Common Nightingale", "order": "Passeriformes", "family": "Muscicapidae",
         "genus": "Luscinia", "species": "L. megarhynchos"},
        {"taxonKey": "2482565", "scientificName": "Ciconia ciconia",
         "vernacularName": "White Stork", "order": "Ciconiiformes", "family": "Ciconiidae",
         "genus": "Ciconia", "species": "C. ciconia"},
    ]


if __name__ == "__main__":
    
    collect_species()
    sys.exit(0)
