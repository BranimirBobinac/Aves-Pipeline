

import sys
import json
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from pymongo import UpdateOne
from utils.config import KAFKA_BROKER, KAFKA_TOPIC, COLLECTION_OBSERVATIONS
from utils.mongo_client import get_collection


def consume_observations():
    
    col = get_collection(COLLECTION_OBSERVATIONS)

    print(f"[Korak 2] Spajam se na Kafka broker: {KAFKA_BROKER}")
    print(f"[Korak 2] Topic: {KAFKA_TOPIC}")

    try:
        # KafkaConsumer - čita poruke iz Kafka topic-a
        consumer = KafkaConsumer(
            KAFKA_TOPIC,                        # Ime topic-a
            bootstrap_servers=[KAFKA_BROKER],   # Adresa brokera
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),  # JSON deserijalizacija
            auto_offset_reset="earliest",        # Počni od najstarije poruke
            enable_auto_commit=True,             # Automatski potvrdi čitanje
            group_id="birds-pipeline",           # Consumer group ID
            consumer_timeout_ms=5000             # Timeout ako nema poruka (5s)
        )

    except NoBrokersAvailable:
        print("[Korak 2]  Kafka nije dostupna. Koristim testne poruke za demonstraciju.")
        _insert_mock_observations(col)
        return

    operations = []
    count = 0

   
    for message in consumer:
        obs = message.value 
        count += 1

        
        taxon_code = obs.get("taxon_code") or obs.get("taxonCode", "UNKNOWN")
        location = obs.get("location", {
            "latitude": obs.get("lat", 0),
            "longitude": obs.get("lon", 0)
        })

        # Upsert po kombinaciji taxon_code + timestamp (sprečava duplikate)
        obs_id = obs.get("id") or f"{taxon_code}_{obs.get('timestamp', datetime.utcnow().isoformat())}"

        
        operations.append(
            UpdateOne(
                {"observation_id": obs_id},       
                {
                    "$set": {
                        "observation_id": obs_id,
                        "taxon_code": taxon_code,
                        "location": location,
                        "observed_at": obs.get("timestamp", datetime.utcnow().isoformat()),
                        "observer": obs.get("observer", "unknown"),
                        "biological_data": {
                            k: v for k, v in obs.items()
                            if k not in ("taxon_code", "taxonCode", "location",
                                         "timestamp", "observer", "id", "lat", "lon")
                        }
                    },
                    "$setOnInsert": {"imported_at": datetime.utcnow()}
                },
                upsert=True
            )
        )

       
        if len(operations) >= 100:
            col.bulk_write(operations)
            print(f"[Korak 2] Zapisano {count} opažanja...")
            operations = []

    # Zapiši preostale operacije
    if operations:
        col.bulk_write(operations)

    consumer.close()  # Zatvori Kafka konekciju

    # Kreiraj indekse za brže pretraživanje
    col.create_index("taxon_code")
    col.create_index("location")
    col.create_index([("taxon_code", 1), ("observed_at", -1)])

    total = col.count_documents({})
    print(f"[Korak 2]  Ukupno opažanja u bazi: {total} (novo primljeno: {count})")


def _insert_mock_observations(col):
    """
    Testna opažanja - demonstriraju različite biološke strukture podataka.
    Različiti ornitolozi šalju RAZLIČITA polja (fleksibilna MongoDB shema).
    """
    mock_obs = [
        # Ornitolog 1 - šalje body_temperature i migration_status
        {
            "observation_id": "obs_001",
            "taxon_code": "2481139",  # Turdus merula
            "location": {"latitude": 45.81, "longitude": 15.98},
            "observed_at": "2024-05-15T08:30:00",
            "observer": "dr_horvat",
            "biological_data": {
                "body_temperature": 40.5,
                "migration_status": "resident",
                "behavior": "singing",
                "habitat": "urban_garden"
            }
        },
        # Ornitolog 2 - šalje wing_span i flight_pattern (RAZLIČITA POLJA!)
        {
            "observation_id": "obs_002",
            "taxon_code": "2481926",  # Parus major
            "location": {"latitude": 45.82, "longitude": 15.97},
            "observed_at": "2024-05-15T09:15:00",
            "observer": "prof_petrovic",
            "biological_data": {
                "wing_span_cm": 24.5,
                "flight_pattern": "undulating",
                "flock_size": 3,
                "feeding_behavior": "foraging"
            }
        },
        # Ornitolog 3 - šalje body_size i weight
        {
            "observation_id": "obs_003",
            "taxon_code": "2494043",  # Erithacus rubecula
            "location": {"latitude": 45.80, "longitude": 15.99},
            "observed_at": "2024-05-16T07:00:00",
            "observer": "citizen_scientist_1",
            "biological_data": {
                "body_size_cm": 14,
                "weight_g": 18.3,
                "plumage": "adult",
                "notes": "singing from treetop"
            }
        },
        {
            "observation_id": "obs_004",
            "taxon_code": "2481139",  # Turdus merula (ponovo)
            "location": {"latitude": 45.815, "longitude": 15.985},
            "observed_at": "2024-05-16T10:30:00",
            "observer": "dr_horvat",
            "biological_data": {
                "body_temperature": 40.2,
                "migration_status": "resident",
                "behavior": "foraging"
            }
        },
        {
            "observation_id": "obs_005",
            "taxon_code": "2480862",  # Fringilla coelebs
            "location": {"latitude": 45.825, "longitude": 15.975},
            "observed_at": "2024-05-17T08:00:00",
            "observer": "prof_petrovic",
            "biological_data": {
                "wing_span_cm": 26.0,
                "flock_size": 12,
                "migration_status": "summer_visitor"
            }
        },
    ]

    operations = [
        UpdateOne(
            {"observation_id": obs["observation_id"]},
            {"$set": obs, "$setOnInsert": {"imported_at": datetime.utcnow()}},
            upsert=True
        )
        for obs in mock_obs
    ]
    col.bulk_write(operations)
    col.create_index("taxon_code")
    print(f"[Korak 2]  Uneseno {len(mock_obs)} testnih opažanja iz Kafke (simulacija).")


if __name__ == "__main__":
    consume_observations()
    sys.exit(0)
