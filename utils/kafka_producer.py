

import json
import time
import random
import argparse
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from utils.config import KAFKA_BROKER, KAFKA_TOPIC

# Testne vrste za slanje
SAMPLE_SPECIES = [
    {"taxon_code": "2481139", "name": "Turdus merula"},
    {"taxon_code": "2481926", "name": "Parus major"},
    {"taxon_code": "2494043", "name": "Erithacus rubecula"},
    {"taxon_code": "2480862", "name": "Fringilla coelebs"},
    {"taxon_code": "2481070", "name": "Corvus cornix"},
    {"taxon_code": "2481864", "name": "Columba palumbus"},
    {"taxon_code": "5788664", "name": "Apus apus"},
]

# Lokacije za slanje opažanja
LOCATIONS = [
    {"latitude": 45.815, "longitude": 15.982},  # Zagreb centar
    {"latitude": 45.820, "longitude": 15.975},  # Zagreb sjever
    {"latitude": 45.798, "longitude": 15.968},  # Zagreb jug
    {"latitude": 45.793, "longitude": 16.011},  # Maksimir
    {"latitude": 46.305, "longitude": 16.336},  # Varaždin
]

def generate_observation() -> dict:
    """
    Generiraj jedno simulirano opažanje ptice.
    Svako opažanje može imati RAZLIČITA biološka polja
    (simulira različite ornitologe s različitim metodama mjerenja).
    """
    species = random.choice(SAMPLE_SPECIES)
    location = random.choice(LOCATIONS)
    observer_id = random.randint(1, 5)

    # Baza opažanja
    obs = {
        "id": f"obs_{datetime.now().strftime('%Y%m%d%H%M%S')}_{random.randint(1000, 9999)}",
        "taxon_code": species["taxon_code"],
        "location": location,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "observer": f"observer_{observer_id}",
    }

    # Svaki observer šalje RAZLIČITA biološka polja
    # Observer 1 i 2: šalje temperature i migration_status
    if observer_id in (1, 2):
        obs.update({
            "body_temperature": round(random.uniform(39.5, 41.5), 1),
            "migration_status": random.choice(["resident", "summer_visitor", "winter_visitor"]),
            "behavior": random.choice(["singing", "foraging", "perching", "flying"]),
            "habitat": random.choice(["urban_garden", "forest_edge", "farmland", "wetland"]),
        })
    # Observer 3: šalje wing_span i flock_size
    elif observer_id == 3:
        obs.update({
            "wing_span_cm": round(random.uniform(15, 50), 1),
            "flock_size": random.randint(1, 20),
            "flight_pattern": random.choice(["undulating", "straight", "soaring", "hovering"]),
        })
    # Observer 4 i 5: šalje body measurements
    else:
        obs.update({
            "body_size_cm": round(random.uniform(10, 60), 1),
            "weight_g": round(random.uniform(10, 200), 1),
            "plumage": random.choice(["adult", "juvenile", "winter", "summer"]),
            "sex": random.choice(["male", "female", "unknown"]),
        })

    return obs


def send_observations(count: int = 10, delay: float = 0.5):
    """
    Pošalji 'count' opažanja na Kafka topic.

    Args:
        count: Broj opažanja za slanje
        delay: Pauza između poruka u sekundama
    """
    print(f"[Producer] Spajam se na Kafka broker: {KAFKA_BROKER}")

    try:
        # KafkaProducer - šalje poruke na Kafka topic
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            # value_serializer - pretvori dict u JSON bytes
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            # Čekaj potvrdu od brokera
            acks='all'
        )
    except NoBrokersAvailable:
        print("[Producer]  Kafka nije dostupna! Pokreni: docker-compose up -d")
        return

    print(f"[Producer] Šaljem {count} opažanja na topic '{KAFKA_TOPIC}'...")

    for i in range(count):
        obs = generate_observation()
        # send() šalje poruku asinkrono
        # key - opcionalni ključ za particioniranje
        future = producer.send(
            KAFKA_TOPIC,
            value=obs,
            key=obs["taxon_code"].encode("utf-8")  # Particioniraj po vrsti
        )
        # get() - čekaj potvrdu da je poruka primljena
        record = future.get(timeout=10)
        print(f"  [{i+1}/{count}] Sent: {obs['taxon_code']} → "
              f"partition={record.partition}, offset={record.offset}")
        time.sleep(delay)

    # flush() - osiguraj da su sve poruke stvarno poslane
    producer.flush()
    producer.close()
    print(f"[Producer]  Uspješno poslano {count} opažanja.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Bird Observation Producer")
    parser.add_argument("--count", type=int, default=10,
                        help="Broj opažanja za slanje (default: 10)")
    parser.add_argument("--delay", type=float, default=0.3,
                        help="Pauza između poruka u sekundama (default: 0.3)")
    args = parser.parse_args()
    send_observations(args.count, args.delay)
