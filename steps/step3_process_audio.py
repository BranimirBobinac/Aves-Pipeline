

import sys
import os
import json
import uuid
from datetime import datetime
import requests
from pymongo import UpdateOne
from utils.config import (
    AUDIO_DIR, AVES_CLASSIFY_URL,
    MINIO_BUCKET_AUDIO, MINIO_BUCKET_LOGS,
    COLLECTION_CLASSIFICATIONS, COLLECTION_SPECIES,
    AUDIO_LOCATION_LAT, AUDIO_LOCATION_LON
)
from utils.mongo_client import get_collection
from utils.minio_client import get_minio_client, ensure_buckets, upload_file, upload_json_log


def process_audio():
    """
    Glavni tok obrade audio datoteka.

    Pipeline za svaku datoteku:
    audio_file -> MinIO upload -> classify API -> log u MinIO -> MongoDB
    """
    # Kreiraj MinIO buckete ako ne postoje
    ensure_buckets()

    # Pronađi audio datoteke u konfiguriranom direktoriju
    audio_files = _find_audio_files(AUDIO_DIR)

    if not audio_files:
        print(f"[Korak 3] Nema audio datoteka u {AUDIO_DIR}. Kreiram testne...")
        _create_sample_audio_files(AUDIO_DIR)
        audio_files = _find_audio_files(AUDIO_DIR)

    print(f"[Korak 3] Pronađeno {len(audio_files)} audio datoteka.")

    classifications_col = get_collection(COLLECTION_CLASSIFICATIONS)
    species_col = get_collection(COLLECTION_SPECIES)
    minio = get_minio_client()

    processed = 0

    for audio_path in audio_files:
        filename = os.path.basename(audio_path)
        print(f"\n[Korak 3] Obrađujem: {filename}")

        # ── 1. UPLOAD U MINIO ──────────────────────────────────
        # Generiraj jedinstveno ime objekta s timestamp-om
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        object_name = f"audio/{timestamp}_{filename}"

        try:
            upload_file(audio_path, object_name, MINIO_BUCKET_AUDIO)
            # Generiraj MinIO URL za preuzimanje
            minio_url = f"http://{os.getenv('MINIO_ENDPOINT', 'localhost:9000')}/{MINIO_BUCKET_AUDIO}/{object_name}"
        except Exception as e:
            print(f"[Korak 3]  Upload failed za {filename}: {e}")
            continue

        # ── 2. KLASIFIKACIJA PTICA (AI API) ───────────────────
        classification_result = _classify_audio(audio_path, filename)

        # ── 3. SPREMI LOG U MINIO ──────────────────────────────
        log_data = {
            "request_id": str(uuid.uuid4()),     # Jedinstveni ID zahtjeva
            "timestamp": datetime.utcnow().isoformat(),
            "audio_file": filename,
            "minio_object": object_name,
            "api_url": AVES_CLASSIFY_URL,
            "location": {
                "latitude": AUDIO_LOCATION_LAT,
                "longitude": AUDIO_LOCATION_LON
            },
            "response": classification_result    # Odgovor klasifikatora
        }

        # Spremi log kao JSON u MinIO bucket za logove
        log_object_name = f"logs/{timestamp}_{filename}.json"
        upload_json_log(log_data, log_object_name, MINIO_BUCKET_LOGS)

        # ── 4. SPREMI REZULTATE U MONGODB ─────────────────────
        detected_birds = classification_result.get("detections", [])

        for detection in detected_birds:
            taxon_key = str(detection.get("taxon_key", ""))
            confidence = detection.get("confidence", 0.0)

            # Pronađi taksonomske podatke iz MongoDB species kolekcije
            # (eager lookup - MongoDB ekvivalent JOIN-u)
            species_doc = species_col.find_one({"taxon_key": taxon_key})
            species_info = {
                "taxon_key": taxon_key,
                "scientific_name": species_doc.get("scientific_name", "") if species_doc else detection.get("scientific_name", ""),
                "common_name": species_doc.get("common_name", "") if species_doc else detection.get("common_name", ""),
                "family": species_doc.get("family", "") if species_doc else "",
                "order": species_doc.get("order", "") if species_doc else "",
            }

            # Spremi klasifikacijski zapis u MongoDB
            classifications_col.update_one(
                {
                    "audio_object": object_name,
                    "taxon_key": taxon_key
                },
                {
                    "$set": {
                        "audio_file": filename,
                        "audio_object": object_name,    # MinIO referenca
                        "minio_url": minio_url,
                        "log_object": log_object_name,  # MinIO log referenca
                        "taxon_key": taxon_key,
                        "species": species_info,         # Embedded taksonomija
                        "confidence": confidence,
                        "location": {
                            "latitude": AUDIO_LOCATION_LAT,
                            "longitude": AUDIO_LOCATION_LON
                        },
                        "classified_at": datetime.utcnow(),
                        "raw_detection": detection
                    }
                },
                upsert=True
            )

        processed += 1
        detected_count = len(detected_birds)
        print(f"[Korak 3]  {filename}: detektirano {detected_count} vrsta")

    # Kreiraj indekse
    classifications_col.create_index("taxon_key")
    classifications_col.create_index("confidence")
    classifications_col.create_index([("species.scientific_name", 1)])

    total = classifications_col.count_documents({"confidence": {"$gt": 0}})
    print(f"\n[Korak 3]  Obrađeno {processed} datoteka. Ukupno klasifikacija u bazi: {total}")


def _classify_audio(audio_path: str, filename: str) -> dict:
    """
    Pošalji audio datoteku klasifikacijskom API-ju.

    API: POST https://aves.regoch.net/api/classify
    Body: multipart/form-data s audio datotekom

    Vraća: dict s listom detektiranih vrsta i pouzdanošću
    """
    try:
        with open(audio_path, "rb") as f:
           
            files = {"audio": (filename, f, "audio/wav")}
            response = requests.post(
                AVES_CLASSIFY_URL,
                files=files,
                timeout=30
            )
            response.raise_for_status()
            return response.json()

    except (requests.RequestException, FileNotFoundError) as e:
        print(f"   API nedostupan ({e}). Koristim simulirani odgovor.")
        return _mock_classification(filename)


def _mock_classification(filename: str) -> dict:
    """
    Simulirani odgovor klasifikatora za testiranje bez pravog API-ja.
    Vraća realistične podatke o detekciji ptica.
    """
    import random

    # Mapiranje test datoteka na poznate vrste
    file_species_map = {
        "blackbird": {"taxon_key": "2481139", "scientific_name": "Turdus merula",
                      "common_name": "Blackbird"},
        "robin": {"taxon_key": "2494043", "scientific_name": "Erithacus rubecula",
                  "common_name": "European Robin"},
        "great_tit": {"taxon_key": "2481926", "scientific_name": "Parus major",
                      "common_name": "Great Tit"},
        "chaffinch": {"taxon_key": "2480862", "scientific_name": "Fringilla coelebs",
                      "common_name": "Common Chaffinch"},
        "swift": {"taxon_key": "5788664", "scientific_name": "Apus apus",
                  "common_name": "Common Swift"},
    }

    # Odaberi vrstu na osnovi naziva datoteke ili random
    detected = []
    fn_lower = filename.lower().replace(" ", "_")

    primary = None
    for key, species in file_species_map.items():
        if key in fn_lower:
            primary = species
            break

    if primary is None:
        # Random odabir za neprepoznate datoteke
        primary = random.choice(list(file_species_map.values()))

    detected.append({**primary, "confidence": round(random.uniform(0.75, 0.98), 3)})

    # 30% šansa da ima i sekundarni detekcija
    if random.random() < 0.3:
        secondary = random.choice([s for s in file_species_map.values()
                                   if s["taxon_key"] != primary["taxon_key"]])
        detected.append({**secondary, "confidence": round(random.uniform(0.40, 0.65), 3)})

    return {
        "model": "BirdNET-v2.4-mock",
        "audio_duration_seconds": random.uniform(3, 15),
        "detections": detected,
        "processing_time_ms": random.randint(200, 1500)
    }


def _find_audio_files(directory: str) -> list:
    """
    Pronađi sve audio datoteke u direktoriju.
    Podržani formati: .wav, .mp3, .ogg, .flac, .m4a
    """
    if not os.path.exists(directory):
        return []

    audio_extensions = {".wav", ".mp3", ".ogg", ".flac", ".m4a"}
    return [
        os.path.join(directory, f)
        for f in os.listdir(directory)
        if os.path.splitext(f)[1].lower() in audio_extensions
    ]


def _create_sample_audio_files(directory: str):
    """
    Kreiraj testne audio datoteke (prazne WAV datoteke).
    U pravom projektu bi ovo bile stvarne snimke ptica.
    """
    os.makedirs(directory, exist_ok=True)
    sample_names = [
        "blackbird_morning.wav",
        "robin_garden.wav",
        "great_tit_forest.wav",
        "chaffinch_meadow.wav",
        "unknown_bird_1.wav",
    ]
    for name in sample_names:
        path = os.path.join(directory, name)
        # Minimalni WAV header za validnu datoteku
        with open(path, "wb") as f:
            # RIFF WAV header (44 bytes minimum)
            f.write(b"RIFF\x24\x00\x00\x00WAVEfmt \x10\x00\x00\x00"
                    b"\x01\x00\x01\x00\x44\xac\x00\x00\x88\x58\x01\x00"
                    b"\x02\x00\x10\x00data\x00\x00\x00\x00")
        print(f"[Korak 3] Kreirana testna datoteka: {name}")


if __name__ == "__main__":
    process_audio()
    sys.exit(0)
