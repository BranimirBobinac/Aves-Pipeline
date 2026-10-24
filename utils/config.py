

import os
from dotenv import load_dotenv

# Učitaj varijable iz .env datoteke u os.environ
load_dotenv()

# MONGODB
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongouser:mongopass123@localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "birds_db")

# Nazivi MongoDB kolekcija (= tablice u relacijskim bazama)
COLLECTION_SPECIES = "species"          # Taksonomski podaci o vrstama
COLLECTION_OBSERVATIONS = "observations"  # Kafka opažanja ornitologa
COLLECTION_CLASSIFICATIONS = "classifications"  # Rezultati AI klasifikacije

# MINIO 
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET_AUDIO = os.getenv("MINIO_BUCKET_AUDIO", "bird-audio")
MINIO_BUCKET_LOGS = os.getenv("MINIO_BUCKET_LOGS", "bird-logs")
MINIO_USE_SSL = os.getenv("MINIO_USE_SSL", "false").lower() == "true"

# KAFKA 
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "bird-observations")

# EXTERNAL API 
AVES_BASE_URL = os.getenv("AVES_BASE_URL", "https://aves.regoch.net")
AVES_SPECIES_URL = f"{AVES_BASE_URL}/species"
AVES_CLASSIFY_URL = f"{AVES_BASE_URL}/api/classify"

# AUDIO 
AUDIO_DIR = os.getenv("AUDIO_DIR", "./audio_samples")
AUDIO_LOCATION_LAT = float(os.getenv("AUDIO_LOCATION_LAT", "45.8150"))
AUDIO_LOCATION_LON = float(os.getenv("AUDIO_LOCATION_LON", "15.9819"))
