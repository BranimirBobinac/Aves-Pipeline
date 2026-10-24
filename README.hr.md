# Projekt 2 - Bird Observation Pipeline (Python + MongoDB + MinIO + Kafka)

## Tehnologije
| Tehnologija | Uloga |
|-------------|-------|
| **Python 3.11** | Programski jezik |
| **MongoDB 7.0** | NoSQL baza - taksonomija, opažanja, klasifikacije |
| **MinIO** | S3-kompatibilna pohrana - audio datoteke i logovi |
| **Apache Kafka** | Message broker - opažanja ornitologa |
| **Snakemake** | Pipeline orkestrator (alternativa: `python main.py`) |
| **Docker** | Sve usluge pokrenute kao kontejneri |

## Struktura projekta
```
projekt2/
├── docker-compose.yml         ← MongoDB + MinIO + Kafka + Zookeeper
├── .env                       ← Konfiguracijske varijable
├── requirements.txt           ← Python ovisnosti
├── Snakefile                  ← Pipeline definicija (Snakemake)
├── main.py                    ← Alternativni entrypoint (bez Snakemake)
├── steps/
│   ├── step1_collect_species.py  ← Dohvat taksonomije s aves.regoch.net
│   ├── step2_consume_kafka.py    ← Čitanje Kafka opažanja → MongoDB
│   ├── step3_process_audio.py    ← Audio → MinIO + klasifikacija → MongoDB
│   └── step4_generate_report.py  ← CSV izvještaj s fuzzy filtrom
├── utils/
│   ├── config.py              ← Konfiguracija iz .env
│   ├── mongo_client.py        ← MongoDB konekcija
│   ├── minio_client.py        ← MinIO/S3 operacije
│   └── kafka_producer.py      ← Test producer za Kafka
├── audio_samples/             ← Stavi .wav/.mp3 datoteke ovdje
├── output/                    ← CSV izvještaji
└── .github/
    └── workflows/
        └── bird_pipeline.yml  ← GitHub Actions workflow
```

## Preduvjeti
- [Python 3.11+](https://python.org)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Git

## Pokretanje

### 1. Pokreni Docker usluge
```bash
cd projekt2
docker-compose up -d
```

Pričekaj ~15 sekundi da se sve usluge pokrenu:
```bash
docker ps
# Trebao bi vidjeti:
# birds_mongo      Up  0.0.0.0:27017->27017/tcp
# birds_minio      Up  0.0.0.0:9000->9000/tcp, 0.0.0.0:9001->9001/tcp
# birds_kafka      Up  0.0.0.0:9092->9092/tcp
# birds_zookeeper  Up  0.0.0.0:2181->2181/tcp
```

### 2. Instaliraj Python ovisnosti
```bash
pip install -r requirements.txt
```

### 3. (Opcionalno) Pošalji testne Kafka poruke
```bash
python utils/kafka_producer.py --count 10
```

### 4a. Pokreni pipeline s Python entrypoint-om
```bash
# Svi koraci
python main.py

# Samo određeni korak
python main.py --step 1       # Prikupljanje vrsta
python main.py --step 2       # Kafka opažanja
python main.py --step 3       # Audio obrada
python main.py --step 4       # CSV izvještaj

# S fuzzy filtrom (za korak 4)
python main.py --step 4 --species "Blackbird"
python main.py --step 4 --species "Turdus"      # Genus filter
python main.py --step 4 --species "Robin"
```

### 4b. Pokreni pipeline sa Snakemake
```bash
# Svi koraci
snakemake --cores 1

# S fuzzy filtrom
snakemake --cores 1 --config species_filter="Blackbird"

# Prikaži DAG (treba graphviz)
snakemake --dag | dot -Tsvg > pipeline_dag.svg
```

### 5. Pristupi MinIO web konzoli
```
URL: http://localhost:9001
Username: minioadmin
Password: minioadmin123
```
Ovdje možeš vidjeti uploadane audio datoteke u bucketu `bird-audio`
i logove klasifikacija u bucketu `bird-logs`.

### 6. Pristupi MongoDB podacima
```bash
# MongoDB CLI
docker exec -it birds_mongo mongosh -u mongouser -p mongopass123

# U mongosh konzoli:
use birds_db
db.species.countDocuments()        # Broj vrsta
db.observations.find().limit(5)    # Prva 5 opažanja
db.classifications.find().limit(5) # Prve klasifikacije
```

### 7. Zaustavi Docker
```bash
docker-compose down      # Zaustavi (podaci ostaju)
docker-compose down -v   # Zaustavi + briši podatke
```

## Pipeline koraci

### Korak 1 - Prikupljanje taksonomije
```
GET https://aves.regoch.net/species
         ↓
MongoDB kolekcija "species"
(upsert po taxon_key - nema duplikata)
```

### Korak 2 - Kafka opažanja
```
Kafka topic "bird-observations"
         ↓
MongoDB kolekcija "observations"
(fleksibilna shema - svaki observer šalje različita biološka polja!)
```

### Korak 3 - Audio obrada
```
audio_samples/*.wav
    ↓ upload
MinIO bucket "bird-audio"
    ↓ POST classify
https://aves.regoch.net/api/classify
    ↓ log
MinIO bucket "bird-logs" (JSON log)
    ↓ rezultat + taksonomija
MongoDB kolekcija "classifications"
```

### Korak 4 - CSV izvještaj
```
MongoDB "classifications" + "observations"
    ↓ aggregation pipeline (JOIN)
pandas DataFrame
    ↓ čišćenje + transformacija
    ↓ (opcionalno) fuzzy filter
output/bird_report_TIMESTAMP.csv
```

## MongoDB shema (bez fiksne sheme - NoSQL!)

### Kolekcija: species
```json
{
  "taxon_key": "2481139",
  "scientific_name": "Turdus merula",
  "common_name": "Blackbird",
  "order": "Passeriformes",
  "family": "Turdidae",
  "genus": "Turdus"
}
```

### Kolekcija: observations (fleksibilna shema!)
```json
{
  "taxon_code": "2481139",
  "location": {"latitude": 45.81, "longitude": 15.98},
  "observer": "dr_horvat",
  "biological_data": {
    "body_temperature": 40.5,    // Jedni šalju temperaturu
    "migration_status": "resident"
    // Drugi šalju wing_span, flock_size...
    // MongoDB prihvaća sve!
  }
}
```

### Kolekcija: classifications
```json
{
  "audio_file": "blackbird_morning.wav",
  "audio_object": "audio/20240515_blackbird.wav",  // MinIO putanja
  "minio_url": "http://localhost:9000/bird-audio/...",
  "log_object": "logs/20240515_blackbird.json",    // MinIO log
  "taxon_key": "2481139",
  "species": {
    "scientific_name": "Turdus merula",
    "common_name": "Blackbird",
    "family": "Turdidae"
  },
  "confidence": 0.912,
  "location": {"latitude": 45.815, "longitude": 15.982}
}
```

## GitHub Actions
Workflow se okida **ručno**:
1. GitHub → tvoj repo → Actions tab
2. "Bird Observation Pipeline" → "Run workflow"
3. Unesi opcionalni `species_filter`
4. Klikni zeleni "Run workflow"

CSV izvještaj će biti dostupan kao GitHub Artifact.
