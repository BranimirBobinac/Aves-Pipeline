# Bird Observation Pipeline

A data pipeline that collects, stores, classifies and reports on bird observations. It combines three data sources into one report:

- **Taxonomy data**: bird species fetched from the `aves.regoch.net` REST API
- **Field observations**: messages from ornithologists streamed through **Apache Kafka**
- **Audio recordings**: bird sound files uploaded to **MinIO** (S3-compatible storage) and sent to an AI classification API

The data goes into **MongoDB**, gets joined and cleaned with **pandas**, and comes out as a CSV report with per-species statistics. You can filter the report by species name with a fuzzy filter, so typos still match. Snakemake orchestrates the pipeline (or you can run it with a plain Python entrypoint), and a GitHub Actions workflow can run it on demand.

---

## Technologies

| Technology | Purpose |
|------------|---------|
| **Python 3.11** | Main programming language |
| **MongoDB 7.0** (`pymongo`) | NoSQL database for species taxonomy, observations and classifications. The schema is flexible, so each observer can send different biological fields |
| **MinIO** (`minio` SDK) | S3-compatible object storage for audio files and JSON logs of classification requests |
| **Apache Kafka** + **Zookeeper** (`kafka-python`) | Message broker that carries the ornithologists' observations |
| **Snakemake** | Workflow manager that defines the pipeline as a DAG of dependent rules |
| **pandas** | Data cleaning, transformation, aggregation and CSV export |
| **thefuzz** + **python-Levenshtein** | Fuzzy string matching for the species filter |
| **requests** | HTTP calls to the `aves.regoch.net` API (species list and audio classification) |
| **python-dotenv** | Loads configuration from the `.env` file |
| **Docker / Docker Compose** | Runs MongoDB, MinIO, Kafka and Zookeeper as containers |
| **GitHub Actions** | CI workflow that runs the pipeline on demand and uploads the reports as artifacts |

---

## Project structure

```
projekt2/
├── docker-compose.yml          # MongoDB, MinIO, Kafka, Zookeeper
├── .env                        # Configuration (connection strings, buckets, topic, API URL)
├── requirements.txt            # Python dependencies
├── Snakefile                   # Pipeline definition (Snakemake)
├── main.py                     # Alternative entrypoint (no Snakemake)
├── steps/
│   ├── step1_collect_species.py   # Fetch species taxonomy → MongoDB "species"
│   ├── step2_consume_kafka.py     # Consume Kafka observations → MongoDB "observations"
│   ├── step3_process_audio.py     # Audio → MinIO + classify API → MongoDB "classifications"
│   └── step4_generate_report.py   # Aggregate, clean, fuzzy-filter → CSV report
├── utils/
│   ├── config.py               # Reads settings from .env
│   ├── mongo_client.py         # MongoDB connection helpers
│   ├── minio_client.py         # Bucket creation, file/JSON upload, presigned URLs
│   └── kafka_producer.py       # Test producer that sends simulated observations
├── audio_samples/              # .wav / .mp3 / .ogg / .flac / .m4a files to classify
├── output/                     # Generated CSV reports, Snakemake flags and logs
└── .github/workflows/
    └── bird_pipeline.yml       # GitHub Actions workflow (manual trigger)
```

---

## How the pipeline works

```
             ┌────────────────────────┐
             │ Step 1: collect_species│  GET aves.regoch.net/species → MongoDB "species"
             └───────────┬────────────┘
             ┌───────────┴───────────┐
             ▼                       ▼
┌────────────────────────┐ ┌────────────────────────┐
│ Step 2: consume_kafka  │ │ Step 3: process_audio  │
│ Kafka topic            │ │ audio → MinIO          │
│ → MongoDB              │ │ → POST /api/classify   │
│   "observations"       │ │ → JSON log → MinIO     │
└───────────┬────────────┘ │ → MongoDB              │
            │              │   "classifications"    │
            │              └───────────┬────────────┘
            └───────────┬──────────────┘
                        ▼
             ┌────────────────────────┐
             │ Step 4: generate_report│  $lookup join → pandas → fuzzy filter → CSV
             └────────────────────────┘
```

### Step 1: Collect species taxonomy
Fetches the species list from `https://aves.regoch.net/species` and upserts it into the `species` collection, keyed on `taxon_key`, so there are no duplicates. It also creates indexes on `taxon_key` (unique), `scientific_name` and `common_name`. If the collection already has data, the step is skipped. If the API can't be reached, it falls back to 15 built-in European species.

### Step 2: Consume Kafka observations
Reads every message from the `bird-observations` topic, starting from the earliest offset. Each message is stored in the `observations` collection. The common fields (`taxon_code`, `location`, `observed_at`, `observer`) go at the top level, and all remaining fields go into a free-form `biological_data` sub-document. This shows MongoDB's flexible schema at work: one observer sends body temperature and migration status, another sends wing span and flock size. Writes are batched (100 at a time) and upserted on `observation_id`. If Kafka isn't available, the step inserts sample observations instead.

### Step 3: Process audio
For each audio file in `audio_samples/`:
1. Uploads the file to the MinIO bucket `bird-audio`
2. Sends it to `https://aves.regoch.net/api/classify` (multipart POST)
3. Saves a JSON log of the request and response to the MinIO bucket `bird-logs`
4. For every detected species, writes a document to `classifications` with the taxonomy pulled from the `species` collection embedded in it, plus the confidence score, location and MinIO references

If the classification API is unavailable, the step uses a simulated response that guesses the species from the file name. If the folder is empty, it creates placeholder WAV files.

### Step 4: Generate the report
Runs a MongoDB aggregation that joins `classifications` with `observations` on `taxon_key = taxon_code` (`$lookup`), then loads the result into pandas. There it:
- drops records without a scientific name, normalises whitespace in the names and rounds the confidence values
- optionally applies the **fuzzy filter** (`token_sort_ratio`, threshold 60, matched against both scientific and common names), so `"Blakbrid"` still finds *Blackbird*
- groups by species and computes the statistics: number of classifications, average/min/max confidence, total Kafka observations, first and last classification time, and number of unique observers
- writes the result to `output/bird_report[_<filter>]_<timestamp>.csv` and prints a top-10 table and a text bar chart to the console

Sample output:

```csv
scientific_name,common_name,family,order,classified_observations,avg_confidence,max_confidence,min_confidence,total_kafka_observations,lat,lon,first_classified,last_classified,unique_observers
Turdus merula,Blackbird,Turdidae,Passeriformes,10,0.806,0.976,0.434,20,45.815,15.9819,2026-06-19 00:25,2026-09-04 08:28,1
Parus major,Great Tit,Paridae,Passeriformes,10,0.793,0.974,0.578,10,45.815,15.9819,2026-06-19 00:25,2026-09-04 08:28,1
```

---

## Getting started

### Prerequisites
- Python 3.11+
- Docker Desktop (or Docker Engine + Docker Compose)
- Git
- *(optional)* Graphviz, to render the pipeline DAG

### 1. Start the infrastructure
```bash
cd projekt2
docker-compose up -d
```

Wait about 15 seconds, then check that all four containers are running:
```bash
docker ps
# birds_mongo      0.0.0.0:27017->27017
# birds_minio      0.0.0.0:9000->9000, 0.0.0.0:9001->9001
# birds_kafka      0.0.0.0:9092->9092
# birds_zookeeper  0.0.0.0:2181->2181
```

### 2. Install the Python dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure (optional)
The default values in `.env` match `docker-compose.yml`, so the project works out of the box. Edit `.env` if you change ports or credentials, or if you want to set a different default location for the audio recordings (`AUDIO_LOCATION_LAT` / `AUDIO_LOCATION_LON`).

### 4. Send test observations to Kafka (optional)
```bash
python -m utils.kafka_producer --count 10 --delay 0.3
```
This sends simulated observations for several species and locations around Zagreb and Varaždin, with different fields depending on the observer.

### 5. Run the pipeline

**Option A: Python entrypoint**
```bash
python main.py                          # all steps
python main.py --step 1                 # only step 1 (species)
python main.py --step 2                 # only step 2 (Kafka)
python main.py --step 3                 # only step 3 (audio)
python main.py --step 4                 # only step 4 (report)
python main.py --step 4 --species "Blackbird"   # report with fuzzy filter
python main.py --step 4 --species "Turdus"      # filter by genus
python main.py --output ./my_reports    # custom output directory
```

**Option B: Snakemake**
```bash
snakemake --cores 1                                     # full pipeline
snakemake --cores 1 --config species_filter="Robin"     # with fuzzy filter
snakemake report_filter --cores 1 --config species_filter="Robin"   # report only (steps 1–3 already done)
snakemake clean --cores 1                               # delete flags and logs (data in the databases is kept)
snakemake --dag | dot -Tsvg > pipeline_dag.svg          # render the DAG
```

Snakemake keeps track of finished steps with `output/stepN_done.flag` files. To run the whole pipeline again, delete them with `snakemake clean` first.

> **Note:** the Snakefile runs each step as `python steps/stepN_*.py`. If you get `ModuleNotFoundError: No module named 'utils'`, add the project root to the Python path first:
> `export PYTHONPATH=$PWD` (Linux/macOS) or `set PYTHONPATH=%CD%` (Windows).

### 6. Look at the results

**CSV reports:** in the `output/` folder.

**MinIO web console:** http://localhost:9001 (user `minioadmin`, password `minioadmin123`). The uploaded audio is in the bucket `bird-audio`, and the classification logs are in `bird-logs`.

**MongoDB:**
```bash
docker exec -it birds_mongo mongosh -u mongouser -p mongopass123
```
```js
use birds_db
db.species.countDocuments()
db.observations.find().limit(5)
db.classifications.find().limit(5)
```

### 7. Stop the services
```bash
docker-compose down       # stop the containers, keep the data
docker-compose down -v    # stop the containers and delete the volumes (all data)
```

---

## Running with GitHub Actions

The workflow `.github/workflows/bird_pipeline.yml` is started manually (`workflow_dispatch`):

1. Open the repository on GitHub and go to the **Actions** tab
2. Select **Bird Observation Pipeline** and click **Run workflow**
3. Optionally enter a `species_filter` and choose which step to run (`all`, `collect_species`, `consume_kafka`, `process_audio`, `generate_report`)

The workflow starts MongoDB and MinIO as service containers, installs the dependencies, writes a `.env` file and runs Snakemake. The CSV reports are uploaded as a build artifact (kept for 30 days) and the logs as a separate artifact (kept for 7 days). The workflow doesn't start a Kafka service, so step 2 falls back to its built-in sample observations.

---

## MongoDB data model

**`species`**
```json
{
  "taxon_key": "2481139",
  "scientific_name": "Turdus merula",
  "common_name": "Blackbird",
  "order": "Passeriformes",
  "family": "Turdidae",
  "genus": "Turdus",
  "raw": { "...": "original API response" }
}
```

**`observations`** (the fields in `biological_data` differ from observer to observer)
```json
{
  "observation_id": "obs_20260904082821_1234",
  "taxon_code": "2481139",
  "location": { "latitude": 45.815, "longitude": 15.982 },
  "observed_at": "2026-09-04T08:28:21+00:00",
  "observer": "observer_1",
  "biological_data": {
    "body_temperature": 40.5,
    "migration_status": "resident",
    "behavior": "singing",
    "habitat": "urban_garden"
  },
  "imported_at": "2026-09-04T08:28:30"
}
```

**`classifications`**
```json
{
  "audio_file": "blackbird_morning.wav",
  "audio_object": "audio/20260904_082821_blackbird_morning.wav",
  "minio_url": "http://localhost:9000/bird-audio/audio/20260904_082821_blackbird_morning.wav",
  "log_object": "logs/20260904_082821_blackbird_morning.wav.json",
  "taxon_key": "2481139",
  "species": {
    "scientific_name": "Turdus merula",
    "common_name": "Blackbird",
    "family": "Turdidae",
    "order": "Passeriformes"
  },
  "confidence": 0.912,
  "location": { "latitude": 45.815, "longitude": 15.9819 },
  "classified_at": "2026-09-04T08:28:22"
}
```

---

## Configuration reference (`.env`)

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGO_URI` | `mongodb://mongouser:mongopass123@localhost:27017/` | MongoDB connection string |
| `MONGO_DB` | `birds_db` | Database name |
| `MINIO_ENDPOINT` | `localhost:9000` | MinIO API address |
| `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | `minioadmin` / `minioadmin123` | MinIO credentials |
| `MINIO_BUCKET_AUDIO` | `bird-audio` | Bucket for the audio files |
| `MINIO_BUCKET_LOGS` | `bird-logs` | Bucket for the classification logs |
| `MINIO_USE_SSL` | `false` | Use HTTPS for MinIO |
| `KAFKA_BROKER` | `localhost:9092` | Kafka bootstrap server |
| `KAFKA_TOPIC` | `bird-observations` | Topic for the observations |
| `AVES_BASE_URL` | `https://aves.regoch.net` | Base URL of the species and classification API |
| `AUDIO_DIR` | `./audio_samples` | Folder with the audio files |
| `AUDIO_LOCATION_LAT` / `AUDIO_LOCATION_LON` | `45.8150` / `15.9819` | Default recording location (Zagreb) |

---

## Fault tolerance

Every step keeps working when an external service is down, so the whole pipeline can be shown end to end even offline:

| If this is unavailable… | …the pipeline does this |
|-------------------------|-------------------------|
| Species API | Uses 15 built-in species |
| Kafka broker | Inserts 5 sample observations |
| Classification API | Generates a simulated BirdNET-style response |
| Audio files | Creates placeholder WAV files |

> The credentials in `.env` and `docker-compose.yml` are for local development only. Don't use them in production.
