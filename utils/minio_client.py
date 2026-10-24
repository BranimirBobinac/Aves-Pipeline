

import io
import json
from minio import Minio
from minio.error import S3Error
from utils.config import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
    MINIO_BUCKET_AUDIO, MINIO_BUCKET_LOGS, MINIO_USE_SSL
)


def get_minio_client() -> Minio:
    """
    Kreiraj i vrati MinIO klijent.
    Minio(endpoint, access_key, secret_key, secure=False/True)
    """
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_USE_SSL  
    )


def ensure_buckets():
    """
    Kreiraj S3 buckete ako ne postoje.
    Bucket = direktorij/kontejner za datoteke u S3/MinIO.
    """
    client = get_minio_client()
    for bucket in [MINIO_BUCKET_AUDIO, MINIO_BUCKET_LOGS]:
        # bucket_exists() provjerava postoji li bucket
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)  # Kreiraj novi bucket
            print(f"[MinIO] Kreiran bucket: {bucket}")
        else:
            print(f"[MinIO] Bucket već postoji: {bucket}")


def upload_file(local_path: str, object_name: str, bucket: str = MINIO_BUCKET_AUDIO) -> str:
    """
    Prenesi lokalnu datoteku u MinIO bucket.

    Args:
        local_path: Putanja do lokalne datoteke
        object_name: Ime objekta u MinIO (može sadržavati 'direktorij/')
        bucket: Ime bucketa

    Returns:
        object_name - jedinstveni identifikator objekta u MinIO
    """
    client = get_minio_client()
    # fput_object() - upload datoteke iz lokalne putanje
    client.fput_object(bucket, object_name, local_path)
    print(f"[MinIO] Uploadano: {local_path} → {bucket}/{object_name}")
    return object_name


def upload_json_log(data: dict, object_name: str, bucket: str = MINIO_BUCKET_LOGS) -> str:
    """
    Spremi JSON podatke direktno u MinIO bez lokalne datoteke.
    Koristimo za logove API poziva klasifikacije.

    Proces: dict -> JSON string -> bytes -> MinIO
    """
    client = get_minio_client()
    
    json_bytes = json.dumps(data, indent=2, default=str).encode("utf-8")
    
    client.put_object(
        bucket,
        object_name,
        io.BytesIO(json_bytes), 
        length=len(json_bytes),
        content_type="application/json"
    )
    print(f"[MinIO] Log spreman: {bucket}/{object_name}")
    return object_name


def get_file_url(object_name: str, bucket: str = MINIO_BUCKET_AUDIO) -> str:
    """
    Generiraj presigned URL za privremeni pristup datoteci.
    URL vrijedi 7 dana (604800 sekundi).
    """
    from datetime import timedelta
    client = get_minio_client()
    # presigned_get_object() - URL koji ne zahtijeva autentifikaciju
    return client.presigned_get_object(bucket, object_name, expires=timedelta(days=7))
