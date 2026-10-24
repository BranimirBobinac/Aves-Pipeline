

import sys
import os
import argparse
from datetime import datetime
import pandas as pd
from thefuzz import fuzz, process as fuzzy_process  # Fuzzy string matching
from utils.config import COLLECTION_CLASSIFICATIONS, COLLECTION_OBSERVATIONS
from utils.mongo_client import get_collection


def generate_report(species_filter: str = None, output_dir: str = "./output"):
    """
    Generiraj konačni CSV izvještaj.

    Args:
        species_filter: Opcionalni fuzzy filter po nazivu vrste
                        (npr. "Blackbird", "Turdus", "Robin")
        output_dir: Direktorij za spremi CSV
    """
    os.makedirs(output_dir, exist_ok=True)

    print("[Korak 4] Dohvaćam podatke iz MongoDB...")

    # ── 1. DOHVAT PODATAKA IZ MONGODB ──────────────────────────
    classifications_col = get_collection(COLLECTION_CLASSIFICATIONS)
    observations_col = get_collection(COLLECTION_OBSERVATIONS)

    
    pipeline = [
        {
            "$match": {"confidence": {"$gt": 0}}  # Samo pozitivne detekcije
        },
        {
            # $lookup = LEFT JOIN u MongoDB
            
            "$lookup": {
                "from": COLLECTION_OBSERVATIONS,
                "localField": "taxon_key",      
                "foreignField": "taxon_code",  
                "as": "obs_data"               
            }
        },
        {
            "$project": {
                "_id": 0,
                "audio_file": 1,
                "taxon_key": 1,
                "scientific_name": "$species.scientific_name",
                "common_name": "$species.common_name",
                "family": "$species.family",
                "order": "$species.order",
                "confidence": 1,
                "latitude": "$location.latitude",
                "longitude": "$location.longitude",
                "classified_at": 1,
                "observation_count": {"$size": "$obs_data"}, 
                "observers": {
                    "$map": {
                        "input": "$obs_data",
                        "as": "o",
                        "in": "$$o.observer"     
                    }
                }
            }
        }
    ]

    results = list(classifications_col.aggregate(pipeline))

    if not results:
        print("[Korak 4] Nema klasifikacija za izvještaj.")
        return None

    print(f"[Korak 4] Dohvaćeno {len(results)} klasifikacijskih zapisa.")

    # ── 2. PANDAS DATAFRAME ────────────────────────────────────
    # Pretvori MongoDB rezultate u pandas DataFrame za obradu
    df = pd.DataFrame(results)

    # ── 3. ČIŠĆENJE I TRANSFORMACIJA PODATAKA ─────────────────
    print("[Korak 4] Čišćenje podataka...")

    # Ukloni redove s praznim scientific_name
    df = df[df["scientific_name"].notna() & (df["scientific_name"] != "")]

    # Zaokruži confidence na 3 decimale
    df["confidence"] = df["confidence"].round(3)

    # Normaliziraj nazive vrsta (ukloni višestruke razmake, trim)
    df["scientific_name"] = df["scientific_name"].str.strip().str.replace(r"\s+", " ", regex=True)
    df["common_name"] = df["common_name"].str.strip()

    # Pretvori classified_at u čitljivi format
    df["classified_at"] = pd.to_datetime(df["classified_at"]).dt.strftime("%Y-%m-%d %H:%M")

    # Pretvori observers listu u string (comma-separated)
    df["observers"] = df["observers"].apply(
        lambda obs: ", ".join(set(obs)) if isinstance(obs, list) else ""
    )

    # ── 4. FUZZY FILTER ───────────────────────────
    if species_filter:
        print(f"[Korak 4] Primjenjujem fuzzy filter: '{species_filter}'")
        df = _apply_fuzzy_filter(df, species_filter)

        if df.empty:
            print(f"[Korak 4] Nema rezultata za filter '{species_filter}'.")
            return None

        print(f"[Korak 4] Nakon fuzzy filtriranja: {len(df)} zapisa.")

    # ── 5. GRUPIRAMO I RAČUNAMO STATISTIKE ────────────────────
    stats = df.groupby(["scientific_name", "common_name", "family", "order"]).agg(
        classified_observations=("confidence", "count"),   
        avg_confidence=("confidence", "mean"),             
        max_confidence=("confidence", "max"),
        min_confidence=("confidence", "min"),
        total_kafka_observations=("observation_count", "sum"), 
        lat=("latitude", "first"),
        lon=("longitude", "first"),
        first_classified=("classified_at", "min"),
        last_classified=("classified_at", "max"),
        unique_observers=("observers", lambda x: len(set(", ".join(x).split(", "))))
    ).reset_index()

    # Zaokruži prosječne vrijednosti
    stats["avg_confidence"] = stats["avg_confidence"].round(3)

    # Sortiraj po broju klasifikacija (silazno)
    stats = stats.sort_values("classified_observations", ascending=False)

    # ── 6. SPREMI CSV ──────────────────────────────────────────
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filter_suffix = f"_{species_filter.replace(' ', '_')}" if species_filter else ""
    csv_filename = f"bird_report{filter_suffix}_{timestamp}.csv"
    csv_path = os.path.join(output_dir, csv_filename)

    
    stats.to_csv(csv_path, index=False, encoding="utf-8")

    print(f"\n[Korak 4]  Izvještaj spreman: {csv_path}")
    print(f"[Korak 4] Ukupno vrsta u izvještaju: {len(stats)}")
    print(f"\n{'Vrsta':<30} {'Obično ime':<25} {'Klasifikacija':<15} {'Avg confidence':<15}")
    print("-" * 90)

    # Prikaži top 10 u konzoli
    for _, row in stats.head(10).iterrows():
        print(f"{row['scientific_name']:<30} {row['common_name']:<25} "
              f"{row['classified_observations']:<15} {row['avg_confidence']:<15.3f}")

    return csv_path


def _apply_fuzzy_filter(df: pd.DataFrame, filter_query: str,
                         threshold: int = 60) -> pd.DataFrame:
    """
    Fuzzy string matching filtriranje vrsta.

    Fuzzy matching pronalazi slične stringove čak i uz tipfelere.
    Primjeri:
        "Blackbird" pronalazi "Turdus merula" (common_name = "Blackbird")
        "Robbin" (typo) pronalazi "European Robin"
        "Turdus" pronalazi sve vrste roda Turdus

    Args:
        df: DataFrame s klasifikacijama
        filter_query: Upit za pretraživanje
        threshold: Minimalna sličnost (0-100), default 60

    Returns:
        Filtrirani DataFrame
    """
   
    all_names = pd.concat([
        df["scientific_name"].dropna(),
        df["common_name"].dropna()
    ]).unique().tolist()

    
    matches = fuzzy_process.extract(
        filter_query,
        all_names,
        scorer=fuzz.token_sort_ratio, 
        limit=20                        
    )

    # Filtriraj samo one s dostatnom sličnošću
    matched_names = {name for name, score in matches if score >= threshold}

    print(f"  Fuzzy matches (threshold={threshold}): {matched_names}")

    # Filtriraj DataFrame po pronađenim imenima
    mask = (
        df["scientific_name"].isin(matched_names) |
        df["common_name"].isin(matched_names)
    )

    return df[mask].copy()


def visualize_report(csv_path: str):
    """
    Opcionalna vizualizacija CSV izvještaja u konzoli.
    U produkciji bi ovo moglo biti matplotlib/plotly grafikon.
    """
    if not csv_path or not os.path.exists(csv_path):
        return

    df = pd.read_csv(csv_path)
    print("\n" + "=" * 60)
    print("VIZUALIZACIJA - Top vrsta po klasifikacijama")
    print("=" * 60)

    max_obs = df["classified_observations"].max()

    for _, row in df.head(10).iterrows():
        bar_len = int((row["classified_observations"] / max_obs) * 30)
        bar = "█" * bar_len
        print(f"{row['common_name'][:20]:<22} {bar} {row['classified_observations']}")


if __name__ == "__main__":
    # Parsiranje argumenata komandne linije
    parser = argparse.ArgumentParser(
        description="Generiraj CSV izvještaj o klasifikacijama ptica"
    )
    parser.add_argument(
        "--species", "-s",
        type=str,
        default=None,
        help="Fuzzy filter po nazivu vrste (npr. 'Blackbird', 'Turdus')"
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default="./output",
        help="Izlazni direktorij za CSV (default: ./output)"
    )
    parser.add_argument(
        "--visualize", "-v",
        action="store_true",
        help="Prikaži bar chart u konzoli"
    )

    args = parser.parse_args()

    csv = generate_report(
        species_filter=args.species,
        output_dir=args.output
    )

    if args.visualize and csv:
        visualize_report(csv)

    sys.exit(0)
