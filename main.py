

import sys
import argparse
from datetime import datetime

#docker exec -it medical_postgres psql -U meduser -d medical_db
#python utils/kafka_producer.py --count 10

def main():
    """Pokreni Bird Observation Pipeline."""

    parser = argparse.ArgumentParser(
        description="Bird Observation Pipeline - entrypoint",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Primjeri:
  python main.py                           # Svi koraci
  python main.py --step 1                  # Samo prikupljanje vrsta
  python main.py --step 2                  # Samo Kafka konzumiranje
  python main.py --step 3                  # Samo obrada audio-a
  python main.py --step 4                  # Samo generiranje izvještaja
  python main.py --step 4 --species Turdus # Izvještaj s fuzzy filtrom
  python main.py --species "Robin"         # Svi koraci + filter
        """
    )

    parser.add_argument(
        "--step", "-s",
        type=int,
        choices=[1, 2, 3, 4],
        default=None,
        help="Pokrenuti samo određeni korak (1-4)"
    )
    parser.add_argument(
        "--species",
        type=str,
        default=None,
        help="Fuzzy filter po nazivu vrste za Korak 4"
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default="./output",
        help="Izlazni direktorij (default: ./output)"
    )

    args = parser.parse_args()

    print("=" * 60)
    print("   BIRD OBSERVATION PIPELINE")
    print(f"   Pokrenuto: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Uvozi koraka
    from steps.step1_collect_species import collect_species
    from steps.step2_consume_kafka import consume_observations
    from steps.step3_process_audio import process_audio
    from steps.step4_generate_report import generate_report, visualize_report

    # Odaberi koji koraci će se pokrenuti
    if args.step is None:
        # Pokreni sve korake
        steps = [1, 2, 3, 4]
    else:
        steps = [args.step]

    # Izvodi korake redom
    for step in steps:
        print(f"\n{'─'*50}")
        print(f"  KORAK {step}")
        print(f"{'─'*50}")

        if step == 1:
            collect_species()

        elif step == 2:
            consume_observations()

        elif step == 3:
            process_audio()

        elif step == 4:
            csv_path = generate_report(
                species_filter=args.species,
                output_dir=args.output
            )
            if csv_path:
                visualize_report(csv_path)

    print(f"\n{'='*60}")
    print("   PIPELINE ZAVRŠEN")
    print(f"   Završeno: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
