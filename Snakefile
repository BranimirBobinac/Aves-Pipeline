

import os
from datetime import datetime

# ── KONFIGURACIJA ──────────────────────────────────────────────
# Parametri koji se mogu proslijediti pri pokretanju
# Primjer: snakemake --config species_filter="Turdus"
SPECIES_FILTER = config.get("species_filter", "")  # Opcionalni fuzzy filter
OUTPUT_DIR = config.get("output_dir", "output")     # Izlazni direktorij

# Timestamp za jedinstvene izlazne datoteke
TIMESTAMP = datetime.utcnow().strftime("%Y%m%d_%H%M%S")


# ── FINALNI CILJ ───────────────────────────────────────────────
# "all" je defaultni cilj - ono što Snakemake želi producirat
# Snakemake gradi lanac ovisnosti unatrag (backward chaining)
rule all:
    """Pokretanje cijelog pipelinea - od prikupljanja do izvještaja."""
    input:
        expand("{output_dir}/pipeline_complete.flag", output_dir=OUTPUT_DIR)


# ── KORAK 1: Prikupi taksonomske podatke ───────────────────────
rule collect_species:
    """
    Dohvati podatke o vrstama ptica s aves.regoch.net
    i pohrani u MongoDB kolekciju 'species'.
    """
    output:
        touch("{output_dir}/step1_done.flag")  # touch() kreira praznu datoteku kao signal
    log:
        "{output_dir}/logs/step1.log"
    shell:
        """
        echo "[Pipeline] Korak 1: Prikupljanje taksonomskih podataka..."
        python steps/step1_collect_species.py 2>&1 | tee {log}
        echo "Korak 1 završen." >> {log}
        """


# ── KORAK 2: Konzumiraj Kafka opažanja ─────────────────────────
rule consume_kafka:
    """
    Pročitaj sve poruke s Kafka broker-a i pohrani
    opažanja ptica u MongoDB kolekciju 'observations'.
    """
    input:
        "{output_dir}/step1_done.flag"  # Ovisi o Koraku 1
    output:
        touch("{output_dir}/step2_done.flag")
    log:
        "{output_dir}/logs/step2.log"
    shell:
        """
        echo "[Pipeline] Korak 2: Konzumiranje Kafka opažanja..."
        python steps/step2_consume_kafka.py 2>&1 | tee {log}
        echo "Korak 2 završen." >> {log}
        """


# ── KORAK 3: Obradi audio datoteke ─────────────────────────────
rule process_audio:
    """
    Upload audio datoteka u MinIO i klasificiraj ptice
    pomoću AI modela. Spremi rezultate u MongoDB.
    """
    input:
        "{output_dir}/step1_done.flag"  # Treba taksonomske podatke za linkanje
    output:
        touch("{output_dir}/step3_done.flag")
    log:
        "{output_dir}/logs/step3.log"
    shell:
        """
        echo "[Pipeline] Korak 3: Obrada audio datoteka..."
        python steps/step3_process_audio.py 2>&1 | tee {log}
        echo "Korak 3 završen." >> {log}
        """


# ── KORAK 4: Generiraj CSV izvještaj ───────────────────────────
rule generate_report:
    """
    Generiraj finalni CSV izvještaj s statistikama.
    Primijeni fuzzy filter ako je zadan parametar species_filter.
    """
    input:
        step2="{output_dir}/step2_done.flag",  # Treba opažanja
        step3="{output_dir}/step3_done.flag"   # Treba klasifikacije
    output:
        touch("{output_dir}/step4_done.flag")
    log:
        "{output_dir}/logs/step4.log"
    params:
        # Proslijedi species_filter ako postoji
        filter_arg=f"--species '{SPECIES_FILTER}'" if SPECIES_FILTER else ""
    shell:
        """
        echo "[Pipeline] Korak 4: Generiranje izvještaja..."
        python steps/step4_generate_report.py \
            --output {wildcards.output_dir} \
            {params.filter_arg} \
            --visualize 2>&1 | tee {log}
        echo "Korak 4 završen." >> {log}
        """


# ── ZAVRŠNI KORAK ──────────────────────────────────────────────
rule finalize:
    """Označi pipeline kao završen i prikaži sažetak."""
    input:
        "{output_dir}/step4_done.flag"
    output:
        touch("{output_dir}/pipeline_complete.flag")
    shell:
        """
        echo ""
        echo "=================================================="
        echo "  PIPELINE ZAVRŠEN USPJEŠNO!"
        echo "  Rezultati su u direktoriju: {wildcards.output_dir}/"
        echo "=================================================="
        ls -la {wildcards.output_dir}/*.csv 2>/dev/null || echo "  (nema CSV datoteka)"
        """


# ── OPCIONALNI CILJ: Samo fuzzy filtriran izvještaj ────────────
rule report_filter:
    """
    Generiraj samo izvještaj s fuzzy filtrom.
    Pretpostavlja da su koraci 1-3 već izvršeni.

    Pokretanje: snakemake report_filter --config species_filter="Robin"
    """
    params:
        filter_arg=f"--species '{SPECIES_FILTER}'" if SPECIES_FILTER else "--species 'all'"
    shell:
        """
        python steps/step4_generate_report.py \
            --output {OUTPUT_DIR} \
            {params.filter_arg} \
            --visualize
        """


# ── ČIŠĆENJE ───────────────────────────────────────────────────
rule clean:
    """Obriši sve flag datoteke i logove (ne briše baze podataka!)"""
    shell:
        f"""
        rm -f {OUTPUT_DIR}/*.flag
        rm -f {OUTPUT_DIR}/logs/*.log
        echo "Obrisane flag datoteke i logovi."
        """
