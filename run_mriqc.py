import csv
import argparse
from mriqc import run_mriqc_on_data
from pathlib import Path


def parse_csv_for_unique_pairs(csv_file):
    unique_pairs = set()
    with open(csv_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            unique_pairs.add((row['Subject'], row['Session']))
    print(unique_pairs)
    return unique_pairs


def call_mriqc_for_each_pair(unique_pairs, rawdata_dir, mriqc_outdir_root,
                             temp_dir, bsub, specific_nodes):
    for subject_id, session_id in unique_pairs:
        print(f"Processing {subject_id} {session_id}")
        run_mriqc_on_data(str(rawdata_dir), subject_id, session_id,
                          str(mriqc_outdir_root), temp_dir, bsub,
                          specific_nodes)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run MRIQC on unique subject-session pairs extracted from "
                    "a CSV file.")
    parser.add_argument("csv_file",
                        help="Path to the CSV file containing subject-session "
                             "pairs.")
    parser.add_argument("rawdata_dir",
                        help="Path to the BIDS dataset root directory.")
    parser.add_argument("mriqc_outdir_root",
                        help="Path to save MRIQC outputs.")
    parser.add_argument("--temp_dir", type=str,
                        default='/data/predict1/home/rez3/tmp',
                        help="Location of MRIQC working directory")
    parser.add_argument("--bsub", action="store_true",
                        help="Use bsub to submit jobs")
    parser.add_argument("--specific_nodes", nargs='*', default=[],
                        help="List of specific nodes for job submission")

    args = parser.parse_args()

    # Convert rawdata_dir and mriqc_outdir_root to Path objects
    rawdata_dir = Path(args.rawdata_dir)
    mriqc_outdir_root = Path(args.mriqc_outdir_root)

    unique_pairs = parse_csv_for_unique_pairs(args.csv_file)
    call_mriqc_for_each_pair(unique_pairs, rawdata_dir, mriqc_outdir_root,
                             args.temp_dir, args.bsub, args.specific_nodes)
