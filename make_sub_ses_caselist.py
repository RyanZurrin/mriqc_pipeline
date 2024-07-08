# make_sub_ses_caselist.py
import csv
import argparse
from pathlib import Path


def parse_csv_for_unique_pairs(input_csv_file, output_csv_file):
    unique_pairs = set()
    with open(input_csv_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            unique_pairs.add((row['Subject'], row['Session']))

    with open(output_csv_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Subject', 'Session'])  # Write header
        for pair in sorted(unique_pairs):
            writer.writerow(pair)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate a CSV file with unique subject-session pairs for MRIQC.")
    parser.add_argument("-i", "--input_csv",
                        help="Path to the input CSV file containing the missing cases.")
    parser.add_argument("-o", "--output_csv",
                        help="Path to the output CSV file to store unique subject-session pairs.")

    args = parser.parse_args()

    # Make sure the output directory exists
    output_csv_path = Path(args.output_csv)
    output_csv_path.parent.mkdir(parents=True, exist_ok=True)

    parse_csv_for_unique_pairs(args.input_csv, args.output_csv)

    print(
        f"Unique subject-session pairs have been written to {args.output_csv}")
