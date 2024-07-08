# txt_to_csv.py
import csv
import argparse
from tqdm import tqdm


def parse_line_to_csv_format(line):
    # Split the path into its components
    parts = line.strip().split('/')
    # Extract the relevant parts including 'sub-' and 'ses-'
    subject = parts[-4]
    session = parts[-3]
    folder = parts[-2]
    file_name = parts[-1]
    # Remove extensions
    file_name = file_name.replace('.nii.gz', '').replace('.json', '')
    return [subject, session, folder, file_name]


def convert_txt_to_csv(input_txt_file, output_csv_file):
    with open(input_txt_file, 'r') as txt_file:
        lines = txt_file.readlines()

    with open(output_csv_file, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['Subject', 'Session', 'Folder', 'File'])

        for line in tqdm(lines, desc="Converting TXT to CSV"):
            csv_writer.writerow(parse_line_to_csv_format(line))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert text file paths to CSV format while preserving 'sub-' and 'ses-' prefixes.")
    parser.add_argument("-i", "--input_txt",
                        help="Path to the input text file with the file paths.")
    parser.add_argument("-o", "--output_csv",
                        help="Path to the output CSV file.")

    args = parser.parse_args()

    convert_txt_to_csv(args.input_txt, args.output_csv)

    print(f"CSV file has been created at {args.output_csv}")
