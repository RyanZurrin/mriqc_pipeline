# import argparse
#
#
# def find_missing_entries(nifti_file, json_file, output_diff_file):
#     """
#     Compare the two lists of file paths and write the entries that are in the NIfTI list but not in the JSON list.
#
#     Parameters
#     ----------
#     nifti_file : str
#         Path to the text file containing NIfTI paths.
#     json_file : str
#         Path to the text file containing JSON paths.
#     output_diff_file : str
#         Path to the output text file where differences will be written.
#     """
#     with open(nifti_file, 'r') as file:
#         nifti_paths = set(file.read().splitlines())
#
#     with open(json_file, 'r') as file:
#         json_paths = set(file.read().splitlines())
#
#     missing_paths = nifti_paths - json_paths
#
#     with open(output_diff_file, 'w') as file:
#         for path in sorted(missing_paths):
#             file.write(path + '\n')
#
#
# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(
#         description="Find missing entries between NIfTI paths and JSON paths.")
#     parser.add_argument("nifti_file",
#                         help="Path to the text file containing modified NIfTI paths.")
#     parser.add_argument("json_file",
#                         help="Path to the text file containing modified JSON paths.")
#     parser.add_argument("output_diff_file",
#                         help="Path to the output text file for missing entries.")
#     args = parser.parse_args()
#
#     find_missing_entries(args.nifti_file, args.json_file, args.output_diff_file)

# find_missing_entries.py
import argparse
import csv


def read_csv_file(csv_file):
    entries = set()
    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header
        for row in reader:
            entries.add(tuple(row))
    return entries


def find_missing_entries(nifti_csv, json_csv, output_diff_file):
    nifti_entries = read_csv_file(nifti_csv)
    json_entries = read_csv_file(json_csv)

    missing_entries = nifti_entries - json_entries

    with open(output_diff_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Subject', 'Session', 'Folder', 'File'])
        for entry in sorted(missing_entries):
            writer.writerow(entry)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Find missing entries between NIfTI paths and JSON paths in CSV files.")
    parser.add_argument("-n", "--nifti_csv",
                        help="Path to the CSV file containing NIfTI paths.")
    parser.add_argument("-j", "--json_csv",
                        help="Path to the CSV file containing JSON paths.")
    parser.add_argument("-o", "--output_diff_file",
                        help="Path to the output CSV file for missing entries.")
    args = parser.parse_args()

    find_missing_entries(args.nifti_csv, args.json_csv, args.output_diff_file)

    print(f"Missing entries have been written to {args.output_diff_file}")
