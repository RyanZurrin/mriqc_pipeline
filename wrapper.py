# wrapper.py
import subprocess
import argparse
from tqdm import tqdm
from pathlib import Path
import pandas as pd



def run_collect_files(rawdata_dir, mriqc_output_dirs, nifti_output_txt,
                      json_output_txt):
    subprocess.run(
        ['python', '/data/predict1/home/rez3/bin/code/mriqc_pipeline'
                   '/collect_files.py', rawdata_dir] + mriqc_output_dirs +
        ['--nifti_output_txt', nifti_output_txt, '--json_output_txt',
         json_output_txt])


def run_convert_txt_to_csv(input_txt, output_csv):
    subprocess.run(
        ['python', '/data/predict1/home/rez3/bin/code/mriqc_pipeline'
                   '/txt_to_csv.py', '--input_txt', input_txt, '--output_csv',
         output_csv])


def run_find_missing_entries(nifti_csv, json_csv, output_diff_file):
    subprocess.run(
        ['python', '/data/predict1/home/rez3/bin/code/mriqc_pipeline'
                   '/find_missing_entries.py', '--nifti_csv', nifti_csv,
         '--json_csv', json_csv, '--output_diff_file', output_diff_file])


def run_make_sub_ses_caselist(input_csv, output_csv):
    subprocess.run(
        ['python', '/data/predict1/home/rez3/bin/code/mriqc_pipeline'
                   '/make_sub_ses_caselist.py', '--input_csv', input_csv,
         '--output_csv', output_csv])
    
def merge_unique_csv_files(csv_file_1, csv_file_2, output_csv):
    df1 = pd.read_csv(csv_file_1)
    df2 = pd.read_csv(csv_file_2)
    merged_df = pd.concat([df1, df2]).drop_duplicates().reset_index(drop=True)
    merged_df.to_csv(output_csv, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Wrapper script to automate the process.")
    parser.add_argument("rawdata_dir",
                        help="Path to the rawdata directory.")
    parser.add_argument("mriqc_output_dirs", nargs='+',
                        help="Paths to the MRIQC output directories.")
    parser.add_argument("-n", "--nifti_output_txt",
                        default="/data/predict1/home/rez3/bin/code"
                                "/mriqc_pipeline/nifti_files.txt",
                        help="Path to output text file for NIfTI files.")
    parser.add_argument("-j", "--json_output_txt",
                        default="/data/predict1/home/rez3/bin/code"
                                "/mriqc_pipeline/json_files.txt",
                        help="Path to output text file for JSON files.")
    parser.add_argument("-nc", "--nifti_csv",
                        default="/data/predict1/home/rez3/bin/code"
                                "/mriqc_pipeline/nifti_files.csv",
                        help="Path to output CSV file for NIfTI files.")
    parser.add_argument("-jc", "--json_csv",
                        default="/data/predict1/home/rez3/bin/code"
                                "/mriqc_pipeline/json_files.csv",
                        help="Path to output CSV file for JSON files.")
    parser.add_argument("-o", "--output_diff_file",
                        default="missing_entries.csv",
                        help="Path to output CSV file for missing entries.")
    parser.add_argument("-u", "--unique_pairs_csv",
                        default="/data/predict1/home/rez3/bin/csv_files"
                                "/mriqc_run_cases.csv",
                        help="Path to output CSV file for unique "
                             "subject-session pairs.")
    parser.add_argument("-r", "--rerun_csv",
                        default="/data/predict1/home/rez3/bin/csv_files"
                                "/mriqc_subs_to_rerun.csv",
                        help="Path to CSV file for cases to rerun.")    


    args = parser.parse_args()

    # Define the steps and their descriptions
    steps = [
        {"function": run_collect_files, "args": (
        args.rawdata_dir, args.mriqc_output_dirs, args.nifti_output_txt,
        args.json_output_txt), "description": "Collecting files"},
        {"function": run_convert_txt_to_csv,
         "args": (args.nifti_output_txt, args.nifti_csv),
         "description": "Converting NIfTI txt to CSV"},
        {"function": run_convert_txt_to_csv,
         "args": (args.json_output_txt, args.json_csv),
         "description": "Converting JSON txt to CSV"},
        {"function": run_find_missing_entries,
         "args": (args.nifti_csv, args.json_csv, args.output_diff_file),
         "description": "Finding missing entries"},
        {"function": run_make_sub_ses_caselist,
         "args": (args.output_diff_file, args.unique_pairs_csv),
         "description": "Generating unique subject-session pairs"},
        {"function": merge_unique_csv_files,
         "args": (args.unique_pairs_csv, args.rerun_csv, args.rerun_csv),
         "description": "Merging with existing rerun CSV"}
    ]

    # Run each step with a progress bar
    for step in tqdm(steps, desc="Overall Progress", unit="step"):
        tqdm.write(step["description"])
        step["function"](*step["args"])

    print(
        f"Process completed. Final caselist is at {args.unique_pairs_csv}")
