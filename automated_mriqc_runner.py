import csv
import subprocess
import argparse
from pathlib import Path
import logging
from datetime import datetime
import uuid

# Set up logging
logging.basicConfig(filename='mriqc_job_submission.log', filemode='a',
                    format='%(asctime)s - %(message)s', level=logging.INFO)


def create_csv(subject_id, session_id, csv_dir):
    """Create a CSV file for the given subject and session with a precise timestamp."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S%f")
    unique_id = str(uuid.uuid4())[:8]  # Short unique identifier
    filename = f"{csv_dir}/temp_{timestamp}_{unique_id}.csv"
    with open(filename, mode='w', newline='') as file:
        fieldnames = ['Subject', 'Session']
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow({'Subject': subject_id, 'Session': session_id})
    return filename


def submit_job(job_csv_path, rawdata_dir, mriqc_outdir_root, logs_root_dir,
               subject_id, session_id):
    subject_logs_dir = Path(logs_root_dir, subject_id, session_id)
    subject_logs_dir.mkdir(parents=True, exist_ok=True)
    log_out_path = subject_logs_dir / f'%J.out'
    log_err_path = subject_logs_dir / f'%J.err'
    command = f"""
    /usr/share/lsf/9.1/linux2.6-glibc2.3-x86_64/bin/bsub -o {log_out_path} \
         -e {log_err_path} \
         -M 18000 -n 2 -R "span[hosts=1]" -R "rusage[mem=18000]" -q pri_pnl -W 72:00 \
         "python /data/predict1/home/rez3/bin/code/mriqc_pipeline/run_mriqc.py {job_csv_path} \
         {rawdata_dir} {mriqc_outdir_root}"
    """
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    stdout, _ = process.communicate()
    job_id = parse_job_id(stdout.decode())
    return job_id


def parse_job_id(bsub_output):
    import re
    match = re.search(r'Job <(\d+)> is submitted', bsub_output)
    if match:
        return match.group(1)
    return None


def manage_csv_files(input_csv, rerun_csv, csv_dir, processed_entries):
    # Read the current CSV entries and filter out the processed ones
    with open(input_csv, 'r', newline='') as file:
        reader = csv.DictReader(file)
        entries = [row for row in reader if
                   (row['Subject'], row['Session']) not in processed_entries]

    # Write the updated entries back to the original CSV
    with open(input_csv, 'w', newline='') as file:
        fieldnames = ['Subject', 'Session']
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(entries)

    # Append the processed entries to the rerun CSV
    with open(rerun_csv, 'a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        for entry in processed_entries:
            writer.writerow({'Subject': entry[0], 'Session': entry[1]})


def main(csv_file, csv_dir, rawdata_dir, mriqc_outdir_root, logs_root_dir,
         rerun_csv, num_subjects):
    Path(logs_root_dir).mkdir(parents=True, exist_ok=True)
    Path(csv_dir).mkdir(parents=True, exist_ok=True)
    processed_entries = []
    with open(csv_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        count = 0
        for row in reader:
            if count < num_subjects:
                job_csv = create_csv(row['Subject'], row['Session'], csv_dir)
                job_id = submit_job(job_csv, rawdata_dir, mriqc_outdir_root,
                                    logs_root_dir, row['Subject'],
                                    row['Session'])
                logging.info(
                    f"Submitted {row['Subject']} {row['Session']} with job ID {job_id}")
                processed_entries.append((row['Subject'], row['Session']))
                count += 1
            else:
                break
    manage_csv_files(csv_file, rerun_csv, csv_dir, processed_entries)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Submit MRIQC jobs for each subject-session pair.")
    parser.add_argument("-c", "--csv_file", required=True,
                        help="Path to the CSV file containing subject-session "
                             "pairs.")
    parser.add_argument("-d", "--csv_dir",
                        default='/data/predict1/home/rez3/bin/csv_files'
                                '/generated_csvs',
                        help="Path to save created CSV files.")
    parser.add_argument("-r", "--rawdata_dir",
                        default='/data/predict1/data_from_nda/MRI_ROOT/rawdata/',
                        help="Path to the BIDS dataset root directory.")
    parser.add_argument("-o", "--mriqc_outdir_root",
                        default='/data/predict1/data_from_nda/MRI_ROOT'
                                '/rez3_derivatives/mriqc',
                        help="Path to save MRIQC outputs.")
    parser.add_argument("-l", "--logs_root_dir",
                        default='/data/predict1/home/rez3/bin/logs',
                        help="Root directory for logs.")
    parser.add_argument("-N", "--num_subjects", type=int, default=float('inf'),
                        help="Maximum number of subjects to process.")
    parser.add_argument("-R", "--rerun_csv",
                        default='/data/predict1/home/rez3/bin/csv_files'
                                '/reran_cases.csv',
                        help="Path to the CSV file for tracking rerun cases.")
    args = parser.parse_args()

    main(args.csv_file, args.csv_dir, args.rawdata_dir, args.mriqc_outdir_root,
         args.logs_root_dir, args.rerun_csv, args.num_subjects)
