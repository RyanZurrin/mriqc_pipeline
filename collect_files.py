# collect_files.py
import os
import argparse
from tqdm import tqdm


def collect_nifti_files(rawdata_dir, output_txt):
    """
    Collect all NIfTI files that match the specification (anat and func, excluding auxiliary and sbref)
    and write their paths to a text file.

    Parameters
    ----------
    rawdata_dir : str
        Path to the rawdata directory.
    output_txt : str
        Path to the output text file.
    """
    files_to_collect = []
    for root, _, files in os.walk(rawdata_dir):
        if os.path.basename(root) in ['anat', 'func']:
            for file in files:
                if file.endswith('.nii.gz') and not any(
                        x in file for x in ['auxiliary', 'sbref']):
                    files_to_collect.append(os.path.join(root, file))

    with open(output_txt, 'w') as txt_file:
        for file_path in tqdm(files_to_collect, desc="Collecting NIfTI files"):
            txt_file.write(file_path + '\n')


def collect_json_files(mriqc_output_dirs, output_txt):
    """
    Collect all JSON files from multiple MRIQC output directories and write their paths to a text file.

    Parameters
    ----------
    mriqc_output_dirs : list of str
        List of paths to the MRIQC output directories.
    output_txt : str
        Path to the output text file.
    """
    files_to_collect = []
    for mriqc_output_dir in mriqc_output_dirs:
        for root, _, files in os.walk(mriqc_output_dir):
            for file in files:
                if file.endswith('.json'):
                    files_to_collect.append(os.path.join(root, file))

    with open(output_txt, 'w') as txt_file:
        for file_path in tqdm(files_to_collect, desc="Collecting JSON files"):
            txt_file.write(file_path + '\n')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Collect paths of NIfTI and JSON files from raw data and MRIQC output directories respectively.")
    parser.add_argument("rawdata_dir", help="Path to the rawdata directory.")
    parser.add_argument("mriqc_output_dirs", nargs='+',
                        help="Paths to the MRIQC output directories.")
    parser.add_argument("-n", "--nifti_output_txt", default="nifti_files.txt",
                        help="Path to output text file for NIfTI files.")
    parser.add_argument("-j", "--json_output_txt", default="json_files.txt",
                        help="Path to output text file for JSON files.")
    args = parser.parse_args()

    collect_nifti_files(args.rawdata_dir, args.nifti_output_txt)
    collect_json_files(args.mriqc_output_dirs, args.json_output_txt)
