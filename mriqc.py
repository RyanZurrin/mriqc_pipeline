#!/usr/bin/env python3
import argparse
from pathlib import Path
from subprocess import Popen, PIPE
import re
import json
import os


def remove_DataSetTrailingPadding_from_json_files(
        rawdata_dir: Path,
        subject_id: str,
        session_id: str) -> None:
    """Remove DataSetTrailingPadding from the existing json files"""
    # ensure rawdata_dir is a Path object
    rawdata_dir = Path(rawdata_dir)
    session_path = rawdata_dir / Path(subject_id) / Path(session_id)
    json_files = list(Path(session_path).glob('*/*json'))
    for json_file in json_files:
        with open(json_file, 'r') as fp:
            data = json.load(fp)
        if 'global' in data.keys():
            # anat
            if 'DataSetTrailingPadding' in data['global']['slices'].keys():
                data['global']['slices']['DataSetTrailingPadding'] = 'removed'
                os.chmod(json_file, 0o744)
                with open(json_file, 'w') as fp:
                    json.dump(data, fp, indent=1)
                os.chmod(json_file, 0o444)

        if 'time' in data.keys():
            # fmri
            if 'DataSetTrailingPadding' in data['time']['samples'].keys():
                data['time']['samples']['DataSetTrailingPadding'] = 'removed'
                os.chmod(json_file, 0o744)
                with open(json_file, 'w') as fp:
                    json.dump(data, fp, indent=1)
                os.chmod(json_file, 0o444)


def run_mriqc_on_data(rawdata_dir, subject_id, session_id, mriqc_outdir_root,
                      temp_dir, bsub, specific_nodes):
    img_loc =  "/data/predict1/home/rez3/singularity_containers/mriqc-22.0.6.simg"
    singularity = '/apps/released/gcc-toolchain/gcc-4.x/singularity/' \
                  'singularity-3.7.0/bin/singularity'

    work_dir = Path(temp_dir) / 'mriqc' / subject_id / session_id
    print(f"Working directory: {work_dir}")
    try:
        work_dir.mkdir(exist_ok=True, parents=True)
    except PermissionError:
        print(f"Permission denied to create {work_dir}")
        exit(1)
    try:
       Path(mriqc_outdir_root).mkdir(exist_ok=True, parents=True)
    except PermissionError:
        print(f"Permission denied to create {mriqc_outdir_root}")
        exit(1)

    remove_DataSetTrailingPadding_from_json_files(rawdata_dir, subject_id,
                                                  session_id)

    # check if hostname is dna007, or contains eris in the name and if so use singularity variable
    if 'dna007' in os.uname()[1] or 'eris' in os.uname()[1]:
        print("Running on DNA007 or ERIS")
        command = f'{singularity} run -e \
            -B {rawdata_dir}:/data:ro \
            -B {work_dir}:/work \
            -B {mriqc_outdir_root}:/out \
            -B /data/pnl/soft/pnlpipe3/freesurfer/license.txt:/opt/freesurfer/license.txt \
            {img_loc} \
            /data /out participant \
            -w /work --participant-label {subject_id} \
            --session-id {session_id.split("-")[1]} \
            --nprocs 8 --mem 24G --omp-nthreads 8 \
            --no-sub \
            --verbose-reports'
    else:
        print("Running on other nodes")
        command = f'singularity run -e \
            -B {rawdata_dir}:/data:ro \
            -B {work_dir}:/work \
            -B {mriqc_outdir_root}:/out \
            -B /data/pnl/soft/pnlpipe3/freesurfer/license.txt:/opt/freesurfer/license.txt \
            {img_loc} \
            /data /out participant \
            -w /work --participant-label {subject_id} \
            --session-id {session_id.split("-")[1]} \
            --nprocs 8 --mem 24G --omp-nthreads 8 \
            --no-sub \
            --verbose-reports'

    if bsub:
        if not specific_nodes:
            command = f'bsub -q pri_pnl \
                    -o {mriqc_outdir_root}/mriqc.out \
                    -e {mriqc_outdir_root}/mriqc.err \
                    -n 3 -J mriqc_{subject_id}_{session_id} \
                    {command}'
        else:
            nodes = ' '.join(specific_nodes)
            command = f'bsub -q pri_pnl \
                    -o {mriqc_outdir_root}/mriqc.out \
                    -e {mriqc_outdir_root}/mriqc.err \
                    -m "{nodes}" \
                    -n 3 -J mriqc_{subject_id}_{session_id} \
                    {command}'

    command = re.sub('\s+', ' ', command)
    print(command)

    p = Popen(command, shell=True, stdout=PIPE, bufsize=1)
    for line in iter(p.stdout.readline, b''):
        print(line.decode(), end='')
    p.stdout.close()
    p.wait()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run MRIQC on given data.")
    parser.add_argument("rawdata_dir", type=str,
                        help="Root of the BIDS nifti structure")
    parser.add_argument("subject_id", type=str,
                        help="Subject ID, including 'sub-'")
    parser.add_argument("session_id", type=str,
                        help="Session ID, including 'ses-'")
    parser.add_argument("mriqc_outdir_root", type=str,
                        help="Root of the MRIQC out dir.")
    parser.add_argument("--temp_dir", type=str,
                        default='/data/predict1/home/rez3/tmp',
                        help="Location of MRIQC working directory")
    parser.add_argument("--bsub", action='store_true',
                        help="Use bsub to submit jobs")
    parser.add_argument("--specific_nodes", nargs='*', default=[],
                        help="List of specific nodes for job submission")

    args = parser.parse_args()

    run_mriqc_on_data(args.rawdata_dir,
                      args.subject_id,
                      args.session_id,
                      args.mriqc_outdir_root,
                      args.temp_dir,
                      args.bsub,
                      args.specific_nodes)
