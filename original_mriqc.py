#!/usr/bin/env python3
import argparse
from subprocess import run, PIPE
from pathlib import Path


def run_mriqc(rawdata_dir,
              work_dir,
              mriqc_outdir_root,
              analysis_level='participant',
              participant_label=None,
              session_id=None,
              nprocs=1,
              mem_gb=16,
              no_sub=True,
              verbose=True):
    """
    Run MRIQC on a BIDS dataset using Singularity.
    """
    img_loc = "/data/predict1/home/rez3/singularity_containers/mriqc-22.0.6.simg"
    fs_license = "/data/pnl/soft/pnlpipe3/freesurfer/license.txt"

    # Format the command with the correct bindings and options
    singularity_cmd = [
        "singularity", "run",
        "-B", f"{rawdata_dir}:/data:ro",
        "-B", f"{work_dir}:/work",
        "-B", f"{mriqc_outdir_root}:/out",
        "-B", f"{fs_license}:/opt/freesurfer/license.txt",
        img_loc,
        "/data", "/out", analysis_level,
        "-w", "/work",
    ]

    # Adding optional parameters
    if participant_label:
        singularity_cmd += ["--participant-label"] + participant_label
    if session_id:
        singularity_cmd += ["--session-id"] + session_id
    singularity_cmd += ["--nprocs", str(nprocs), "--mem", str(mem_gb)]
    if no_sub:
        singularity_cmd.append("--no-sub")
    if verbose:
        singularity_cmd.append("--verbose-reports")

    # Convert command list to a string for printing
    command_str = ' '.join(str(x) for x in singularity_cmd)
    print("Running MRIQC with the following command:")
    print(command_str)

    # Execute the command
    result = run(singularity_cmd, stdout=PIPE, stderr=PIPE, text=True)

    if result.returncode == 0:
        print("MRIQC finished successfully.")
    else:
        print("MRIQC encountered an error.")
        print(result.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run MRIQC on a BIDS dataset.")
    parser.add_argument("rawdata_dir", type=str,
                        help="Path to the BIDS dataset root directory.")
    parser.add_argument("work_dir", type=str,
                        help="Path for MRIQC working directory.")
    parser.add_argument("mriqc_outdir_root", type=str,
                        help="Path to save MRIQC outputs.")
    parser.add_argument("--participant_label", nargs="+",
                        help="Participant label(s) to analyze.")
    parser.add_argument("--session_id", nargs="+",
                        help="Session ID(s) to analyze.")
    parser.add_argument("--nprocs", type=int, default=1,
                        help="Number of CPUs available to MRIQC.")
    parser.add_argument("--mem_gb", type=int, default=16,
                        help="Upper bound memory limit for MRIQC processes in GB.")
    parser.add_argument("--no_sub", action="store_true", default=True,
                        help="Do not submit metrics to MRIQC's online repository.")
    parser.add_argument("--verbose", action="store_true", default=True,
                        help="Enable verbose output.")

    args = parser.parse_args()

    # Ensure output and working directories exist
    Path(args.mriqc_outdir_root).mkdir(parents=True, exist_ok=True)
    Path(args.work_dir).mkdir(parents=True, exist_ok=True)

    run_mriqc(
        rawdata_dir=args.rawdata_dir,
        work_dir=args.work_dir,
        mriqc_outdir_root=args.mriqc_outdir_root,
        participant_label=args.participant_label,
        session_id=args.session_id,
        nprocs=args.nprocs,
        mem_gb=args.mem_gb,
        no_sub=args.no_sub,
        verbose=args.verbose
    )
