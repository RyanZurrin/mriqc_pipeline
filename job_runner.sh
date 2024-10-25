#!/bin/bash

# Set PATH environment variable
PATH=/PHShome/rez3/anaconda3/bin:/PHShome/rez3/anaconda3/condabin:/apps/released/gcc-toolchain/gcc-4.x/singularity/singularity-3.7.0/bin:/apps/released/built-by-outside-authors-x86linuxtarget/go/go-1.14.5/go/bin:/data/pnl/soft/pnlpipe3/fs7.1.0/bin:/data/pnl/soft/pnlpipe3/fs7.1.0/fsfast/bin:/data/pnl/soft/pnlpipe3/fs7.1.0/tktools:/data/pnl/soft/pnlpipe3/fsl/bin:/data/pnl/soft/pnlpipe3/fs7.1.0/mni/bin:/usr/share/lsf/9.1/linux2.6-glibc2.3-x86_64/etc:/usr/share/lsf/9.1/linux2.6-glibc2.3-x86_64/bin:/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/sbin:/opt/puppetlabs/bin:/PHShome/rez3/.local/bin:/PHShome/rez3/bin:/data/pnl/soft/pnlpipe3/pnlNipype/scripts:/data/pnl/soft/pnlpipe3/pnlNipype/exec:/data/pnl/soft/pnlpipe3/conversion/conversion:/data/pnl/soft/pnlpipe3/pnlpipe/soft_dir/BRAINSTools-build/DCMTK-build/bin:/data/pnl/soft/pnlpipe3/dcm2niix/build/bin:/data/pnl/soft/pnlpipe3/mrtrix3-centos7/bin:/data/pnl/soft/pnlpipe3/cmake-3.14.2-Linux-x86_64/bin:/data/pnl/soft/pnlpipe3/git-lfs/bin:/data/pnl/soft/pnlpipe3/CNN-Diffusion-MRIBrain-Segmentation/pipeline:/data/pnl/soft/pnlpipe3/bin:/data/pnl/soft/pnlpipe3/afnibin:/PHShome/rez3/bin
export PATH

# Number of subjects to process per batch
N_count=20

# Path to store the execution count
count_file="/data/predict1/home/rez3/bin/code/mriqc_pipeline/count.txt"

# Read the current count or default to 0 if the file doesn't exist
count=$(cat "$count_file" 2>/dev/null || echo 0)

# Increment the count
((count++))

# Save the updated count back to the file
echo "$count" > "$count_file"

# Path to log file
log_file="/data/predict1/home/rez3/bin/code/mriqc_pipeline/mriqc_job_submission.log"

# Get running job IDs
running_jobs=$(bjobs -noheader -o "jobid")

# Initialize an array to store running subjects
running_subjects=()

# Get subjects of running jobs from the log file
while IFS= read -r job_id; do
  subject=$(grep "job ID $job_id" "$log_file" | awk '{print $5}')
  if [ -n "$subject" ]; then
    running_subjects+=("$subject")
  fi
done <<< "$running_jobs"

# Debug log: print running subjects
echo "Running subjects: ${running_subjects[@]}"

# Clean tmp directory while preserving running subjects
tmp_dir="/data/predict1/home/rez3/tmp/mriqc"
for dir in "$tmp_dir"/*; do
  if [ -d "$dir" ]; then
    keep=false
    for subject in "${running_subjects[@]}"; do
      if [[ "$dir" == *"$subject"* ]]; then
        keep=true
        break
      fi
    done
    if [ "$keep" = false ]; then
      echo "Removing directory: $dir"
      rm -rf "$dir"
    else
      echo "Preserving directory: $dir"
    fi
  fi
done

# Your original command here
/usr/bin/python3.6 /data/predict1/home/rez3/bin/code/mriqc_pipeline/automated_mriqc_runner.py -c /data/predict1/home/rez3/bin/csv_files/mriqc_run_cases.csv -N ${N_count}
