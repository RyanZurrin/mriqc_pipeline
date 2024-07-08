import os
import json


# Return status string and error message, if needed
def get_status(subject, session, mriqc_log_path, rawdata_path, mriqc_output_path):
    # Check if the two folders exist
    ses_dir = mriqc_log_path + subject + '/' + session

    # If no session folder, not ran
    if not os.path.isdir(ses_dir):
        return "Not Ran", None

    # If no output files but sub/ses folder exist, running
    if not has_err_and_out_files(ses_dir):
        return "Running", None

    # If output files do exist, perform more rigorous check
    else:
        out, err = is_completed_without_errors(subject, session, mriqc_log_path, rawdata_path, mriqc_output_path)
        if out:
            return "Completed", err
        return "Errors", err


# Returns True if it is completed without errors, otherwise returns False and a descriptive error message
def is_completed_without_errors(subject, session, mriqc_log_path, rawdata_path, mriqc_output_path):
    sub_ses_dir = subject + '/' + session

    # First, check if the sub/ses directory exists in mriqc output
    mriqc_sub_ses_dir = mriqc_output_path + sub_ses_dir
    if not os.path.isdir(mriqc_sub_ses_dir):
        return False, "Missing subject/session directory in mriqc output (Should be here: " + mriqc_sub_ses_dir + ")"

    # Next, check to make sure /anat and /func are inside
    if not os.path.isdir(mriqc_sub_ses_dir + '/anat'):
        return False, 'Missing /anat directory (Should be here: ' + mriqc_sub_ses_dir + '/anat'
    if not os.path.isdir(mriqc_sub_ses_dir + '/func'):
        return False, 'Missing /func directory (Should be here: ' + mriqc_sub_ses_dir + '/func'

    # List the JSON files inside the directory
    anat_json_list = os.listdir(mriqc_sub_ses_dir + '/anat')
    func_json_list = os.listdir(mriqc_sub_ses_dir + '/func')

    # If there are 2 files in anat and 4 in func, check contents of /anat json files for the keys
    if len(anat_json_list) == 2 and len(func_json_list) == 4:
        result = all(does_json_have_keys(mriqc_sub_ses_dir + '/anat/' + file) for file in anat_json_list)
        if result:
            return True, None
        return False, "Missing keys in an /anat json file(s)"

    # Get files in rawdata directory
    rawdata_sub_ses_dir = rawdata_path + sub_ses_dir

    # Make sure the directories exist before getting their contents
    if not os.path.isdir(rawdata_sub_ses_dir + '/anat') or not os.path.isdir(rawdata_sub_ses_dir + '/func'):
        return False, "rawdata is missing /anat or /func"
    anat_rawdata_list = os.listdir(rawdata_sub_ses_dir + '/anat')
    func_rawdata_list = os.listdir(rawdata_sub_ses_dir + '/func')

    # If at least 1 file in /anat and 2 in /func
    if len(anat_json_list) > 0 and len(func_json_list) > 1:
        if (not do_files_correspond(anat_json_list, anat_rawdata_list)
                or not do_files_correspond(func_json_list, func_rawdata_list)):
            return False, "Mismatched files in /mriqc output and /rawdata"
        else:
            return (True, "Wrong number of json files: " + str(len(anat_json_list))
                    + " in /anat " + str(len(func_json_list)) + " in /func")

    # No json files
    return False, "No json files"


# Compares a list of json files against a list of files in rawdata folder
# If each json file has a corresponding rawdata file, return true
def do_files_correspond(json_list, rawdata_list):
    rawdata_set = set(rawdata_list)
    for json_file in json_list:
        corresponding_file = os.path.splitext(json_file)[0] + '.nii.gz'
        if corresponding_file not in rawdata_set:
            return False
    return True


# Returns True if the given directory path contains a .out and .err file
def has_err_and_out_files(ses_dir):
    has_out_file = False
    has_err_file = False

    # Loop through each file in the directory
    for filename in os.listdir(ses_dir):
        if filename.endswith(".out"):
            has_out_file = True
        elif filename.endswith(".err"):
            has_err_file = True

    return has_err_file and has_out_file


# Returns True if json file has the specified keys
def does_json_have_keys(filepath):
    required_keys = [
        "cjv", "cnr", "efc", "fber", "fwhm_avg", "fwhm_x", "fwhm_y", "fwhm_z",
        "icvs_csf", "icvs_gm", "icvs_wm", "inu_med", "inu_range"
    ]
    with open(filepath, 'r') as file:
        data = json.load(file)
    missing_keys = [key for key in required_keys if key not in data]
    return not missing_keys

