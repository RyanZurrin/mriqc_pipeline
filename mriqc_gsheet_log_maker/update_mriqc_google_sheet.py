#!/usr/bin/env python3

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from tqdm import tqdm
import argparse
from get_status import get_status
import os
import datetime


def authorize_google_sheets(_json_keyfile):
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    try:
        gc = gspread.authorize(
            ServiceAccountCredentials.from_json_keyfile_name(_json_keyfile,
                                                             scope))
        return gc
    except Exception as e:
        print(f"Error: {e}")
        return None


def main(gspread_id,
         google_sheet_name,
         gspread_key,
         rerun_csv_path,
         temp_csvs_path,
         mriqc_log_path,
         rawdata_path,
         mriqc_output_path):
    # Returns a datetime object from the name of a file in format
    # 'temp_YYYYMMDD_...'
    def parse_filename(file_name):
        date_str = file_name.split('_')[1]
        return datetime.datetime.strptime(date_str, '%Y%m%d')

    # Returns subject-session string from input csv (to be used as key)
    def get_subject_session_key(subject_session_csv_path):
        df = pd.read_csv(subject_session_csv_path)
        subject = df['Subject'].iloc[0]
        session = df['Session'].iloc[0]
        return f"{subject},{session}"

    # Load CSV file paths
    csv_files = [file for file in os.listdir(temp_csvs_path) if
                 file.endswith('.csv')]
    date_dict = {}

    # Create dictionary, where the key is the subject-session string and the
    # value is the date
    for file in csv_files:
        file_path = os.path.join(temp_csvs_path, file)
        key = get_subject_session_key(file_path)
        value = parse_filename(file)
        date_dict[key] = value

    # Start progress bar
    tqdm.pandas()

    # Authorize Google Sheets
    gc = authorize_google_sheets(gspread_key)
    sheet = gc.open_by_key(gspread_id).worksheet(google_sheet_name)

    # Create Status and Error columns
    data = pd.read_csv(rerun_csv_path)
    data[['Status', 'Error']] = data.progress_apply(
        lambda row: get_status(row['Subject'], row['Session'],
                               mriqc_log_path,
                               rawdata_path,
                               mriqc_output_path),
        axis=1, result_type='expand'
    )

    # Insert date column before everything else
    data.insert(0, 'Date', data.apply(
        lambda row: date_dict.get(f"{row['Subject']},"
                                  f"{row['Session']}",
                                  pd.NaT), axis=1)
                )
    data.sort_values('Date', ascending=False, inplace=True)

    # Make the datetime into a string
    data['Date'] = "'" + data['Date'].dt.strftime('%Y/%m/%d')
    data.fillna("", inplace=True)

    # Upload to Google Sheet
    formatted = [data.columns.values.tolist()] + data.values.tolist()
    sheet.clear()
    sheet.update(formatted, 'A1', value_input_option='USER_ENTERED')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Looks through CSV files of subject/sessions to rerun and "
                    "adds their status to a Google Sheet")

    parser.add_argument('-i', '--spreadsheet_id',
                        help='ID of the Google Sheet to append to',
                        default='1H0EOfP3TB36kf3jCGkIk0sQ4psRLFilwyWP1Nzx-X5s')

    parser.add_argument('-n', '--google-sheet_name',
                        help='Name of the Google Sheet to append to as it '
                             'appears in your drive',
                        default='Sheet2')

    parser.add_argument('-g', '--gspread_key_path',
                        help='Path to GSpread google sheet key',
                        default="/data/predict1/home/rez3/bin/code"
                                "/mriqc_pipeline/mriqc_gsheet_log_maker/keys"
                                "/mriqc-423218-d362b8e1343d.json")

    parser.add_argument('-r', '--rerun_csv_path',
                        help='Path to the CSV file containing subject/session '
                             'reruns',
                        default='/data/predict1/home/rez3/bin/csv_files'
                                '/mriqc_subs_to_rerun.csv')

    parser.add_argument('-t', '--temp_csvs_path',
                        help='Path to the directory containing temp CSVs',
                        default='/data/predict1/home/rez3/bin/csv_files'
                                '/generated_csvs/')

    parser.add_argument('-l', '--mriqc_log_path',
                        help='Path to the directory containing mriqc logs',
                        default='/data/predict1/home/rez3/bin/logs/')

    parser.add_argument('-d', '--rawdata_path',
                        help='Path to the directory containing rawdata',
                        default='/data/predict1/data_from_nda/MRI_ROOT/rawdata/')

    parser.add_argument('-o', '--mriqc_output_path',
                        help='Path to the directory containing mriqc output',
                        default='/data/predict1/data_from_nda/MRI_ROOT'
                                '/rez3_derivatives/mriqc/')

    args = parser.parse_args()

    main(args.spreadsheet_id,
         args.google_sheet_name,
         args.gspread_key_path,
         args.rerun_csv_path,
         args.temp_csvs_path,
         args.mriqc_log_path,
         args.rawdata_path,
         args.mriqc_output_path)
