# Created by Kash Todi.
# www.kashyaptodi.com
# Script to download videos and captions from PCS
# Usage: python3 download_from_pcs <PATH_TO_PCS.CSV>
# Requires FFMPEG (http://www.ffmpeg.org).
# Requires wget module: > pip install wget

import sys
import os
import csv
import subprocess
import wget
import shutil
from multiprocessing import Pool

id_field = "Paper ID"
video_field = "Pre-recorded Video Presentation (Required)"
caption_field = "Pre-recorded Video Presentation Captions (Required)"
use_doi_names = None
destination_dir = None
skip_existing = False
downloaded_videos = []
downloaded_captions = []


def convert_to_format(filepath, format):
    # Uses ffmpeg to convert files to target format, if possible
    extention = os.path.splitext(filepath)[-1].lower()
    if extention == format.lower():
        return None
    path_no_extension = os.path.splitext(filepath)[0]
    try:
        subprocess.check_output(
            ["ffmpeg", '-i', filepath, '-hide_banner', '-loglevel', 'error', '-y', path_no_extension+format])
    except:
        print(f"Convertion to {format.upper()} failed for "+filepath)
        return None
    else:
        return path_no_extension + format


def get_valid_bool_input(prompt):
    # Helper function to get y/n input from user
    result = None
    while result not in [True, False]:
        result = input(f"{prompt} (y/n): ").lower()
        if result in ["y", "yes"]:
            return True
        if result in ["n", "no"]:
            return False
        print("Invalid input!")


def rename_and_move(source_filename, target_id, destination_directory):
    # Helper function to rename a file and move it to destination directory
    file_extension = os.path.splitext(source_filename)[-1]
    target_filename = f"{target_id}{file_extension}"
    os.rename(source_filename, target_filename)
    shutil.move(os.path.join(os.getcwd(), target_filename), os.path.join(
        os.path.join(os.getcwd(), destination_directory), target_filename))


def process_row(data):
    # Process one row of data - download video and captions, convert to desired format, and move to folders
    row, skip_existing, use_doi_names, downloaded_videos, downloaded_captions, convert_to_vtt = data
    id = row[id_field]
    output_name = id
    print("TEST")
    if use_doi_names and row["DOI"] != "":
        # Use DOI for renaming
        output_name = row["DOI"].rsplit('/', 1)[-1]
    if skip_existing and any(i in downloaded_videos for i in [id, output_name]):
        # Video and caption files exist. Skip this one.
        print(f"Skipping {id}")
    else:
        print(f"Downloading {id}")
        video_url = row[video_field]
        try:
            filename = wget.download(video_url)
        except:
            filename = None
        if filename:
            converted_filename = convert_to_format(filename, ".mp4")
            if converted_filename and converted_filename != filename:
                os.remove(filename)
                filename = converted_filename
            rename_and_move(filename, output_name, "Videos")

    if caption_field in row.keys() and skip_existing and any(i in downloaded_captions for i in [id, output_name]):
        pass
    else:
        caption_url = row[caption_field]
        try:
            filename = wget.download(caption_url)
        except:
            filename = None
        if filename:
            if convert_to_vtt:
                converted_filename = convert_to_format(
                    filename, ".vtt")  # convert to VTT
            else:
                converted_filename = convert_to_format(
                    filename, ".srt")  # covnert to default SRT format
            if converted_filename and converted_filename != filename:
                os.remove(filename)
                filename = converted_filename
            rename_and_move(filename, output_name, "Subtitles")


if __name__ == "__main__":
    # [REQUIRED] Provide path to PCS CSV as argument
    with open(sys.argv[1], encoding='utf-8-sig', newline='') as csvfile:
        pcs_data = list(csv.DictReader(csvfile))

    print(f"Downloading files from PCS (if available). Using column names:")
    print(
        f"ID: {id_field}\nVideo field: {video_field}\nCaption field:{caption_field}")

    os.chdir(os.path.dirname(sys.argv[1]))

    destination_dir = os.path.splitext(os.path.basename(sys.argv[1]))[0]

    if os.path.exists(destination_dir):
        skip_existing = None
        while skip_existing not in [True, False]:
            skip_existing = input(
                "Skip already downloaded files? (Y/N): ").lower()
            if skip_existing in ["y", "yes"]:
                skip_existing = True
                downloaded_videos = [os.path.splitext(
                    file)[0] for file in os.listdir(f"{destination_dir}/Videos")]
                print("Already downloaded files will be skipped.")
                print(f"Downloaded videos: {downloaded_videos}")
                downloaded_captions = [os.path.splitext(
                    file)[0] for file in os.listdir(f"{destination_dir}/Subtitles")]
            elif skip_existing in ["n", "no"]:
                skip_existing = False
                print("Existing files will be re-downloaded.")
            else:
                print("Invalid input!")

    # Check if ID field is present
    if id_field not in pcs_data[0].keys():
        print(pcs_data[0].keys())
        print(
            f"Column header {id_field} is not in PCS data.\nDouble-check column headers and ensure the CSV file is in UTF-8 encoding.")
        exit()

    # Check if video column is present

    if video_field not in pcs_data[0].keys():
        print(pcs_data[0].keys())
        print(
            f"Column header {video_field} is not in PCS data.\nDouble-check and ensure the CSV file is in UTF-8 encoding")
        exit()

    # Check if caption column is present
    if caption_field not in pcs_data[0].keys():
        print(
            f"Column header {caption_field} is not in PCS data. Double-check and ensure the CSV file is in UTF-8 encoding")
        confirm = input("Continue without captions? (y/n)").lower()
        if confirm != "y" or confirm == "yes":
            print("Cancelled. Quitting!")
            exit()

    if "DOI" not in pcs_data[0].keys():
        print("Column header \'DOI\' not in PCS data. Will use PCS ID for file names.")
        use_doi_names = False
    else:
        use_doi_names = get_valid_bool_input(
            "Default names are PCS ID. Should file names use DOI if available?")

    convert_to_vtt = get_valid_bool_input(
        "Should subtitles be converted to VTT (required for ACM DL)?")

    confirm = get_valid_bool_input("Start Downloading?")
    if not confirm:
        print("Cancelled download. Quitting!")
        exit()

    # Make directories to store files
    os.makedirs(destination_dir, exist_ok=True)
    os.chdir(destination_dir)
    os.makedirs("Videos", exist_ok=True)
    os.makedirs("Subtitles", exist_ok=True)

    if pcs_data:
        with Pool(5) as p:
            p.map(process_row, [(row, skip_existing, use_doi_names,
                  downloaded_videos, downloaded_captions, convert_to_vtt) for row in pcs_data])

    print(f"Files saved in directory: {destination_dir}")
