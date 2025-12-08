import zstandard
import os
import json
import sys
import csv
from datetime import datetime
import logging.handlers
import traceback
import re


# put the path to the input file, or a folder of files to process all of
input_file = r"\\PATH\january_output.zst"
# put the name or path to the output file. The file extension from below will be added automatically. If the input file is a folder, the output will be treated as a folder as well
output_file = r"\\PATH\january_categorized"

output_format = "csv"


single_field = None
write_bad_lines = True

from_date = datetime.strptime("2024-12-01", "%Y-%m-%d")
to_date = datetime.strptime("2025-03-31", "%Y-%m-%d")


# dict of categories and keywords to create new additional columns, could be modified

KEYWORD_CATEGORIES = {
    "government_mentioned": [
        "Mayor", "Bass", "Caruso", "officials", "council", "administration", "governor", "Newsom", "bureaucrats",
        "politicians", "government", "federal", "state", "local", "authorities", "leadership", "elected",
        "representatives", "senators", "congressmen", "lawmakers", "legislature", "agency", "agencies",
        "department", "departments", "supervisor", "supervisors", "commissioner", "commissioners", "regime",
        "establishment", "insiders", "swamp", "elites", "ruling", "power", "powers", "policymakers",
        "regulators", "executive", "municipal", "city", "county", "Democrat", "Democrats", "Democratic",
        "GOP", "Republican", "Republicans", "political", "politics", "partisan", "left", "right", "liberal",
        "progressive", "conservative"
    ],
    "fire_mentioned": [
        "fire", "fires", "wildfire", "wildfires", "blaze", "flames", "burn", "burned", "burning", "arson",
        "combustion", "ash", "flare"
    ],
    "lafd_related": [
        "Hydrants", "Crowley", "firefighters", "LAFD", "reservoir", "watersupply"
    ],
    "urban_planning_related": [
        "2028", "Smart LA", "Smart City", "ITA", "rebuild", "Olympics", "Agenda2030", "LA2.0",
        "landgrab", "rezoning", "gentrification"
    ],
    "fabricated_fires_related": [
        "DEW", "Lasers", "geo-engineered", "weaponization"
    ],
    "celebrity_related": [
        "Diddy", "Sean", "tunnels", "celebrities", "Playboy", "Combs", "Getty", "Hollywood",
        "elite", "trafficking"
    ],
    "ai_related": [
        "Artificial intelligence", "AI", "generated", "ChatGPT"
    ],
    "disinformation_related": [
        "fake", "disinformation", "misinformation"
    ],
    "antisemitic_related": [
        "Jewish", "Resnicks", "DEI", "Jews", "Jew", "Antisemitic"
    ],
    "weather_related": [
        "cloudseeding", "Katja Friedrichz", "winds", "Santa", "climate", "drought", "conditions", "weather",
        "manipulation", "geoengineering"
    ],
    "conspiracy_mentioned": [
        "Conspiracy", "awakened", "propaganda", "manipulation"
    ],
    "humanitarian_aid_mentioned": [
        "Aid", "shelter", "evacuation", "homeless", "victims", "survivors", "volunteers", "emergency",
        "supplies", "helping", "support"
    ]
}

CATEGORIES_LOWER = {}
for category, keywords in KEYWORD_CATEGORIES.items():
    clean_name = re.sub(r'[^a-z0-9_]+', '', category.lower())
    CATEGORIES_LOWER[clean_name] = [k.lower() for k in keywords]

field = None
values_file = None

exact_match = False
inverse = False


log = logging.getLogger("bot")
log.setLevel(logging.INFO)
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
log_str_handler = logging.StreamHandler()
log_str_handler.setFormatter(log_formatter)
log.addHandler(log_str_handler)
if not os.path.exists("logs"):
	os.makedirs("logs")
log_file_handler = logging.handlers.RotatingFileHandler(os.path.join("logs", "bot.log"), maxBytes=1024*1024*16, backupCount=5)
log_file_handler.setFormatter(log_formatter)
log.addHandler(log_file_handler)


def write_line_zst(handle, line):
	handle.write(line.encode('utf-8'))
	handle.write("\n".encode('utf-8'))


def write_line_json(handle, obj):
	handle.write(json.dumps(obj))
	handle.write("\n")


def write_line_single(handle, obj, field):
	if field in obj:
		handle.write(obj[field])
	else:
		log.info(f"{field} not in object {obj['id']}")
	handle.write("\n")


def write_line_csv(writer, obj, is_submission, category_data):
    output_list = []

    output_list.append(str(obj.get('score', '')))
    output_list.append(str(obj.get('created_utc', '')))
    if is_submission:
       output_list.append(obj.get('title', ''))
    output_list.append(f"u/{obj.get('author', '[deleted]')}")
    if 'permalink' in obj:
       output_list.append(f"https://www.reddit.com{obj['permalink']}")
    else:
       output_list.append(f"https://www.reddit.com/r/{obj.get('subreddit', '')}/comments/{obj.get('link_id', '')[3:]}/_/{obj.get('id', '')}")
    if is_submission:
       if obj.get('is_self'):
          output_list.append(obj.get('selftext', ''))
       else:
          output_list.append(obj.get('url', ''))
    else:
       output_list.append(obj.get('body', ''))

    output_list.extend(category_data)

    try:
        writer.writerow(output_list)
    except Exception as e:
        log.warning(f"Raw record error CSV {obj.get('id', '')}: {e}")




def read_and_decode(reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
	chunk = reader.read(chunk_size)
	bytes_read += chunk_size
	if previous_chunk is not None:
		chunk = previous_chunk + chunk
	try:
		return chunk.decode()
	except UnicodeDecodeError:
		if bytes_read > max_window_size:
			raise UnicodeError(f"Unable to decode frame after reading {bytes_read:,} bytes")
		log.info(f"Decoding error with {bytes_read:,} bytes, reading another chunk")
		return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)


def read_lines_zst(file_name):
	with open(file_name, 'rb') as file_handle:
		buffer = ''
		reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)
		while True:
			chunk = read_and_decode(reader, 2**27, (2**29) * 2)

			if not chunk:
				break
			lines = (buffer + chunk).split("\n")

			for line in lines[:-1]:
				yield line.strip(), file_handle.tell()

			buffer = lines[-1]

		reader.close()


def process_file(input_file, csv_writer, from_date, to_date):

    log.info(f"Starting processing: {os.path.basename(input_file)}")
    file_size = os.stat(input_file).st_size
    created = None
    processed_lines = 0
    bad_lines = 0
    total_lines = 0

    base_headers = ['comment_id', 'created_utc', 'author', 'score', 'body', 'subreddit', 'link_id', 'permalink']
    category_headers = list(CATEGORIES_LOWER.keys())

    for line, file_bytes_processed in read_lines_zst(input_file):
        total_lines += 1
        if total_lines % 250000 == 0:
           created_str = created.strftime('%Y-%m-%d %H:%M:%S') if created else "Starting"
           log.info(f"File {os.path.basename(input_file)}: {created_str} : {total_lines:,} lines read : {processed_lines:,} processed : {bad_lines:,} bad : {(file_bytes_processed / file_size) * 100:.1f}%")

        try:
            obj = json.loads(line)
            created = datetime.utcfromtimestamp(int(obj['created_utc']))

            if not (from_date <= created <= to_date):
                continue

            processed_lines += 1

            body_text = obj.get('body', '')
            body_lower = body_text.lower() if body_text else ''

            base_row_dict = {
                'comment_id': obj.get('id', ''),
                'created_utc': str(obj.get('created_utc', '')),
                'author': obj.get('author', ''),
                'score': str(obj.get('score', '')),
                'body': body_text,
                'subreddit': obj.get('subreddit', ''),
                'link_id': obj.get('link_id', ''),
                'permalink': f"https://www.reddit.com{obj['permalink']}" if 'permalink' in obj else f"https://www.reddit.com/r/{obj.get('subreddit', '')}/comments/{obj.get('link_id', '')[3:]}/_/{obj.get('id', '')}"
            }

            category_row_dict = {}
            for category_name, keywords_list in CATEGORIES_LOWER.items():
                found = 0
                if body_lower and any(keyword in body_lower for keyword in keywords_list):
                   found = 1
                category_row_dict[category_name] = found


            output_list = [base_row_dict.get(h, '') for h in base_headers] + \
                          [category_row_dict.get(h, 0) for h in category_headers]
            csv_writer.writerow(output_list)

        except (KeyError, json.JSONDecodeError, TypeError) as err:
            bad_lines += 1
            if write_bad_lines and total_lines % 5000 == 0:
                log.warning(f"Skipping bad line {total_lines:,} in {os.path.basename(input_file)}. Error: {err}. Line: {line[:150]}")
        except Exception as e:
            bad_lines += 1
            if write_bad_lines and total_lines % 5000 == 0:
                log.warning(f"Skipping bad line {total_lines:,} due to other error: {e}. Line: {line[:150]}")

    log.info(f"Finished processing: {os.path.basename(input_file)}. Total lines: {total_lines:,}, Processed (in date range): {processed_lines:,}, Bad lines: {bad_lines:,}")

if __name__ == "__main__":
    if single_field is not None:
        log.warning("single_field is set but will be ignored. Output format forced to CSV.")
        output_format = "csv"

    if output_format != "csv":
       log.error(f"This script is configured to output CSV only. Please set output_format = 'csv'. Current: '{output_format}'")
       sys.exit()

    log.info(f"Script started. Mode: Process folder into single CSV.")
    log.info(f"Category columns to be added: {list(CATEGORIES_LOWER.keys())}")
    log.info(f"Date range filter: {from_date.strftime('%Y-%m-%d')} to {to_date.strftime('%Y-%m-%d')}")

    input_files_to_process = []
    if not os.path.exists(input_file):
        log.error(f"ERROR: Input path does not exist: {input_file}")
        sys.exit()

    if os.path.isdir(input_file):
        log.info(f"Input path is a directory. Searching for .zst files in: {input_file}")
        for filename in sorted(os.listdir(input_file)):
            if not os.path.isdir(os.path.join(input_file, filename)) and filename.endswith(".zst"):
                input_files_to_process.append(os.path.join(input_file, filename))
        if not input_files_to_process:
             log.error(f"ERROR: No .zst files found in directory: {input_file}")
             sys.exit()
    else:
        log.error(f"ERROR: input_file must be a directory (e.g., 'january_output'), not a single file.")
        sys.exit()

    log.info(f"Found {len(input_files_to_process)} input .zst files. Processing into ONE output file: {output_file}")

    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir)
            log.info(f"Created output directory: {output_dir}")
        except OSError as e:
            log.error(f"ERROR: Could not create output directory: {output_dir}. Error: {e}")
            sys.exit()

    base_headers = ['comment_id', 'created_utc', 'author', 'score', 'body', 'subreddit', 'link_id', 'permalink']
    category_headers = list(CATEGORIES_LOWER.keys()) 
    all_headers = base_headers + category_headers

    try:
        with open(output_file, 'w', encoding='UTF-8', newline='') as handle:
            writer = csv.writer(handle)

            writer.writerow(all_headers)
            log.info(f"CSV headers written to {os.path.basename(output_file)}")

            start_time = datetime.now()
            log.info(f"Starting processing loop at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            for i, file_in in enumerate(input_files_to_process, 1):
                log.info(f"--- Processing file {i}/{len(input_files_to_process)} ---")
                try:
                    process_file(file_in, writer, from_date, to_date)
                except Exception as err:
                    log.error(f"CRITICAL ERROR processing file {os.path.basename(file_in)}: {err}", exc_info=True)
                    log.error(f"Attempting to continue with the next file...")

            end_time = datetime.now()
            log.info(f"Finished processing loop at {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            log.info(f"Total processing time: {end_time - start_time}")

    except IOError as e:
        log.error(f"ERROR: Could not open or write to output file: {output_file}. Error: {e}")
        sys.exit()
    except Exception as e:
        log.error(f"An unexpected error occurred during the main process: {e}", exc_info=True)
        sys.exit()

    log.info(f"--- SCRIPT FINISHED. Output file should be ready: {output_file} ---")