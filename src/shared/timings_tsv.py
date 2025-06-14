import json
import os

def time_str_to_int(time_str):
    """Convert time in 'HH:MM' or 'H:MM' format to an integer HHMM."""
    try:
        parts = time_str.strip().split(':')
        if len(parts) != 2:
            raise ValueError("Invalid time format")
        hours, minutes = int(parts[0]), int(parts[1])
        return hours * 100 + minutes
    except Exception as e:
        print(f"Skipping bad time: {time_str} -> {e}")
        return None

def duration_str_to_minutes(duration_str):
    """Convert duration in 'HH:MM' or 'H:MM' format to total minutes."""
    try:
        parts = duration_str.strip().split(':')
        if len(parts) != 2:
            raise ValueError("Invalid duration format")
        hours, minutes = int(parts[0]), int(parts[1])
        return hours * 60 + minutes
    except Exception as e:
        print(f"Skipping bad duration: {duration_str} -> {e}")
        return None

def process_tsv_to_json(input_file, output_file):
    result = {}
    input_file = os.path.abspath(input_file)
    output_file = os.path.abspath(output_file)
    with open(input_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    for line_no, line in enumerate(lines[1:], start=2):  # skip header, count lines properly
        parts = line.strip().split('\t')
        if len(parts) != 3:
            print(f"Skipping malformed line {line_no}: {line.strip()}")
            continue

        route_no, times_str, duration_str = parts
        times = times_str.strip().split()
        duration_minutes = duration_str_to_minutes(duration_str)

        if duration_minutes is None:
            continue  # skip this row if duration is invalid

        entries = []
        for time in times:
            start_time = time_str_to_int(time)
            if start_time is not None:
                entries.append({"start": start_time, "duration": duration_minutes})

        if entries:
            result[route_no] = entries

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=4)
        print(f"Written {len(result)} routes to {output_file}")

if __name__ == "__main__":
    process_tsv_to_json('../../in/helpers/construct_timings/timings.tsv', './start_times.json')
