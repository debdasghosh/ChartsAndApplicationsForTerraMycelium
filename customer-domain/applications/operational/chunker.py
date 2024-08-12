import json
import os
import sys

def chunk_json(input_filename, max_size_in_mb):
    # Strip the file extension and prepare output filename prefix
    output_filename = os.path.splitext(input_filename)[0] + "_chunk"

    i = 0                               
    with open(input_filename, 'r') as input_file:
        chunk_file = open(f"{output_filename}{i}.json", "w")        
        for line in input_file:
            chunk_file.write(line)
            chunk_file.flush()  # Ensure it's written to disk to get accurate size
            if os.path.getsize(f"{output_filename}{i}.json") > max_size_in_mb * 1024 * 1024:
                chunk_file.close()
                i += 1
                chunk_file = open(f"{output_filename}{i}.json", "w")
        chunk_file.close()


if __name__ == "__main__":
    filename = sys.argv[1]
    max_size_in_mb = int(sys.argv[2])  # convert string to int
    chunk_json(filename, max_size_in_mb)
