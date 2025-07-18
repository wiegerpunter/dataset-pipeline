#!/bin/bash

# Usage: ./phase1.sh <input_csv_file>
# Root folder where synthFromDisk resides
#CONFIG_FILE="/app/split/config.json"
#sizeFactor=$(jq -r '.sizeFactor | tostring' "$CONFIG_FILE")

ROOT_FOLDER="./app/data/input/data/synthFromDisk/23.0"
echo "Using ROOT_FOLDER: $ROOT_FOLDER"


set -e

# Loop over all relevant CSV files recursively
find "$ROOT_FOLDER" -mindepth 2 -maxdepth 2 -type f -name "*.csv" | while read -r INPUT_FILE; do

#if [ $# -ne 1 ]; then
#    echo "Usage: $0 <input_csv_file>"
#    exit 1
#fi
#
#INPUT_FILE="$1"\
  # Check if it's a queries file
  if [[ "$INPUT_FILE" == *"queries"* ]]; then
      echo "Skipping queries file: $INPUT_FILE"
      continue
  fi


  echo "Processing file: $INPUT_FILE"
  BASENAME=$(basename "$INPUT_FILE" .csv)

  # Extract fields based on underscores
  IFS='_' read -r prefix numAttrs domain sizeFactor datasetResiduSize numZipfianAttrs zipfAlpha numUniformAttrs perc <<< "$BASENAME"

  # Compute residu size safely
  RESIDU_EXPONENT=${sizeFactor%%.*}
  RESIDU_SIZE=$((2 ** RESIDU_EXPONENT))
  DATA_FOLDER=$(dirname "$INPUT_FILE")

  echo "Extracted parameters:"
  echo "sizeFactor: $sizeFactor"
  echo "numZipfianAttrs: $numZipfianAttrs"
  echo "zipfAlpha: $zipfAlpha"
  echo "numUniformAttrs: $numUniformAttrs"
  echo "perc: $perc"
  echo "Computed residu size: $RESIDU_SIZE"
  #
  ## Extract the 5th underscore-separated field
  #RESIDU_EXPONENT_RAW=$(echo "$BASENAME" | awk -F'_' '{print $5}')
  #
  ## Take only the integer part (everything before the dot)
  #RESIDU_EXPONENT=${RESIDU_EXPONENT_RAW%%.*}
  #
  ## Now compute residu size safely
  #RESIDU_SIZE=$((2 ** RESIDU_EXPONENT))
  DATA_FOLDER=$(dirname "$INPUT_FILE")
  echo "Data folder: $DATA_FOLDER"
  SPLIT_FOLDER="$DATA_FOLDER/split_sets"

  # Create the split_sets folder if it doesn't exist
  mkdir -p "$SPLIT_FOLDER"


  # Construct output file name suffix
  SUFFIX="_${sizeFactor}_${numZipfianAttrs}_${zipfAlpha}_${numUniformAttrs}_${perc}.csv"

  # Run awk to split the file based on id and sign
  awk -F',' -v residu_size="$RESIDU_SIZE" -v split_folder="$SPLIT_FOLDER" -v suffix="$SUFFIX" '
  {
      id = $1 + 0;
      sign = $NF + 0;
      if (sign == 1 && id < residu_size)
          print > (split_folder "/residu" suffix);
      else if (sign == 1)
          print > (split_folder "/noise_inserts" suffix);
      else if (sign == -1)
          print > (split_folder "/noise_deletes" suffix);
  }
  ' "$INPUT_FILE"

  #
  ## Run awk to split the file based on id and sign
  #awk -F',' -v residu_size="$RESIDU_SIZE" -v data_folder="$DATA_FOLDER" '
  #{
  #    id = $1 + 0;
  #    sign = $NF + 0;
  #    if (sign == 1 && id < residu_size)
  #        print > (data_folder  "/residu.csv");
  #    else if (sign == 1)
  #        print > (data_folder "/noise_inserts.csv");
  #    else if (sign == -1)
  #        print > (data_folder "/noise_deletes.csv");
  #}
  #' "$INPUT_FILE"
#
#  # Shuffle the files
#  echo "Shuffling inserts and deletes"

#  # Shuffle only if files exist and are not empty
#  if [ -s "$SPLIT_FOLDER/residu$SUFFIX" ]; then
#      shuf -o "$SPLIT_FOLDER/residu_shuffled$SUFFIX" "$SPLIT_FOLDER/residu$SUFFIX"
#  fi
#
#  if [ -s "$SPLIT_FOLDER/noise_inserts$SUFFIX" ]; then
#      shuf -o "$SPLIT_FOLDER/noise_inserts_shuffled$SUFFIX" "$SPLIT_FOLDER/noise_inserts$SUFFIX"
#  fi
#
#  if [ -s "$SPLIT_FOLDER/noise_deletes$SUFFIX" ]; then
#      shuf -o "$SPLIT_FOLDER/noise_deletes_shuffled$SUFFIX" "$SPLIT_FOLDER/noise_deletes$SUFFIX"
#  fi
done
