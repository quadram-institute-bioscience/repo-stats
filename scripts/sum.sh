#!/bin/bash

# Check if a filename is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <csv_file>"
  exit 1
fi

file="$1"

# Read header and split into array
header=$(head -1 "$file")
IFS=',' read -r -a columns <<< "$header"

# Initialize indices
views_idx=-1
stars_idx=-1
forks_idx=-1
downloads_idx=-1

# Find indices of required columns
for i in "${!columns[@]}"; do
  case "${columns[$i]}" in
    views_14d) views_idx=$i ;;
    stars) stars_idx=$i ;;
    forks) forks_idx=$i ;;
    total_downloads) downloads_idx=$i ;;
  esac
done

# Check if all indices found
if [ $views_idx -eq -1 ] || [ $stars_idx -eq -1 ] || [ $forks_idx -eq -1 ] || [ $downloads_idx -eq -1 ]; then
  echo "One or more required columns not found in the CSV header."
  exit 1
fi

# Use awk to sum the columns (awk arrays are 1-based, so add 1 to indices)
awk -F',' -v v_idx=$((views_idx+1)) -v s_idx=$((stars_idx+1)) -v f_idx=$((forks_idx+1)) -v d_idx=$((downloads_idx+1)) '
  NR>1 {views+=$v_idx; stars+=$s_idx; forks+=$f_idx; downloads+=$d_idx}
  END {
    print "Total views_14d: " views
    print "Total stars: " stars
    print "Total forks: " forks
    print "Total downloads: " downloads
  }
' "$file"
