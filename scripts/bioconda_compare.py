#!/usr/bin/env python3
"""
Script to retrieve conda package download statistics and compare across time periods.
"""

import subprocess
import re
import sys
import csv
from typing import List, Dict

def get_conda_stats(package_name: str, start_month: str, end_month: str) -> int:
    """
    Execute condastats command and extract the total download count.
    
    Args:
        package_name: Name of the conda package
        start_month: Start month in YYYY-MM format
        end_month: End month in YYYY-MM format
    
    Returns:
        Total download count as integer
    """
    # Build the condastats command
    cmd = [
        'condastats', 'overall', package_name,
        '--start_month', start_month,
        '--end_month', end_month
    ]
    
    try:
        # Execute the command and capture output
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        output = result.stdout.strip()
        
        # Extract the number from the output using regex
        # Pattern matches the package name followed by whitespace and a number
        pattern = rf'{re.escape(package_name)}\s+(\d+)'
        match = re.search(pattern, output)
        
        if match:
            return int(match.group(1))
        else:
            print(f"Warning: Could not parse output for {package_name}", file=sys.stderr)
            print(f"Output was: {output}", file=sys.stderr)
            return 0
            
    except subprocess.CalledProcessError as e:
        print(f"Error running condastats for {package_name}: {e}", file=sys.stderr)
        return 0
    except Exception as e:
        print(f"Unexpected error for {package_name}: {e}", file=sys.stderr)
        return 0

def generate_stats_table(packages: List[str]) -> List[Dict[str, str]]:
    """
    Generate download statistics table for given packages across two time periods.
    
    Args:
        packages: List of package names to analyze
    
    Returns:
        List of dictionaries containing package stats
    """
    # Define the two time periods
    period1_start, period1_end = "2023-04", "2024-03"
    period2_start, period2_end = "2024-04", "2025-03"
    
    results = []
    
    for package in packages:
        print(f"Processing {package}...", file=sys.stderr)
        
        # Get stats for both periods
        stats_2023_2024 = get_conda_stats(package, period1_start, period1_end)
        stats_2024_2025 = get_conda_stats(package, period2_start, period2_end)
        
        # Add to results
        results.append({
            'PackageName': package,
            '2023-2024': str(stats_2023_2024),
            '2024-2025': str(stats_2024_2025)
        })
    
    return results

def write_csv_table(data: List[Dict[str, str]], filename: str = 'conda_stats.csv'):
    """
    Write the statistics data to a CSV file.
    
    Args:
        data: List of dictionaries containing package statistics
        filename: Output CSV filename
    """
    if not data:
        print("No data to write", file=sys.stderr)
        return
    
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = ['PackageName', '2023-2024', '2024-2025']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        writer.writerows(data)
    
    print(f"Results written to {filename}", file=sys.stderr)

def print_table(data: List[Dict[str, str]]):
    """
    Print the statistics table to console.
    
    Args:
        data: List of dictionaries containing package statistics
    """
    print("\nPackageName,2023-2024,2024-2025")
    for row in data:
        print(f"{row['PackageName']},{row['2023-2024']},{row['2024-2025']}")

def main():
    """Main function to run the conda stats analysis."""
    # List of packages to analyze
    packages = [
        'perl-fastx-reader',
        'seqfu',
        'n50',
        'graphanalyzer',
        'cdhit-reader',
        'lotus2',
        'dadaist2',
        'bamtocov',
        'socru',
        'parallel-virfinder',
        'metaprokka'
    ]
    
    print("Retrieving conda download statistics...", file=sys.stderr)
    print(f"Analyzing {len(packages)} packages", file=sys.stderr)
    
    # Generate the statistics table
    stats_data = generate_stats_table(packages)
    
    # Output results
    print_table(stats_data)
    write_csv_table(stats_data)

if __name__ == "__main__":
    main()
