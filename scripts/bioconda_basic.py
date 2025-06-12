#!/usr/bin/env python3

import argparse
import requests
import pandas as pd
from typing import List, Dict, Optional
import time
import sys
import subprocess
import json
from datetime import datetime, timedelta

ANACONDA_API_TEMPLATE = "https://api.anaconda.org/package/bioconda/{pkg}"

def get_download_stats(package_name: str, verbose: bool = False) -> Dict:
    """Get download statistics from conda S3 data using pandas."""
    
    try:
        # Get last year of data
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        
        # Generate file paths for last year
        file_urls = []
        current = start_date.replace(day=1)
        while current <= end_date:
            year = current.year
            month = current.strftime("%m")
            file_url = f"s3://anaconda-package-data/conda/monthly/{year}/{year}-{month}.parquet"
            file_urls.append(file_url)
            
            # Move to next month
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)
        
        if verbose:
            print(f"  Fetching download stats for {package_name} from {len(file_urls)} monthly files...")
        
        total_downloads = 0
        months_found = 0
        
        for file_url in file_urls:
            try:
                # Read parquet file with pandas
                df = pd.read_parquet(file_url, storage_options={"anon": True}, engine="pyarrow")
                
                # Filter for our package
                pkg_data = df[df['pkg_name'] == package_name]
                
                if not pkg_data.empty:
                    month_downloads = pkg_data['counts'].sum()
                    total_downloads += month_downloads
                    months_found += 1
                    if verbose:
                        print(f"    Found {month_downloads:,} downloads in {file_url.split('/')[-1]}")
                        
            except Exception as e:
                if verbose:
                    print(f"    Could not read {file_url.split('/')[-1]}: {e}")
                continue
        
        if total_downloads > 0:
            return {
                "downloads": total_downloads,
                "data_source": "conda_s3",
                "note": f"Last year total from {months_found} monthly files"
            }
        else:
            return {
                "downloads": 0,
                "data_source": "conda_s3", 
                "note": "No download data found in last year"
            }
            
    except Exception as e:
        if verbose:
            print(f"  Error fetching download stats for {package_name}: {e}")
        
        return {
            "downloads": "N/A",
            "data_source": "error",
            "note": f"Error accessing S3 data: {str(e)}"
        }

def parse_args():
    parser = argparse.ArgumentParser(description="Fetch Bioconda package stats and output CSV.")
    parser.add_argument("input_file", help="Path to input file with Bioconda package names")
    parser.add_argument("-o", "--output", default="bioconda_stats.csv", help="Output CSV file path")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    return parser.parse_args()

def read_package_list(filepath: str) -> List[str]:
    """Read package names from file, filtering out empty lines and comments."""
    try:
        with open(filepath) as f:
            packages = [line.strip() for line in f if line.strip() and not line.startswith("#")]
        return packages
    except FileNotFoundError:
        print(f"Error: Input file '{filepath}' not found")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading input file: {e}")
        sys.exit(1)

def fetch_package_stats(pkg_name: str, verbose: bool = False) -> Dict:
    """Fetch package statistics from Anaconda API."""
    url = ANACONDA_API_TEMPLATE.format(pkg=pkg_name)
    
    try:
        if verbose:
            print(f"Fetching stats for: {pkg_name}")
        
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            
            # Get version count from releases/files info
            versions_count = len(data.get("releases", []))
            files_count = len(data.get("files", []))
            
            # Try to get download statistics
            download_info = get_download_stats(pkg_name, verbose)
            
            return {
                "package": pkg_name,
                "downloads": download_info["downloads"],
                "download_source": download_info["data_source"],
                "download_note": download_info["note"],
                "latest_version": data.get("latest_version", "N/A"),
                "total_versions": versions_count,
                "total_files": files_count,
                "summary": data.get("summary", "").replace(",", ";"),  # Clean commas for CSV
                "license": data.get("license", "N/A"),
                "home_page": data.get("home", "N/A"),
                "dev_url": data.get("dev_url", "N/A"),
                "doc_url": data.get("doc_url", "N/A"),
                "created_at": data.get("created_at", "N/A"),
                "modified_at": data.get("modified_at", "N/A"),
                "status": "success"
            }
        elif response.status_code == 404:
            return {
                "package": pkg_name,
                "downloads": "N/A",
                "download_source": "none",
                "download_note": "Package not found",
                "latest_version": "N/A",
                "total_versions": 0,
                "total_files": 0,
                "summary": "Package not found",
                "license": "N/A", 
                "home_page": "N/A",
                "dev_url": "N/A",
                "doc_url": "N/A",
                "created_at": "N/A",
                "modified_at": "N/A",
                "status": "not_found"
            }
        else:
            return {
                "package": pkg_name,
                "downloads": "N/A",
                "download_source": "none",
                "download_note": f"HTTP {response.status_code}",
                "latest_version": "N/A",
                "total_versions": 0,
                "total_files": 0,
                "summary": f"HTTP {response.status_code}",
                "license": "N/A",
                "home_page": "N/A",
                "dev_url": "N/A",
                "doc_url": "N/A", 
                "created_at": "N/A",
                "modified_at": "N/A",
                "status": "error"
            }
            
    except requests.exceptions.Timeout:
        return {
            "package": pkg_name,
            "downloads": "N/A",
            "download_source": "none",
            "download_note": "Request timeout",
            "latest_version": "N/A",
            "total_versions": 0,
            "total_files": 0,
            "summary": "Request timeout",
            "license": "N/A",
            "home_page": "N/A",
            "dev_url": "N/A",
            "doc_url": "N/A",
            "created_at": "N/A",
            "modified_at": "N/A",
            "status": "timeout"
        }
    except Exception as e:
        return {
            "package": pkg_name,
            "downloads": "N/A",
            "download_source": "none",
            "download_note": f"Error: {str(e)}",
            "latest_version": "N/A",
            "total_versions": 0,
            "total_files": 0,
            "summary": f"Error: {str(e)}",
            "license": "N/A",
            "home_page": "N/A",
            "dev_url": "N/A",
            "doc_url": "N/A",
            "created_at": "N/A",
            "modified_at": "N/A",
            "status": "error"
        }

def main():
    args = parse_args()
    
    # Read package list
    packages = read_package_list(args.input_file)
    print(f"Processing {len(packages)} packages...")
    
    # Fetch stats for each package
    stats = []
    successful = 0
    
    for i, pkg in enumerate(packages, 1):
        result = fetch_package_stats(pkg, args.verbose)
        stats.append(result)
        
        if result["status"] == "success":
            successful += 1
            
        if args.verbose:
            print(f"Progress: {i}/{len(packages)} - {pkg}: {result['status']}")
        
        # Small delay to be respectful to the API
        if i < len(packages):
            time.sleep(0.2)
    
    # Create DataFrame and save to CSV
    df = pd.DataFrame(stats)
    df.to_csv(args.output, index=False)
    
    # Print summary
    print(f"Results saved to: {args.output}")
    print(f"Successfully processed: {successful}/{len(packages)} packages")
    
    # Show status breakdown
    status_counts = df['status'].value_counts()
    print("\nStatus breakdown:")
    for status, count in status_counts.items():
        print(f"  {status}: {count}")

if __name__ == "__main__":
    main()