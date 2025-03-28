
# rGetEDGAR

<!-- badges: start -->

[<img src="https://img.shields.io/badge/lifecycle-stable-brightgreen.svg"
alt="Lifecycle: stable" />](https://lifecycle.r-lib.org/articles/stages.html#stable)
[<img src="https://www.r-pkg.org/badges/version/rGetEDGAR"
alt="CRAN status" />](https://cran.r-project.org/package=rGetEDGAR)

<!-- badges: end -->

## Overview

rGetEDGAR provides tools for downloading, processing, and analyzing SEC
EDGAR filings in R. Built for both researchers and practitioners, the
package offers efficient parallel processing, smart rate limiting that
respects SEC guidelines, comprehensive error handling, and optimized
storage formats.

### Key Features

- **Complete Pipeline**: Download master indices, document links, and
  actual filing documents

- **Rate Limiting**: Automatically adheres to SEC’s 10 requests/second
  guidelines

- **Hybrid Storage**: Uses both SQLite (for queries) and Parquet (for
  analytics) formats

- **Error Handling**: Gracefully manages network issues and malformed
  content

- **Flexible Filtering**: Filter by company, form type, document type,
  and time period

- **Incremental Updates**: Only download new filings to save bandwidth
  and storage

## Installation

``` r
# From CRAN (once available)
# install.packages("rGetEDGAR")

# Development version from GitHub
# install.packages("devtools")
devtools::install_github("MatthiasUckert/rGetEDGAR")

# Alternative with pak 
# install.packages("pak")
pak::pak("MatthiasUckert/rGetEDGAR")
```

## Quick Start Guide

Basic Workflow

``` r
library(rGetEDGAR)

# 1. Create data directory
data_dir <- "edgar_data"
dir.create(data_dir, showWarnings = FALSE)

# 2. Set your email (required by SEC)
my_email <- "your.email@example.com"  # Replace with your email

# 3. Download master index (metadata about all filings)
edgar_get_master_index(
  .dir = data_dir,
  .user = my_email,
  .from = 2023.1,  # Start from Q1 2023
  .to = 2023.4     # Up to Q4 2023
)

# 4. Extract document links for Apple Inc. (CIK: 0000320193) 10-K filings
edgar_get_document_links(
  .dir = data_dir,
  .user = my_email,
  .hash_idx = NULL  # Process all available filings
)

# 5. Download the actual 10-K documents
edgar_download_document(
  .dir = data_dir,
  .user = my_email,
  .doc_ids = NULL  # Download all available documents
)

# 6. Parse downloaded documents
edgar_parse_documents(
  .dir = data_dir,
  .workers = 4  # Use 4 cores for parallel processing
)
```

## Detailed Documentation

### Package Architecture

rGetEDGAR follows a staged approach to downloading and processing SEC
EDGAR data:

1.  **Master Index** → 2. **Document Links** → 3. **Document Download**
    → 4. **Document Parsing**

Each stage builds on the previous one, creating a complete pipeline for
working with SEC filings.

### Function Reference

#### Stage 1: Master Index

``` r
edgar_get_master_index(
  .dir,        # Directory to store data
  .user,       # Your email address
  .from = NULL, # Start year.quarter (e.g., 2020.1 for Q1 2020)
  .to = NULL,   # End year.quarter
  .verbose = TRUE
)
```

Downloads and processes the SEC EDGAR master index files, which contain
metadata about all filings submitted to the EDGAR system.

#### Stage 2: Document Links

``` r
edgar_get_document_links(
  .dir,       # Directory where data is stored
  .user,      # Your email address
  .hash_idx,  # Specific HashIndex values to process (or NULL for all)
  .workers = 1L,
  .verbose = TRUE
)
```

Downloads and processes document links from filing index pages,
extracting metadata such as document descriptions, types, and URLs.

#### Stage 3: Document Download

``` r
edgar_download_document(
  .dir,        # Directory where data is stored
  .user,       # Your email address
  .doc_ids,    # Specific document IDs to download (or NULL for all)
  .workers = 1L,
  .verbose = TRUE
)
```

Downloads actual document files from SEC EDGAR filings, handling
different formats and organizing them into a structured directory
layout.

#### Stage 4: Document Parsing

``` r
edgar_parse_documents(
  .dir,        # Directory where data is stored
  .doc_ids = NULL,
  .workers = 1L,
  .verbose = TRUE
)
```

Processes downloaded documents by extracting text content, standardizing
the text, and storing the results in a structured format for analysis.

### Utility Functions

The package includes several utility functions to help with data
management and processing:

``` r
# Set up directory structure
get_directories(.dir)

# Standardize text with configurable options
standardize_text(.str, .options = standardize_options())

# Read and extract text from HTML content
read_html(.html, .options = html_options())

# Display HTML content in browser (for debugging)
show_html(.html)
```

## Advanced Usage

### Filtering by Company

To download filings for specific companies, you need their Central Index
Key (CIK):

``` r
# Example: Download Apple Inc. (0000320193) and Tesla Inc. (0001318605) filings
edgar_get_document_links(
  .dir = data_dir,
  .user = my_email,
  .hash_idx = ...,  # HashIndex values for Apple and Tesla filings
  .workers = 4L
)
```

### Filtering by Form Type

To download specific form types (e.g., 10-K, 10-Q, 8-K):

``` r
# First, get the HashIndex values for 10-K filings
cik_apple <- "0000320193"  # Apple Inc.
form_10k <- "10-K"

# Get HashIndex values for Apple 10-K filings
hash_indices <- arrow::open_dataset(file.path(data_dir, "MasterIndex/MasterIndex")) %>%
  dplyr::filter(CIK == cik_apple, FormType == form_10k) %>%
  dplyr::distinct(HashIndex) %>%
  dplyr::collect() %>%
  dplyr::pull(HashIndex)

# Download document links for those filings
edgar_get_document_links(
  .dir = data_dir,
  .user = my_email,
  .hash_idx = hash_indices,
  .workers = 4L
)
```

### Parallel Processing

For faster downloads, increase the number of workers:

``` r
# Use 8 cores for parallel processing
edgar_download_document(
  .dir = data_dir,
  .user = my_email,
  .doc_ids = NULL,
  .workers = 8L
)
```

Note: Be mindful of SEC’s rate limits (10 requests per second) when
increasing worker count.

### Storage Considerations

Downloaded data can grow quite large, especially when downloading
multiple years of filings. The package uses a combination of Parquet
files (for efficient storage and analytics) and SQLite databases (for
queries during processing).

For a full index of all SEC filings from 1993 to present, expect to use:

- Master Index: ~2-3 GB
- Document Links: ~20-30 GB
- Raw Documents: Highly variable, can range from hundreds of GB to
  several TB depending on filtering

## SEC EDGAR Access Guidelines

When using rGetEDGAR, please comply with SEC’s EDGAR access
requirements:

1.  **Identify Yourself**: Always provide a legitimate email address in
    the `.user` parameter
2.  **Limit Requests**: The package automatically limits to 10 requests
    per second
3.  **Proper Use**: Use the data for legitimate research or analysis
    purposes
4.  **No Redistribution**: Do not redistribute bulk SEC EDGAR data
    commercially

For full guidelines, see: [SEC’s EDGAR Public Dissemination Service
Policies](https://www.sec.gov/developer)

## Troubleshooting

### Common Issues

**Error: Connection timed out** - The SEC may be blocking your requests
due to rate limiting - Solution: Reduce the number of workers or wait
before trying again

**Error: Cannot write Parquet file** - You may not have write
permissions for the directory - Solution: Check directory permissions or
use a different location

**Warning: No new files to download**

- All requested files have already been downloaded
- This is normal behavior for incremental updates

### Logging

The package maintains several log files in the data directory:

- `LogMasterIndex.csv`: Logs master index download operations
- `LogDocLinks.csv`: Logs document link download operations
- `LogDocDataOriginal.csv`: Logs document download operations
- `LogDocDataParsed.csv`: Logs document parsing operations

Check these logs for detailed information about any issues.

## Contributing

Contributions to rGetEDGAR are welcome! Please follow these steps:

1.  Fork the repository

2.  Create a feature branch (`git checkout -b feature/amazing-feature`)

3.  Make your changes

4.  Run tests and checks (`devtools::check()`)

5.  Commit your changes (`git commit -m 'Add amazing feature'`)

6.  Push to the branch (`git push origin feature/amazing-feature`)

7.  Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file
for details.

## Citation

If you use rGetEDGAR in your research, please cite it:

…
