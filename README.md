
<!-- README.md is generated from README.Rmd. Please edit that file -->

# rGetEDGAR

<!-- badges: start -->
<!-- badges: end -->

## Overview

rGetEDGAR is an R package designed for efficient downloading and
processing of SEC EDGAR filings. It features parallel processing
capabilities, smart rate limiting that respects SEC guidelines, and
robust error handling. The package stores data in both SQLite and
Parquet formats for optimal performance and flexibility.

## Key Features

- Parallel downloading of master indices and document links

- Smart rate limiting (10 requests/second as per SEC guidelines)

- Efficient storage in both SQLite and Parquet formats

- Automated backups

- Incremental updates

- Flexible filtering by company, document type, and time period

## Installation

You can install the development version of rGetEDGAR from
[GitHub](https://github.com/) with:

``` r
# install.packages("pak")
pak::pak("MatthiasUckert/rGetEDGAR")
```

## Usage

### Basic Example

``` r
library(rGetEDGAR)

# Create a directory for EDGAR data
data_dir <- fs::dir_create("edgar_data")

# Set your email for SEC requests
my_email <- "your.email@domain.com"  # Replace with your email
```

### 1. Download Master Index

First, download the master index files which contain metadata about all
SEC filings:

``` r
edgar_get_master_index(
  .dir = data_dir,
  .user = my_email,
  .from = 2023.1,  # Start from Q1 2023
  .to = 2023.4,    # Up to Q4 2023
  .verbose = TRUE
)
```

### 2. Get Document Links

Then retrieve the document links for the filings you’re interested in:

``` r
edgar_download_document(
  .dir = data_dir,
  .user = my_email,
  .from = 2023.1,
  .to = 2023.4,
  .ciks = "1318605",
  .types = "10-K",    # Only 10-K filings
  .workers = 10L,
  .verbose = TRUE
)
```

### 3. Download Documents

Finally, download the actual documents:

``` r
edgar_download_document(
  .dir = data_dir,
  .user = my_email,
  .from = 2023.1,
  .to = 2023.4,
  .ciks = "1318605",
  .types = "10-K",    # Only 10-K filings
  .workers = 10L,
  .verbose = TRUE
)
```

## Important Notes

1.  **SEC Requirements**: You must provide a valid email address when
    accessing EDGAR. The SEC uses this to track usage and contact users
    if there are issues.

2.  **Rate Limits**: The SEC limits requests to 10 per second. rGetEDGAR
    handles this automatically, but be mindful of running multiple
    instances.

3.  **Storage**: Depending on your filtering criteria, the downloaded
    data can be substantial. Ensure you have sufficient disk space.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
For major changes, please open an issue first to discuss what you would
like to change.

## License

This project is licensed under the MIT License - see the LICENSE file
for details.
