if (FALSE) {
  .msg <- "Test Message"
  .verbose <- TRUE
  .line <- "\n"

  .from <- NULL
  .to <- NULL

  .t0 <- Sys.time()
  .i <- 125
  .n <- 100000

  .from = NULL
  .to = NULL
  .ciks = NULL
  .types = NULL
}


# Filter EDGAR Data ---------------------------------------------------------------------------
#' Validate and Process EDGAR Parameters
#'
#' @description
#' Internal helper function to validate and process input parameters for EDGAR functions
#'
#' @param .from Numeric value for start period (year.quarter)
#' @param .to Numeric value for end period (year.quarter)
#' @param .ciks Character vector of CIK numbers
#' @param .types Character vector of document types
#'
#' @return A list containing processed parameters:
#' \itemize{
#'   \item from: Start period (defaults to 1993.1)
#'   \item to: End period (defaults to current quarter)
#'   \item ciks: Processed CIK numbers
#'   \item types: Processed document types
#' }
#'
#' @keywords internal
get_edgar_params <- function(.from = NULL, .to = NULL, .ciks = NULL, .types = NULL) {
  # Process time periods
  year_current_ <- lubridate::year(Sys.Date())
  qtr_current_ <- lubridate::quarter(Sys.Date()) - 1L
  current_period_ <- as.numeric(paste0(year_current_, ".", qtr_current_))

  # Create output list
  list(
    from = ifelse(is.null(.from), 1993.1, .from),
    to = ifelse(is.null(.to), current_period_, .to),
    ciks = if (is.null(.ciks)) NA_character_ else .ciks,
    types = if (is.null(.types)) NA_character_ else .types
  )
}


#' Filter EDGAR Data by CIK and Document Type
#'
#' @description
#' Internal helper function to filter EDGAR data based on CIK numbers and document types
#'
#' @param .tab Data frame or tibble containing EDGAR data
#' @param .params List of parameters from get_edgar_params()
#'
#' @return Filtered data frame or tibble
#'
#' @keywords internal
filter_edgar_data <- function(.tab, .params) {
  tab_ <- .tab
  # Filter by CIK if specified
  if (!anyNA(.params$ciks)) {
    tab_ <- dplyr::filter(tab_, CIK %in% .params$ciks)
  }

  # Filter by document type if specified
  if (!anyNA(.params$types)) {
    tab_ <- dplyr::filter(tab_, Type %in% .params$types)
  }

  return(tab_)
}

#' Print Verbose Messages
#'
#' @description
#' Prints status messages with consistent formatting if verbose mode is enabled.
#'
#' @param .msg Character string containing the message to print
#' @param .verbose Logical indicating whether to print the message
#' @param .line Character string specifying the line ending (default: "\\n")
#'
#' @keywords internal
print_verbose <- function(.msg, .verbose, .line = "\n") {
  if (.verbose) {
    cat(.line, paste0(.msg, paste(rep(" ", 40), collapse = "")))
  }
}



# Utils ---------------------------------------------------------------------------------------
#' Set Up Directory Structure
#'
#' @description
#' Creates and returns the directory structure for storing EDGAR data.
#'
#' @param .dir Character string specifying the base directory
#'
#' @return A list containing paths for MasterIndex, DocumentLinks, and DocumentData
#'
#' @keywords internal
get_directories <- function(.dir) {
  dir_ <- fs::dir_create(.dir)
  dir_master_ <- fs::dir_create(file.path(dir_, "MasterIndex"))
  dir_links_ <- fs::dir_create(file.path(dir_, "DocumentLinks"))
  list(
    MasterIndex = list(
      Sqlite = file.path(dir_master_, "MasterIndex.sqlite"),
      Parquet = file.path(dir_master_, "MasterIndex.parquet")
    ),
    DocumentLinks = list(
      Sqlite = file.path(dir_links_, "DocumentLinks.sqlite"),
      Parquet = file.path(dir_links_, "DocumentLinks.parquet")
    ),
    DocumentData = fs::dir_create(file.path(dir_, "DocumentData"))
  )
}

#' Generate Year-Quarter Combinations
#'
#' @description
#' Creates a table of all possible year-quarter combinations within the specified date range.
#' Uses validated parameters from get_edgar_params() for date range processing.
#'
#' @param .from Numeric value specifying the start year.quarter (e.g., 2020.1 for Q1 2020).
#'             If NULL, defaults to 1993.1
#' @param .to Numeric value specifying the end year.quarter.
#'           If NULL, defaults to current quarter
#'
#' @return A tibble with columns:
#' \itemize{
#'   \item Year: Integer representing the year
#'   \item Quarter: Integer representing the quarter (1-4)
#'   \item YearQuarter: Numeric combining year and quarter (e.g., 2020.1)
#' }
#'
#' @details
#' The function creates all possible combinations of years and quarters between
#' the specified date range. It uses get_edgar_params() to validate and process
#' the input parameters, ensuring consistent date range handling across the package.
#'
#' @keywords internal
#'
#' @seealso get_edgar_params
get_year_qtr_table <- function(.from = NULL, .to = NULL) {
  params_ <- get_edgar_params(.from, .to)
  tidyr::expand_grid(
    Year = 1993:lubridate::year(Sys.Date()),
    Quarter = 1:4
  ) %>%
    dplyr::mutate(
      YearQuarter = as.numeric(paste0(Year, ".", Quarter))
    ) %>%
    dplyr::filter(dplyr::between(YearQuarter, params_$from, params_$to))
}

#' Format Numbers for Display
#'
#' @description
#' Formats numbers as either comma-separated values or percentages.
#'
#' @param .num Numeric value to format
#' @param .type Character string specifying format type: "c" for comma or "p" for percentage
#'
#' @return Formatted character string
#'
#' @keywords internal
format_number <- function(.num, .type = c("c", "p")) {
  # Match first argument of .type
  .type <- match.arg(.type)

  if (.type == "c") {
    # Format as number with comma separator
    return(trimws(format(.num, big.mark = ",", scientific = FALSE)))
  } else {
    # Format as percentage with 4 decimal places
    return(trimws(paste0(format(round(.num * 100, 4), nsmall = 4), "%")))
  }
}

#' Format Time Status Message
#'
#' @description
#' Creates a formatted status message showing elapsed time and estimated time remaining.
#'
#' @param .t0 POSIXct start time
#' @param .i Current iteration number
#' @param .n Total number of iterations
#'
#' @return Formatted status message
#'
#' @keywords internal
format_time_status <- function(.t0, .i, .n) {
  ela_ <- difftime(Sys.time(), .t0, units = "secs")
  eta_ <- (ela_ / .i) * (.n - .i)
  ela_ <- dplyr::case_when(
    ela_ > 60 * 60 * 24 ~ sprintf("%.2f days", ela_ / (60 * 60 * 24)),
    ela_ > 60 * 60 ~ sprintf("%.2f hours", ela_ / (60 * 60)),
    ela_ > 60 ~ sprintf("%.2f mins", ela_ / 60),
    TRUE ~ sprintf("%.2f secs", ela_)
  )
  eta_ <- dplyr::case_when(
    eta_ > 60 * 60 * 24 ~ sprintf("%.2f days", eta_ / (60 * 60 * 24)),
    eta_ > 60 * 60 ~ sprintf("%.2f hours", eta_ / (60 * 60)),
    eta_ > 60 ~ sprintf("%.2f mins", eta_ / 60),
    TRUE ~ sprintf("%.2f secs", eta_)
  )
  sprintf(" | Elapsed: %s | ETA: %s", ela_, eta_)
}

#' Standardize String Content
#'
#' @description
#' Standardizes string content by removing extra whitespace and converting to ASCII.
#'
#' @param .str Character string to standardize
#'
#' @return Standardized character string
#'
#' @keywords internal
standardize_string <- function(.str) {
  str_ <- .str
  str_ <- stringi::stri_replace_all_regex(str_, "([[:blank:]|[:space:]])+", " ")
  str_ <- stringi::stri_trans_general(str_, "Latin-ASCII")
  str_ <- trimws(str_)
  return(str_)
}


# Warnings ------------------------------------------------------------------------------------
#' Generate Master Index Warning
#'
#' @description
#' Generates a warning message for master index fetch failures.
#'
#' @param .year Year of the failed fetch
#' @param .qtr Quarter of the failed fetch
#'
#' @keywords internal
get_warning_master_index <- function(.year, .qtr) {
  warning(paste0("Error in fetching Year-Quarter: ", .year, "-", .qtr, ", continue..."), call. = FALSE)
}

#' Generate Document Links Warning
#'
#' @description
#' Generates a warning message for document links parsing failures.
#'
#' @param .cik CIK number of the failed parsing
#'
#' @keywords internal
get_warning_document_links <- function(.cik) {
  warning(paste0("Error in parsing Document Links for CIK: ", .cik, ", continue..."), call. = FALSE)
}

#' Generate Document Download Warning
#'
#' @description
#' Generates a warning message for document download failures.
#'
#' @param .cik CIK number of the failed download
#'
#' @keywords internal
get_warning_document_download <- function(.cik) {
  warning(paste0("Error in downloading Document for CIK: ", .cik, ", continue..."), call. = FALSE)
}



# Requests ------------------------------------------------------------------------------------
#' Get HTTP Request Header
#'
#' @description
#' Creates HTTP request headers for SEC EDGAR API calls.
#'
#' @param .user User agent string for HTTP requests
#'
#' @return httr header object
#'
#' @keywords internal
get_header <- function(.user) {
  httr::add_headers(Connection = "keep-alive", `User-Agent` = .user)
}

#' Make HTTP GET Request
#'
#' @description
#' Makes an HTTP GET request with error handling.
#'
#' @param .url URL to request
#' @param .user User agent string
#'
#' @return httr response object or error
#'
#' @keywords internal
make_get_request <- function(.url, .user) {
  try(httr::GET(url = .url, get_header(.user)), silent = TRUE)
}

#' Parse HTTP Response Content
#'
#' @description
#' Parses HTTP response content with UTF-8 encoding.
#'
#' @param .result httr response object
#'
#' @return Parsed content as text
#'
#' @keywords internal
parse_content <- function(.result) {
  try(httr::content(.result, as = "text", encoding = "UTF-8"), silent = TRUE)
}

# Get Master Index ----------------------------------------------------------------------------
#' Initialize Master Index Database
#'
#' @description
#' Creates SQLite database schema for master index if it doesn't exist.
#'
#' @param .path Path to SQLite database file
#'
#' @keywords internal
initial_master_index_database <- function(.path) {
  if (file.exists(.path)) {
    return(invisible(NULL))
  }
  con_ <- DBI::dbConnect(RSQLite::SQLite(), .path)

  DBI::dbExecute(con_, "
  CREATE TABLE master_index (
    HashIndex TEXT,
    Year INTEGER,
    Quarter INTEGER,
    YearQuarter REAL,
    CIK TEXT,
    CompanyName TEXT,
    FormType TEXT,
    DateFiled DATE,
    UrlFullText TEXT,
    UrlIndexPage TEXT
  )
")
  DBI::dbDisconnect(con_)
}

#' Get Processed Year-Quarters
#'
#' @description
#' Retrieves list of year-quarters that have already been processed.
#'
#' @param .con Database connection object
#'
#' @return Vector of processed year-quarters
#'
#' @keywords internal
get_processed_year_qtr <- function(.con) {
  dplyr::tbl(.con, "master_index") %>%
    dplyr::distinct(YearQuarter) %>%
    dplyr::collect() %>%
    dplyr::pull(YearQuarter)
}

#' Convert Content to DataFrame
#'
#' @description
#' Converts raw master index content to a structured data frame.
#'
#' @param .content Raw content string
#' @param .year Year of the content
#' @param .qtr Quarter of the content
#'
#' @return Tibble with processed master index data
#'
#' @keywords internal
content_to_dataframe <- function(.content, .year, .qtr) {
  tab_ <- tibble::tibble(text = stringi::stri_split_lines1(.content)) %>%
    dplyr::slice(-(1:9)) %>%
    dplyr::mutate(text = stringi::stri_split_fixed(text, "|")) %>%
    tidyr::unnest_wider(text, names_sep = "_") %>%
    janitor::row_to_names(1) %>%
    janitor::clean_names(case = "big_camel") %>%
    dplyr::slice(-1)

  col_ <- c("Cik", "CompanyName", "FormType", "DateFiled", "Filename")
  if (!all(colnames(tab_) == col_)) {
    out_ <- tibble::tibble()
  } else {
    out_ <- tab_ %>%
      dplyr::rename(UrlFullText = Filename, CIK = Cik) %>%
      dplyr::mutate(
        UrlFullText = rvest::url_absolute(UrlFullText, "https://www.sec.gov/Archives/"),
        UrlIndexPage = paste0(fs::path_ext_remove(UrlFullText), "-index.html"),
        CIK = stringi::stri_pad_left(CIK, 10, "0"),
        DateFiled = lubridate::as_date(DateFiled),
        Year = .year,
        Quarter = .qtr,
        YearQuarter = as.numeric(paste0(.year, ".", .qtr)),
        HashIndex = purrr::map_chr(UrlIndexPage, digest::digest)
      ) %>%
      dplyr::select(HashIndex, Year, Quarter, YearQuarter, dplyr::everything())
  }

  return(out_)
}

#' Check Master Index Columns
#'
#' @description
#' Validates the column names of a master index data frame.
#'
#' @param .tab Tibble to validate
#'
#' @return Logical indicating if columns are correct
#'
#' @keywords internal
check_master_index_cols <- function(.tab) {
  cols_ <- c(
    "HashIndex", "Year", "Quarter", "YearQuarter", "CIK", "CompanyName",
    "FormType", "DateFiled", "UrlFullText", "UrlIndexPage"
  )
  all(colnames(.tab) == cols_)
}


# Get Document Links --------------------------------------------------------------------------
#' Initialize Document Links Database
#'
#' @description
#' Creates SQLite database schema for document links if it doesn't exist.
#'
#' @param .path Path to SQLite database file
#'
#' @keywords internal
initial_document_links_database <- function(.path) {
  if (file.exists(.path)) {
    return(invisible(NULL))
  }

  con_ <- DBI::dbConnect(RSQLite::SQLite(), .path)

  # Create the document links table with appropriate data types
  DBI::dbExecute(con_, "
    CREATE TABLE document_links (
      HashIndex TEXT,
      HashDocument TEXT,
      CIK TEXT,
      Year INTEGER,
      Quarter INTEGER,
      YearQuarter REAL,
      Seq TEXT,
      Description TEXT,
      Document TEXT,
      Type TEXT,
      Size TEXT,
      UrlDocument TEXT
    )
  ")

  DBI::dbDisconnect(con_)
}

#' Get Processed Hash Indexes
#'
#' @description
#' Retrieves list of hash indexes that have already been processed.
#'
#' @param .con Database connection object
#'
#' @return Vector of processed hash indexes
#'
#' @keywords internal
get_processed_hash_index <- function(.con) {
  dplyr::tbl(.con, "document_links") %>%
    dplyr::distinct(HashIndex) %>%
    dplyr::collect() %>%
    dplyr::pull(HashIndex)
}

#' Get Document Link
#'
#' @description
#' Downloads and processes document links from an index page.
#'
#' @param .url URL of the index page
#' @param .user User agent string
#'
#' @return Tibble with document links data
#'
#' @keywords internal
help_get_document_link <- function(.url, .user) {
  result_ <- httr::GET(
    url = .url,
    httr::add_headers(Connection = "keep-alive", `User-Agent` = .user)
  )

  code_ <- httr::status_code(result_)

  if (!code_ == 200) {
    warning(paste0("Error in fetching Year-Quarter: ", .url, ", continue..."))
    return(tibble::tibble())
  }

  content_ <- try(httr::content(result_, as = "text", encoding = "UTF-8"), silent = TRUE)

  if (inherits(content_, "try-error")) {
    warning(paste0("Error in parsing Year-Quarter: ", .url, ", continue..."))
    return(tibble::tibble())
  }

  nodes_ <- rvest::read_html(content_) %>%
    rvest::html_elements(css = "#formDiv") %>%
    rvest::html_elements("table")

  out_ <- purrr::map(
    .x = nodes_,
    .f = ~ rvest::html_table(.x) %>%
      dplyr::mutate(
        UrlDocument = rvest::html_attr(rvest::html_elements(.x, "a"), "href"),
        UrlDocument = rvest::url_absolute(UrlDocument, "https://www.sec.gov/Archives/")
      )
  ) %>%
    dplyr::bind_rows() %>%
    dplyr::mutate(
      HashDocument = purrr::map_chr(UrlDocument, digest::digest),
      .before = Seq
    ) %>%
    dplyr::mutate(
      Seq = as.integer(Seq),
      Size = as.integer(Size),
      Seq = dplyr::if_else(is.na(Seq), -1L, Seq),
      dplyr::across(dplyr::where(is.character), trimws),
      dplyr::across(dplyr::where(is.character), ~ dplyr::if_else(. == "", NA_character_, .)),
    )

  Sys.sleep(.101)
  return(out_)
}

# Download Documents --------------------------------------------------------------------------
#' Get Processed Documents
#'
#' @description
#' Retrieves list of documents that have already been processed.
#'
#' @param .dir Directory containing processed documents
#'
#' @return Vector of processed document hashes
#'
#' @keywords internal
get_processed_documents <- function(.dir) {
  arr_ <- arrow::open_dataset(.dir)

  if (nrow(arr_) == 0) {
    return(NA_character_)
  } else {
    arr_ %>%
      dplyr::distinct(HashDocument) %>%
      dplyr::collect() %>%
      dplyr::pull()
  }
}


#' Download Document
#'
#' @description
#' Downloads and processes a single document.
#'
#' @param .url URL of the document
#' @param .user User agent string
#' @param .workers Number of parallel workers
#'
#' @return Tibble with document content and processed text
#'
#' @keywords internal
help_download_document <- function(.url, .user, .workers) {
  Sys.sleep(1 / .workers + 0.01)

  # Get Call -- -- -- -- -- -- -- -- -- -- -- -
  result_ <- make_get_request(.url, .user)

  if (inherits(result_, "try-error") || !httr::status_code(result_) == 200) {
    return(tibble::tibble(HTML = NA_character_, TextRaw = NA_character_, TextMod = NA_character_))
  }

  # Parse Content -- -- -- -- -- -- -- -- -- -
  content_ <- parse_content(result_)
  if (inherits(content_, "try-error") || is.na(content_)) {
    return(tibble::tibble(HTML = NA_character_, TextRaw = NA_character_, TextMod = NA_character_))
  }


  html_ <- try(rvest::read_html(content_, options = "HUGE"), silent = TRUE)
  if (inherits(html_, "try-error")) {
    return(tibble::tibble(HTML = content_, TextRaw = NA_character_, TextMod = NA_character_))
  }

  text_raw_ <- try(rvest::html_text(html_), silent = TRUE)
  if (inherits(text_raw_, "try-error")) {
    return(tibble::tibble(HTML = content_, TextRaw = NA_character_, TextMod = NA_character_))
  }

  text_mod_ <- standardize_string(text_raw_)

  return(tibble::tibble(HTML = content_, TextRaw = text_raw_, TextMod = text_mod_))
}

