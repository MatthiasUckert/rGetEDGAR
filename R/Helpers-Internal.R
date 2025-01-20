

if (FALSE) {
  .msg <- "Test Message"
  .verbose <- TRUE
  .line <- "\n"

  .from <- NULL
  .to <- NULL

  .t0 <- Sys.time()
  .i <- 125
  .n <- 100000

  .from <- NULL
  .to <- NULL
  .ciks <- NULL
  .types <- NULL
  lp_ <- get_directories(.dir)
  .path <- lp_$Logs$DocumentLinks
  .loop <- "YearQuarter: 2023.1, Loop: 5"
  .function <- "function"
  .part <- "Retrieve Data"
  .error <- "Test Message"
}

#' Log EDGAR Data Retrieval Operations
#'
#' This function creates or appends to a log file tracking EDGAR data retrieval operations.
#' It uses fast writing operations optimized for high-frequency logging.
#'
#' @param .path Character. Path to the log file
#' @param .loop Character. Current loop iteration identifier (e.g., "YearQuarter: 2023.1, Loop: 5")
#' @param .function Character. Name of the function being logged
#' @param .part Character. Part of the process being logged (e.g., "Retrieve Data")
#' @param .hash_index Character. Hash value for the index
#' @param .hash_document Character. Hash value for the document
#' @param .error Character. Error message or status message to log
#'
#' @return Invisibly returns TRUE if successful
#'
#' @keywords internal
save_logging <- function(.path, .loop, .function, .part, .hash_index, .hash_document, .error) {
  # Create log entry with timestamp
  log_entry <- tibble::tibble(
    TimeStamp = format(Sys.time(), "%Y-%m-%d %H:%M:%OS6"),
    Loop = .loop,
    FunctionName = .function,
    FunctionPart = .part,
    HashIndex = .hash_index,
    HashDocument = .hash_document,
    ErrorMsg = .error
  )

  if (!file.exists(.path)) {
    data.table::fwrite(
      x = log_entry,
      file = .path,
      sep = "|",
      col.names = TRUE,
      encoding = "UTF-8"
    )
  } else {
    data.table::fwrite(
      x = log_entry,
      file = .path,
      append = TRUE,
      sep = "|",
      col.names = FALSE,
      encoding = "UTF-8"
    )
  }

  invisible(TRUE)
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
#' @param .formtypes Character vector of document types
#'
#' @return A list containing processed parameters:
#' \itemize{
#'   \item from: Start period (defaults to 1993.1)
#'   \item to: End period (defaults to current quarter)
#'   \item ciks: Processed CIK numbers
#'   \item formtypes: Processed document types
#' }
#'
#' @keywords internal
get_edgar_params <- function(.from = NULL, .to = NULL, .ciks = NULL, .formtypes = NULL) {
  # Process time periods
  year_current_ <- lubridate::year(Sys.Date())
  qtr_current_ <- lubridate::quarter(Sys.Date()) - 1L
  current_period_ <- as.numeric(paste0(year_current_, ".", qtr_current_))

  # Create output list
  list(
    from = ifelse(is.null(.from), 1993.1, .from),
    to = ifelse(is.null(.to), current_period_, .to),
    ciks = if (is.null(.ciks)) NA_character_ else .ciks,
    formtypes = if (is.null(.formtypes)) NA_character_ else .formtypes
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
  if (!is.na(.params$ciks)) {
    tab_ <- dplyr::filter(tab_, CIK %in% .params$ciks)
  }

  # Filter by document type if specified
  if (!all(is.na(.params$formtypes))) {
    tab_ <- dplyr::filter(tab_, FormType %in% .params$formtypes)
  }

  if (!is.na(.params$from)) {
    tab_ <- dplyr::filter(tab_, YearQuarter >= .params$from)
  }

  if (!is.na(.params$to)) {
    tab_ <- dplyr::filter(tab_, YearQuarter <= .params$to)
  }


  return(tab_)
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
parse_content <- function(.result, .type = c("text", "raw")) {
  if (.type == "text") {
    try(httr::content(.result, as = "text", encoding = "UTF-8"), silent = TRUE)
  } else {
    try(httr::content(.result, "raw"), silent = TRUE)
  }

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
#' Initialize SEC EDGAR Database Schema
#'
#' @description
#' Creates SQLite database schema and empty Parquet files for SEC EDGAR data storage
#' if they don't exist. Handles both document links and landing page schemas.
#'
#' @param .dir Path to the base directory for SEC data storage
#' @param .source Character string specifying the database type to initialize:
#'               either "DocumentLinks" or "LandingPage"
#' @keywords internal
initialize_edgar_database <- function(.dir, .source = c("DocumentLinks", "LandingPage")) {
  # Input validation
  .source <- match.arg(.source)

  # Get directory paths
  lp_ <- get_directories(.dir)

  # Define schemas and structures for each source type
  schemas <- list(
    DocumentLinks = list(
      sqlite_path = lp_$DocumentLinks$Sqlite,
      parquet_path = lp_$DocumentLinks$Parquet,
      table_name = "document_links",
      sql_schema = "
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
          UrlDocument TEXT,
          Error INTEGER
        )",
      empty_data = tibble::tibble(
        HashIndex = NA_character_,
        HashDocument = NA_character_,
        CIK = NA_character_,
        Year = NA_integer_,
        Quarter = NA_integer_,
        YearQuarter = NA_real_,
        Seq = NA_character_,
        Description = NA_character_,
        Document = NA_character_,
        Type = NA_character_,
        Size = NA_character_,
        UrlDocument = NA_character_,
        Error = NA
      )
    ),
    LandingPage = list(
      sqlite_path = lp_$LandingPage$Sqlite,
      parquet_path = lp_$LandingPage$Parquet,
      table_name = "landing_page",
      sql_schema = "
        CREATE TABLE landing_page (
          HashIndex TEXT,
          CIK TEXT,
          Year INTEGER,
          Quarter INTEGER,
          YearQuarter REAL,
          HTML TEXT,
          Error INTEGER
        )",
      empty_data = tibble::tibble(
        HashIndex = NA_character_,
        CIK = NA_character_,
        Year = NA_integer_,
        Quarter = NA_integer_,
        YearQuarter = NA_real_,
        HTML = NA_character_,
        Error = NA
      )
    )
  )[[.source]]

  # Initialize SQLite database if it doesn't exist
  if (!file.exists(schemas$sqlite_path)) {
    con_ <- DBI::dbConnect(RSQLite::SQLite(), schemas$sqlite_path)
    on.exit(suppressWarnings(DBI::dbDisconnect(con_)), add = TRUE)
    DBI::dbExecute(con_, schemas$sql_schema)
  }

  # Initialize Parquet file if it doesn't exist
  if (!file.exists(schemas$parquet_path)) {
    schemas$empty_data %>%
      arrow::write_parquet(schemas$parquet_path)
  }
  Sys.sleep(1)
}

#' Get Master Index Records To Be Processed
#'
#' @description
#' Identifies and prepares master index records that haven't been processed yet. The function:
#' 1. Retrieves already processed records from the document links database
#' 2. Filters the master index dataset based on provided parameters
#' 3. Excludes already processed records
#' 4. Prepares the data for parallel processing by splitting it into chunks
#'
#' @param .dir Base directory for the SEC data storage
#' @param .params List of filtering parameters from get_edgar_params()
#' @param .workers Number of parallel workers to use for processing
#'
#' @return Writes a parquet file to the temporary directory containing records to be processed,
#'         with an additional 'Split' column for parallel processing
#'
#' @keywords internal
get_to_be_processed_master_index <- function(.dir, .params, .workers) {
  lp_ <- get_directories(.dir)

  if (dir.exists(lp_$Temporary$DocumentLinks)) {
    fs::file_delete(lp_$Temporary$DocumentLinks)
  }

  prc_ <- arrow::open_dataset(lp_$DocumentLinks$Parquet) %>%
    dplyr::distinct(HashIndex) %>%
    dplyr::collect() %>%
    dplyr::pull(HashIndex)

  out_ <- arrow::open_dataset(lp_$MasterIndex$Parquet) %>%
    filter_edgar_data(.params) %>%
    dplyr::filter(!HashIndex %in% prc_) %>%
    dplyr::select(HashIndex, Year, Quarter, YearQuarter, CIK, UrlIndexPage) %>%
    dplyr::arrange(YearQuarter, CIK) %>%
    dplyr::collect() %>%
    dplyr::group_by(YearQuarter) %>%
    dplyr::mutate(
      Split = ceiling(dplyr::row_number() / .workers),
      .before = HashIndex
    )


  if (nrow(out_) > 0) {
    arrow::write_dataset(out_, lp_$Temporary$DocumentLinks)
  } else {
    return(invisible(NULL))
  }
}

#' Create Error Table for SEC EDGAR Data Processing
#'
#' @description
#' Creates standardized error response tables when SEC EDGAR data processing fails.
#' This ensures consistent error handling and data structure even when the retrieval
#' or processing of data encounters problems.
#'
#' @param .source Character string specifying the type of error table to create:
#'               either "DocumentLinks" or "LandingPage"
#'
#' @return A tibble with empty/NA values and Error=TRUE.
#' @keywords internal
create_error_table <- function(.source = c("DocumentLinks", "LandingPage")) {
  # Input validation
  .source <- match.arg(.source, c("DocumentLinks", "LandingPage"))

  # Define error table structures
  error_structures <- list(
    DocumentLinks = tibble::tibble(
      HashDocument = NA_character_,
      Seq = NA_integer_,
      Description = NA_character_,
      Document = NA_character_,
      Type = NA_character_,
      Size = NA_integer_,
      UrlDocument = NA_character_,
      Error = TRUE
    ),
    LandingPage = tibble::tibble(
      HTML = NA_character_,
      Error = TRUE
    )
  )

  # Return appropriate error table
  error_structures[[.source]]
}


#' Write SEC EDGAR Link Data to Different Storage Formats
#'
#' @description
#' Writes or transfers SEC EDGAR data (document links or landing pages) between
#' different storage formats (SQLite and Parquet). This function provides a unified
#' interface for handling data storage operations in the SEC EDGAR database.
#'
#' @param .dir Base directory for the SEC data storage
#' @param .tab Optional data frame or tibble containing new data to be written.
#'            Required when writing to SQLite, ignored when converting to Parquet.
#' @param .source Character string specifying the data type: either "DocumentLinks"
#'               or "LandingPage"
#' @param .target Character string specifying the target format: either "sqlite"
#'               or "parquet"
#' @keywords internal
write_link_data <- function(.dir, .tab = NULL, .source = c("DocumentLinks", "LandingPage"),
                            .target = c("sqlite", "parquet")) {
  # Get directory paths
  lp_ <- get_directories(.dir)

  # Set parameters based on source type
  params <- list(
    DocumentLinks = list(
      sqlite_path = lp_$DocumentLinks$Sqlite,
      parquet_path = lp_$DocumentLinks$Parquet,
      table_name = "document_links",
      distinct_cols = c("HashIndex", "HashDocument")
    ),
    LandingPage = list(
      sqlite_path = lp_$LandingPage$Sqlite,
      parquet_path = lp_$LandingPage$Parquet,
      table_name = "landing_page",
      distinct_cols = "HashIndex"
    )
  )[[.source]]

  # Connect to SQLite database
  con_ <- DBI::dbConnect(RSQLite::SQLite(), params$sqlite_path)
  on.exit(suppressWarnings(DBI::dbDisconnect(con_)), add = TRUE)

  if (.target == "sqlite") {
    DBI::dbWriteTable(con_, params$table_name, .tab, append = TRUE)
  } else {
    dplyr::tbl(con_, params$table_name) %>%
      dplyr::collect() %>%
      dplyr::distinct(!!!dplyr::syms(params$distinct_cols), .keep_all = TRUE) %>%
      arrow::write_parquet(params$parquet_path)
  }
  invisible(gc())
}

#' Backup SEC EDGAR Database Files
#'
#' @description
#' Creates periodic backups of the SEC EDGAR databases (document links or landing pages).
#' The function:
#' 1. Runs every 3 hours (at hours 3, 6, 9, ..., 24)
#' 2. Creates a compressed zip backup of the SQLite database
#' 3. Updates the corresponding parquet file with current data
#'
#' @param .dir Base directory for the SEC data storage
#' @param .source Character string specifying the data type to backup: either
#'               "DocumentLinks" or "LandingPage"
#' @keywords internal
backup_link_data <- function(.dir, .source = c("DocumentLinks", "LandingPage")) {

  # Get directory paths
  lp_ <- get_directories(.dir)

  # Set parameters based on source type
  params <- list(
    DocumentLinks = list(
      backup_dir = lp_$DocumentLinks$BackUps,
      sqlite_path = lp_$DocumentLinks$Sqlite,
      file_prefix = "DocumentLinks"
    ),
    LandingPage = list(
      backup_dir = lp_$LandingPage$BackUps,
      sqlite_path = lp_$LandingPage$Sqlite,
      file_prefix = "LandingPage"
    )
  )[[.source]]

  # Check if it's backup time (every 3 hours)
  if (lubridate::hour(Sys.time()) %in% seq(3, 24, 3)) {
    # Create timestamp and backup filepath
    time_ <- format(Sys.time(), "%Y-%m-%d-%H")
    file_ <- file.path(params$backup_dir,
                       paste0(time_, "_", params$file_prefix, ".zip"))

    # Create backup if it doesn't exist
    if (!file.exists(file_)) {
      # Create zip backup
      suppressWarnings(
        zip::zip(file_, params$sqlite_path, compression_level = 9)
      )

      # Update parquet file
      write_link_data(.dir, .source = .source, .target = "parquet")
    }
  }
}



#' Pull Distinct Column Values from a Table
#'
#' @description
#' Extracts distinct values from a specified column in a database table,
#' arranges them, and returns them as a vector.
#'
#' @param .tab A database table or tibble
#' @param .col An unquoted column name to extract values from
#'
#' @return A vector containing distinct, sorted values from the specified column
pull_column <- function(.tab, .col) {
  .tab %>%
    dplyr::distinct({{ .col }}) %>%
    dplyr::arrange({{ .col }}) %>%
    dplyr::collect() %>%
    dplyr::pull({{ .col }})
}

#' Finalize SEC EDGAR Tables with Join Operations
#'
#' @description
#' Performs final processing and joining operations on SEC EDGAR document links
#' or landing page tables.
#'
#' @param .tab The main table to be processed (document links or landing page data)
#' @param .join The table to join with (typically contains HashIndex and related metadata)
#' @param .type Character string specifying the type of table to finalize:
#'        either "DocumentLinks" or "LandingPage"
#'
#' @return A processed and joined table with standardized column selection
finalize_tables <- function(.tab, .join, .type = c("DocumentLinks", "LandingPage")) {
  if (.type == "DocumentLinks") {
    out_ <- try(.tab %>%
      dplyr::mutate(Error = as.integer(Error)) %>%
      dplyr::left_join(
        y = .join,
        by = dplyr::join_by(HashIndex),
        relationship = "many-to-one"
      ) %>%
      dplyr::select(
        HashIndex, HashDocument, CIK,
        Year, Quarter, YearQuarter,
        Seq, Description, Document, Type, Size, UrlDocument,
        Error
      ), silent = TRUE)
  } else {
    out_ <- try(.tab %>%
      dplyr::mutate(Error = as.integer(Error)) %>%
      dplyr::left_join(
        y = .join,
        by = dplyr::join_by(HashIndex),
        relationship = "many-to-one"
      ) %>%
      dplyr::select(
        HashIndex, CIK,
        Year, Quarter, YearQuarter,
        HTML, Error
      ), silent = TRUE)
  }
}

#' Get Document Link
#'
#' @description
#' Downloads and processes document links from an index page.
#'
#' @param .index_row A single ro of the MasterIndex Table
#' @param .user User agent string
#'
#' @return Tibble with document links data
#'
#' @keywords internal
help_get_document_link <- function(.index_row, .user) {
  error_list <- list(
    DocumentLinks = create_error_table("DocumentLinks"),
    LangingPage = create_error_table("LandingPage")
  )

  url_ <- .index_row$UrlIndexPage
  result_ <- make_get_request(url_, .user)
  if (inherits(result_, "try-error")) {
    save_logging(
      .path = .index_row$PathLog,
      .loop = paste0(.index_row$YearQuarter, ": ", stringi::stri_pad_left(.index_row$Split, 6, "0")),
      .function = "help_get_document_link",
      .part = "1. make_get_request: try-catch",
      .hash_index = .index_row$HashIndex,
      .hash_document = NA_character_,
      .error = paste(as.character(result_), collapse = " ")
    )
    return(error_list)
  }

  status_ <- httr::status_code(result_)

  if (status_ != 200) {
    status_msgs_ <- list(
      "429" = list(message = "Rate limit exceeded (429)", action = function() Sys.sleep(60)),
      "403" = list(message = "Access forbidden (403)", action = function() NULL),
      "404" = list(message = "Page not found (404).", action = function() NULL),
      "503" = list(message = "Service unavailable (503)", action = function() NULL),
      "000" = list(message = "Unexpected status code (000)", action = function() NULL)
    )

    # Get status handler or use default for unknown status codes
    parse_status_ <- ifelse(status_ %in% c(403, 404, 429, 503), as.character(status_), "000")
    status_handler_ <- status_msgs_[[parse_status_]]
    status_msg_ <- gsub("000", status_, status_handler_$message)

    save_logging(
      .path = .index_row$PathLog,
      .loop = paste0(.index_row$YearQuarter, ": ", stringi::stri_pad_left(.index_row$Split, 6, "0")),
      .function = "help_get_document_link",
      .part = "1. make_get_request: status",
      .hash_index = .index_row$HashIndex,
      .hash_document = NA_character_,
      .error = status_msg_
    )

    # Execute any associated action (like sleeping for rate limits)
    status_handler_$action()

    return(error_list)
  }

  content_ <- parse_content(result_, .type = "text")
  if (inherits(content_, "try-error")) {
    save_logging(
      .path = .index_row$PathLog,
      .loop = paste0(.index_row$YearQuarter, ": ", stringi::stri_pad_left(.index_row$Split, 6, "0")),
      .function = "help_get_document_link",
      .part = "2. parse_content",
      .hash_index = .index_row$HashIndex,
      .hash_document = NA_character_,
      .error = paste(as.character(content_), collapse = " ")
    )
    return(error_list)
  }
  if (is.na(content_)) {
    save_logging(
      .path = .index_row$PathLog,
      .loop = paste0(.index_row$YearQuarter, ": ", stringi::stri_pad_left(.index_row$Split, 6, "0")),
      .function = "help_get_document_link",
      .part = "2. parse_content",
      .hash_index = .index_row$HashIndex,
      .hash_document = NA_character_,
      .error = "Content is NA"
    )
    return(error_list)
  }

  nodes_ <- rvest::read_html(content_) %>%
    rvest::html_elements(css = "#formDiv") %>%
    rvest::html_elements("table")

  if (length(nodes_) == 0) {
    save_logging(
      .path = .index_row$PathLog,
      .loop = paste0(.index_row$YearQuarter, ": ", stringi::stri_pad_left(.index_row$Split, 6, "0")),
      .function = "help_get_document_link",
      .part = "3. parse_nodes",
      .hash_index = .index_row$HashIndex,
      .hash_document = NA_character_,
      .error = "No table nodes found in #formDiv"
    )
    return(error_list)
  }

  out_links_ <- try(purrr::map(
    .x = nodes_,
    .f = ~ rvest::html_table(.x) %>%
      dplyr::mutate(UrlDocument = rvest::html_attr(rvest::html_elements(.x, "a"), "href"))
  ) %>%
    dplyr::bind_rows() %>%
    dplyr::mutate(
      HashDocument = paste0(UrlDocument, "-", Seq),
      HashDocument = purrr::map_chr(HashDocument, digest::digest),
      UrlDocument = rvest::url_absolute(UrlDocument, "https://www.sec.gov/Archives/"),
      dplyr::across(c(Seq, Size), ~ dplyr::if_else(!is.na(.), as.integer(.), -1L)),
      dplyr::across(c(Description, Document, Type, UrlDocument), as.character),
      dplyr::across(dplyr::where(is.character), ~ dplyr::if_else(trimws(.) == "", NA_character_, trimws(.))),
      Error = FALSE
    ) %>% dplyr::select(
      HashDocument, Seq, Description, Document, Type, Size, UrlDocument, Error
    ), silent = TRUE)

  out_html_ <- tibble::tibble(HTML = content_, Error = FALSE)


  if (inherits(out_links_, "try-error")) {
    save_logging(
      .path = .index_row$PathLog,
      .loop = paste0(.index_row$YearQuarter, ": ", stringi::stri_pad_left(.index_row$Split, 6, "0")),
      .function = "help_get_document_link",
      .part = "4. parse_links",
      .hash_index = .index_row$HashIndex,
      .hash_document = NA_character_,
      .error = paste(as.character(out_links_), collapse = " ")
    )
    return(error_list)
  }

  return(list(
    DocumentLinks = out_links_,
    LangingPage = out_html_
  ))
}



# Download Documents --------------------------------------------------------------------------

#' Download and Save SEC EDGAR Documents
#'
#' @description
#' Downloads and saves SEC EDGAR documents in appropriate formats based on their file extension.
#' Handles text-based documents (htm, xml, xsd, txt) by converting them to parquet,
#' and binary files (pdf, gif, jpg) by saving them directly.
#'
#' @param .doc_row A row with download information
#' @param .user Character string of the user agent
#'
#' @return A Document
#' @keywords internal
help_download_document <- function(.doc_row, .user) {

  # Create output directory if it doesn't exist
  fs::dir_create(.doc_row$DirOut)

  # Make HTTP request
  result_ <- make_get_request(.doc_row$UrlDocument, .user)
  if (inherits(result_, "try-error") || !httr::status_code(result_) == 200) {
    return(invisible(NULL))
  }

  type_ <- ifelse(.doc_row$OutExt == ".parquet", "text", "raw")
  content_ <- parse_content(result_, type_)

  if (inherits(content_, "try-error")) {
    return(invisible(NULL))
  }

  if (type_ == "text") {
    tibble::tibble(
      HashDocument = .doc_row$HashDocument,
      Text = content_
    ) %>% arrow::write_parquet(.doc_row$PathOut)
  } else {
    writeBin(content_, .doc_row$PathOut)
  }
}


#' Prepare Temporary Document Download Data
#'
#' @description
#' Creates a temporary dataset of documents to be downloaded from SEC EDGAR,
#' organizing them by CIK and document type. The function processes document URLs
#' and prepares file paths for storage.
#'
#' @param .dir Character string specifying the base directory for data storage
#' @param .from Numeric value specifying the start year.quarter (e.g., 2020.1)
#' @param .to Numeric value specifying the end year.quarter
#' @param .ciks Character vector of Central Index Key (CIK) numbers to filter
#' @param .formtypes Character vector of form types to filter (e.g., "10-K", "10-Q")
#' @param .doctypes Character vector of document types to filter
#'
#' @return No return value, called for side effects (writes to temporary Parquet file)
#'
#' @keywords internal
get_temprorary_document_download <- function(.dir, .from, .to, .ciks, .formtypes, .doctypes) {

  lp_ <- get_directories(.dir)

  edgar_read_document_links(.dir, .from, .to, .ciks, .formtypes, .doctypes, FALSE) %>%
    dplyr::select(HashDocument, CIK, DocTypeMod, UrlDocument) %>%
    dplyr::collect() %>%
    dplyr::mutate(
      FileExt = tools::file_ext(UrlDocument),
      OutExt = dplyr::case_when(
        FileExt %in% c("html", "htm", "xml", "xsd", "txt") ~ ".parquet",
        FileExt %in% c("gif", "jpg", "pdf") ~ paste0(".", FileExt)
      ),
      DirOut = file.path(lp_$DocumentData, CIK, gsub("\\s", "", DocTypeMod)),
      PathOut = file.path(DirOut, paste0(HashDocument, OutExt))
    ) %>%
    # dplyr::filter(is.na(OutExt)) %>%
    dplyr::filter(!FileExt == "") %>%
    dplyr::filter(!file.exists(PathOut)) %>%
    dplyr::mutate(
      DocNum = dplyr::row_number(),
      Split = ceiling(DocNum / 10)
    ) %>%
    arrow::write_parquet(lp_$Temporary$DocumentData)
}




#' #' Get Processed Documents
#' #'
#' #' @description
#' #' Retrieves list of documents that have already been processed.
#' #'
#' #' @param .dir Directory containing processed documents
#' #'
#' #' @return Vector of processed document hashes
#' #'
#' #' @keywords internal
#' get_processed_documents <- function(.dir) {
#'   arr_ <- arrow::open_dataset(.dir)
#'
#'   if (nrow(arr_) == 0) {
#'     return(NA_character_)
#'   } else {
#'     arr_ %>%
#'       dplyr::distinct(HashDocument) %>%
#'       dplyr::collect() %>%
#'       dplyr::pull()
#'   }
#' }
#'
#'
#' #' Download Document
#' #'
#' #' @description
#' #' Downloads and processes a single document.
#' #'
#' #' @param .url URL of the document
#' #' @param .user User agent string
#' #'
#' #' @return Tibble with document content and processed text
#' #'
#' #' @keywords internal
#' help_download_document <- function(.url, .user) {
#'   # Get Call -- -- -- -- -- -- -- -- -- -- -- -
#'   result_ <- make_get_request(.url, .user)
#'
#'   if (inherits(result_, "try-error") || !httr::status_code(result_) == 200) {
#'     return(tibble::tibble(HTML = NA_character_, TextRaw = NA_character_, TextMod = NA_character_, Error = TRUE))
#'   }
#'
#'   # Parse Content -- -- -- -- -- -- -- -- -- -
#'   content_ <- parse_content(result_)
#'   if (inherits(content_, "try-error") || is.na(content_)) {
#'     return(tibble::tibble(HTML = NA_character_, TextRaw = NA_character_, TextMod = NA_character_, Error = TRUE))
#'   }
#'
#'
#'   html_ <- try(rvest::read_html(content_, options = "HUGE"), silent = TRUE)
#'   if (inherits(html_, "try-error")) {
#'     return(tibble::tibble(HTML = content_, TextRaw = NA_character_, TextMod = NA_character_, Error = FALSE))
#'   }
#'
#'   text_raw_ <- try(rvest::html_text(html_), silent = TRUE)
#'   if (inherits(text_raw_, "try-error")) {
#'     return(tibble::tibble(HTML = content_, TextRaw = NA_character_, TextMod = NA_character_, Error = FALSE))
#'   }
#'
#'   text_mod_ <- standardize_string(text_raw_)
#'
#'   return(tibble::tibble(HTML = content_, TextRaw = text_raw_, TextMod = text_mod_, Error = FALSE))
#' }
#'
#' #' Get Document Links To Be Downloaded
#' #'
#' #' @description
#' #' Identifies and prepares document links that haven't been downloaded yet. The function:
#' #' 1. Checks already downloaded documents in the main document data directory
#' #' 2. Filters the document links dataset based on provided parameters
#' #' 3. Excludes already downloaded documents
#' #' 4. Prepares the data for parallel processing
#' #'
#' #' @param .dir Base directory for the SEC data storage
#' #' @param .params List of filtering parameters from get_edgar_params()
#' #' @param .workers Number of parallel workers to use for processing
#' #'
#' #' @return Writes a parquet file to the temporary directory containing links to be processed,
#' #'         with an additional 'Split' column for parallel processing. Only includes
#' #'         documents with valid file extensions.
#' #'
#' #' @keywords internal
#' get_to_be_processed_download_links <- function(.dir, .params, .workers) {
#'   lp_ <- get_directories(.dir)
#'   arr_ <- arrow::open_dataset(lp_$DocumentData$Main)
#'   if (nrow(arr_) == 0) {
#'     prc_ <- NA_character_
#'   } else {
#'     prc_ <- arr_ %>%
#'       dplyr::distinct(HashDocument) %>%
#'       dplyr::collect() %>%
#'       dplyr::pull(HashDocument)
#'   }
#'
#'
#'   arrow::open_dataset(lp_$DocumentLinks$Parquet) %>%
#'     filter_edgar_data(.params) %>%
#'     dplyr::filter(!HashDocument %in% prc_) %>%
#'     dplyr::select(HashDocument, Year, Quarter, YearQuarter, CIK, Type, UrlDocument) %>%
#'     dplyr::arrange(CIK, YearQuarter) %>%
#'     dplyr::collect() %>%
#'     dplyr::mutate(ext = tools::file_ext(UrlDocument)) %>%
#'     dplyr::filter(!ext == "") %>%
#'     dplyr::select(-ext) %>%
#'     dplyr::mutate(
#'       Split = ceiling(dplyr::row_number() / .workers),
#'       .before = HashDocument
#'     ) %>%
#'     arrow::write_parquet(lp_$DocumentData$Temporary)
#' }
#'
#' #' Write SEC Document Data to Parquet Files
#' #'
#' #' @description
#' #' Writes or appends SEC document data to company-specific Parquet files. The function
#' #' organizes documents by CIK (company identifier) and handles both new file creation
#' #' and updates to existing files.
#' #'
#' #' @param .tab A tibble/data frame containing document data with the following columns:
#' #'   \itemize{
#' #'     \item HashDocument: Unique identifier for the document
#' #'     \item Year, Quarter, YearQuarter: Time period identifiers
#' #'     \item CIK: Company identifier
#' #'     \item Type: Document type
#' #'     \item HTML: Raw HTML content
#' #'     \item TextRaw: Extracted raw text
#' #'     \item TextMod: Processed/standardized text
#' #'     \item PathOut: Full path for the output Parquet file
#' #'   }
#' #'
#' #' @details
#' #' The function:
#' #' 1. Splits the input data by PathOut (company-specific file paths)
#' #' 2. For each company:
#' #'    - If a Parquet file exists: Reads existing data and appends new data
#' #'    - If no file exists: Creates a new Parquet file
#' #' 3. Removes the PathOut column before writing
#' #' 4. Maintains one Parquet file per company (CIK)
#' #'
#' #' @note
#' #' - The function assumes the parent directories in PathOut already exist
#' #' - Data is appended without checking for duplicates
#' #' - The function processes files in memory, so consider memory usage with large datasets
#' #'
#' #' @keywords internal
#' write_document_data <- function(.tab) {
#'   purrr::iwalk(
#'     .x = split(dplyr::select(.tab, -PathOut), .tab$PathOut),
#'     .f = ~ {
#'       if (file.exists(.y)) {
#'         arrow::write_parquet(dplyr::bind_rows(arrow::read_parquet(.y), .x), .y)
#'       } else {
#'         arrow::write_parquet(.x, .y)
#'       }
#'     }
#'   )
#' }
#'

#'
#' #' Create Error Table for Document Link Processing
#' #'
#' #' @description
#' #' Creates a standardized error response table when document link processing fails.
#' #' This ensures consistent error handling and data structure even when the retrieval
#' #' or processing of document links encounters problems.
#' #'
#' #' @return A tibble with empty/NA values for document link fields and Error=TRUE:
#' #'   \itemize{
#' #'     \item HashDocument: Character, document hash identifier
#' #'     \item Seq: Integer, sequence number
#' #'     \item Description: Character, document description
#' #'     \item Document: Character, document name
#' #'     \item Type: Character, document type
#' #'     \item Size: Integer, document size
#' #'     \item UrlDocument: Character, document URL
#' #'     \item Error: Logical, always TRUE for error tables
#' #'   }
#' #'
#' #' @keywords internal
#' error_table_document_link <- function() {
#'   tibble::tibble(
#'     HashDocument = NA_character_,
#'     Seq = NA_integer_,
#'     Description = NA_character_,
#'     Document = NA_character_,
#'     Type = NA_character_,
#'     Size = NA_integer_,
#'     UrlDocument = NA_character_,
#'     Error = TRUE
#'   )
#' }
#'
#' #' Create Error Table for Document Link Processing
#' #'
#' #' @description
#' #' Creates a standardized error response table when document link processing fails.
#' #' This ensures consistent error handling and data structure even when the retrieval
#' #' or processing of document links encounters problems.
#' #'
#' #' @return A tibble with empty/NA values for document link fields and Error=TRUE:
#' #'   \itemize{
#' #'     \item HTML: Character, document URL
#' #'     \item Error: Logical, always TRUE for error tables
#' #'   }
#' #'
#' #' @keywords internal
#' error_table_landing_page <- function() {
#'   tibble::tibble(
#'     HTML = NA_character_,
#'     Error = TRUE
#'   )
#' }
#'

#'
#' #' Initialize Document Links Database
#' #'
#' #' @description
#' #' Creates SQLite database schema for document links if it doesn't exist.
#' #'
#' #' @param .dir Path to SQLite database file
#' #'
#' #' @keywords internal
#' initial_document_links_database <- function(.dir) {
#'   lp_ <- get_directories(.dir)
#'
#'   if (!file.exists(lp_$DocumentLinks$Sqlite)) {
#'     con_ <- DBI::dbConnect(RSQLite::SQLite(), lp_$DocumentLinks$Sqlite)
#'     DBI::dbExecute(con_, "
#'     CREATE TABLE document_links (
#'       HashIndex TEXT,
#'       HashDocument TEXT,
#'       CIK TEXT,
#'       Year INTEGER,
#'       Quarter INTEGER,
#'       YearQuarter REAL,
#'       Seq TEXT,
#'       Description TEXT,
#'       Document TEXT,
#'       Type TEXT,
#'       Size TEXT,
#'       UrlDocument TEXT,
#'       Error INTEGER
#'     )")
#'     suppressWarnings(DBI::dbDisconnect(con_))
#'   }
#'
#'   if (!file.exists(lp_$DocumentLinks$Parquet)) {
#'     tibble::tibble(
#'       HashIndex = NA_character_, HashDocument = NA_character_, CIK = NA_character_,
#'       Year = NA_integer_, Quarter = NA_integer_, YearQuarter = NA_real_,
#'       Seq = NA_character_, Description = NA_character_, Document = NA_character_,
#'       Type = NA_character_, Size = NA_character_, UrlDocument = NA_character_,
#'       Error = NA
#'     ) %>% arrow::write_parquet(lp_$DocumentLinks$Parquet)
#'   }
#' }
#'
#' #' Initialize Document Links Database
#' #'
#' #' @description
#' #' Creates SQLite database schema for document links if it doesn't exist.
#' #'
#' #' @param .dir Path to SQLite database file
#' #'
#' #' @keywords internal
#' initial_landing_page_database <- function(.dir) {
#'   lp_ <- get_directories(.dir)
#'
#'   if (!file.exists(lp_$LandingPage$Sqlite)) {
#'     con_ <- DBI::dbConnect(RSQLite::SQLite(), lp_$LandingPage$Sqlite)
#'     DBI::dbExecute(con_, "
#'     CREATE TABLE landing_page (
#'       HashIndex TEXT,
#'       CIK TEXT,
#'       Year INTEGER,
#'       Quarter INTEGER,
#'       YearQuarter REAL,
#'       HTML TEXT,
#'       Error INTEGER
#'     )")
#'     suppressWarnings(DBI::dbDisconnect(con_))
#'   }
#'
#'   if (!file.exists(lp_$LandingPage$Parquet)) {
#'     tibble::tibble(
#'       HashIndex = NA_character_, CIK = NA_character_,
#'       Year = NA_integer_, Quarter = NA_integer_, YearQuarter = NA_real_,
#'       HTML = NA_character_,
#'       Error = NA
#'     ) %>% arrow::write_parquet(lp_$LandingPage$Parquet)
#'   }
#' }








#'
#'
#'
#' #' Write Document Links to Parquet File
#' #'
#' #' @description
#' #' Transfers the entire document links table from SQLite database to a Parquet file format.
#' #' This function provides a way to maintain a Parquet copy of the document links data,
#' #' which can be more efficient for certain types of data analysis and filtering operations.
#' #'
#' #' @param .dir Base directory for the SEC data storage
#' #' @keywords internal
#' write_document_links_to_parquet <- function(.dir) {
#'   lp_ <- get_directories(.dir)
#'   con_ <- DBI::dbConnect(RSQLite::SQLite(), lp_$DocumentLinks$Sqlite)
#'   dplyr::tbl(con_, "document_links") %>%
#'     dplyr::collect() %>%
#'     dplyr::distinct(HashIndex, HashDocument, .keep_all = TRUE) %>%
#'     arrow::write_parquet(lp_$DocumentLinks$Parquet)
#'
#'   suppressWarnings({
#'     DBI::dbDisconnect(con_)
#'   })
#' }
#'
#' #' Write Document Links to SQLite Database
#' #'
#' #' @description
#' #' Appends new document links data to the existing SQLite database. This function
#' #' serves as a way to incrementally add new document links to the persistent storage.
#' #'
#' #' @param .dir Base directory for the SEC data storage
#' #' @param .tab Data frame or tibble containing new document links to be appended.
#' #'            Must match the structure of the 'document_links' table.
#' #' @keywords internal
#' write_document_links_to_sqlite <- function(.dir, .tab) {
#'   lp_ <- get_directories(.dir)
#'   con_ <- DBI::dbConnect(RSQLite::SQLite(), lp_$DocumentLinks$Sqlite)
#'   DBI::dbWriteTable(con_, "document_links", .tab, append = TRUE)
#'
#'   suppressWarnings({
#'     DBI::dbDisconnect(con_)
#'   })
#' }
#'
#'
#' #' Write Document Links to Parquet File
#' #'
#' #' @description
#' #' Transfers the entire document links table from SQLite database to a Parquet file format.
#' #' This function provides a way to maintain a Parquet copy of the document links data,
#' #' which can be more efficient for certain types of data analysis and filtering operations.
#' #'
#' #' @param .dir Base directory for the SEC data storage
#' #' @keywords internal
#' write_landing_page_to_parquet <- function(.dir) {
#'   lp_ <- get_directories(.dir)
#'   con_ <- DBI::dbConnect(RSQLite::SQLite(), lp_$LandingPage$Sqlite)
#'   dplyr::tbl(con_, "landing_page") %>%
#'     dplyr::collect() %>%
#'     dplyr::distinct(HashIndex, .keep_all = TRUE) %>%
#'     arrow::write_parquet(lp_$LandingPage$Parquet)
#'
#'   suppressWarnings({
#'     DBI::dbDisconnect(con_)
#'   })
#' }
#'
#' #' Write Document Links to SQLite Database
#' #'
#' #' @description
#' #' Appends new document links data to the existing SQLite database. This function
#' #' serves as a way to incrementally add new document links to the persistent storage.
#' #'
#' #' @param .dir Base directory for the SEC data storage
#' #' @param .tab Data frame or tibble containing new document links to be appended.
#' #'            Must match the structure of the 'document_links' table.
#' #' @keywords internal
#' write_landing_page_to_sqlite <- function(.dir, .tab) {
#'   lp_ <- get_directories(.dir)
#'   con_ <- DBI::dbConnect(RSQLite::SQLite(), lp_$LandingPage$Sqlite)
#'   DBI::dbWriteTable(con_, "landing_page", .tab, append = TRUE)
#'
#'   suppressWarnings({
#'     DBI::dbDisconnect(con_)
#'   })
#' }
#'
#'
#'
#' #' Backup Document Links Database
#' #'
#' #' @description
#' #' Creates periodic backups of the document links database. The function:
#' #' 1. Runs every 3 hours (at hours 3, 6, 9, ..., 24)
#' #' 2. Creates a compressed zip backup of the SQLite database
#' #' 3. Updates the parquet file with current data
#' #'
#' #' @param .dir Base directory for the SEC data storage
#' #' @keywords internal
#' backup_document_links <- function(.dir) {
#'   lp_ <- get_directories(.dir)
#'   if (lubridate::hour(Sys.time()) %in% seq(3, 24, 3)) {
#'     time_ <- format(Sys.time(), "%Y-%m-%d-%H")
#'     file_ <- file.path(lp_$DocumentLinks$BackUps, paste0(time_, "_DocumentLinks.zip"))
#'     if (!file.exists(file_)) {
#'       suppressWarnings(zip::zip(file_, lp_$DocumentLinks$Sqlite, compression_level = 9))
#'       write_document_links_to_parquet(.dir)
#'     }
#'   }
#' }
#'
#' #' Backup Document Links Database
#' #'
#' #' @description
#' #' Creates periodic backups of the document links database. The function:
#' #' 1. Runs every 3 hours (at hours 3, 6, 9, ..., 24)
#' #' 2. Creates a compressed zip backup of the SQLite database
#' #' 3. Updates the parquet file with current data
#' #'
#' #' @param .dir Base directory for the SEC data storage
#' #' @keywords internal
#' backup_landing_page <- function(.dir) {
#'   lp_ <- get_directories(.dir)
#'   if (lubridate::hour(Sys.time()) %in% seq(3, 24, 3)) {
#'     time_ <- format(Sys.time(), "%Y-%m-%d-%H")
#'     file_ <- file.path(lp_$LandingPage$BackUps, paste0(time_, "_DocumentLinks.zip"))
#'     if (!file.exists(file_)) {
#'       suppressWarnings(zip::zip(file_, lp_$LandingPage$Sqlite, compression_level = 9))
#'       write_landing_page_to_parquet(.dir)
#'     }
#'   }
#' }
