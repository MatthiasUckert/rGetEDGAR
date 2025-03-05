# Main Functions ------------------------------------------------------------------------------
#' Download SEC EDGAR Document Links
#'
#' @description
#' Downloads and processes document links from SEC EDGAR filing index pages.
#' This function extracts all document links and metadata from filing index pages
#' for further analysis.
#'
#' @param .dir
#' Character string specifying the directory where the downloaded data will be stored.
#' This directory should contain the necessary sub directory structure.
#' @param .user
#' Character string specifying the user agent to be used in HTTP requests to the SEC EDGAR server.
#' This should typically be an email address to comply with SEC's fair access policy.
#' @param .hash_idx
#' Character vector of HashIndex values to process. If NULL, all available index entries
#' will be processed.
#' @param .workers
#' Integer specifying the number of parallel workers for downloading. Defaults to 1.
#' Higher values increase download speed but also increase server load.
#' @param .verbose
#' Logical indicating whether to print progress messages to the console during processing.
#' Default is TRUE.
#'
#' @details
#' Document metadata extracted includes:
#' \itemize{
#'   \item Document sequence number within the filing
#'   \item Document description
#'   \item Document type (e.g., EX-10.1, 10-K)
#'   \item File size in bytes
#'   \item URL to the actual document
#'   \item Various hash identifiers for tracking and de-duplication
#' }
#'
#' The function implements rate limiting to comply with SEC EDGAR's fair access policy,
#' and uses parallel processing to optimize download speeds within those constraints.
#'
#' @return No return value, called for side effects (downloading and saving data files).
#' @export
edgar_get_document_links <- function(.dir, .user, .hash_idx, .workers = 1L, .verbose = TRUE) {
  lp_ <- get_directories(.dir)
  plog_ <- lp_$DocLinks$FilLog
  seq_ <- get_tbp_hashindex(.dir, .hash_idx, .workers)

  error_logging(plog_, "INFO", paste0("Starting Download (nDocs: ", sum(lengths(seq_) * .workers), ")"))

  con_prcs_ <- DBI::dbConnect(RSQLite::SQLite(), lp_$DocLinks$DirTemp$FilToBePrc)

  future::plan("multisession", workers = .workers)
  for (i in seq_along(seq_)) {
    yq0_ <- names(seq_)[i]
    yq1_ <- gsub(".", "-", yq0_, fixed = TRUE)
    tot_ <- max(seq_[[i]])

    path_htmls_ <- file.path(lp_$DocLinks$DirMain$HTMLs, paste0("DocHTMLs_", yq1_, ".parquet"))
    path_links_ <- file.path(lp_$DocLinks$DirMain$Links, paste0("DocLinks_", yq1_, ".parquet"))

    path_sqlite_ <- file.path(lp_$DocLinks$DirTemp$DirSqlite, paste0(yq1_, ".sqlite"))
    try(fs::file_delete(path_sqlite_), silent = TRUE)
    inidb_get_doclinks(path_sqlite_)
    con_sqlite_ <- DBI::dbConnect(RSQLite::SQLite(), path_sqlite_)

    for (j in seq_[[i]]) {
      t0_ <- Sys.time()

      print_doclinks_loop(yq1_, j, tot_, .workers, .verbose)

      use_ <- dplyr::tbl(con_prcs_, "tobeprocessed") %>%
        dplyr::filter(YearQuarter == yq0_, Seq == j) %>%
        dplyr::collect() %>%
        dplyr::mutate(UrlIndexPage = purrr::set_names(UrlIndexPage, HashIndex)) %>%
        dplyr::distinct()

      tab_htmls_ <- dplyr::bind_rows(furrr::future_map(
        .x = use_$UrlIndexPage,
        .f = ~ get_landing_html(.x, .user, .verbose, plog_)
      ), .id = "HashIndex") %>%
        dplyr::left_join(dplyr::select(use_, CIK, YearQuarter, HashIndex), by = "HashIndex") %>%
        dplyr::select(HashIndex, CIK, YearQuarter, HTML)

      tab_links_ <- dplyr::bind_rows(purrr::map(
        .x = split(tab_htmls_, tab_htmls_$HashIndex),
        .f = ~ get_doclinks(.x, plog_)
      ))

      DBI::dbWriteTable(con_sqlite_, "DocHTMLs", tab_htmls_, append = TRUE)
      DBI::dbWriteTable(con_sqlite_, "DocLinks", tab_links_, append = TRUE)

      elapsed_ <- as.numeric(difftime(Sys.time(), t0_))
      if (!elapsed_ >= 1.05) {
        Sys.sleep(1.05 - elapsed_)
      }
    }

    out_htmls_ <- dplyr::collect(dplyr::tbl(con_sqlite_, "DocHTMLs"))
    out_links_ <- dplyr::collect(dplyr::tbl(con_sqlite_, "DocLinks"))
    if (file.exists(path_htmls_)) {
      arrow::read_parquet(path_htmls_, mmap = FALSE) %>%
        dplyr::bind_rows(out_htmls_) %>%
        dplyr::distinct(HashIndex, .keep_all = TRUE) %>%
        arrow::write_parquet(path_htmls_)
    } else {
      arrow::write_parquet(out_htmls_, path_htmls_)
    }

    if (file.exists(path_links_)) {
      arrow::read_parquet(path_links_, mmap = FALSE) %>%
        dplyr::bind_rows(out_links_) %>%
        dplyr::distinct(DocID, .keep_all = TRUE) %>%
        arrow::write_parquet(path_links_)
    } else {
      arrow::write_parquet(out_links_, path_links_)
    }
    DBI::dbDisconnect(con_sqlite_)
    try(fs::file_delete(path_sqlite_), silent = TRUE)

    msg_out_ <- paste0("Downloaded Year-Quarter: ", yq1_)
    det_out_ <- paste0("Rows: ", nrow(out_htmls_))
    error_logging(plog_, "INFO", msg_out_, det_out_)
  }

  DBI::dbDisconnect(con_prcs_)
  try(fs::file_delete(lp_$DocLinks$DirTemp$FilToBePrc), silent = TRUE)
  future::plan("default")
}

# Helper Functions ----------------------------------------------------------------------------
#' Print Document Links Processing Loop Status
#'
#' @description
#' Formats and prints processing status information for the document links download loop.
#' This function provides feedback on the progress of SEC EDGAR document link processing.
#'
#' @param .yq
#' Character string representing the year-quarter being processed (e.g., "2023-1").
#' @param .loop
#' Integer representing the current batch number being processed.
#' @param .tot
#' Integer representing the total number of batches to process.
#' @param .workers
#' Integer representing the number of parallel workers in use.
#' @param .verbose
#' Logical indicating whether to print progress messages to the console.
#'
#' @details
#' The function formats counters with comma separators for readability
#' and displays a progress indicator showing the current batch number
#' relative to the total batches to be processed.
#'
#' @return No return value, called for side effects (console output).
#'
#' @keywords internal
print_doclinks_loop <- function(.yq, .loop, .tot, .workers, .verbose) {
  nloop_ <- scales::comma(.loop * .workers)
  ntot_ <- scales::comma(.tot * .workers)
  msg_ <- paste0("Processing: ", .yq, " (", nloop_, "/", ntot_, ")")
  print_verbose(msg_, .verbose, "\r")
}

#' Initialize SQLite Database for Document Links
#'
#' @description
#' Creates and initializes an SQLite database file with the necessary tables and indexes
#' for storing SEC EDGAR document link data. This function sets up the database schema
#' for temporary processing of document links.
#'
#' @param .path
#' Character string specifying the file path where the SQLite database will be created.
#'
#' @details
#' The function creates two main tables:
#' \itemize{
#'   \item DocHTMLs: Stores the raw HTML content of filing index pages
#'   \item DocLinks: Stores the parsed document links and metadata
#' }
#'
#' Each table is indexed on the YearQuarter field to optimize query performance.
#' If the specified database file already exists, the function returns silently
#' without modifying the existing database.
#'
#' The DocHTMLs table includes the following columns:
#' \itemize{
#'   \item HashIndex: Unique identifier for the filing
#'   \item CIK: Central Index Key of the filing company
#'   \item YearQuarter: Filing year and quarter as numeric (e.g., 2023.1)
#'   \item HTML: Raw HTML content of the filing index page
#' }
#'
#' The DocLinks table includes the following columns:
#' \itemize{
#'   \item HashIndex: Unique identifier for the filing
#'   \item CIK: Central Index Key of the filing company
#'   \item DocID: Unique identifier for the document
#'   \item HashDocument: Hash of the document URL and sequence
#'   \item YearQuarter: Filing year and quarter as numeric
#'   \item Seq: Document sequence number
#'   \item Description: Document description
#'   \item Document: Document filename
#'   \item Type: Document type (e.g., EX-10.1)
#'   \item Size: Document size in bytes
#'   \item UrlDocument: URL to the actual document
#' }
#'
#' @return No return value, called for side effects (database creation).
#'
#' @keywords internal
inidb_get_doclinks <- function(.path) {
  if (file.exists(.path)) {
    return(invisible(NULL))
  }
  con_ <- DBI::dbConnect(RSQLite::SQLite(), .path)

  table_ <- "DocHTMLs"
  query0_ <- "CREATE TABLE DocHTMLs (HashIndex TEXT, CIK TEXT, YearQuarter REAL, HTML TEXT)"
  DBI::dbExecute(con_, query0_)
  query1_ <- paste0("CREATE INDEX IF NOT EXISTS YearQuarter", table_, " ON ", table_, "(YearQuarter)")
  DBI::dbExecute(con_, query1_)

  table_ <- "DocLinks"
  con_ <- DBI::dbConnect(RSQLite::SQLite(), .path)
  query0_ <- "CREATE TABLE DocLinks (
  HashIndex TEXT, CIK TEXT, DocID TEXT, HashDocument TEXT, YearQuarter REAL,
  Seq INTEGER, Description TEXT, Document TEXT, Type TEXT, Size INTEGER, UrlDocument TEXT
  )"
  DBI::dbExecute(con_, query0_)
  query1_ <- paste0("CREATE INDEX IF NOT EXISTS YearQuarter", table_, " ON ", table_, "(YearQuarter)")
  DBI::dbExecute(con_, query1_)


  DBI::dbDisconnect(con_)
}

#' Prepare "To Be Processed" HashIndex Queue
#'
#' @description
#' Prepares a queuing system for SEC EDGAR document links to be processed in batches.
#' This function creates a temporary SQLite database to track which filing index pages
#' need to be processed and assigns them to processing batches.
#'
#' @param .dir
#' Character string specifying the directory where the downloaded data will be stored.
#' This directory should contain the necessary sub directory structure.
#' @param .hash_idx
#' Character vector of HashIndex values to process. If NULL, all available index entries
#' will be processed.
#' @param .workers
#' Integer specifying the number of parallel workers for downloading. Defaults to 1.
#' Higher values increase download speed but also increase server load.
#'
#' @details
#' The created SQLite database contains a "tobeprocessed" table with the following columns:
#' \itemize{
#'   \item HashIndex: Unique identifier for the filing
#'   \item CIK: Central Index Key of the filing company
#'   \item YearQuarter: Filing year and quarter as numeric
#'   \item Seq: Batch sequence number
#'   \item UrlIndexPage: URL to the filing index page
#' }
#'
#' @return
#' A list where each element represents a year-quarter period and contains
#' the sequence numbers of batches to be processed for that period.
#'
#' @keywords internal
get_tbp_hashindex <- function(.dir, .hash_idx, .workers) {
  lp_ <- get_directories(.dir)

  path_ <- lp_$DocLinks$DirTemp$FilToBePrc
  table_ <- "tobeprocessed"
  try(fs::file_delete(path_), silent = TRUE)

  con_ <- DBI::dbConnect(RSQLite::SQLite(), path_)
  query0_ <- "CREATE TABLE tobeprocessed (HashIndex TEXT, CIK TEXT, YearQuarter REAL, Seq INTEGER, UrlIndexPage TEXT)"
  DBI::dbExecute(con_, query0_)
  query1_ <- paste0("CREATE INDEX IF NOT EXISTS Seq", table_, " ON ", table_, "(Seq)")
  DBI::dbExecute(con_, query1_)
  query2_ <- paste0("CREATE INDEX IF NOT EXISTS YearQuarter", table_, " ON ", table_, "(YearQuarter)")
  DBI::dbExecute(con_, query2_)


  arr_prc_ <- arrow::open_dataset(lp_$DocLinks$DirMain$Links)
  if (nrow(arr_prc_) == 0) {
    prc_ <- NA_character_
  } else {
    prc_ <- dplyr::collect(dplyr::distinct(arr_prc_, HashIndex))[["HashIndex"]]
  }

  arr_idx_ <- arrow::open_dataset(lp_$MasterIndex$DirParquet)
  if (!is.null(.hash_idx)) {
    arr_idx_ <- dplyr::filter(arr_idx_, HashIndex %in% .hash_idx)
  }
  arr_idx_ <- dplyr::collect(dplyr::filter(arr_idx_, !HashIndex %in% prc_))

  tab_ <- arr_idx_ %>%
    dplyr::group_by(YearQuarter) %>%
    dplyr::mutate(Seq = ceiling(dplyr::row_number() / .workers)) %>%
    dplyr::ungroup() %>%
    dplyr::select(HashIndex, CIK, YearQuarter, Seq, UrlIndexPage)

  DBI::dbWriteTable(con_, table_, tab_, overwrite = TRUE)
  DBI::dbDisconnect(con_)

  out_ <- dplyr::distinct(tab_, YearQuarter, Seq)
  split(out_$Seq, out_$YearQuarter)
}

#' Get HTML Content of SEC EDGAR Filing Index Page
#'
#' @description
#' Downloads the HTML content of an SEC EDGAR filing index page. This function
#' handles the HTTP request and error checking for retrieving filing index pages.
#'
#' @param .url
#' Character string specifying the URL of the filing index page to download.
#' @param .user
#' Character string specifying the user agent to be used in HTTP requests.
#' This should typically be an email address to comply with SEC's fair access policy.
#' @param .verbose
#' Logical indicating whether to print progress messages to the console.
#' Default is FALSE.
#'
#' @details
#' The function makes an HTTP GET request to the specified URL using the provided
#' user agent. It includes error handling to manage network issues or invalid responses.
#'
#' If the request is successful, the function returns the HTML content as a tibble
#' with a single row and column. If an error occurs, the function returns an empty
#' tibble with an NA value, allowing the calling function to continue processing
#' other URLs without interruption.
#'
#' @return
#' A tibble with a single column 'HTML' containing the HTML content of the requested page.
#' If an error occurs, returns an empty tibble with NA value.
#'
#' @keywords internal
get_landing_html <- function(.url, .user, .verbose, .path_log) {
  result_ <- make_get_request(.url, .user)
  if (!check_error_request(result_, .verbose, .path_log)) {
    return(tibble::tibble(HTML = NA_character_, .rows = 0L))
  }
  content_ <- parse_content(result_, .type = "text")
  if (!check_error_content(content_, .verbose, .path_log)) {
    return(tibble::tibble(HTML = NA_character_, .rows = 0L))
  }
  tibble::tibble(HTML = content_)
}

#' Extract Document Links from SEC EDGAR Filing Index Page
#'
#' @description
#' Parses the HTML content of an SEC EDGAR filing index page to extract document links
#' and metadata. This function handles the extraction of document information from
#' the structured tables on the index page.
#'
#' @param .tab_row
#' A single-row tibble containing HTML content and metadata for a filing index page,
#' including HashIndex, CIK, YearQuarter, and HTML columns.
#' @param .path_log
#' Optional character string specifying the path to the log file. If NULL,
#' uses the default log path from the directory structure.
#'
#' @details
#' The function performs the following steps:
#' \itemize{
#'   \item Parses the HTML content using the `rvest` package
#'   \item Locates table elements containing document listings
#'   \item Extracts document metadata from each table row
#'   \item Processes URLs to ensure they are absolute
#'   \item Generates unique identifiers for each document
#'   \item Standardizes data types and handles missing values
#' }
#'
#' The function includes error handling to manage cases where the HTML structure
#' doesn't match expectations or when tables cannot be parsed correctly. In these
#' cases, errors are logged and an empty tibble is returned.
#'
#' @return
#' A tibble with extracted document links and metadata, including the following columns:
#' \itemize{
#'   \item HashIndex: Unique identifier for the filing
#'   \item CIK: Central Index Key of the filing company
#'   \item DocID: Unique identifier for the document
#'   \item HashDocument: Hash of the document URL and sequence
#'   \item YearQuarter: Filing year and quarter as numeric
#'   \item Seq: Document sequence number
#'   \item Description: Document description
#'   \item Document: Document file name
#'   \item Type: Document type (e.g., EX-10.1)
#'   \item Size: Document size in bytes
#'   \item UrlDocument: URL to the actual document
#' }
#'
#' If parsing fails, returns an empty tibble.
#'
#' @keywords internal
get_doclinks <- function(.tab_row, .path_log) {
  html_ <- rvest::read_html(.tab_row$HTML)

  nodes_ <- rvest::html_elements(rvest::html_elements(html_, css = "#formDiv"), "table")
  if (length(nodes_) == 0) {
    nodes_ <- rvest::html_elements(rvest::html_elements(html_, css = ".formDiv"), "table")
  }

  if (length(nodes_) == 0) {
    error_logging(.path_log, "ERROR", "No Nodes with Links Found", NULL)
    return(tibble::tibble())
  }

  tab_ <- try(dplyr::bind_rows(purrr::map(
    .x = nodes_,
    .f = ~ rvest::html_table(.x) %>%
      dplyr::mutate(UrlDocument = rvest::html_attr(rvest::html_elements(.x, "a"), "href"))
  )), silent = TRUE)

  if (inherits(tab_, "try-error")) {
    error_logging(.path_log, "ERROR", "Transformation of Links not Successful", NULL)
    return(tibble::tibble())
  }

  tab_ %>%
    dplyr::mutate(
      CIK = .tab_row$CIK,
      HashIndex = .tab_row$HashIndex,
      YearQuarter = .tab_row$YearQuarter,
      HashDocument = paste0(UrlDocument, "-", Seq),
      HashDocument = purrr::map_chr(HashDocument, digest::digest),
      DocID = paste0(CIK, "-", HashDocument),
      UrlDocument = rvest::url_absolute(UrlDocument, "https://www.sec.gov/Archives/"),
      dplyr::across(c(Seq, Size), ~ dplyr::if_else(!is.na(.), as.integer(.), -1L)),
      dplyr::across(c(Description, Document, Type, UrlDocument), as.character),
      dplyr::across(dplyr::where(is.character), ~ dplyr::if_else(trimws(.) == "", NA_character_, trimws(.))),
    ) %>%
    dplyr::select(
      HashIndex, CIK, DocID, HashDocument, YearQuarter, dplyr::everything()
    )
}


# DeBug ---------------------------------------------------------------------------------------
if (FALSE) {
  # devtools::load_all(".")
  library(rGetEDGAR)
  forms <- c(
    "10-K", "10-K/A", "10-Q", "10-Q/A", "8-K", "8-K/A", "20-F", "20-F/A",
    "S-1", "S-1/A", "S-4", "S-4/A", "F-1", "F-1/A", "F-4", "F-4/A",
    "CT ORDER"
  )
  user <- unname(ifelse(
    Sys.info()["sysname"] == "Darwin",
    "PetroParkerLosSpiderHombreABC12@Outlook.com",
    "TonyStarkIronManWeaponXYZ847263@Outlook.com"
  ))

  dir_debug <- fs::dir_create("../_package_debug/rGetEDGAR")
  lp_ <- get_directories(dir_debug)

  for (f in forms) {
    .hash_idx <- arrow::open_dataset(lp_$MasterIndex$DirParquet) %>%
      dplyr::filter(FormType %in% f) %>%
      dplyr::distinct(HashIndex) %>%
      dplyr::collect() %>%
      dplyr::pull()

    edgar_get_document_links(
      .dir = dir_debug,
      .user = user,
      .hash_idx = .hash_idx,
      .workers = 10L,
      .verbose = TRUE
    )
  }
}
