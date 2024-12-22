#' Download SEC EDGAR Master Index Files
#'
#' @description
#' Downloads and processes the master index files from the SEC EDGAR database.
#' These files contain metadata about all SEC filings for a given quarter.
#'
#' @param .dir Character string specifying the directory where the downloaded data will be stored
#' @param .user Character string specifying the user agent to be used in HTTP requests
#' @param .from Numeric value specifying the start year.quarter (e.g., 2020.1 for Q1 2020).
#'             If NULL, defaults to 1993.1
#' @param .to Numeric value specifying the end year.quarter.
#'           If NULL, defaults to current quarter
#' @param .verbose Logical indicating whether to print progress messages
#'
#' @details
#' The function performs the following steps:
#' 1. Creates necessary directory structure if it doesn't exist
#' 2. Initializes SQLite database for master index if it doesn't exist
#' 3. Downloads master.idx files for each quarter in the specified range
#' 4. Processes and stores the data in both SQLite and Parquet formats
#'
#' The master index includes:
#' - CIK (Central Index Key)
#' - Company Name
#' - Form Type
#' - Date Filed
#' - URL to full text
#'
#' @return No return value, called for side effects
#'
#' @examples
#' \dontrun{
#' edgar_get_master_index(
#'   .dir = "edgar_data",
#'   .user = "your@email.com",
#'   .from = 2020.1,
#'   .to = 2021.4
#' )
#' }
edgar_get_master_index <- function(.dir, .user, .from = NULL, .to = NULL, .verbose = TRUE) {
  lp_ <- get_directories(.dir)

  initial_master_index_database(lp_$MasterIndex$Sqlite)

  con_master_ <- DBI::dbConnect(RSQLite::SQLite(), lp_$MasterIndex$Sqlite)
  urls_ <- get_year_qtr_table(.from, .to) %>%
    dplyr::mutate(
      UrlMasterIndex = "https://www.sec.gov/Archives/edgar/full-index",
      UrlMasterIndex = file.path(UrlMasterIndex, Year, paste0("QTR", Quarter), "master.idx"),
      Print = paste0("Downloading Year-Quarter: ", Year, "-", Quarter)
    ) %>%
    dplyr::filter(!YearQuarter %in% get_processed_year_qtr(con_master_))

  if (nrow(urls_) == 0) {
    print_verbose("Master Index Complete", TRUE, "\r")
    return(invisible(NULL))
  }

  for (i in seq_len(nrow(urls_))) {
    use_ <- urls_[i, ]
    print_verbose(use_$Print, .verbose, "\r")

    # Get Call -- -- -- -- -- -- -- -- -- -- -- -
    result_ <- make_get_request(use_$UrlMasterIndex, .user)
    if (inherits(result_, "try-error") || !httr::status_code(result_) == 200) {
      get_warning_master_index(use_$Year, use_$Quarter)
      next
    }

    # Parse Content -- -- -- -- -- -- -- -- -- -
    content_ <- parse_content(result_)
    if (inherits(content_, "try-error") || is.na(content_)) {
      get_warning_master_index(use_$Year, use_$Quarter)
      next
    }

    # Convert to Dataframe -- -- -- -- -- -- -- -
    out_ <- content_to_dataframe(content_, use_$Year, use_$Quarter)
    if (nrow(out_) == 0 || !check_master_index_cols(out_)) {
      get_warning_master_index(use_$Year, use_$Quarter)
      next
    }

    # Write Output -- -- -- -- -- -- -- -- -- -
    DBI::dbWriteTable(con_master_, "master_index", out_, append = TRUE)
  }

  # Convert to Parquet -- -- -- -- -- -- -- ---
  dplyr::tbl(con_master_, "master_index") %>%
    dplyr::collect() %>%
    arrow::write_parquet(lp_$MasterIndex$Parquet)

  # Diconnect Database -- -- -- -- -- -- -- ---
  DBI::dbDisconnect(con_master_)
  on.exit(DBI::dbDisconnect(con_master_))

  print_verbose("Master Index Complete", TRUE, "\r")
}

#' Download SEC EDGAR Document Links
#'
#' @description
#' Downloads and processes document links from SEC EDGAR filing index pages.
#' This function extracts all document links and metadata from filing index pages.
#'
#' @param .dir Character string specifying the directory where the downloaded data will be stored
#' @param .user Character string specifying the user agent to be used in HTTP requests
#' @param .from Numeric value specifying the start year.quarter (e.g., 2020.1 for Q1 2020)
#' @param .to Numeric value specifying the end year.quarter
#' @param .ciks Character vector of CIK numbers to filter for specific companies
#' @param .verbose Logical indicating whether to print progress messages
#'
#' @details
#' The function:
#' 1. Reads from the master index database
#' 2. For each filing, visits the index page
#' 3. Extracts all document links and metadata
#' 4. Stores results in both SQLite and Parquet formats
#'
#' Document metadata includes:
#' - Document sequence number
#' - Description
#' - Document type
#' - Size
#' - URL to actual document
#'
#' @return No return value, called for side effects
#'
#' @examples
#' \dontrun{
#' edgar_get_document_links(
#'   .dir = "edgar_data",
#'   .user = "your@email.com",
#'   .from = 2020.1,
#'   .to = 2021.4,
#'   .ciks = c("0000320193", "0001018724")  # Apple and Amazon
#' )
#' }
edgar_get_document_links <- function(.dir, .user, .from = NULL, .to = NULL, .ciks = NULL, .verbose = TRUE) {
  lp_ <- get_directories(.dir)
  initial_document_links_database(lp_$DocumentLinks$Sqlite)

  con_master_ <- DBI::dbConnect(RSQLite::SQLite(), lp_$MasterIndex$Sqlite)
  con_links_ <- DBI::dbConnect(RSQLite::SQLite(), lp_$DocumentLinks$Sqlite)

  params_ <- get_edgar_params(.from, .to, .ciks)

  use_ <- dplyr::tbl(con_master_, "master_index") %>%
    dplyr::filter(dplyr::between(YearQuarter, params_$from, params_$to)) %>%
    dplyr::collect() %>%
    dplyr::filter(!HashIndex %in% get_processed_hash_index(con_links_)) %>%
    filter_edgar_data(params_) %>%
    dplyr::select(HashIndex, Year, Quarter, YearQuarter, CIK, UrlIndexPage) %>%
    dplyr::arrange(CIK, YearQuarter) %>%
    dplyr::mutate(
      AllRow = format_number(dplyr::row_number(), "c"),
      AllTot = format_number(dplyr::n(), "c"),
      AllPer = format_number(dplyr::row_number() / dplyr::n(), "p"),
    ) %>%
    dplyr::mutate(Print = paste0(
      "Query: ", AllRow, " of ", AllTot, " (", AllPer, ")"
    )) %>%
    tibble::as_tibble() %>%
    dplyr::select(HashIndex, Year, Quarter, YearQuarter, CIK, UrlIndexPage, Print)

  if (nrow(use_) == 0) {
    print_verbose("Document Index Complete", TRUE, "\r")
    return(invisible(NULL))
  }


  t0_ <- Sys.time()
  for (i in seq_len(nrow(use_))) {
    msg_ <- paste0(use_$Print[i], format_time_status(t0_, i, nrow(use_)))
    print_verbose(msg_, .verbose, "\r")
    tab_ <- try(help_get_document_link(use_$UrlIndexPage[i], .user), silent = TRUE)

    if (inherits(tab_, "try-error")) {
      get_warning_document_links(tab_$CIK[i])
      next
    }

    bnd_ <- use_[i, c("HashIndex", "Year", "Quarter", "YearQuarter", "CIK")]
    out_ <- dplyr::bind_cols(tab_, bnd_) %>%
      dplyr::select(
        HashIndex, HashDocument, CIK,
        Year, Quarter, YearQuarter,
        Seq, Description, Document, Type, Size, UrlDocument
      )

    # Write Output -- -- -- -- -- -- -- -- -- -
    DBI::dbWriteTable(con_links_, "document_links", out_, append = TRUE)
  }

  # Convert to Parquet -- -- -- -- -- -- -- ---
  dplyr::tbl(con_links_, "document_links") %>%
    dplyr::collect() %>%
    arrow::write_parquet(lp_$DocumentLinks$Parquet)

  # Diconnect Database -- -- -- -- -- -- -- ---
  DBI::dbDisconnect(con_master_)
  DBI::dbDisconnect(con_links_)

  on.exit(DBI::dbDisconnect(con_links_))
  on.exit(DBI::dbDisconnect(con_master_))
}

#' Download SEC EDGAR Documents
#'
#' @description
#' Downloads actual documents from SEC EDGAR filings and processes their content.
#' Supports parallel downloading for improved performance.
#'
#' @param .dir Character string specifying the directory where the downloaded data will be stored
#' @param .user Character string specifying the user agent to be used in HTTP requests
#' @param .from Numeric value specifying the start year.quarter (e.g., 2020.1 for Q1 2020)
#' @param .to Numeric value specifying the end year.quarter
#' @param .ciks Character vector of CIK numbers to filter for specific companies
#' @param .types Character vector of document types to filter (e.g., "10-K", "10-Q")
#' @param .workers Integer specifying the number of parallel workers for downloading
#' @param .verbose Logical indicating whether to print progress messages
#'
#' @details
#' The function:
#' 1. Reads from the document links database
#' 2. Downloads documents in parallel using the specified number of workers
#' 3. Processes HTML content and extracts text
#' 4. Stores both raw HTML and processed text
#' 5. Saves results in Parquet format by CIK
#'
#' For each document, stores:
#' - Original HTML
#' - Raw extracted text
#' - Modified/cleaned text
#'
#' @return No return value, called for side effects
#'
#' @examples
#' \dontrun{
#' edgar_download_document(
#'   .dir = "edgar_data",
#'   .user = "your@email.com",
#'   .from = 2020.1,
#'   .to = 2021.4,
#'   .ciks = c("0000320193"),  # Apple Inc
#'   .types = c("10-K", "10-Q"),
#'   .workers = 4
#' )
#' }
edgar_download_document <- function(.dir, .user, .from = NULL, .to = NULL, .ciks = NULL, .types = NULL, .workers = 1L, .verbose = TRUE) {
  lp_ <- get_directories(.dir)

  con_links_ <- DBI::dbConnect(RSQLite::SQLite(), lp_$DocumentLinks$Sqlite)

  params_ <- get_edgar_params(.from, .to, .ciks, .types)

  use_ <- dplyr::tbl(con_links_, "document_links") %>%
    dplyr::filter(dplyr::between(YearQuarter, params_$from, params_$to)) %>%
    dplyr::collect() %>%
    dplyr::mutate(UrlDocument = purrr::set_names(UrlDocument, HashDocument)) %>%
    dplyr::mutate(ext = tools::file_ext(UrlDocument)) %>%
    dplyr::filter(!ext == "") %>%
    dplyr::select(-ext) %>%
    dplyr::filter(!HashDocument %in% get_processed_documents(lp_$DocumentData)) %>%
    filter_edgar_data(params_) %>%
    dplyr::select(HashDocument, Year, Quarter, YearQuarter, CIK, Type, UrlDocument) %>%
    tibble::as_tibble() %>%
    tidyr::nest(.by = "CIK") %>%
    dplyr::mutate(
      AllRow = format_number(dplyr::row_number(), "c"),
      AllTot = format_number(dplyr::n(), "c"),
      AllPer = format_number(dplyr::row_number() / dplyr::n(), "p"),
    ) %>%
    dplyr::mutate(Print = paste0(
      "CIK: ", CIK, " - ", AllRow, " of ", AllTot, " (", AllPer, ")"
    )) %>%
    dplyr::select(CIK, data, Print) %>%
    dplyr::mutate(
      PathOut = file.path(lp_$DocumentData, paste0(CIK, ".parquet"))
    )
  on.exit(DBI::dbDisconnect(con_links_))

  if (nrow(use_) == 0) {
    print_verbose("All Documents Downloaded", TRUE, "\r")
    return(invisible(NULL))
  }

  t0_ <- Sys.time()
  future::plan("multisession", workers = .workers)
  for (i in seq_len(nrow(use_))) {
    msg_ <- paste0(use_$Print[i], format_time_status(t0_, i, nrow(use_)))
    print_verbose(msg_, .verbose, "\r")

    docs_ <- use_$data[[i]]

    if (nrow(docs_) == 0) {
      next
    }


    tab_ <- furrr::future_map(
      .x = purrr::set_names(docs_$UrlDocument, docs_$HashDocument),
      .f = ~ help_download_document(.x, .user, .workers),
      .options = furrr::furrr_options(seed = TRUE),
      .progress = FALSE
    ) %>% dplyr::bind_rows(.id = "HashDocument")

    out_ <- try(tab_ %>%
      dplyr::left_join(
        y = docs_,
        by = "HashDocument",
        relationship = "one-to-one"
      ) %>%
      dplyr::mutate(CIK = use_$CIK[i]) %>%
      dplyr::select(
        HashDocument, Year, Quarter, YearQuarter, CIK, Type, HTML, TextRaw, TextMod
      ), silent = TRUE)

    if (inherits(out_, "try-error")) {
      get_warning_document_download(use_$CIK[i])
      next
    }

    if (file.exists(use_$PathOut[i])) {
      arrow::read_parquet(use_$PathOut[i]) %>%
        dplyr::bind_rows(out_) %>%
        arrow::write_parquet(use_$PathOut[i])
    } else {
      arrow::write_parquet(out_, use_$PathOut[i])
    }
  }
  future::plan("default")
  on.exit(future::plan("default"))

  print_verbose("All Documents Downloaded", TRUE, "\r")

}


# DeBug ---------------------------------------------------------------------------------------
if (FALSE) {
  devtools::load_all(".")
  .dir <- fs::dir_create("../_package_debug/rGetEDGAR")
  .verbose <- TRUE
  .years <- 2000:2001
  .ciks <- "0001000015"
  .user <- "Matt@domain.com"
  .url <- "https://www.sec.gov/Archives/edgar/data/1397047/000121390019003916/0001213900-19-003916-index.htm"
  .from <- 2000.1
  .to <- 2001.4
}


# PipeLine ------------------------------------------------------------------------------------
if (FALSE) {
  devtools::load_all(".")

  edgar_get_master_index(
    .dir = fs::dir_create("../_package_debug/rGetEDGAR"),
    .user = "MatthiasUckert@Outlook.com",
    .from = NULL,
    .to = NULL,
    .verbose = TRUE
  )

  edgar_get_document_links(
    .dir = fs::dir_create("../_package_debug/rGetEDGAR"),
    .user = "MatthiasUckert@Outlook.com",
    .from = 1995.1,
    .to = 1995.4,
    .ciks = NULL,
    .verbose = TRUE
  )

  edgar_download_document(
    .dir = fs::dir_create("../_package_debug/rGetEDGAR"),
    .user = "MatthiasUckert@Outlook.com",
    .from = 1995.1,
    .to = 1995.4,
    .ciks = NULL,
    .types = NULL,
    .workers = 5L,
    .verbose = TRUE
  )
}
