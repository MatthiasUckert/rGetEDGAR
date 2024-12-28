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
#'
#' @export
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
      print_verbose("Some error occured, no worries we are continuing :) ...", .verbose, "\r")
      next
    }

    # Parse Content -- -- -- -- -- -- -- -- -- -
    content_ <- parse_content(result_)
    if (inherits(content_, "try-error") || is.na(content_)) {
      print_verbose("Some error occured, no worries we are continuing :) ...", .verbose, "\r")
      next
    }

    # Convert to Dataframe -- -- -- -- -- -- -- -
    out_ <- content_to_dataframe(content_, use_$Year, use_$Quarter)
    if (nrow(out_) == 0 || !check_master_index_cols(out_)) {
      print_verbose("Some error occured, no worries we are continuing :) ...", .verbose, "\r")
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
#' @param .workers Integer specifying the number of parallel workers for downloading
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
#'
#' @export
edgar_get_document_links <- function(.dir, .user, .from = NULL, .to = NULL, .ciks = NULL, .workers = 1L, .verbose = TRUE) {
  lp_ <- get_directories(.dir)

  initial_document_links_database(.dir)
  Sys.sleep(1)

  params_ <- get_edgar_params(.from, .to, .ciks)
  get_to_be_processed_master_index(.dir, params_, .workers)

  tmp_ <- lp_$DocumentLinks$Temporary
  arr_ <- arrow::open_dataset(tmp_)

  if (nrow(arr_) == 0) {
    print_verbose("DocumentLinks Index Complete", TRUE, "\r")
    return(invisible(NULL))
  } else {
    yqtr_ <- sort(dplyr::collect(dplyr::distinct(arr_, YearQuarter))[["YearQuarter"]])
  }

  last_time_ <- Sys.time()
  init_time_ <- Sys.time()


  future::plan("multisession", workers = .workers)
  i = j = 1
  for (i in seq_len(length(yqtr_))) {
    idx_ <- arrow::open_dataset(tmp_) %>%
      dplyr::filter(YearQuarter == yqtr_[i]) %>%
      dplyr::distinct(Split) %>%
      dplyr::arrange(Split) %>%
      dplyr::collect() %>%
      dplyr::pull(Split)

    for (j in idx_) {
      wait_info_ <- loop_wait_time(last_time_, .workers)
      last_time_ <- wait_info_$new_time
      msg_loop_ <- paste0(yqtr_[i], ": ", format_loop(j, length(idx_), .workers))
      msg_time_ <- format_time(init_time_, j, length(idx_))
      print_verbose(paste(msg_loop_, msg_time_, wait_info_$msg, sep = " | "), .verbose, "\r")

      use_ <- dplyr::collect(dplyr::filter(arr_, Split == i))
      use_ <- dplyr::distinct(use_, HashIndex, .keep_all = TRUE)

      tab_ <- furrr::future_map(
        .x = purrr::set_names(use_$UrlIndexPage, use_$HashIndex),
        .f = ~ help_get_document_link(.url = .x, .user),
        .options = furrr::furrr_options(seed = TRUE)
      ) %>%
        dplyr::bind_rows(.id = "HashIndex") %>%
        dplyr::filter(!Error)

      if (nrow(tab_) == 0) {
        print_verbose("Some error occured, no worries we are continuing :) ...", .verbose, "\r")
        next
      }

      out_ <- try(tab_ %>%
        dplyr::left_join(use_, by = dplyr::join_by(HashIndex), relationship = "many-to-one") %>%
        dplyr::select(
          HashIndex, HashDocument, CIK,
          Year, Quarter, YearQuarter,
          Seq, Description, Document, Type, Size, UrlDocument
        ), silent = TRUE)

      if (inherits(out_, "try-error")) {
        print_verbose("Some error occured, no worries we are continuing :) ...", .verbose, "\r")
        next
      }

      # Write Output -- -- -- -- -- -- -- -- -- -
      suppressWarnings(write_document_links_to_sqlite(.dir, out_))
      backup_document_links(.dir)
    }
  }



  suppressWarnings(write_document_links_to_parquet(.dir))
  future::plan("default")
  suppressWarnings(invisible(on.exit(future::plan("default"))))
  fs::file_delete(lp_$DocumentLinks$Temporary)
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
#'
#' @export
edgar_download_document <- function(.dir, .user, .from = NULL, .to = NULL, .ciks = NULL, .types = NULL, .workers = 1L, .verbose = TRUE) {
  lp_ <- get_directories(.dir)

  params_ <- get_edgar_params(.from, .to, .ciks, .types)
  get_to_be_processed_download_links(.dir, params_, .workers)

  arr_ <- arrow::open_dataset(lp_$DocumentData$Temporary)

  if (nrow(arr_) == 0) {
    print_verbose("Document Index Complete", TRUE, "\r")
    return(invisible(NULL))
  } else {
    idx_ <- sort(dplyr::collect(dplyr::distinct(arr_, Split))[["Split"]])
  }

  last_time_ <- Sys.time()
  init_time_ <- Sys.time()


  future::plan("multisession", workers = .workers)
  for (i in idx_) {

    wait_info_ <- loop_wait_time(last_time_, .workers)
    last_time_ <- wait_info_$new_time
    msg_loop_ <- format_loop(i, length(idx_), .workers)
    msg_time_ <- format_time(init_time_, i, length(idx_))
    print_verbose(paste(msg_loop_, msg_time_, wait_info_$msg, sep = " | "), .verbose, "\r")

    use_ <- dplyr::collect(dplyr::filter(arr_, Split == i))
    use_ <- dplyr::mutate(use_, PathOut = file.path(lp_$DocumentData$Main, paste0(CIK, ".parquet")))
    use_ <- dplyr::distinct(use_, HashDocument, .keep_all = TRUE)

    tab_ <- furrr::future_map(
      .x = purrr::set_names(use_$UrlDocument, use_$HashDocument),
      .f = ~ help_download_document(.x, .user),
      .options = furrr::furrr_options(seed = TRUE),
      .progress = FALSE
    ) %>% dplyr::bind_rows(.id = "HashDocument") %>%
      dplyr::filter(!Error)

    if (nrow(tab_) == 0) {
      print_verbose("Some error occured, no worries we are continuing :) ...", .verbose, "\r")
      next
    }

    out_ <- try(
      expr = tab_ %>%
        dplyr::left_join(use_, by = "HashDocument", relationship = "one-to-one") %>%
        dplyr::select(HashDocument, Year, Quarter, YearQuarter, CIK, Type, HTML, TextRaw, TextMod, PathOut),
      silent = TRUE
    )

    if (inherits(out_, "try-error")) {
      print_verbose("Some error occured, no worries we are continuing :) ...", .verbose, "\r")
      next
    }

    write_document_data(out_)

  }
  suppressWarnings(future::plan("default"))
  suppressWarnings(on.exit(future::plan("default")))
  fs::file_delete(lp_$DocumentData$Temporary)

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
  library(rGetEDGAR)
  source("R/Helpers-External.R")
  source("R/Helpers-Internal.R")
  source("R/Utils.R")

  edgar_get_master_index(
    .dir = fs::dir_create("../_package_debug/rGetEDGAR"),
    .user = "PeterParker@Outlook.com",
    .from = NULL,
    .to = NULL,
    .verbose = TRUE
  )

  edgar_get_document_links(
    .dir = fs::dir_create("../_package_debug/rGetEDGAR"),
    .user = "PeterParker@Outlook.com",
    .from = 1995.1,
    .to = 1995.2,
    .ciks = NULL,
    .workers = 10L,
    .verbose = TRUE
  )

  edgar_download_document(
    .dir = fs::dir_create("../_package_debug/rGetEDGAR"),
    .user = "PeterParker@Outlook.com",
    .from = 1995.1,
    .to = 1995.4,
    .ciks = NULL,
    .types = NULL,
    .workers = 10L,
    .verbose = TRUE
  )
}
