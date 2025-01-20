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
  suppressWarnings(DBI::dbDisconnect(con_master_))
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
#' @param .formtypes Character vector of FormTypes
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
#'   .ciks = c("0000320193", "0001018724") # Apple and Amazon
#' )
#' }
#'
#' @export
edgar_get_document_links <- function(.dir, .user, .from = NULL, .to = NULL, .ciks = NULL, .formtypes = NULL, .workers = 1L, .verbose = TRUE) {
  lp_ <- get_directories(.dir)

  # Initialize Databases
  purrr::walk(c("DocumentLinks", "LandingPage"), ~ initialize_edgar_database(.dir, .x))

  print_verbose("Updating Data", TRUE, "\r")
  write_link_data(.dir, out_links_, "DocumentLinks", "parquet")
  write_link_data(.dir, out_htmls_, "LandingPage", "parquet")
  invisible(gc())


  # Get To be Processed Index Files
  params_ <- get_edgar_params(.from, .to, .ciks, .formtypes)
  get_to_be_processed_master_index(.dir, .params = params_, .workers)

  if (!dir.exists(lp_$Temporary$DocumentLinks)) {
    print_verbose("DocumentLinks Index Complete", TRUE, "\n")
    return(invisible(NULL))
  } else {
    arr0_ <- arrow::open_dataset(lp_$Temporary$DocumentLinks)
    num_ <- format(nrow(arr0_), big.mark = ",")
    print_verbose(paste0("IndexLinks to be Processed: ", num_), TRUE, "\n")
    yqtr_ <- pull_column(arr0_, YearQuarter)
  }


  last_time_ <- Sys.time()
  future::plan("multisession", workers = .workers)
  # i = j = 1
  for (i in seq_len(length(yqtr_))) {
    init_time_ <- Sys.time()
    arr1_ <- dplyr::filter(arr0_, YearQuarter == yqtr_[i])
    idx_ <- pull_column(arr1_, Split)

    print_verbose("", .verbose, "\n")
    for (j in idx_) {
      wait_info_ <- loop_wait_time(last_time_, .workers)
      last_time_ <- wait_info_$new_time
      msg_ <- paste(
        paste0(yqtr_[i], ": ", format_loop(j, length(idx_), .workers)),
        format_time(init_time_, j, length(idx_)),
        wait_info_$msg,
        sep = " | "
      )
      print_verbose(msg_, .verbose, "\r")

      use_ <- arr1_ %>%
        dplyr::filter(Split == j) %>%
        dplyr::collect() %>%
        dplyr::distinct(HashIndex, .keep_all = TRUE) %>%
        dplyr::mutate(
          PathLog = lp_$Logs$DocumentLinks,
          UrlIndexPage = purrr::set_names(UrlIndexPage, HashIndex)
        )

      # .index_row <- split(use_, use_$HashIndex)[[1]]
      lst_ <- try(R.utils::withTimeout(
        expr = furrr::future_map(
          .x = split(use_, use_$HashIndex),
          .f = ~ help_get_document_link(.index_row = .x, .user),
          .options = furrr::furrr_options(seed = TRUE)
        ) %>%
          purrr::transpose() %>%
          purrr::map(~ dplyr::bind_rows(.x, , .id = "HashIndex")),
        timeout = 30,
        onTimeout = "error"
      ), silent = TRUE)

      if (inherits(lst_, "try-error")) {
        save_logging(
          .path = lp_$Logs$DocumentLinks,
          .loop = paste0(yqtr_[i], ": ", stringi::stri_pad_left(j, 6, "0")),
          .function = "edgar_get_document_links",
          .part = "1. process_batch",
          .hash_index = paste(use_$HashIndex, collapse = "-"),
          .hash_document = NA_character_,
          .error = paste0("Timeout (30sec) or error in batch processing: ", paste(as.character(lst_), collapse = " "))
        )
        Sys.sleep(60)
        print_verbose("Some error occurred, no worries we are continuing :) ...", .verbose, "\r")
        next
      }

      if (nrow(lst_$DocumentLinks) == 0) {
        save_logging(
          .path = lp_$Logs$DocumentLinks,
          .loop = paste0(yqtr_[i], ": ", stringi::stri_pad_left(j, 6, "0")),
          .function = "edgar_get_document_links",
          .part = "2. check_results",
          .hash_index = paste(use_$HashIndex, collapse = "-"),
          .hash_document = NA_character_,
          .error = "No document links found in batch"
        )
        Sys.sleep(60)
        print_verbose("Some error occurred, no worries we are continuing :) ...", .verbose, "\r")
        next
      }

      out_links_ <- finalize_tables(lst_$DocumentLinks, use_, "DocumentLinks")
      out_htmls_ <- finalize_tables(lst_$LangingPage, use_, "LangingPage")

      if (inherits(out_links_, "try-error") | inherits(out_htmls_, "try-error")) {
        save_logging(
          .path = lp_$Logs$DocumentLinks,
          .loop = paste0(yqtr_[i], ": ", stringi::stri_pad_left(j, 6, "0")),
          .function = "edgar_get_document_links",
          .part = "3. finalize_tables",
          .hash_index = paste(use_$HashIndex, collapse = "-"),
          .hash_document = NA_character_,
          .error = paste(
            "Error in finalizing tables:",
            if (inherits(out_links_, "try-error")) paste(as.character(out_links_), collapse = " "),
            if (inherits(out_htmls_, "try-error")) paste(as.character(out_htmls_), collapse = " ")
          )
        )
        Sys.sleep(60)
        print_verbose("Some error occurred, no worries we are continuing :) ...", .verbose, "\r")
        next
      }

      # Write Output -- -- -- -- -- -- -- -- -- -
      write_link_data(.dir, out_links_, "DocumentLinks", "sqlite")
      write_link_data(.dir, out_htmls_, "LandingPage", "sqlite")
      backup_link_data(.dir, "DocumentLinks")
      backup_link_data(.dir, "LandingPage")
    }
  }
  write_link_data(.dir, NULL, "DocumentLinks", "parquet")
  write_link_data(.dir, NULL, "LandingPage", "parquet")
  invisible(gc())
  future::plan("default")
  on.exit(future::plan("default"))
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
#' @param .formtypes Character vector of document types to filter (e.g., "10-K", "10-Q")
#' @param .doctypes Character vector of document types to filter (e.g., "10-K", "10-Q")
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
#'   .ciks = c("0000320193"), # Apple Inc
#'   .formtypes = c("10-K", "10-Q"),
#'   .workers = 4
#' )
#' }
#'
#' @export
edgar_download_document <- function(.dir, .user, .from = NULL, .to = NULL, .ciks = NULL, .formtypes = NULL, .doctypes = NULL, .verbose = TRUE) {
  lp_ <- get_directories(.dir)
  get_temprorary_document_download(.dir, .from, .to, .ciks, .formtypes, .doctypes)


  t0_global <- Sys.time() # Keep track of overall time
  n_total <- nrow(arrow::open_dataset(lp_$Temporary$DocumentData)) # Total number to process


  future::plan("multisession", workers = 10L)
  for (i in seq_len(n_total)) {
    use_ <- arrow::open_dataset(lp_$Temporary$DocumentData) %>%
      dplyr::filter(Split == i) %>%
      dplyr::collect()
    lst_ <- split(use_, use_$HashDocument)
    t0_ <- Sys.time()

    furrr::future_walk(
      .x = lst_,
      .f = ~ help_download_document(.doc_row = .x, .user),
      .progress = FALSE,
      .options = furrr::furrr_options(seed = TRUE)
    )
    t1_ <- Sys.time()

    # Rate limiting
    ela_ <- as.numeric(difftime(t1_, t0_, units = "secs"))
    if (ela_ < 1) {
      Sys.sleep(1 - ela_ + .05)
    }

    # Calculate progress metrics
    elapsed_total <- as.numeric(difftime(t1_, t0_global, units = "secs"))
    rate <- (i * 10) / elapsed_total
    eta <- (n_total - (i * 10)) / rate

    cat(
      "\rDocument:", format(i * 10, big.mark = ","), "of", format(n_total, big.mark = ","),
      "| Elapsed:", format(round(elapsed_total / 60, 1), big.mark = ","), "min",
      "| ETA:", format(round(eta / 60, 1), big.mark = ","), "min",
      "|", round(rate, 1), "Docs/Sec                "
    )
  }
  future::plan("default")
}


# DeBug ---------------------------------------------------------------------------------------
if (FALSE) {
  devtools::load_all(".")
  library(rGetEDGAR)
  forms <- c("10-K", "10-K/A", "10-Q", "10-Q/A", "8-K", "8-K/A", "20-F", "20-F/A", "S1", "S4", "F1", "F4")
  forms <- c("10-K", "10-K/A", "10-Q", "10-Q/A", "8-K", "8-K/A", "20-F", "20-F/A")
  user <- ifelse(
    Sys.info()["sysname"] == "Darwin",
    "PetroParkerLosSpiderHombreABC002581@Outlook.com",
    "TonyStarkIronManWeaponXYZ847263@Outlook.com"
  )

  # Master Index
  edgar_get_master_index(
    .dir = fs::dir_create("../_package_debug/rGetEDGAR"),
    .user = user,
    .from = NULL,
    .to = NULL,
    .verbose = TRUE
  )

  edgar_read_master_index(
    .dir = fs::dir_create("../_package_debug/rGetEDGAR"),
    .from = 1993.1,
    .to = 2024.4,
    .ciks = NULL,
    .formtypes = forms,
    .collect = TRUE
  )

  # Document Links
  edgar_get_document_links(
    .dir = fs::dir_create("../_package_debug/rGetEDGAR"),
    .user = user,
    .from = 2014.1,
    .to = 2024.4,
    .ciks = NULL,
    .formtypes = forms,
    .workers = 5L,
    .verbose = TRUE
  )

  edgar_read_document_links(
    .dir = fs::dir_create("../_package_debug/rGetEDGAR"),
    .from = 1995.1,
    .to = 2024.4,
    .ciks = NULL,
    .formtypes = forms,
    .doctypes = NULL,
    .collect = TRUE
  )

  edgar_download_document(
    .dir = fs::dir_create("../_package_debug/rGetEDGAR"),
    .user = user,
    .from = 1995.1,
    .to = 2024.4,
    .ciks = NULL,
    .formtypes = c("Exhibit 10", "10-K", "10-Q", "8-K", "20-F"),
    .doctypes = doc,
    .verbose = TRUE
  )

  for (doc in c("Exhibit 10", "10-K", "10-Q", "8-K", "20-F")) {
    cat("\nDownloading:", doc, "\n")

  }
}
