#' Download SEC EDGAR Master Index Files
#'
#' @description
#' Downloads and processes the master index files from the SEC EDGAR database.
#' These files contain metadata about all SEC filings for a given quarter.
#'
#' @param .dir Character string specifying the directory where the downloaded data will be stored
#' @param .user Character string specifying the user agent to be used in HTTP requests
#' @param .from Numeric value specifying the start year.quarter (e.g., 2020.1 for Q1 2020). If NULL, defaults to 1993.1
#' @param .to Numeric value specifying the end year.quarter. If NULL, defaults to current quarter
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

  con_ <- DBI::dbConnect(RSQLite::SQLite(), lp_$MasterIndex$Sqlite)
  urls_ <- get_year_qtr_table(.from, .to) %>%
    dplyr::mutate(
      UrlMasterIndex = "https://www.sec.gov/Archives/edgar/full-index",
      UrlMasterIndex = file.path(UrlMasterIndex, Year, paste0("QTR", Quarter), "master.idx"),
      Print = paste0("Downloading Year-Quarter: ", Year, "-", Quarter)
    ) %>%
    dplyr::filter(!YearQuarter %in% get_processed_year_qtr(con_))

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
    DBI::dbWriteTable(con_, "master_index", out_, append = TRUE)
  }

  # Convert to Parquet -- -- -- -- -- -- -- ---
  table_ <- "master_index"
  query_idx_ <- paste0("CREATE INDEX IF NOT EXISTS idx_yq_", table_, " ON ", table_, "(YearQuarter)")
  DBI::dbExecute(con_, query_idx_)
  query_dis_ <- paste("SELECT DISTINCT YearQuarter FROM", table_)
  yq_ <- sort(dplyr::pull(DBI::dbGetQuery(con_, query_dis_), YearQuarter))

  for (i in seq_len(length(yq_))) {
    use_yq_ <- yq_[i]
    nam_yq_ <- paste0("MasterIndex_", gsub("\\.", "-", use_yq_), ".parquet")
    fil_yq_ <- file.path(lp_$MasterIndex$Parquet, nam_yq_)
    msg_yq_ <- paste0("Saving Data (MasterIndex): ", use_yq_)
    print_verbose(msg_yq_, .verbose, .line = "\r")

    dplyr::tbl(con_, table_) %>%
      dplyr::filter(YearQuarter == use_yq_) %>%
      dplyr::collect() %>%
      arrow::write_parquet(fil_yq_)
  }

  # Diconnect Database -- -- -- -- -- -- -- ---
  suppressWarnings(DBI::dbDisconnect(con_))
  on.exit(DBI::dbDisconnect(con_))

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

  # Initialize Databases and Saving Results
  vec_ <- c("DocumentLinks", "LandingPage")
  purrr::walk(vec_, ~ initialize_edgar_database(.dir, .x))
  purrr::walk(vec_, ~ write_link_data(.dir, NULL, .x, "parquet", .verbose))
  invisible(gc())

  tab_fils_ <- list_data(.dir)
  params_ <- get_edgar_params(.from, .to, .ciks, .formtypes)

  # i <- j <- 1
  future::plan("multisession", workers = .workers)
  last_time_ <- Sys.time()
  for (i in seq_len(nrow(tab_fils_))) {
    init_time_ <- Sys.time()
    yq_ <- tab_fils_$YearQuarter[i]
    use_ <- help_doclinks_get_idxs(
      .path_doclinks = tab_fils_$DocumentLinks[i],
      .path_masteridx = tab_fils_$MasterIndex[i],
      .params = get_edgar_params(.from, .to, .ciks, .formtypes),
      .workers = .workers
    )
    if (nrow(use_) == 0) next

    print_verbose("", .verbose, "\n")
    for (j in seq_len(nrow(use_))) {
      wait_info_ <- loop_wait_time(last_time_, .workers)
      last_time_ <- wait_info_$new_time
      msg_loop_ <- paste0(yq_, ": ", format_loop(j, nrow(use_), .workers))
      msg_time_ <- format_time(init_time_, j, nrow(use_))
      print_verbose(paste(msg_loop_, msg_time_, wait_info_$msg, sep = " | "), .verbose, "\r")

      # .index_row <- split(use_$data[[j]], use_$data[[j]]$HashIndex)[[1]]
      lst_ <- try(R.utils::withTimeout(
        expr = furrr::future_map(
          .x = split(use_$data[[j]], use_$data[[j]]$HashIndex),
          .f = ~ help_get_document_link(.index_row = .x, .user),
          .options = furrr::furrr_options(seed = TRUE)
        ) %>%
          purrr::transpose() %>%
          purrr::map(~ dplyr::bind_rows(.x, , .id = "HashIndex")),
        timeout = 30,
        onTimeout = "error"
      ), silent = TRUE)

      if (inherits(lst_, "try-error")) {
        Sys.sleep(60)
        print_verbose("Some error occurred, no worries we are continuing :) ...", .verbose, "\r")
        next
      }

      out_links_ <- finalize_tables(lst_$DocumentLinks, use_$data[[j]], "DocumentLinks")
      out_htmls_ <- finalize_tables(lst_$LangingPage, use_$data[[j]], "LangingPage")

      if (inherits(out_links_, "try-error") | inherits(out_htmls_, "try-error")) {
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
  future::plan("default")

  vec_ <- c("DocumentLinks", "LandingPage")
  purrr::walk(vec_, ~ write_link_data(.dir, NULL, .x, "parquet", .verbose))
  invisible(gc())

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
#'   .ciks = c("0000320193"), # Apple Inc
#'   .formtypes = c("10-K", "10-Q"),
#'   .workers = 4
#' )
#' }
#'
#' @export
edgar_download_document <- function(.dir, .user, .from = NULL, .to = NULL, .ciks = NULL, .formtypes = NULL, .doctypes = NULL, .workers = 1L, .verbose = TRUE) {
  lp_ <- get_directories(.dir)
  prc_ <- fs::path_ext_remove(unlist(purrr::map(
    .x = unname(list_files(lp_$DocumentData$Original)[["path"]]),
    .f = ~ zip::zip_list(.x)[["filename"]]
  )))

  tab_fils_ <- dplyr::filter(list_data(.dir), !is.na(DocumentLinks))

  time_ <- format(Sys.time(), format = "%Y-%m-%d-%H-%M-%S")
  dir_tmp_ <- file.path(lp_$Temporary$DocumentData, time_)

  for (i in seq_len(nrow(tab_fils_))) {
    name_yq_ <- tab_fils_$YearQuarter[i]
    path_yq_ <- tab_fils_$DocumentLinks[i]
    dir_yq_ <- fs::dir_create(paste0(dir_tmp_, "_", name_yq_))
    zip_yq_ <- file.path(lp_$DocumentData$Original, paste0(basename(dir_yq_), ".zip"))

    print_verbose(paste0(name_yq_, ": Checking Documents ..."), .verbose, .line = "\r")
    use_all_ <- edgar_read_document_links(
      .dir, path_yq_, .from, .to, .ciks, .formtypes, .doctypes, FALSE
    ) %>%
      dplyr::filter(Error == 0) %>%
      dplyr::mutate(DocName = paste0(CIK, "-", HashDocument)) %>%
      dplyr::filter(!DocName %in% prc_) %>%
      dplyr::collect() %>%
      dplyr::mutate(
        FileExt = paste0(".", tools::file_ext(UrlDocument)),
        PathTMP = file.path(dir_yq_, paste0(DocName, FileExt)),
        UrlDocument = purrr::set_names(UrlDocument, DocName)
      ) %>%
      dplyr::filter(!FileExt == ".") %>%
      dplyr::mutate(Split = ceiling(dplyr::row_number() / 10))

    if (nrow(use_all_) == 0) {
      fs::dir_delete(dir_yq_)
      next
    }

    lst_url_ <- split(use_all_$UrlDocument, use_all_$Split)
    lst_out_ <- split(use_all_$PathTMP, use_all_$Split)

    future::plan("multisession", workers = 10L)
    t0_start_ <- Sys.time() # Overall start time for ETA
    print_verbose("", .verbose, "\n")
    for (j in seq_along(lst_url_)) {
      t0_loop_ <- Sys.time()
      furrr::future_walk2(
        .x = lst_url_[[j]],
        .y = lst_out_[[j]],
        .f = ~ help_download_url_request(.x, .user, .y),
        .progress = FALSE,
        .options = furrr::furrr_options(seed = TRUE)
      )
      diff_loop_ <- as.numeric(difftime(Sys.time(), t0_loop_, units = "secs"))
      diff_total_ <- as.numeric(difftime(Sys.time(), t0_start_, units = "secs"))
      Sys.sleep(max(1.05 - diff_loop_, 0))
      msg_time_ <- format_time(t0_start_, j * 10, length(lst_url_) * 10)
      msg_rate_ <- paste0(" | Rate: ", round((j * 10) / diff_total_, 2), " Req/Sec")
      msg_loop_ <- paste0(format_number(j * 10), "/", format_number(length(lst_url_) * 10))
      msg_total_ <- paste0(tab_fils_$YearQuarter[i], ": ", msg_loop_, " | ", msg_time_, msg_rate_)
      print_verbose(msg_total_, .verbose, .line = "\r")
    }
    future::plan("default")

    zip::zipr(
      zipfile = zip_yq_,
      files = list_files(dir_yq_)[["path"]],
      compression_level = 9
    )
    fs::dir_delete(dir_yq_)
  }
}

#' Download SEC EDGAR Documents
#'
#' @description
#' Downloads actual documents from SEC EDGAR filings and processes their content.
#' Supports parallel downloading for improved performance.
#'
#' @param .dir Character string specifying the directory where the downloaded data will be stored
#' @param .workers Integer specifying the number of parallel workers for downloading
#' @param .verbose Logical indicating whether to print progress messages
#'
#' @return No return value, called for side effects
#'
#' @export
edgar_parse_documents <- function(.dir, .workers = 1L, .verbose = TRUE) {
  lp_ <- get_directories(.dir)

  fil_tobe_parsed_ <- help_get_parse_files(.dir, .verbose)
  arr_tobe_parsed_ <- arrow::open_dataset(fil_tobe_parsed_)
  cik_tobe_parsed_ <- dplyr::collect(dplyr::distinct(arr_tobe_parsed_, CIK))[["CIK"]]
  nrows_ <- format_number(nrow(arr_tobe_parsed_))
  nciks_ <- format_number(length(cik_tobe_parsed_))
  rm(arr_tobe_parsed_)


  msg_ <- paste0("Parsing Documents: ", nrows_, " Documents (", nciks_, " CIKs)")
  print_verbose(msg_, .verbose, .line = "\n\n")
  future::plan("multisession", workers = .workers)
  furrr::future_walk(
    .x = cik_tobe_parsed_,
    .f = ~ help_parse_files(fil_tobe_parsed_, .x),
    .options = furrr::furrr_options(seed = TRUE),
    .progress = .verbose
  )
  future::plan("default")
}

# DeBug ---------------------------------------------------------------------------------------
if (FALSE) {
  "19,969"
  devtools::load_all(".")
  library(rGetEDGAR)
  forms <- c(
    "10-K", "10-K/A",
    "10-Q", "10-Q/A",
    "8-K", "8-K/A",
    "20-F", "20-F/A",
    "S-1", "S-1/A",
    "S-4", "S-4/A",
    "F-1", "F-1/A",
    "F-4", "F-4/A",
    "CT ORDER"
  )
  user <- unname(ifelse(
    Sys.info()["sysname"] == "Darwin",
    "PetroParkerLosSpiderHombreABC12@Outlook.com",
    "TonyStarkIronManWeaponXYZ847263@Outlook.com"
  ))

  # Master Index
  edgar_get_master_index(
    .dir = fs::dir_create("../_package_debug/rGetEDGAR"),
    .user = user,
    .from = NULL,
    .to = NULL,
    .verbose = TRUE
  )

  # Document Links
  edgar_get_document_links(
    .dir = fs::dir_create("../_package_debug/rGetEDGAR"),
    .user = user,
    .from = NULL,
    .to = NULL,
    .ciks = NULL,
    .formtypes = forms,
    .workers = 5L,
    .verbose = TRUE
  )

  edgar_download_document(
    .dir = fs::dir_create("../_package_debug/rGetEDGAR"),
    .user = user,
    .from = NULL,
    .to = NULL,
    .ciks = NULL,
    .formtypes = forms,
    .doctypes = c("Exhibit 10"),
    .workers = 5L,
    .verbose = TRUE
  )

  edgar_parse_documents(
    .dir = fs::dir_create("../_package_debug/rGetEDGAR"),
    .workers = 5L,
    .verbose = TRUE
  )




  tab_master <- edgar_read_master_index(
    .dir = fs::dir_create("../_package_debug/rGetEDGAR"),
    .from = NULL,
    .to = NULL,
    .ciks = NULL,
    .formtypes = forms,
    .collect = TRUE
  )

  tab_docs <- edgar_read_document_links(
    .dir = fs::dir_create("../_package_debug/rGetEDGAR"),
    .from = NULL,
    .to = NULL,
    .ciks = NULL,
    .formtypes = forms,
    .doctypes = NULL,
    .collect = TRUE
  )
}




# ToBedeleted ---------------------------------------------------------------------------------

#'
#' #' Download SEC EDGAR Documents
#' #'
#' #' @description
#' #' Downloads actual documents from SEC EDGAR filings and processes their content.
#' #' Supports parallel downloading for improved performance.
#' #'
#' #' @param .dir Character string specifying the directory where the downloaded data will be stored
#' #' @param .user Character string specifying the user agent to be used in HTTP requests
#' #' @param .from Numeric value specifying the start year.quarter (e.g., 2020.1 for Q1 2020)
#' #' @param .to Numeric value specifying the end year.quarter
#' #' @param .ciks Character vector of CIK numbers to filter for specific companies
#' #' @param .formtypes Character vector of document types to filter (e.g., "10-K", "10-Q")
#' #' @param .doctypes Character vector of document types to filter (e.g., "10-K", "10-Q")
#' #' @param .verbose Logical indicating whether to print progress messages
#' #'
#' #' @details
#' #' The function:
#' #' 1. Reads from the document links database
#' #' 2. Downloads documents in parallel using the specified number of workers
#' #' 3. Processes HTML content and extracts text
#' #' 4. Stores both raw HTML and processed text
#' #' 5. Saves results in Parquet format by CIK
#' #'
#' #' For each document, stores:
#' #' - Original HTML
#' #' - Raw extracted text
#' #' - Modified/cleaned text
#' #'
#' #' @return No return value, called for side effects
#' #'
#' #' @examples
#' #' \dontrun{
#' #' edgar_download_document(
#' #'   .dir = "edgar_data",
#' #'   .user = "your@email.com",
#' #'   .from = 2020.1,
#' #'   .to = 2021.4,
#' #'   .ciks = c("0000320193"), # Apple Inc
#' #'   .formtypes = c("10-K", "10-Q"),
#' #'   .workers = 4
#' #' )
#' #' }
#' #'
#' #' @export
#' edgar_download_document <- function(.dir, .user, .from = NULL, .to = NULL, .ciks = NULL, .formtypes = NULL, .doctypes = NULL, .verbose = TRUE) {
#'   lp_ <- get_directories(.dir)
#'   get_temprorary_document_download(.dir, .from, .to, .ciks, .formtypes, .doctypes)
#'
#'
#'   t0_global <- Sys.time() # Keep track of overall time
#'   n_total <- nrow(arrow::open_dataset(lp_$Temporary$DocumentData)) # Total number to process
#'
#'
#'   future::plan("multisession", workers = 10L)
#'   for (i in seq_len(n_total)) {
#'     use_ <- arrow::open_dataset(lp_$Temporary$DocumentData) %>%
#'       dplyr::filter(Split == i) %>%
#'       dplyr::collect() %>%
#'       dplyr::distinct(HashDocument, .keep_all = TRUE) %>%
#'       dplyr::filter(!is.na(OutExt))
#'     lst_ <- split(use_, use_$HashDocument)
#'     t0_ <- Sys.time()
#'
#'     # .doc_row = lst_[[1]]
#'     furrr::future_walk(
#'       .x = lst_,
#'       .f = ~ help_download_document(.doc_row = .x, .user),
#'       .progress = FALSE,
#'       .options = furrr::furrr_options(seed = TRUE)
#'     )
#'     t1_ <- Sys.time()
#'
#'     # Rate limiting
#'     ela_ <- as.numeric(difftime(t1_, t0_, units = "secs"))
#'     if (ela_ < 1) {
#'       Sys.sleep(1 - ela_ + .05)
#'     }
#'
#'     # Calculate progress metrics
#'     elapsed_total <- as.numeric(difftime(t1_, t0_global, units = "secs"))
#'     rate <- (i * 10) / elapsed_total
#'     eta <- (n_total - (i * 10)) / rate
#'
#'     cat(
#'       "\rDocument:", format(i * 10, big.mark = ","), "of", format(n_total, big.mark = ","),
#'       "| Elapsed:", format(round(elapsed_total / 60, 1), big.mark = ","), "min",
#'       "| ETA:", format(round(eta / 60, 1), big.mark = ","), "min",
#'       "|", round(rate, 1), "Docs/Sec                "
#'     )
#'   }
#'   future::plan("default")
#' }
#'
#'

#
# tictoc::tic("HTTR Command")
# furrr::future_walk2(
#   .x = lst_url_[[1]],
#   .y = lst_out_[[1]],
#   .f = ~ help_download_url_request(.x, .user, .y),
#   .progress = FALSE,
#   .options = furrr::furrr_options(seed = TRUE)
# )
# tictoc::toc()
# future::plan("default")



# help_download_make_request <- function(.url, .user, .path_out) {
#   req_ <- httr2::req_headers(httr2::request(.url), Connection = "keep-alive", `User-Agent` = .user)
#   sprintf(
#     'curl -s "%s" %s -o "%s"',
#     req_$url,
#     paste(sprintf('-H "%s: %s"', names(req_$headers), unlist(req_$headers)), collapse = " "),
#     .path_out
#   )
# }
#
# help_download_cmd_command <- function(.cmd, .wait = FALSE) {
#   system(.cmd, ignore.stdout = TRUE, intern = TRUE, wait = .wait)
# }


# # Get To be Processed Index Files
# params_ <- get_edgar_params(.from, .to, .ciks, .formtypes)
# get_to_be_processed_master_index(.dir, .params = params_, .workers)
#
# if (!dir.exists(lp_$Temporary$DocumentLinks)) {
#   print_verbose("DocumentLinks Index Complete", TRUE, "\n")
#   return(invisible(NULL))
# } else {
#   arr0_ <- arrow::open_dataset(lp_$Temporary$DocumentLinks)
#   num_ <- format(nrow(arr0_), big.mark = ",")
#   print_verbose(paste0("IndexLinks to be Processed: ", num_), TRUE, "\n")
#   yqtr_ <- pull_column(arr0_, YearQuarter)
# }
#
#
# last_time_ <- Sys.time()
# future::plan("multisession", workers = .workers)
# # i = j = 1
# for (i in seq_len(length(yqtr_))) {
#   init_time_ <- Sys.time()
#   arr1_ <- dplyr::filter(arr0_, YearQuarter == yqtr_[i])
#   idx_ <- pull_column(arr1_, Split)
#
#   print_verbose("", .verbose, "\n")
#   for (j in idx_) {
#     wait_info_ <- loop_wait_time(last_time_, .workers)
#     last_time_ <- wait_info_$new_time
#     msg_ <- paste(
#       paste0(yqtr_[i], ": ", format_loop(j, length(idx_), .workers)),
#       format_time(init_time_, j, length(idx_)),
#       wait_info_$msg,
#       sep = " | "
#     )
#     print_verbose(msg_, .verbose, "\r")
#
#     use_ <- arr1_ %>%
#       dplyr::filter(Split == j) %>%
#       dplyr::collect() %>%
#       dplyr::distinct(HashIndex, .keep_all = TRUE) %>%
#       dplyr::mutate(
#         PathLog = lp_$Logs$DocumentLinks,
#         UrlIndexPage = purrr::set_names(UrlIndexPage, HashIndex)
#       )
#
#     # .index_row <- split(use_, use_$HashIndex)[[1]]
#     lst_ <- try(R.utils::withTimeout(
#       expr = furrr::future_map(
#         .x = split(use_, use_$HashIndex),
#         .f = ~ help_get_document_link(.index_row = .x, .user),
#         .options = furrr::furrr_options(seed = TRUE)
#       ) %>%
#         purrr::transpose() %>%
#         purrr::map(~ dplyr::bind_rows(.x, , .id = "HashIndex")),
#       timeout = 30,
#       onTimeout = "error"
#     ), silent = TRUE)
#
#     if (inherits(lst_, "try-error")) {
#       save_logging(
#         .path = lp_$Logs$DocumentLinks,
#         .loop = paste0(yqtr_[i], ": ", stringi::stri_pad_left(j, 6, "0")),
#         .function = "edgar_get_document_links",
#         .part = "1. process_batch",
#         .hash_index = paste(use_$HashIndex, collapse = "-"),
#         .hash_document = NA_character_,
#         .error = paste0("Timeout (30sec) or error in batch processing: ", paste(as.character(lst_), collapse = " "))
#       )
#       Sys.sleep(60)
#       print_verbose("Some error occurred, no worries we are continuing :) ...", .verbose, "\r")
#       next
#     }
#
#     if (nrow(lst_$DocumentLinks) == 0) {
#       save_logging(
#         .path = lp_$Logs$DocumentLinks,
#         .loop = paste0(yqtr_[i], ": ", stringi::stri_pad_left(j, 6, "0")),
#         .function = "edgar_get_document_links",
#         .part = "2. check_results",
#         .hash_index = paste(use_$HashIndex, collapse = "-"),
#         .hash_document = NA_character_,
#         .error = "No document links found in batch"
#       )
#       Sys.sleep(60)
#       print_verbose("Some error occurred, no worries we are continuing :) ...", .verbose, "\r")
#       next
#     }
#
#     out_links_ <- finalize_tables(lst_$DocumentLinks, use_, "DocumentLinks")
#     out_htmls_ <- finalize_tables(lst_$LangingPage, use_, "LangingPage")
#
#     if (inherits(out_links_, "try-error") | inherits(out_htmls_, "try-error")) {
#       save_logging(
#         .path = lp_$Logs$DocumentLinks,
#         .loop = paste0(yqtr_[i], ": ", stringi::stri_pad_left(j, 6, "0")),
#         .function = "edgar_get_document_links",
#         .part = "3. finalize_tables",
#         .hash_index = paste(use_$HashIndex, collapse = "-"),
#         .hash_document = NA_character_,
#         .error = paste(
#           "Error in finalizing tables:",
#           if (inherits(out_links_, "try-error")) paste(as.character(out_links_), collapse = " "),
#           if (inherits(out_htmls_, "try-error")) paste(as.character(out_htmls_), collapse = " ")
#         )
#       )
#       Sys.sleep(60)
#       print_verbose("Some error occurred, no worries we are continuing :) ...", .verbose, "\r")
#       next
#     }
#
#     # Write Output -- -- -- -- -- -- -- -- -- -
#     write_link_data(.dir, out_links_, "DocumentLinks", "sqlite")
#     write_link_data(.dir, out_htmls_, "LandingPage", "sqlite")
#     backup_link_data(.dir, "DocumentLinks")
#     backup_link_data(.dir, "LandingPage")
#   }
# }
# write_link_data(.dir, NULL, "DocumentLinks", "parquet")
# write_link_data(.dir, NULL, "LandingPage", "parquet")
# invisible(gc())
# future::plan("default")
# on.exit(future::plan("default"))
#



# ToBeChecked ---------------------------------------------------------------------------------


#
#
# edgar_download_document3 <- function(.dir, .user, .from = NULL, .to = NULL, .ciks = NULL, .formtypes = NULL, .doctypes = NULL, .verbose = TRUE) {
#   lp_ <- get_directories(.dir)
#   get_temprorary_document_download(.dir, .from, .to, .ciks, .formtypes, .doctypes)
#
#
#   t0_global <- Sys.time() # Keep track of overall time
#   n_total <- nrow(arrow::open_dataset(lp_$Temporary$DocumentData)) # Total number to process
#
#   use_ <- arrow::open_dataset(lp_$Temporary$DocumentData) %>%
#     dplyr::filter(Split == i) %>%
#     dplyr::collect() %>%
#     dplyr::distinct(HashDocument, .keep_all = TRUE)  %>%
#     dplyr::filter(!is.na(OutExt))
#
#   lst_requests <- purrr::map(
#     .x = use_$UrlDocument,
#     .f = ~ httr2::request(.x) %>%
#       httr2::req_headers(
#         Connection = "keep-alive",
#         `User-Agent` = .user
#       ) %>%
#       httr2::req_dry_run(quiet = TRUE)
#   )
#
#   lst_headers <- purrr::map(
#     .x = lst_requests,
#     .f = ~ purrr::imap_chr(.x$headers, ~sprintf("-H '%s: %s'", .y, .x)) %>%
#       paste(collapse = " ")
#   )
#
#   try(httr::GET(url = use_$UrlDocument[1], get_header(.user)), silent = TRUE)
#
#   library(httr2)
#
#
#
#
#   # Construct headers from the nested list structure
#   headers_str <- imap_chr(aaa$headers, ~sprintf("-H '%s: %s'", .y, .x)) %>%
#     paste(collapse = " ")
#
#   readLines("output.txt")
#
#   # Build full command using the aaa list elements
#   curl_cmd <- sprintf(
#     "curl 'https://%s%s' %s > output.txt &",
#     aaa$headers$host,
#     aaa$path,
#     headers_str
#   )
#
#
#   test <- system(curl_cmd, intern = FALSE)
#   test
#
#   library(purrr)
#
#   method <- "GET"
#   path <- "/Archives/edgar/data/1021561/000126645407000141/nuskin_10k-1998.txt"
#   headers <- list(
#     accept = "*/*",
#     `accept-encoding` = "deflate, gzip",
#     connection = "keep-alive",
#     host = "www.sec.gov",
#     `user-agent` = "PetroParkerLosSpiderHombreABC002581@Outlook.com"
#   )
#
#   # Construct headers using purrr
#   headers_str <- imap_chr(headers, ~sprintf("-H '%s: %s'", .y, .x)) %>%
#     paste(collapse = " ")
#
#   # Build full command
#   curl_cmd <- sprintf(
#     "curl '%s%s' %s > output.txt &",
#     headers$host,
#     path,
#     headers_str
#   )
#
#   system(curl_cmd, intern = FALSE)
#
#
#   system("curl -H 'Connection: keep-alive' -H 'User-Agent: PetroParkerLosSpiderHombreABC002581@Outlook.com' -H 'Accept: */*' 'https://www.sec.gov/Archives/edgar/data/1021561/000126645407000141/nuskin_10k-1998.txt' > output.txt &", intern = FALSE)
#
#   httr2::request()
#
#
#   future::plan("multisession", workers = 10L)
#   for (i in seq_len(n_total)) {
#     use_ <- arrow::open_dataset(lp_$Temporary$DocumentData) %>%
#       dplyr::filter(Split == i) %>%
#       dplyr::collect() %>%
#       dplyr::distinct(HashDocument, .keep_all = TRUE)  %>%
#       dplyr::filter(!is.na(OutExt))
#     lst_ <- split(use_, use_$HashDocument)
#     t0_ <- Sys.time()
#
#     # .doc_row = lst_[[1]]
#     furrr::future_walk(
#       .x = lst_,
#       .f = ~ help_download_document(.doc_row = .x, .user),
#       .progress = FALSE,
#       .options = furrr::furrr_options(seed = TRUE)
#     )
#     t1_ <- Sys.time()
#
#     # Rate limiting
#     ela_ <- as.numeric(difftime(t1_, t0_, units = "secs"))
#     if (ela_ < 1) {
#       Sys.sleep(1 - ela_ + .05)
#     }
#
#     # Calculate progress metrics
#     elapsed_total <- as.numeric(difftime(t1_, t0_global, units = "secs"))
#     rate <- (i * 10) / elapsed_total
#     eta <- (n_total - (i * 10)) / rate
#
#     cat(
#       "\rDocument:", format(i * 10, big.mark = ","), "of", format(n_total, big.mark = ","),
#       "| Elapsed:", format(round(elapsed_total / 60, 1), big.mark = ","), "min",
#       "| ETA:", format(round(eta / 60, 1), big.mark = ","), "min",
#       "|", round(rate, 1), "Docs/Sec                "
#     )
#   }
#   future::plan("default")
# }

#
# edgar_get_document_links <- function(.dir, .user, .from = NULL, .to = NULL, .ciks = NULL, .formtypes = NULL, .workers = 1L, .verbose = TRUE) {
#   lp_ <- get_directories(.dir)
#
#   # Initialize Databases and Saving Results
#   vec_ <- c("DocumentLinks", "LandingPage")
#   purrr::walk(vec_, ~ initialize_edgar_database(.dir, .x))
#   purrr::walk(vec_, ~ write_link_data(.dir, NULL, .x, "parquet", .verbose))
#   invisible(gc())
#
#
#   # Get To be Processed Index Files
#   params_ <- get_edgar_params(.from, .to, .ciks, .formtypes)
#   get_to_be_processed_master_index(.dir, .params = params_, .workers)
#
#   if (!dir.exists(lp_$Temporary$DocumentLinks)) {
#     print_verbose("DocumentLinks Index Complete", TRUE, "\n")
#     return(invisible(NULL))
#   } else {
#     arr0_ <- arrow::open_dataset(lp_$Temporary$DocumentLinks)
#     num_ <- format(nrow(arr0_), big.mark = ",")
#     print_verbose(paste0("IndexLinks to be Processed: ", num_), TRUE, "\n")
#     yqtr_ <- pull_column(arr0_, YearQuarter)
#   }
#
#
#   last_time_ <- Sys.time()
#   future::plan("multisession", workers = .workers)
#   # i = j = 1
#   for (i in seq_len(length(yqtr_))) {
#     init_time_ <- Sys.time()
#     arr1_ <- dplyr::filter(arr0_, YearQuarter == yqtr_[i])
#     idx_ <- pull_column(arr1_, Split)
#
#     print_verbose("", .verbose, "\n")
#     for (j in idx_) {
#       wait_info_ <- loop_wait_time(last_time_, .workers)
#       last_time_ <- wait_info_$new_time
#       msg_ <- paste(
#         paste0(yqtr_[i], ": ", format_loop(j, length(idx_), .workers)),
#         format_time(init_time_, j, length(idx_)),
#         wait_info_$msg,
#         sep = " | "
#       )
#       print_verbose(msg_, .verbose, "\r")
#
#       use_ <- arr1_ %>%
#         dplyr::filter(Split == j) %>%
#         dplyr::collect() %>%
#         dplyr::distinct(HashIndex, .keep_all = TRUE) %>%
#         dplyr::mutate(
#           PathLog = lp_$Logs$DocumentLinks,
#           UrlIndexPage = purrr::set_names(UrlIndexPage, HashIndex)
#         )
#
#       # .index_row <- split(use_, use_$HashIndex)[[1]]
#       lst_ <- try(R.utils::withTimeout(
#         expr = furrr::future_map(
#           .x = split(use_, use_$HashIndex),
#           .f = ~ help_get_document_link(.index_row = .x, .user),
#           .options = furrr::furrr_options(seed = TRUE)
#         ) %>%
#           purrr::transpose() %>%
#           purrr::map(~ dplyr::bind_rows(.x, , .id = "HashIndex")),
#         timeout = 30,
#         onTimeout = "error"
#       ), silent = TRUE)
#
#       if (inherits(lst_, "try-error")) {
#         save_logging(
#           .path = lp_$Logs$DocumentLinks,
#           .loop = paste0(yqtr_[i], ": ", stringi::stri_pad_left(j, 6, "0")),
#           .function = "edgar_get_document_links",
#           .part = "1. process_batch",
#           .hash_index = paste(use_$HashIndex, collapse = "-"),
#           .hash_document = NA_character_,
#           .error = paste0("Timeout (30sec) or error in batch processing: ", paste(as.character(lst_), collapse = " "))
#         )
#         Sys.sleep(60)
#         print_verbose("Some error occurred, no worries we are continuing :) ...", .verbose, "\r")
#         next
#       }
#
#       if (nrow(lst_$DocumentLinks) == 0) {
#         save_logging(
#           .path = lp_$Logs$DocumentLinks,
#           .loop = paste0(yqtr_[i], ": ", stringi::stri_pad_left(j, 6, "0")),
#           .function = "edgar_get_document_links",
#           .part = "2. check_results",
#           .hash_index = paste(use_$HashIndex, collapse = "-"),
#           .hash_document = NA_character_,
#           .error = "No document links found in batch"
#         )
#         Sys.sleep(60)
#         print_verbose("Some error occurred, no worries we are continuing :) ...", .verbose, "\r")
#         next
#       }
#
#       out_links_ <- finalize_tables(lst_$DocumentLinks, use_, "DocumentLinks")
#       out_htmls_ <- finalize_tables(lst_$LangingPage, use_, "LangingPage")
#
#       if (inherits(out_links_, "try-error") | inherits(out_htmls_, "try-error")) {
#         save_logging(
#           .path = lp_$Logs$DocumentLinks,
#           .loop = paste0(yqtr_[i], ": ", stringi::stri_pad_left(j, 6, "0")),
#           .function = "edgar_get_document_links",
#           .part = "3. finalize_tables",
#           .hash_index = paste(use_$HashIndex, collapse = "-"),
#           .hash_document = NA_character_,
#           .error = paste(
#             "Error in finalizing tables:",
#             if (inherits(out_links_, "try-error")) paste(as.character(out_links_), collapse = " "),
#             if (inherits(out_htmls_, "try-error")) paste(as.character(out_htmls_), collapse = " ")
#           )
#         )
#         Sys.sleep(60)
#         print_verbose("Some error occurred, no worries we are continuing :) ...", .verbose, "\r")
#         next
#       }
#
#       # Write Output -- -- -- -- -- -- -- -- -- -
#       write_link_data(.dir, out_links_, "DocumentLinks", "sqlite")
#       write_link_data(.dir, out_htmls_, "LandingPage", "sqlite")
#       backup_link_data(.dir, "DocumentLinks")
#       backup_link_data(.dir, "LandingPage")
#     }
#   }
#   write_link_data(.dir, NULL, "DocumentLinks", "parquet")
#   write_link_data(.dir, NULL, "LandingPage", "parquet")
#   invisible(gc())
#   future::plan("default")
#   on.exit(future::plan("default"))
# }
