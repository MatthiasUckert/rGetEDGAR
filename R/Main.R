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
  write_link_data(.dir, NULL, "DocumentLinks", "parquet", .verbose)
  write_link_data(.dir, NULL, "LandingPage", "parquet", .verbose)
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
    if (nrow(use_) == 0) {
      next
    }

    print_verbose("", .verbose, "\n")
    for (j in seq_len(nrow(use_))) {
      wait_info_ <- loop_wait_time(last_time_, .workers)
      last_time_ <- wait_info_$new_time
      msg_loop_ <- paste0(yq_, ": ", format_loop(j, nrow(use_), .workers))
      msg_time_ <- format_time(init_time_, j, nrow(use_))
      print_verbose(paste(msg_loop_, msg_time_, wait_info_$msg, sep = " | "), .verbose, "\r")

      # Sys.sleep(60)
      # .index_row <- split(use_$data[[j]], use_$data[[j]]$HashIndex)[[1]]
      lst_ <- try(purrr::transpose(furrr::future_map(
        .x = split(use_$data[[j]], use_$data[[j]]$HashIndex),
        .f = ~ help_get_document_link(.index_row = .x, .user),
        .options = furrr::furrr_options(seed = TRUE)
      )), silent = TRUE)

      if (inherits(lst_, "try-error")) {
        print_verbose("Some error occurred, no worries we are continuing :) ...", .verbose, "\r")
        Sys.sleep(60)
        next
      }

      out_links_ <- finalize_tables(
        .tab = dplyr::bind_rows(lst_$DocumentLinks, .id = "HashIndex"),
        .join = use_$data[[j]],
        .type = "DocumentLinks"
      )
      out_htmls_ <- finalize_tables(
        .tab = dplyr::bind_rows(lst_$LangingPage, .id = "HashIndex"),
        .join = use_$data[[j]],
        .type = "LangingPage"
      )

      if (inherits(out_links_, "try-error") | inherits(out_htmls_, "try-error")) {
        print_verbose("Some error occurred, no worries we are continuing :) ...", .verbose, "\r")
        Sys.sleep(60)
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
      dplyr::mutate(DocID = paste0(CIK, "-", HashDocument)) %>%
      dplyr::filter(!DocID %in% prc_) %>%
      dplyr::collect() %>%
      dplyr::mutate(
        FileExt = paste0(".", tools::file_ext(UrlDocument)),
        PathTMP = file.path(dir_yq_, paste0(DocID, FileExt)),
        UrlDocument = purrr::set_names(UrlDocument, DocID)
      ) %>%
      dplyr::filter(!FileExt == ".") %>%
      dplyr::mutate(Split = ceiling(dplyr::row_number() / 10))

    if (nrow(use_all_) == 0) {
      fs::dir_delete(dir_yq_)
      next
    }

    # print("Waiting...")
    # Sys.sleep(1000)

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
        .f = ~ help_download_url_request(.url = .x, .user, .path_out = .y),
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
#' @param .doc_ids DocIDs to be processed
#' @param .verbose Logical indicating whether to print progress messages
#'
#' @return No return value, called for side effects
#'
#' @export
edgar_parse_documents <- function(.dir, .workers = 1L, .doc_ids = NULL, .verbose = TRUE) {
  lp_ <- get_directories(.dir)
  yq_ <- gsub("\\.", "-", get_year_qtr_table()[["YearQuarter"]])

  future::plan("multisession", workers = .workers)
  for (i in seq_len(length(yq_))) {
    lst_use_ <- get_non_processed_files(.dir, yq_[i], .doc_ids)

    len_ <- length(lst_use_)
    msg_ <- paste0("Processing YearQuarter: ", yq_[i], " (", format_number(len_), " Docs)")
    print_verbose(msg_, .verbose, .line = "\r")
    if (len_ == 0) {
      next
    }
    # Sys.sleep(100)
    # .tab_row <- lst_use_[[1]]

    if (.verbose) cat("\n")
    furrr::future_walk(
      .x = lst_use_,
      .f = parse_files,
      .options = furrr::furrr_options(seed = TRUE, scheduling = Inf)
    )
  }
  future::plan("default")
}


#' Get Non-Processed SEC EDGAR Files
#'
#' @description
#' Identifies and filters SEC EDGAR documents that haven't been processed yet for a specific year-quarter.
#' Focuses on Exhibit 10 documents and handles file paths for processing.
#'
#' @param .dir Character string specifying the base directory for SEC data
#' @param .yq Character string specifying the year-quarter in format "YYYY-QQ"
#' @param .doc_ids DocIDs to be processed
#'
#' @return List of tibbles containing file information for unprocessed documents:
#'   \itemize{
#'     \item HashIndex: Unique identifier for the filing
#'     \item CIK: Company identifier
#'     \item HashDocument: Document hash
#'     \item DocID: Document name
#'     \item FileName: Original file name
#'     \item PathZIP: Path to source ZIP file
#'     \item PathTmp: Temporary extraction path
#'     \item PathOut: Output path for processed file
#'   }
#'
#' @keywords internal
get_non_processed_files <- function(.dir, .yq, .doc_ids = NULL) {
  lp_ <- get_directories(.dir)

  paths_zips_ <- list_files(lp_$DocumentData$Original) %>%
    dplyr::mutate(YearQuarter = stringi::stri_sub(doc_id, 21, 26)) %>%
    dplyr::filter(YearQuarter == .yq)

  if (nrow(paths_zips_) == 0) {
    return(list())
  }

  file.exists(paths_zips_$path)

  paths_link_ <- list_files(lp_$DocumentLinks$Parquet) %>%
    dplyr::filter(endsWith(doc_id, .yq))

  dir_type_ <- list_files(lp_$DocumentData$Parsed)[["path"]]
  if (length(dir_type_) == 0) {
    fil_prcs_ <- NA_character_
  } else {
    dir_docs_ <- dplyr::filter(list_files(dir_type_), doc_id == .yq)
    fil_prcs_ <- list_files(dir_docs_, .rec = TRUE)[["doc_id"]]
  }


  fil_zip_ <- purrr::map(
    .x = paths_zips_$path,
    .f = ~ dplyr::mutate(tibble::as_tibble(zip::zip_list(.x)), PathZIP = .x)
  ) %>%
    dplyr::bind_rows(.id = "FileZIP") %>%
    dplyr::mutate(
      DocID = fs::path_ext_remove(filename),
      FileExt = tolower(tools::file_ext(filename))
    ) %>%
    dplyr::filter(!DocID %in% fil_prcs_) %>%
    dplyr::filter(FileExt %in% c("txt", "htm", "html", "xml", "xsd")) %>%
    dplyr::left_join(
      y = arrow::open_dataset(paths_link_$path) %>%
        dplyr::select(DocID, CIK, HashIndex, HashDocument, DocTypeRaw = Type) %>%
        dplyr::collect(),
      by = dplyr::join_by(DocID)
    ) %>%
    dplyr::left_join(
      y = dplyr::select(get("Table_DocTypesRaw"), DocTypeRaw, DocTypeMod),
      by = dplyr::join_by(DocTypeRaw)
    ) %>%
    dplyr::mutate(
      DocTypeMod = gsub(" ", "", DocTypeMod),
      DocTypeMod = gsub("\\/|\\.", "-", DocTypeMod),
      PathOut = file.path(lp_$DocumentData$Parsed, gsub(" ", "", DocTypeMod), .yq, paste0(DocID, ".parquet")),
      PathTmp = file.path(lp_$Temporary$DocumentData, filename),
    ) %>%
    dplyr::select(
      HashIndex, CIK, HashDocument, DocID,
      FileName = filename, PathZIP, PathTmp, PathOut
    )

  if (!is.null(.doc_ids)) {
    fil_zip_ <- dplyr::filter(fil_zip_, DocID %in% .doc_ids)
  }

  if (nrow(fil_zip_) == 0) {
    return(list())
  }


  split(fil_zip_, fil_zip_$DocID)
}

#' Parse SEC EDGAR Files
#'
#' @description
#' Processes individual SEC EDGAR documents by extracting from ZIP archives,
#' reading HTML content, standardizing text, and saving as parquet files.
#'
#' @param .tab_row Tibble row containing file information:
#'   \itemize{
#'     \item CIK: Company identifier
#'     \item HashIndex: Filing identifier
#'     \item HashDocument: Document hash
#'     \item PathZIP: Source ZIP path
#'     \item FileName: File to extract
#'     \item PathTmp: Temporary path
#'     \item PathOut: Output path
#'   }
#'
#' @return No return value, called for side effects:
#'   \itemize{
#'     \item Extracts document from ZIP
#'     \item Processes HTML content
#'     \item Saves standardized text as parquet
#'     \item Cleans up temporary files
#'   }
#' @keywords internal
parse_files <- function(.tab_row) {
  if (file.exists(.tab_row$PathOut)) {
    return(tibble::tibble())
  }
  fs::dir_create(dirname(.tab_row$PathOut))
  zip::unzip(
    zipfile = .tab_row$PathZIP,
    files = .tab_row$FileName,
    exdir = dirname(.tab_row$PathTmp),
    overwrite = TRUE
  )

  out_ <- try(tibble::tibble(
    CIK = .tab_row$CIK,
    HashIndex = .tab_row$HashIndex,
    HashDocument = .tab_row$HashDocument,
    HTML = readChar(.tab_row$PathTmp, file.info(.tab_row$PathTmp)$size)
  ) %>%
    dplyr::mutate(
      TextRaw = purrr::map_chr(HTML, read_html),
      TextMod = standardize_text(TextRaw),
      nWords = stringi::stri_count_words(TextMod),
      nChars = nchar(TextMod)
    ), silent = TRUE)

  if (!inherits(out_, "try-error")) {
    arrow::write_parquet(out_, .tab_row$PathOut)
  }
  try(fs::file_delete(.tab_row$PathTmp), silent = TRUE)
}



# DeBug ---------------------------------------------------------------------------------------
if (FALSE) {
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

  dir_debug <- fs::dir_create("../_package_debug/rGetEDGAR")
  lp_ <- get_directories(dir_debug)

  # Master Index
  edgar_get_master_index(
    .dir = dir_debug,
    .user = user,
    .from = NULL,
    .to = NULL,
    .verbose = TRUE
  )

  # Document Links
  edgar_get_document_links(
    .dir = dir_debug,
    .user = user,
    .from = NULL,
    .to = NULL,
    .ciks = NULL,
    .formtypes = forms,
    .workers = 5L,
    .verbose = TRUE
  )

  edgar_download_document(
    .dir = dir_debug,
    .user = user,
    .from = NULL,
    .to = NULL,
    .ciks = NULL,
    .formtypes = forms,
    .doctypes = c("Exhibit 10"),
    .workers = 5L,
    .verbose = TRUE
  )

  tab_ex10 <- dplyr::filter(Table_DocTypesRaw, DocTypeMod == "Exhibit 10")
  scales::comma(sum(tab_ex10$nDocs))
  ids_ex10 <- arrow::open_dataset(lp_$DocumentLinks$Parquet) %>%
    dplyr::filter(Type %in% tab_ex10$DocTypeRaw) %>%
    dplyr::distinct(DocID) %>%
    dplyr::collect() %>%
    dplyr::pull(DocID)
  scales::comma(length(ids_ex10))

  edgar_parse_documents(
    .dir = fs::dir_create("../_package_debug/rGetEDGAR"),
    .workers = 5L,
    .doc_ids = ids_ex10,
    .verbose = TRUE
  )
}



