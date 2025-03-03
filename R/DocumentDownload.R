# Main Functions ------------------------------------------------------------------------------
#' Download SEC EDGAR Documents
#'
#' @description
#' Downloads and processes actual document files from SEC EDGAR filings. This function
#' retrieves the raw document content referenced in filing indexes and supports parallel
#' downloading for improved performance.
#'
#' @param .dir
#' Character string specifying the directory where the downloaded data will be stored.
#' This directory should contain the necessary subdirectory structure.
#' @param .user
#' Character string specifying the user agent to be used in HTTP requests to the SEC EDGAR server.
#' This should typically be an email address to comply with SEC's fair access policy.
#' @param .doc_ids
#' Character vector of document IDs to download. If NULL, all available documents
#' that haven't been previously downloaded will be processed.
#' @param .workers
#' Integer specifying the number of parallel workers for downloading. Defaults to 1.
#' Higher values increase download speed but also increase server load.
#' @param .verbose
#' Logical indicating whether to print progress messages to the console during processing.
#' Default is TRUE.
#'
#' @details
#' The function performs the following steps:
#' \itemize{
#'   \item Identifies which documents need to be downloaded based on the provided document IDs
#'   \item Organizes documents into batches by document type and year-quarter
#'   \item Creates temporary directories for initial storage of downloaded files
#'   \item Downloads documents in parallel using the specified number of workers
#'   \item Compresses downloaded documents into ZIP archives organized by document type and period
#'   \item Maintains proper rate limiting to comply with SEC's fair access policy
#' }
#'
#' The function handles various document formats, including HTML, XML, text, and binary files
#' such as PDFs. It stores downloaded files in a temporary directory structure before
#' compressing them into ZIP archives for permanent storage.
#'
#' The process implements error handling to gracefully manage network issues, malformed
#' content, or other download failures without interrupting the entire batch.
#'
#' @return No return value, called for side effects (downloading and saving data files).
#'
#' @examples
#' \dontrun{
#' edgar_download_document(
#'   .dir = "edgar_data",
#'   .user = "your@email.com",
#'   .doc_ids = c("0000320193-123456789", "0001018724-987654321"),
#'   .workers = 4,
#'   .verbose = TRUE
#' )
#' }
#'
#' @export
edgar_download_document <- function(.dir, .user, .doc_ids, .workers = 1L, .verbose = TRUE) {
  lp_ <- get_directories(.dir)
  plog_ <- lp_$DocumentData$FilLogOriginal
  tab_seq_ <- get_tbp_docids(.dir, .doc_ids, .workers) %>%
    tidyr::nest(.by = c(dir_tmp, dir_out))


  con_prcs_ <- DBI::dbConnect(RSQLite::SQLite(), lp_$DocumentData$Temporary$FilToBePrc)

  future::plan("multisession", workers = .workers)
  for (i in seq_len(nrow(tab_seq_))) {
    use_seq_ <- tab_seq_$data[[i]]

    for (j in seq_len(nrow(use_seq_))) {
      t0_ <- Sys.time()
      yq0_ <- use_seq_$YearQuarter[i]
      yq1_ <- gsub(".", "-", yq0_, fixed = TRUE)
      tot_ <- max(use_seq_$Seq)

      typ_ <- use_seq_$DocType[i]
      seq_ <- use_seq_$Seq[j]
      print_download_loop(yq1_, use_seq_$DocType[1], j, tot_, .workers, .verbose)

      use_ <- dplyr::tbl(con_prcs_, "tobeprocessed") %>%
        dplyr::filter(YearQuarter == yq0_, DocType == typ_, Seq == seq_) %>%
        dplyr::collect() %>%
        dplyr::mutate(
          path_tmp = file.path(tab_seq_$dir_tmp[i], paste0(DocID, ".", tools::file_ext(UrlDocument)))
        )

      .url <- use_$UrlDocument[1]
      .path_out <- use_$path_tmp[1]

      furrr::future_walk2(
        .x = use_$UrlDocument,
        .y = use_$path_tmp,
        .f = ~ download_url_request(.x, .user, .y, plog_, .verbose)
      )
      elapsed_ <- as.numeric(difftime(Sys.time(), t0_))
      if (!elapsed_ >= 1.05) {
        Sys.sleep(1.05 - elapsed_)
      }
    }

    time_ <- format(Sys.time(), "%Y-%m-%d-%H-%M-%S")
    nam_zip_ <- paste0(time_, "_", basename(tab_seq_$dir_out[i]), ".zip")
    fs::dir_create(tab_seq_$dir_out[i])
    zip::zipr(
      zipfile = file.path(tab_seq_$dir_out[i], nam_zip_),
      files = list_files(tab_seq_$dir_tmp[i])[["path"]],
      compression_level = 9
    )

    try(fs::dir_delete(tab_seq_$dir_tmp[i]), silent = TRUE)
    try(fs::dir_delete(lp_$DocumentData$Temporary$FilToBePrc), silent = TRUE)
  }
  future::plan("default")
}

# Helper Functions ----------------------------------------------------------------------------
#' Download Document from URL
#'
#' @description
#' Downloads a document from a specified URL and saves it to the filesystem.
#' This function handles different content types appropriately based on file extension.
#'
#' @param .url
#' Character string specifying the URL of the document to download.
#' @param .user
#' Character string specifying the user agent to be used in HTTP requests.
#' This should typically be an email address to comply with SEC's fair access policy.
#' @param .path_out
#' Character string specifying the file path where the downloaded document will be saved.
#' @param .path_log
#' Character string specifying the path to the log file for error logging.
#' @param .verbose
#' Logical indicating whether to print progress messages to the console during processing.
#' Default is TRUE.
#'
#' @details
#' The function determines the appropriate download method based on the file extension:
#' \itemize{
#'   \item Text-based formats (html, htm, xml, xsd, txt) are downloaded as text
#'   \item Other formats (like PDF, ZIP) are downloaded as binary data
#' }
#'
#' The function creates the necessary directory structure if it doesn't exist.
#' If the target file already exists, the function returns silently without
#' re-downloading to avoid duplicate work.
#'
#' Error handling is implemented to log issues with HTTP requests or content
#' parsing while allowing the process to continue with other documents.
#'
#' @return No return value, called for side effects (downloading and saving a file).
#'
#' @keywords internal
download_url_request <- function(.url, .user, .path_out, .path_log, .verbose) {
  fs::dir_create(dirname(.path_out))
  if (file.exists(.path_out)) {
    return(invisible(NULL))
  }
  type_ <- ifelse(tools::file_ext(.url) %in% c("html", "htm", "xml", "xsd", "txt"), "text", "raw")

  result_ <- make_get_request(.url, .user)
  if (!check_error_request(result_, .verbose, .path_log)) {
    return(invisible(NULL))
  }
  content_ <- parse_content(result_, .type = type_)
  if (!check_error_content(content_, .verbose, .path_log)) {
    return(invisible(NULL))
  }

  if (type_ == "text") {
    try(write(content_, .path_out), silent = TRUE)
  } else {
    try(writeBin(content_, .path_out), silent = TRUE)
  }
}

#' Print Document Download Loop Status
#'
#' @description
#' Formats and prints processing status information for the document download loop.
#' This function provides feedback on the progress of SEC EDGAR document downloading.
#'
#' @param .yq
#' Character string representing the year-quarter being processed (e.g., "2023-1").
#' @param .typ
#' Character string representing the document type being processed (e.g., "10-K").
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
#' relative to the total batches to be processed, along with document type
#' and year-quarter information.
#'
#' @return No return value, called for side effects (console output).
#'
#' @keywords internal
print_download_loop <- function(.yq, .typ, .loop, .tot, .workers, .verbose) {
  nloop_ <- scales::comma(.loop * .workers)
  ntot_ <- scales::comma(.tot * .workers)
  msg_ <- paste0("Processing: ", .typ, " - ", .yq, " (", nloop_, "/", ntot_, ")")
  print_verbose(msg_, .verbose, "\r")
}

#' Prepare "To Be Processed" Document IDs Queue
#'
#' @description
#' Prepares a queuing system for SEC EDGAR documents to be downloaded in batches.
#' This function creates a temporary SQLite database to track which documents
#' need to be processed and assigns them to download batches.
#'
#' @param .dir
#' Character string specifying the root directory where SEC EDGAR data is stored.
#' @param .doc_ids
#' Character vector of document IDs to process. If NULL, all available documents
#' that haven't been previously downloaded will be processed.
#' @param .workers
#' Integer specifying the number of parallel workers for downloading.
#'
#' @return
#' A tibble with distinct combinations of DocType, YearQuarter, Seq, dir_tmp,
#' and dir_out, representing the batches to be processed.
#'
#' @keywords internal
get_tbp_docids <- function(.dir, .doc_ids, .workers) {
  lp_ <- get_directories(.dir)

  path_ <- lp_$DocumentData$Temporary$FilToBePrc
  table_ <- "tobeprocessed"
  try(fs::file_delete(path_), silent = TRUE)

  con_ <- DBI::dbConnect(RSQLite::SQLite(), path_)
  query0_ <- "CREATE TABLE tobeprocessed (DocID TEXT, CIK TEXT, DocType TEXT, YearQuarter REAL, Seq INTEGER, UrlDocument TEXT, dir_out TEXT)"
  DBI::dbExecute(con_, query0_)
  query1_ <- paste0("CREATE INDEX IF NOT EXISTS Seq", table_, " ON ", table_, "(Seq)")
  DBI::dbExecute(con_, query1_)
  query2_ <- paste0("CREATE INDEX IF NOT EXISTS YearQuarter", table_, " ON ", table_, "(YearQuarter)")
  DBI::dbExecute(con_, query2_)
  query3_ <- paste0("CREATE INDEX IF NOT EXISTS DocType", table_, " ON ", table_, "(DocType)")
  DBI::dbExecute(con_, query3_)

  fil_zips_ <- list_files(lp_$DocumentData$Original, .rec = TRUE)

  if (nrow(fil_zips_) == 0) {
    prc_ <- NA_character_
  } else {
    prc_ <- dplyr::bind_rows(purrr::map(
      .x = fil_zips_$path,
      .f = ~ dplyr::mutate(tibble::as_tibble(zip::zip_list(.x)), PathZIP = .x)
    )) %>%
      dplyr::mutate(DocID = fs::path_ext_remove(filename)) %>%
      dplyr::pull(DocID)
  }

  arr_doc_ <- arrow::open_dataset(lp_$DocLinks$DirMain$Links)
  if (!is.null(.doc_ids)) {
    arr_doc_ <- dplyr::filter(arr_doc_, DocID %in% .doc_ids)
  }
  arr_doc_ <- dplyr::collect(dplyr::filter(arr_doc_, !DocID %in% prc_))



  tab_ <- arr_doc_ %>%
    dplyr::mutate(DocExt = tools::file_ext(UrlDocument)) %>%
    dplyr::filter(!DocExt == "") %>%
    dplyr::left_join(
      y = dplyr::select(get("Table_DocTypesRaw"), Type = DocTypeRaw, DocTypeMod),
      by = "Type"
    ) %>%
    dplyr::group_by(DocTypeMod, YearQuarter) %>%
    dplyr::mutate(Seq = ceiling(dplyr::row_number() / .workers)) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(
      TypeSave = gsub("\\-|\\s", "", DocTypeMod),
      YqSave = gsub("\\.", "-", YearQuarter),
      dir_out = file.path(lp_$DocumentData$Original, paste0(TypeSave, "_", YqSave)),
      dir_tmp = file.path(dirname(lp_$DocumentData$Temporary[[1]]), paste0(TypeSave, "_", YqSave))
    ) %>%
    dplyr::select(DocID, CIK, DocType = DocTypeMod, YearQuarter, Seq, UrlDocument, dir_tmp, dir_out)


  DBI::dbWriteTable(con_, table_, tab_, overwrite = TRUE)
  DBI::dbDisconnect(con_)

  dplyr::distinct(tab_, DocType, YearQuarter, Seq, dir_tmp, dir_out)
}


if (FALSE) {
  devtools::load_all(".")
  # library(rGetEDGAR)
  user <- unname(ifelse(
    Sys.info()["sysname"] == "Darwin",
    "PetroParkerLosSpiderHombreABC12@Outlook.com",
    "TonyStarkIronManWeaponXYZ847263@Outlook.com"
  ))

  dir_debug <- fs::dir_create("../_package_debug/rGetEDGAR")
  lp_ <- get_directories(dir_debug)

  tab_ex10 <- dplyr::filter(Table_DocTypesRaw, DocTypeMod == "Exhibit 10")
  scales::comma(sum(tab_ex10$nDocs))
  .doc_ids <- arrow::open_dataset(lp_$DocLinks$DirMain$Links) %>%
    dplyr::filter(Type %in% tab_ex10$DocTypeRaw) %>%
    dplyr::distinct(DocID) %>%
    dplyr::collect() %>%
    dplyr::pull(DocID)
  scales::comma(length(.doc_ids))


  .dir <- dir_debug
  .user <- user
  .doc_ids <- .doc_ids
  .workers <- 10L
  .verbose <- TRUE


  edgar_get_document_links(
    .dir = dir_debug,
    .user = user,
    .hash_idx = .hash_idx,
    .workers = 5L,
    .verbose = TRUE
  )
}
