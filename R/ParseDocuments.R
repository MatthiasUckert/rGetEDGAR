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
edgar_parse_documents <- function(.dir, .doc_ids = NULL, .workers = 1L, .verbose = TRUE) {
  lp_ <- get_directories(.dir)
  prc_ <- list_files(lp_$DocumentData$Parsed, .rec = TRUE)[["doc_id"]]
  if (length(prc_) == 0) {
    prc_ <- NA_character_
  }

  zip_ <- dplyr::bind_rows(purrr::map(
    .x = list_files(lp_$DocumentData$Original, .rec = TRUE)[["path"]],
    .f = ~ dplyr::mutate(tibble::as_tibble(zip::zip_list(.x)), PathZIP = .x)
  )) %>%
    dplyr::mutate(DocID = fs::path_ext_remove(filename)) %>%
    dplyr::filter(!DocID %in% prc_) %>%
    dplyr::select(DocID, PathZIP, filename) %>%
    dplyr::mutate(
      FileExt = tolower(tools::file_ext(filename)),
      dir_out = fs::path_ext_remove(basename(PathZIP)),
      dir_out = gsub("\\d{4}-\\d{2}-\\d{2}-\\d{2}-\\d{2}-\\d{2}_", "", dir_out),
      dir_out = gsub("_", "/", dir_out),
      path_out = file.path(lp_$DocumentData$Parsed, dir_out, paste0(DocID, ".parquet"))
    ) %>%
    dplyr::filter(FileExt %in% c("txt", "htm", "html", "xml", "xsd"))

  if (!is.null(.doc_ids)) {
    zip_ <- dplyr::filter(zip_, DocID %in% .doc_ids)
  }

  if (nrow(zip_) > 0) {
    future::plan("multisession", workers = .workers)
    furrr::future_walk(
      .x = split(zip_, zip_$DocID),
      .f = parse_files,
      .options = furrr::furrr_options(seed = TRUE),
      .progress = .verbose
    )
    future::plan("default")
  }
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
  if (file.exists(.tab_row$path_out)) {
    return(invisible(NULL))
  }

  fs::dir_create(dirname(.tab_row$path_out))
  dir_tmp_ <- tempdir()
  fil_tmp_ <- file.path(dir_tmp_, .tab_row$filename)
  zip::unzip(
    zipfile = .tab_row$PathZIP,
    files = .tab_row$filename,
    exdir = dir_tmp_,
    overwrite = TRUE
  )

  out_ <- try(tibble::tibble(
    DocID = .tab_row$DocID,
    HTML = readChar(fil_tmp_, file.info(fil_tmp_)$size)
  ) %>%
    dplyr::mutate(
      TextRaw = purrr::map_chr(HTML, read_html),
      TextMod = standardize_text(TextRaw),
      nWords = stringi::stri_count_words(TextMod),
      nChars = nchar(TextMod)
    ), silent = TRUE)

  if (!inherits(out_, "try-error")) {
    arrow::write_parquet(out_, .tab_row$path_out)
  }
  try(fs::file_delete(fil_tmp_), silent = TRUE)
  try(fs::dir_delete(dir_tmp_), silent = TRUE)
}


# Debug ---------------------------------------------------------------------------------------
if (FALSE) {
  .tab_row <- zip_[1, ]
  devtools::load_all(".")

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
