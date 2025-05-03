# Main Functions ------------------------------------------------------------------------------
#' Download SEC EDGAR Documents
#'
#' @description
#' Downloads and processes documents from SEC EDGAR based on specified document IDs.
#' This function handles downloading the original documents and creating parsed versions
#' for analysis.
#'
#' @param .dir
#' Character string specifying the directory where the downloaded data will be stored.
#' This directory should contain the necessary sub directory structure.
#' @param .user
#' Character string specifying the user agent to be used in HTTP requests to the SEC EDGAR server.
#' This should typically be an email address to comply with SEC's fair access policy.
#' @param .doc_ids
#' Character vector of DocID values to download. These IDs should correspond to document
#' identifiers in the SEC EDGAR system.
#' @param .keep_orig
#' Logical indicating whether to keep the original downloaded files after processing.
#' If TRUE, original files are compressed into ZIP archives. Default is TRUE.
#' @param .verbose
#' Logical indicating whether to print progress messages to the console during processing.
#' Default is TRUE.
#'
#' @return No return value, called for side effects (downloading and saving data files).
#'
#' @export
edgar_download_document <- function(.dir, .user, .doc_ids, .keep_orig = TRUE, .verbose = TRUE) {
  lp_ <- get_directories(.dir)
  msg_ <- "Determining Documents to Download ..."
  print_verbose(msg_, .verbose, .line = "\r")
  tab_links <- edgar_get_docs_to_download(.dir, .doc_ids)

  msg_ <- gsub("...", paste0("(", scales::comma(nrow(tab_links)), ")"), msg_, fixed = TRUE)
  print_verbose(msg_, .verbose, .line = "\n")
  nst_links <- tab_links %>%
    tidyr::nest(.by = c(TypeSave, YQSave)) %>%
    dplyr::mutate(
      DirOrig = file.path(lp_$DocumentData$Original, TypeSave, YQSave),
      PathZIP = paste0(DirOrig, ".zip")
    )

  for (i in seq_len(nrow(nst_links))) {
    use_links <- nst_links$data[[i]] %>%
      dplyr::mutate(
        nAll = dplyr::n(), nRow = dplyr::row_number(),
        cAll = scales::comma(nAll), cRow = scales::comma(nRow),
        Print = paste0(nst_links$TypeSave[i], ": ", nst_links$YQSave[i], " (", cRow, "/", cAll, ")"),
      ) %>%
      dplyr::arrange(nRow) %>%
      dplyr::select(-c(nAll, nRow, cAll, cRow))

    t0_ <- Sys.time()
    rate_ <- "0.00 Docs/Sec"
    for (j in seq_len(nrow(use_links))) {
      print_verbose(paste0(use_links$Print[j], ": ", rate_, "        "), .verbose, .line = "\r")
      if (j %% 10 == 0) {
        if (difftime(Sys.time(), t0_) <= 1.05) {
          Sys.sleep(1.05 - difftime(Sys.time(), t0_))
        }
        rate_ <- round(10 / as.numeric(difftime(Sys.time(), t0_)), 2)
        rate_ <- paste0(rate_, " Docs/Sec")
        t0_ <- Sys.time()
      }

      if (!file.exists(use_links$PathOrig[j])) {
        edgar_download_url_request(
          .url = use_links$UrlDocument[j],
          .user = .user,
          .path_out = use_links$PathOrig[j],
          .path_log = lp_$DocumentData$FilLogOriginal,
          .verbose = .verbose
        )
      }

      if (!file.exists(use_links$PathParse[j])) {
        edgar_parse_documents(
          .path_src = use_links$PathOrig[j],
          .path_out = use_links$PathParse[j]
        )
      }
    }

    if (.keep_orig) {
      if (file.exists(nst_links$PathZIP[i])) {
        zip::zip_append(
          zipfile = nst_links$PathZIP[i],
          files = use_links$PathOrig[file.exists(use_links$PathOrig)],
          compression_level = 9
        )
      } else {
        zip::zipr(
          zipfile = nst_links$PathZIP[i],
          files = use_links$PathOrig[file.exists(use_links$PathOrig)],
          compression_level = 9
        )
      }
    }


    try(fs::dir_delete(nst_links$DirOrig[i]), silent = TRUE)
  }
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
edgar_download_url_request <- function(.url, .user, .path_out, .path_log, .verbose) {
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

#' Determine Documents to Download from SEC EDGAR
#'
#' @description
#' Prepares a list of SEC EDGAR documents to download based on provided document IDs.
#' This function filters the document links database to identify which documents need
#' to be downloaded and determines appropriate file paths for saving.
#'
#' @param .dir
#' Character string specifying the directory where the downloaded data will be stored.
#' This directory should contain the necessary sub directory structure.
#' @param .doc_ids
#' Character vector of DocID values to download. These IDs should correspond to document
#' identifiers in the SEC EDGAR system.
#'
#' @details
#' The function performs the following operations:
#' \itemize{
#'   \item Queries the document links dataset to find matching document IDs
#'   \item Joins with document type mapping table to standardize document types
#'   \item Creates file paths for both original and parsed document storage
#'   \item Filters out documents that have already been processed
#' }
#'
#' Document types are standardized according to a predefined mapping table,
#' and output paths are generated using a consistent directory structure organized
#' by document type and year-quarter period.
#'
#' @return
#' A tibble containing document metadata and file paths for documents that need
#' to be downloaded, with the following columns:
#' \itemize{
#'   \item YearQuarter: Original filing year and quarter as numeric (e.g., 2023.1)
#'   \item DocTypeMod: Standardized document type
#'   \item DocID: Unique document identifier
#'   \item UrlDocument: URL to download the document
#'   \item YQSave: Year-quarter for directory structure (e.g., 2023-1)
#'   \item TypeSave: Standardized document type for directory structure
#'   \item DocExt: Document file extension
#'   \item PathOrig: File path for original document
#'   \item PathParse: File path for parsed document
#' }
#'
#' @keywords internal
edgar_get_docs_to_download <- function(.dir, .doc_ids) {
  lp_ <- get_directories(.dir)
  doc_types_ <- dplyr::select(get("Table_DocTypesRaw"), DocTypeRaw, DocTypeMod)

  tmp_links_ <- arrow::open_dataset(lp_$DocLinks$DirMain$Links) %>%
    dplyr::filter(DocID %in% .doc_ids) %>%
    dplyr::select(YearQuarter, DocTypeRaw = Type, DocID, UrlDocument) %>%
    dplyr::inner_join(
      y = arrow::as_arrow_table(doc_types_),
      by = dplyr::join_by(DocTypeRaw)
    ) %>%
    dplyr::collect()

  tmp_links_ %>%
    dplyr::mutate(
      YQSave = gsub(".", "-", YearQuarter, fixed = TRUE),
      TypeSave = gsub(" ", "", DocTypeMod, fixed = TRUE),
      TypeSave = gsub("/", "", TypeSave, fixed = TRUE),
      DocExt = tolower(tools::file_ext(UrlDocument))
    ) %>%
    dplyr::filter(!DocExt == "") %>%
    dplyr::select(YearQuarter, DocTypeMod, DocID, UrlDocument, YQSave, TypeSave, DocExt) %>%
    dplyr::mutate(
      PathOrig = file.path(lp_$DocumentData$Original, TypeSave, YQSave, paste0(DocID, ".", DocExt)),
      PathParse = file.path(lp_$DocumentData$Parsed, TypeSave, YQSave, paste0(DocID, ".parquet"))
    ) %>%
    dplyr::filter(!file.exists(PathParse))
}

#' Parse Downloaded SEC EDGAR Documents
#'
#' @description
#' Processes downloaded SEC EDGAR documents into structured data format for analysis.
#' This function handles different file types (HTML, text, PDF) and extracts
#' content for further processing.
#'
#' @param .path_src
#' Character string specifying the file path of the source document to parse.
#' @param .path_out
#' Character string specifying the file path where the parsed document will be saved.
#'
#' @details
#' The function performs different parsing operations based on the document type:
#' \itemize{
#'   \item HTML/HTM files: Extracts text using HTML parsing functions
#'   \item TXT/XML/XSD files: Reads raw content directly
#'   \item PDF files: Extracts text if not encrypted, reports error otherwise
#'   \item Other formats: Reports unsupported format error
#' }
#'
#' For all successfully parsed documents, the function standardizes text format
#' to facilitate consistent analysis. The output is saved as a Parquet file with
#' compression to minimize storage requirements.
#'
#' If parsing fails, the function will still create an output file containing
#' error information, ensuring that the processing pipeline can continue.
#'
#' @return
#' A tibble containing the parsed document with the following columns:
#' \itemize{
#'   \item DocID: Document identifier derived from the source path
#'   \item HTML: Original document content
#'   \item TextRaw: Raw extracted text
#'   \item TextMod: Standardized text after processing
#'   \item DocExt: Document file extension
#'   \item ErrParse: Logical indicating whether parsing encountered an error
#'   \item MsgParse: Error message if parsing failed, NA otherwise
#' }
#'
#' The function also writes this tibble to the specified output path in Parquet format.
#'
#' @keywords internal
edgar_parse_documents <- function(.path_src, .path_out) {
  fs::dir_create(dirname(.path_out))
  file_ext_ <- tolower(tools::file_ext(.path_src))

  if (file_ext_ %in% c("txt", "htm", "html", "xml", "xsd")) {
    orig_ <- try(readChar(.path_src, file.info(.path_src)$size), silent = TRUE)
  } else if (file_ext_ %in% c("pdf")) {
    orig_ <- try(readtext::readtext(.path_src)[["text"]], silent = TRUE)
  } else {
    orig_ <- try(stop(paste0(file_ext_, "is not supported"), call. = FALSE))
  }

  if (!inherits(orig_, "try-error")) {
    if (file_ext_ %in% c("htm", "html")) {
      raw_ <- read_html(orig_)
    } else {
      raw_ <- orig_
    }

    mod_ <- standardize_text(raw_)

    out_ <- tibble::tibble(
      DocID = fs::path_ext_remove(basename(.path_src)),
      HTML = orig_,
      TextRaw = raw_,
      TextMod = mod_,
      DocExt = file_ext_,
      ErrParse = FALSE,
      MsgParse = NA_character_
    )
  } else {
    out_ <- tibble::tibble(
      DocID = fs::path_ext_remove(basename(.path_src)),
      HTML = NA_character_,
      TextRaw = NA_character_,
      TextMod = NA_character_,
      DocExt = file_ext_,
      ErrParse = TRUE,
      MsgParse = paste(as.character(orig_), collapse = "")
    )
  }

  arrow::write_parquet(out_, .path_out, compression = "gzip", compression_level = 9)

  return(out_)
}


# DeBug ---------------------------------------------------------------------------------------
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
  docs <- arrow::open_dataset(lp_$DocLinks$DirMain$Links) %>%
    dplyr::filter(Type %in% tab_ex10$DocTypeRaw) %>%
    dplyr::distinct(DocID) %>%
    dplyr::collect() %>%
    dplyr::pull(DocID)
  scales::comma(length(docs))

  .dir <- dir_debug
  .user <- user
  .doc_ids <- sample(docs, size = 100)
  .verbose <- TRUE

  edgar_download_document(
    .dir = dir_debug,
    .user = user,
    .doc_ids = sample(docs, size = 100),
    .keep_orig = FALSE,
    .verbose = TRUE
  )







  tab_ <- arrow::read_parquet("/Users/matthiasuckert/RProjects/Packages/_package_debug/rGetEDGAR/DocumentData/Parsed/Exhibit10/2000-2/0001064863-73eaf7478434179fae22e342b19d5559.parquet")
}
