# Main Functions ------------------------------------------------------------------------------
#' Download SEC EDGAR Documents
#'
#' @description
#' Downloads and processes documents from the SEC EDGAR system based on a set of document IDs.
#' This function orchestrates the download of original filings and the creation of parsed versions
#' for downstream analysis. Downloads are performed in parallel using a configurable number
#' of workers. Original files can optionally be retained and compressed into ZIP archives.
#'
#' @param .dir
#'   Character string. Path to the base directory where all downloaded data (original and parsed)
#'   will be stored. Must contain the expected subdirectory structure (as returned by \code{get_directories()}).
#' @param .user
#'   Character string. The \emph{User-Agent} (typically an email address) to send in HTTP
#'   requests to the SEC EDGAR servers, to comply with the SEC’s fair access policy.
#' @param .doc_ids
#'   Character vector. A vector of \code{DocID} identifiers corresponding to filings in the
#'   SEC EDGAR database. Only those documents whose parsed version does not already exist
#'   (in the \code{Parsed} folder) will be fetched.
#' @param .keep_orig
#'   Logical (default \code{TRUE}). If \code{TRUE}, the function will compress the downloaded original
#'   files for each \code{TypeSave}/\code{YQSave} grouping into a ZIP archive at the end of processing.
#'   If \code{FALSE}, original files are deleted after parsing.
#' @param .workers
#'   Integer (default \code{1L}). The number of parallel workers to use when parsing documents.
#'   Passed to \code{\link[future]{plan}("multisession", workers = ...)}. Setting this to
#'   greater than 1 will parse multiple files concurrently.
#' @param .verbose
#'   Logical (default \code{TRUE}). If \code{TRUE}, progress messages (e.g., download rates,
#'   messages per file) will be printed to the console. If \code{FALSE}, all internal messages
#'   are suppressed.
#'
#' @return Called for side effects: original files are downloaded
edgar_download_document <- function(.dir, .user, .doc_ids, .keep_orig = TRUE, .workers = 1L, .verbose = TRUE) {
  lp_ <- get_directories(.dir)
  msg_ <- "Determining Documents to Download ..."
  print_verbose(msg_, .verbose, .line = "\r")
  tab_links <- edgar_get_docs_to_download(.dir, .doc_ids)
  .nlinks <- scales::comma(nrow(tab_links))

  msg_ <- gsub("...", paste0("(", .nlinks, ")"), msg_, fixed = TRUE)
  print_verbose(msg_, .verbose, .line = "\n")
  nst_links <- tab_links %>%
    tidyr::nest(.by = c(TypeSave, YQSave)) %>%
    dplyr::mutate(
      DirOrig = file.path(lp_$DocumentData$Original, TypeSave, YQSave),
      PathZIP = paste0(DirOrig, ".zip")
    ) %>%
    dplyr::arrange(TypeSave, YQSave)

  future::plan("multisession", workers = .workers)
  for (i in seq_len(nrow(nst_links))) {
    use_links <- edgar_prepare_print_message(nst_links$data[[i]])
    ini_ <- list(T0 = Sys.time(), Rate = "0.00 Docs/Sec")

    # Download Documents -- -- -- -- --
    for (j in seq_len(nrow(use_links))) {
      msg_ <- use_links$Print[j]
      ini_ <- edgar_print_download_rate(msg_, j, ini_$T0, ini_$Rate, .verbose)

      if (!file.exists(use_links$PathOrig[j])) {
        edgar_download_url_request(
          .url = use_links$UrlDocument[j],
          .user = .user,
          .path_out = use_links$PathOrig[j],
          .path_log = lp_$DocumentData$FilLogOriginal,
          .verbose = .verbose
        )
      }
    }

    # Parse Documents -- -- -- -- --
    print_verbose(msg_, .verbose, .line = "\n")
    use_links <- dplyr::filter(use_links, file.exists(PathOrig))
    furrr::future_walk2(
      .x = use_links$PathOrig,
      .y = use_links$PathParse,
      .f = edgar_parse_documents,
      .options = furrr::furrr_options(seed = TRUE),
      .progress = .verbose
    )

    # Zip or Delete Original Documents -- -- -- -- --
    if (.keep_orig) {
      if (file.exists(nst_links$PathZIP[i])) {
        zip::zip_append(
          zipfile = nst_links$PathZIP[i],
          files = use_links$PathOrig[file.exists(use_links$PathOrig)],
          compression_level = 9
        )
      } else {
        try(fs::dir_delete(nst_links$DirOrig[i]), silent = TRUE)
      }
    }
  }
  future::plan("default")
  on.exit(future::plan("default"))
}


# Helper Functions ----------------------------------------------------------------------------
#' Prepare Console Print Messages for EDGAR Download Progress
#'
#' @description
#' Creates a new column \code{Print} in the input tibble that formats the download progress
#' message for each document. It calculates the total number of rows (\code{nAll}), the
#' current row index (\code{nRow}), and uses these to build a string of the form
#' \code{"<TypeSave>: <YQSave> (<current>/<total>"} for printing during the download loop.
#'
#' @param .tab
#'   A tibble (or data frame) that must contain at least the columns:
#'   \itemize{
#'     \item \code{TypeSave}: Character, indicating the standardized document type.
#'     \item \code{YQSave}: Character, indicating the year-quarter (e.g., \code{"2023-1"}).
#'   }
#'   Typically, \code{.tab} is the output of \code{edgar_get_docs_to_download()} grouped or nested
#'   by \code{TypeSave} and \code{YQSave}.
#'
#' @return
#'   A tibble identical to \code{.tab} but with an additional column:
#'   \itemize{
#'     \item \code{Print}: Character, formatted progress message for console printing.
#'   }
#'
#' @keywords internal
edgar_prepare_print_message <- function(.tab) {
  .tab %>%
    dplyr::mutate(
      nAll = dplyr::n(), nRow = dplyr::row_number(),
      cAll = scales::comma(nAll), cRow = scales::comma(nRow),
      Print = paste0(TypeSave, ": ", YQSave, " (", cRow, "/", cAll, ")"),
    ) %>%
    dplyr::arrange(nRow) %>%
    dplyr::select(-c(nAll, nRow, cAll, cRow))
}

#' Print Download Rate and Update Timing State
#'
#' @description
#' Prints a progress message containing the current download rate (documents per second)
#' to the console and returns an updated timing state. Every 10th call, the function
#' enforces a small sleep (if necessary) to ensure that no more than 10 downloads
#' occur per second. On other calls, it simply reprints the previous rate without delay.
#'
#' @param .msg_ini
#'   Character string. The base message to print (e.g., \code{"10-K: 2023-1 (3/15)"}).
#'   This is prefixed before the rate in the printed output.
#' @param .counter
#'   Integer. The index of the current download (within a loop). Used to determine when
#'   to recalculate the download rate (every 10 downloads).
#' @param .t0
#'   POSIXct. Timestamp recorded at the last rate calculation (or the start of the batch).
#'   Used to compute the elapsed time over 10 downloads.
#' @param .rate
#'   Character string. The previous rate message (e.g., \code{"0.00 Docs/Sec"}). If this
#'   is not the 10th download, the old rate is reused.
#' @param .verbose
#'   Logical. If \code{TRUE}, the function will print to the console; if \code{FALSE},
#'   it will suppress printing entirely.
#'
#' @return
#'   A named list with two elements:
#'   \describe{
#'     \item{\code{T0}}{POSIXct. The new reference time (set to \code{Sys.time()}).}
#'     \item{\code{Rate}}{Character. The computed (or reused) rate string, e.g., \code{"10.00 Docs/Sec"}.}
#'   }
#'
#' @keywords internal
edgar_print_download_rate <- function(.msg_ini, .counter, .t0, .rate, .verbose) {
  if (.counter %% 10 == 0) {
    if (difftime(Sys.time(), .t0) <= 1.025) {
      Sys.sleep(1.025 - difftime(Sys.time(), .t0))
    }
    rate_ <- round(10 / as.numeric(difftime(Sys.time(), .t0)), 2)
    rate_ <- paste0(rate_, " Docs/Sec")
  } else {
    rate_ <- .rate
  }

  print_verbose(paste0(.msg_ini, ": ", rate_, "        "), .verbose, .line = "\r")
  list(T0 = Sys.time(), Rate = rate_)
}

#' Download a Single SEC Document from a URL
#'
#' @description
#' Downloads a single SEC EDGAR document from a specified URL and writes it to disk.
#' Automatically chooses between text-based vs. binary download based on the file extension.
#' Creates any needed parent directories. If the target file already exists, the function
#' exits silently to avoid re-downloading.
#'
#' @param .url
#'   Character string. The full URL of the document to fetch (e.g., an HTML, XML, PDF,
#'   or TXT file hosted on the SEC EDGAR site).
#' @param .user
#'   Character string. The \emph{User-Agent} to include in the HTTP GET request. Should
#'   comply with SEC guidelines (typically an email address).
#' @param .path_out
#'   Character string. The local file path (including filename and extension) where the
#'   downloaded content will be saved. Any non-existent parent directories will be created.
#' @param .path_log
#'   Character string. Path to a log file used to record errors (e.g., HTTP errors or content
#'   parsing failures). If an HTTP request or content parse fails, details are appended here.
#' @param .verbose
#'   Logical (default \code{TRUE}). If \code{TRUE}, errors during download or parse failures
#'   will be printed to the console in addition to being logged.
#' @return
#'   Invisible \code{NULL}. Side effects include writing a new file at \code{.path_out}
#'   (unless the file already exists) and appending any error information to the log file.
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


#' Determine SEC EDGAR Documents Needing Download
#'
#' @description
#' Builds a tibble of metadata and file paths for all requested document IDs (\code{.doc_ids})
#' that have not yet been parsed. It reads the EDGAR “links” dataset (via Arrow), filters
#' for the specified \code{DocID}s, standardizes document types with a lookup table, and
#' constructs filesystem paths for the original and parsed files. Only rows whose parsed
#' Parquet file does not already exist are returned.
#'
#' @param .dir
#'   Character string. Base directory of the SEC EDGAR dataset. Used to locate:
#'   \itemize{
#'     \item The Arrow dataset directory of “link” metadata (via \code{lp_$DocLinks$DirMain$Links})
#'     \item The root folders for \code{DocumentData$Original} and \code{DocumentData$Parsed}
#'   }
#' @param .doc_ids
#'   Character vector. One or more \code{DocID} values to look up in the “link” dataset.
#'   Only those IDs that have no existing parsed file will appear in the returned tibble.
#' @return
#'   A tibble with the following columns:
#'   \describe{
#'     \item{\code{YearQuarter}}{Numeric (e.g., 2023.1) from the “links” dataset.}
#'     \item{\code{DocTypeMod}}{Character. Standardized document type (e.g., “10-K”).}
#'     \item{\code{DocID}}{Character. The unique document identifier.}
#'     \item{\code{UrlDocument}}{Character. The HTTP URL where the original filing sits.}
#'     \item{\code{YQSave}}{Character (e.g., “2023-1”). Used in directory naming.}
#'     \item{\code{TypeSave}}{Character. Cleaned \code{DocTypeMod} with no spaces or “/”.}
#'     \item{\code{DocExt}}{Character. Lower-case file extension (e.g., “html”, “pdf”).}
#'     \item{\code{PathOrig}}{Character. Full path where the downloaded original will be stored.}
#'     \item{\code{PathParse}}{Character. Full path where the parsed Parquet file will be written.}
#'   }
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

#' Parse a Downloaded SEC EDGAR Document to Parquet
#'
#' @description
#' Reads a downloaded SEC EDGAR document (HTML, text, or PDF) from \code{.path_src}, extracts
#' its raw content, standardizes the text, and writes the result as a compressed Parquet file
#' at \code{.path_out}. If parsing fails (e.g., due to a corrupted PDF), an “error” record
#' is written instead, containing the error message.
#'
#' @param .path_src
#'   Character string. File path to the source document to parse. Supported extensions:
#'   \code{txt}, \code{htm}, \code{html}, \code{xml}, \code{xsd}, \code{pdf}. If the file
#'   does not exist or the parsed Parquet file already exists at \code{.path_out}, the
#'   function returns invisibly.
#' @param .path_out
#'   Character string. File path (including “.parquet”) where the parsed output will be written.
#'   Parent directories are created if missing.
#'
#' @return
#'   Invisible \code{NULL}. For each call, a compressed Parquet file is written to disk
#'   at \code{.path_out}. If parsing fails, a one-row Parquet file is still written containing
#'   the \code{ErrParse = TRUE} record.
#'
#' @keywords internal
edgar_parse_documents <- function(.path_src, .path_out) {
  fs::dir_create(dirname(.path_out))
  file_ext_ <- tolower(tools::file_ext(.path_src))
  if (file.exists(.path_out)) {
    return(invisible(NULL))
  }

  quiet <- function(x) {
    sink(tempfile())        # divert stdout to a temp file
    on.exit(sink())         # ensure we restore stdout when done
    invisible(force(x))     # force evaluation of x, return invisibly
  }

  if (file_ext_ %in% c("txt", "htm", "html", "xml", "xsd")) {
    orig_ <- try(readChar(.path_src, file.info(.path_src)$size), silent = TRUE)
  } else if (file_ext_ %in% c("pdf")) {
    orig_ <- quiet(try(readtext::readtext(.path_src)[["text"]], silent = TRUE))
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
}


# DeBug ---------------------------------------------------------------------------------------
if (FALSE) {
  devtools::load_all(".")
  user <- "TonyStarkIronManWeaponXYZ847263@Outlook.com"
  dir_debug <- fs::dir_create("../_package_debug/rGetEDGAR")
  lp_ <- get_directories(dir_debug)

  tab_10ks <- dplyr::filter(Table_DocTypesRaw, DocTypeMod == "10-K")
  tab_docs <- arrow::open_dataset(lp_$DocLinks$DirMain$Links) %>%
    dplyr::filter(Type %in% tab_10ks$DocTypeRaw) %>%
    dplyr::collect() %>%
    dplyr::distinct(DocID, .keep_all = TRUE) %>%
    dplyr::filter(grepl("ix?doc=", UrlDocument, fixed = TRUE)) %>%
    dplyr::arrange(Type, YearQuarter)

  scales::comma(sum(tab_10ks$nDocs))
  scales::comma(nrow(tab_docs))

  .dir <- dir_debug
  .user <- user
  .doc_ids <- tab_docs$DocID[1:1000]
  .keep_orig <- FALSE
  .workers <- 10L
  .verbose <- TRUE



  edgar_download_document(
    .dir = dir_debug,
    .user = user,
    .doc_ids = "0000021076-2b3e3582334324bbc0d77093ba366ca2",
    .keep_orig = FALSE,
    .verbose = TRUE
  )
}
