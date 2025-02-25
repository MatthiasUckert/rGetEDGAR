#' Set Up Directory Structure
#'
#' @description
#' Creates and returns the directory structure for storing EDGAR data.
#'
#' @param .dir Character string specifying the base directory
#'
#' @return A list containing paths for MasterIndex, DocumentLinks, and DocumentData
#'
#' @export
get_directories <- function(.dir) {
  dir_ <- fs::dir_create(.dir)
  dir_master_ <- fs::dir_create(file.path(dir_, "MasterIndex"))
  dir_landing_ <- fs::dir_create(file.path(dir_, "LandingPage"))
  dir_links_ <- fs::dir_create(file.path(dir_, "DocumentLinks"))
  dir_temp_ <- fs::dir_create(file.path(dir_, "Temporary"))
  dir_log_ <- fs::dir_create(file.path(dir_, "Logs"))

  list(
    MasterIndex = list(
      Sqlite = file.path(dir_master_, "MasterIndex.sqlite"),
      # Parquet = file.path(dir_master_, "MasterIndex.parquet")
      Parquet = fs::dir_create(file.path(dir_master_, "MasterIndex"))
    ),
    LandingPage = list(
      Sqlite = file.path(dir_landing_, "LandingPage.sqlite"),
      # Parquet = file.path(dir_landing_, "LandingPage.parquet"),
      Parquet = fs::dir_create(file.path(dir_landing_, "LandingPage")),
      BackUps = fs::dir_create(file.path(dir_landing_, "LandingPageBackUps"))
    ),
    DocumentLinks = list(
      Sqlite = file.path(dir_links_, "DocumentLinks.sqlite"),
      # Parquet = file.path(dir_links_, "DocumentLinks.parquet"),
      Parquet = fs::dir_create(file.path(dir_links_, "DocumentLinks")),
      BackUps = fs::dir_create(file.path(dir_links_, "DocumentLinksBackUps"))
    ),
    DocumentData = list(
      Original = fs::dir_create(file.path(dir_, "DocumentData", "Original")),
      Parsed = fs::dir_create(file.path(dir_, "DocumentData", "Parsed")),
      Additional = fs::dir_create(file.path(dir_, "DocumentData", "Additional"))
    ),
    Temporary = list(
      DocumentLinks = fs::dir_create(file.path(dir_temp_, "TemporaryDocumentLinks")),
      DocumentData = fs::dir_create(file.path(dir_temp_, "TemporaryDocumentData")),
      DocumentParsing = fs::dir_create(file.path(dir_temp_, "TemporaryDocumentParsing"))
    ),
    Logs = list(
      MasterIndex = file.path(dir_log_, "LogMasterIndex.csv"),
      DocumentLinks = file.path(dir_log_, "LogDocumentLinks.csv"),
      DocumentData = file.path(dir_log_, "LogDocumentData.csv")
    )
  )
}


#' Read SEC EDGAR Master Index Data
#'
#' @description
#' Reads and filters the master index data from the stored Parquet files.
#' The master index contains metadata about all SEC filings.
#'
#' @param .dir Character string specifying the directory where the data is stored
#' @param .path Path to Specific File
#' @param .from Numeric value specifying the start year.quarter (e.g., 2020.1 for Q1 2020).
#'             If NULL, defaults to 1993.1
#' @param .to Numeric value specifying the end year.quarter.
#'           If NULL, defaults to current quarter
#' @param .ciks Character vector of CIK numbers to filter for specific companies
#' @param .formtypes Character vector of FormTypes
#' @param .collect Logical indicating whether to collect the data into memory (TRUE)
#'                or return an Arrow Dataset (FALSE)
#'
#' @return If .collect is TRUE, returns a tibble with master index data.
#'         If .collect is FALSE, returns an Arrow Dataset for delayed computation.
#'
#' @details
#' The returned data includes:
#' \itemize{
#'   \item CIK: Central Index Key
#'   \item CompanyName: Name of the company
#'   \item FormType: Type of SEC form
#'   \item DateFiled: Filing date
#'   \item UrlFullText: URL to the full text document
#'   \item UrlIndexPage: URL to the filing index page
#' }
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Read all filings for Apple Inc from 2020
#' data <- edgar_read_master_index(
#'   .dir = "edgar_data",
#'   .from = 2020.1,
#'   .to = 2020.4,
#'   .ciks = "0000320193"
#' )
#'
#' # Get Arrow Dataset for delayed computation
#' dataset <- edgar_read_master_index(
#'   .dir = "edgar_data",
#'   .collect = FALSE
#' )
#' }
edgar_read_master_index <- function(.dir, .path = NULL, .from = NULL, .to = NULL, .ciks = NULL, .formtypes = NULL, .collect = TRUE) {
  params_ <- get_edgar_params(.from, .to, .ciks, .formtypes)

  if (!is.null(.path)) {
    tab_fils <- dplyr::filter(list_data(.dir), MasterIndex == .path)
    msg_ <- paste0("MasterIndex File: '", basename(.path), "' does not exist")
  } else {
    tab_fils <- list_data(.dir)
    msg_ <- paste0("MasterIndex Folder is empty")
  }
  if (nrow(tab_fils) == 0) stop(msg_, call. = FALSE)

  arr_ <- filter_edgar_data(arrow::open_dataset(.path), params_)

  if (.collect) {
    return(dplyr::collect(arr_))
  } else {
    return(arr_)
  }
}

#' Read SEC EDGAR Document Links Data
#'
#' @description
#' Reads and filters the document links data from stored Parquet files.
#' Document links contain metadata about individual documents within each filing.
#'
#' @param .dir Character string specifying the directory where the data is stored
#' @param .path Path to Specific File
#' @param .from Numeric value specifying the start year.quarter (e.g., 2020.1 for Q1 2020).
#'             If NULL, defaults to 1993.1
#' @param .to Numeric value specifying the end year.quarter.
#'           If NULL, defaults to current quarter
#' @param .ciks Character vector of CIK numbers to filter for specific companies
#' @param .formtypes Character vector of form types to filter (e.g., "10-K", "10-Q")
#' @param .doctypes Character vector of document types to filter (e.g., "10-K", "10-Q")
#' @param .collect Logical indicating whether to collect the data into memory (TRUE)
#'                or return an Arrow Dataset (FALSE)
#'
#' @return If .collect is TRUE, returns a tibble with document links data.
#'         If .collect is FALSE, returns an Arrow Dataset for delayed computation.
#'
#' @details
#' The returned data includes:
#' \itemize{
#'   \item HashDocument: Unique identifier for the document
#'   \item CIK: Central Index Key
#'   \item Type: Document type (e.g., 10-K, 10-Q)
#'   \item Description: Document description
#'   \item Size: Document size
#'   \item UrlDocument: URL to the actual document
#' }
#'
#' @export
edgar_read_document_links <- function(.dir, .path = NULL, .from = NULL, .to = NULL, .ciks = NULL, .formtypes = NULL, .doctypes = NULL, .collect = TRUE) {
  if (!is.null(.path)) {
    tab_fils <- dplyr::filter(list_data(.dir), DocumentLinks == .path)
    msg_ <- paste0("DocumentLinks File: '", basename(.path), "' does not exist")
  } else {
    tab_fils <- list_data(.dir)
    msg_ <- paste0("DocumentLinks Folder is empty")
  }
  if (nrow(tab_fils) == 0) stop(msg_, call. = FALSE)

  idx_ <- arrow::open_dataset(tab_fils$MasterIndex) %>%
    filter_edgar_data(get_edgar_params(.from, .to, .ciks, .formtypes)) %>%
    dplyr::distinct(HashIndex) %>%
    dplyr::collect() %>%
    dplyr::pull()

  arr_ <- arrow::open_dataset(tab_fils$DocumentLinks[!is.na(tab_fils$DocumentLinks)]) %>%
    dplyr::filter(HashIndex %in% idx_) %>%
    filter_edgar_data(.params = get_edgar_params(.doctypes = .doctypes))


  if (.collect) {
    return(dplyr::collect(arr_))
  } else {
    return(arr_)
  }

}



#' Standardize String Content
#'
#' @description
#' Standardizes string content by removing extra whitespace and converting to ASCII.
#'
#' @param .str Character string to standardize
#'
#' @return Standardized character string
#'
#' @keywords internal
standardize_string <- function(.str) {
  str_ <- .str
  str_ <- stringi::stri_replace_all_regex(str_, "([[:blank:]|[:space:]])+", " ")
  str_ <- stringi::stri_trans_general(str_, "Latin-ASCII")
  str_ <- trimws(str_)
  return(str_)
}


#' Create standardization options for text processing
#'
#' @param .rm_punct Remove punctuation (default: TRUE)
#' @param .rm_num Remove numbers (default: FALSE)
#' @param .rm_space Remove extra spaces (default: TRUE)
#' @param .to_ascii Convert to ASCII (default: TRUE)
#' @param .to_upper Convert to uppercase (default: TRUE)
#' @param .rm_html Remove HTML tags (default: FALSE)
#' @param .trim Trim whitespace (default: TRUE)
#' @param .rm_special Remove special characters (default: TRUE)
#' @return List of standardization options
#' @export
standardize_options <- function(
    .rm_punct = TRUE,
    .rm_num = FALSE,
    .rm_space = TRUE,
    .to_ascii = TRUE,
    .to_upper = TRUE,
    .rm_html = FALSE,
    .trim = TRUE,
    .rm_special = TRUE
) {
  list(
    remove_punctuation = .rm_punct,
    remove_numbers = .rm_num,
    remove_extra_spaces = .rm_space,
    convert_to_ascii = .to_ascii,
    to_uppercase = .to_upper,
    remove_html = .rm_html,
    trim_whitespace = .trim,
    remove_special_chars = .rm_special
  )
}

#' Standardize text with configurable options using stringi
#'
#' @param .str Input text string
#' @param .options Options list from standardize_options()
#' @return Processed text string
#' @export
standardize_text <- function(.str, .options = standardize_options()) {
  str_ <- .str

  # Remove HTML tags if specified
  if (.options$remove_html) {
    str_ <- stringi::stri_replace_all_regex(str_, "<[^>]*>", " ")
  }

  # Remove punctuation if specified
  if (.options$remove_punctuation) {
    str_ <- stringi::stri_replace_all_regex(str_, "\\p{P}", " ")
  }

  # Remove numbers if specified
  if (.options$remove_numbers) {
    str_ <- stringi::stri_replace_all_regex(str_, "\\p{N}", " ")
  }

  # Remove special characters if specified
  if (.options$remove_special_chars) {
    str_ <- stringi::stri_replace_all_regex(str_, "[^\\p{L}\\p{N}\\s]", " ")
  }

  # Standardize whitespace if specified
  if (.options$remove_extra_spaces) {
    str_ <- stringi::stri_replace_all_regex(str_, "\\s+", " ")
  }

  # Convert to ASCII if specified
  if (.options$convert_to_ascii) {
    str_ <- stringi::stri_trans_general(str_, "Latin-ASCII")
  }

  # Convert to uppercase if specified
  if (.options$to_uppercase) {
    str_ <- stringi::stri_trans_toupper(str_)
  }

  # Trim whitespace if specified
  if (.options$trim_whitespace) {
    str_ <- stringi::stri_trim_both(str_)
  }

  return(str_)
}



#' Create HTML reading options
#'
#' @param .recover Logical; should parser try to recover from errors? (default: TRUE)
#' @param .huge Logical; optimize for huge files? (default: TRUE)
#' @param .noerror Logical; suppress error messages? (default: FALSE)
#' @param .nowarning Logical; suppress warning messages? (default: FALSE)
#' @param .encoding Character encoding to use (default: "UTF-8")
#' @return List of HTML reading options
#' @export
html_options <- function(
    .recover = TRUE,
    .huge = TRUE,
    .noerror = FALSE,
    .nowarning = FALSE,
    .encoding = "UTF-8"
) {
  list(
    xml_opts = {
      opts <- c()
      if (.recover) opts <- c(opts, "RECOVER")
      if (.huge) opts <- c(opts, "HUGE")
      if (.noerror) opts <- c(opts, "NOERROR")
      if (.nowarning) opts <- c(opts, "NOWARNING")
      if (length(opts) == 0) "HUGE" else opts
    },
    encoding = .encoding
  )
}


#' Read and extract text from HTML content
#'
#' @param .html Character string containing HTML content or path to HTML file
#' @param .options List of options from html_options()
#' @return Character string containing extracted text; NA_character_ if parsing fails
#' @export
read_html <- function(.html, .options = html_options()) {
  opt_ <- .options
  doc_ <- try(rvest::read_html(.html, options = opt_$xml_opts, encoding = opt_$encoding), silent = TRUE)
  if (inherits(doc_, "try-error")) {
    return(NA_character_)
  }

  reg_problem_ <- "([\t\n\r\u00A0]|&#(?:9|10|13|160);)"
  ratio_problem_ <- stringi::stri_count_regex(.html, reg_problem_) / nchar(.html)

  if (ratio_problem_ > 0.1) {
    txt_ <- try(rvest::html_text(doc_), silent = TRUE)
  } else {
    txt_ <- try(rvest::html_text2(doc_), silent = TRUE)
  }

  if (inherits(txt_, "try-error")) {
    return(NA_character_)
  }

  return(txt_)
}





#' Display HTML Content in Browser
#'
#' @description
#' Creates a temporary HTML file and opens it in the default web browser
#' for viewing and debugging purposes.
#'
#' @param .html Character string containing valid HTML content to display
#'
#' @details
#' The function:
#' 1. Creates a temporary file with .html extension
#' 2. Writes the provided HTML content to the file
#' 3. Opens the file in the system's default web browser
#'
#' @return No return value, called for side effects (opens browser)
#'
#' @note
#' This is a utility function primarily used for development and debugging
#' purposes to visualize HTML content extracted from SEC EDGAR documents.
#'
#' @examples
#' \dontrun{
#' html_content <- "<html><body><h1>Test</h1></body></html>"
#' utils_showHtml(html_content)
#' }
#'
#' @export
show_html <- function(.html) {
  tmp_ <- tempfile(fileext = ".html")
  write(.html, tmp_)
  utils::browseURL(tmp_)
}

# Debug ---------------------------------------------------------------------------------------
if (FALSE) {
  devtools::load_all()
  forms <- c("10-K", "10-K/A", "10-Q", "10-Q/A", "8-K", "8-K/A", "20-F", "20-F/A")

  edgar_read_master_index(
    .dir = fs::dir_create("../_package_debug/rGetEDGAR"),
    .from = NULL,
    .to = NULL,
    .ciks = NULL,
    .formtypes = c("10-K", "10-Q"),
    .collect = TRUE
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

}
