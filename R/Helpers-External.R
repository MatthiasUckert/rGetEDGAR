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
  dir_links_ <- fs::dir_create(file.path(dir_, "DocumentLinks"))
  dir_links_main_ <- fs::dir_create(file.path(dir_, "DocumentLinks", "DocLinkData"))
  dir_links_temp_ <- fs::dir_create(file.path(dir_, "DocumentLinks", "Temporary"))
  dir_links_back_ <-  fs::dir_create(file.path(dir_, "DocumentLinks", "BackUps"))
  dir_docs_ <- fs::dir_create(file.path(dir_, "DocumentData"))
  dir_docs_tmp_ <- fs::dir_create(file.path(dir_, "DocumentData", "Temporary"))

  list(
    MasterIndex = list(
      DirParquet = fs::dir_create(file.path(dir_master_, "MasterIndex")),
      FilLog = file.path(dir_master_, "LogMasterIndex.csv")
    ),
    DocLinks = list(
      DirTemp = list(
        FilToBePrc = file.path(dir_links_temp_, "ToBeProcessed.sqlite"),
        DirSqlite = fs::dir_create(file.path(dir_links_temp_, "DataSQLite"))
      ),
      DirMain = list(
        HTMLs = fs::dir_create(dir_links_main_, "DataHTMLs"),
        Links = fs::dir_create(dir_links_main_, "DataLinks")
      ),
      DirBackUp = dir_links_back_,
      FilLog = file.path(dir_links_, "LogDocLinks.csv")
    ),
    DocumentData = list(
      Original = fs::dir_create(file.path(dir_docs_, "Original")),
      Parsed = fs::dir_create(file.path(dir_docs_, "Parsed")),
      FilLogOriginal = file.path(dir_docs_, "LogDocDataOriginal.csv"),
      FilLogParsed = file.path(dir_docs_, "LogDocDataParsed.csv"),
      Temporary = list(
        FilToBePrc = file.path(dir_docs_tmp_, "ToBeProcessed.sqlite")
      )
    )
  )
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


#' List Files in Directory with Optional Pattern Matching
#'
#' @description
#' Creates a tibble of files from specified directories with optional pattern matching
#' and recursive searching capabilities.
#'
#' @param .dirs Character vector of directory paths to search
#' @param .reg Character string containing a regular expression pattern for file filtering (default: NULL)
#' @param .rec Logical indicating whether to search recursively in subdirectories (default: FALSE)
#'
#' @details
#' The function performs the following steps:
#' 1. Lists files in each specified directory
#' 2. Creates a document ID from the filename (without extension)
#' 3. Returns paths and IDs in a standardized tibble format
#'
#' @return A tibble with columns:
#' \itemize{
#'   \item doc_id: Document identifier (filename without extension)
#'   \item path: Full file path, named with doc_id
#' }
#'
#' @note
#' This is an internal utility function used by other package functions to
#' standardize file listing and identification across the package.
#'
#' @keywords internal
#' @seealso list_data
list_files <- function(.dirs, .reg = NULL, .rec = FALSE) {
  purrr::map(
    .x = .dirs,
    .f = ~ tibble::tibble(path = list.files(.x, .reg, FALSE, TRUE, .rec))
  ) %>%
    dplyr::bind_rows() %>%
    dplyr::mutate(
      doc_id = fs::path_ext_remove(basename(path)),
      path = purrr::set_names(path, doc_id)
    ) %>%
    dplyr::select(doc_id, path)

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
