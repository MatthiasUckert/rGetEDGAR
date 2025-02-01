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
  params_ <- get_edgar_params(.from, .to, .ciks, .formtypes)

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

  arr_ <- arrow::open_dataset(tab_fils$DocumentLinks) %>%
    dplyr::filter(HashIndex %in% idx_) %>%
    filter_edgar_data(get_edgar_params(.doctypes))

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
