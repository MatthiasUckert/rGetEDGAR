#' Read SEC EDGAR Master Index Data
#'
#' @description
#' Reads and filters the master index data from the stored Parquet files.
#' The master index contains metadata about all SEC filings.
#'
#' @param .dir Character string specifying the directory where the data is stored
#' @param .from Numeric value specifying the start year.quarter (e.g., 2020.1 for Q1 2020).
#'             If NULL, defaults to 1993.1
#' @param .to Numeric value specifying the end year.quarter.
#'           If NULL, defaults to current quarter
#' @param .ciks Character vector of CIK numbers to filter for specific companies
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
edgar_read_master_index <- function(.dir, .from = NULL, .to = NULL, .ciks = NULL, .collect = TRUE) {
  # Get validated parameters
  params <- get_edgar_params(.from = .from, .to = .to, .ciks = .ciks)

  # Get directory structure
  lp_ <- get_directories(.dir)

  # Open and filter dataset
  arr_ <- arrow::open_dataset(lp_$MasterIndex$Parquet) %>%
    dplyr::filter(dplyr::between(YearQuarter, params$from, params$to)) %>%
    filter_edgar_data(params)

  # Return based on collect parameter
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
#' @param .from Numeric value specifying the start year.quarter (e.g., 2020.1 for Q1 2020).
#'             If NULL, defaults to 1993.1
#' @param .to Numeric value specifying the end year.quarter.
#'           If NULL, defaults to current quarter
#' @param .ciks Character vector of CIK numbers to filter for specific companies
#' @param .types Character vector of document types to filter (e.g., "10-K", "10-Q")
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
#'
#' @examples
#' \dontrun{
#' # Read all 10-K filings for Apple Inc from 2020
#' data <- edgar_read_document_links(
#'   .dir = "edgar_data",
#'   .from = 2020.1,
#'   .to = 2020.4,
#'   .ciks = "0000320193",
#'   .types = "10-K"
#' )
#'
#' # Get Arrow Dataset for delayed computation
#' dataset <- edgar_read_document_links(
#'   .dir = "edgar_data",
#'   .collect = FALSE
#' )
#' }
edgar_read_document_links <- function(.dir, .from = NULL, .to = NULL, .ciks = NULL, .types = NULL, .collect = TRUE) {
  # Get validated parameters
  params <- get_edgar_params(.from = .from, .to = .to, .ciks = .ciks, .types = .types)

  # Get directory structure
  lp_ <- get_directories(.dir)

  # Open and filter dataset
  arr_ <- arrow::open_dataset(lp_$DocumentLinks$Parquet) %>%
    dplyr::filter(dplyr::between(YearQuarter, params$from, params$to)) %>%
    filter_edgar_data(params)

  # Return based on collect parameter
  if (.collect) {
    return(dplyr::collect(arr_))
  } else {
    return(arr_)
  }
}
