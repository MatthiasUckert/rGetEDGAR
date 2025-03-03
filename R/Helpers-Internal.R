#' Get HTTP Request Header
#'
#' @description
#' Creates HTTP request headers for SEC EDGAR API calls.
#'
#' @param .user User agent string for HTTP requests
#'
#' @return httr header object
#'
#' @keywords internal
get_header <- function(.user) {
  httr::add_headers(Connection = "keep-alive", `User-Agent` = .user)
}

#' Make HTTP GET Request
#'
#' @description
#' Makes an HTTP GET request with error handling.
#'
#' @param .url URL to request
#' @param .user User agent string
#'
#' @return httr response object or error
#'
#' @keywords internal
make_get_request <- function(.url, .user) {
  try(httr::GET(url = .url, get_header(.user)), silent = TRUE)
}

#' Parse HTTP Response Content
#'
#' @description
#' Parses HTTP response content with UTF-8 encoding.
#'
#' @param .result httr response object
#'
#' @return Parsed content as text
#'
#' @keywords internal
parse_content <- function(.result, .type = c("text", "raw")) {
  if (.type == "text") {
    try(httr::content(.result, as = "text", encoding = "UTF-8"), silent = TRUE)
  } else {
    try(httr::content(.result, "raw"), silent = TRUE)
  }
}


#' Generate Year-Quarter Combinations
#'
#' @description
#' Creates a table of all possible year-quarter combinations within the specified date range.
#' Uses validated parameters from get_edgar_params() for date range processing.
#'
#' @param .from Numeric value specifying the start year.quarter (e.g., 2020.1 for Q1 2020).
#'             If NULL, defaults to 1993.1
#' @param .to Numeric value specifying the end year.quarter.
#'           If NULL, defaults to current quarter
#'
#' @return A tibble with columns:
#' \itemize{
#'   \item Year: Integer representing the year
#'   \item Quarter: Integer representing the quarter (1-4)
#'   \item YearQuarter: Numeric combining year and quarter (e.g., 2020.1)
#' }
#'
#' @details
#' The function creates all possible combinations of years and quarters between
#' the specified date range. It uses get_edgar_params() to validate and process
#' the input parameters, ensuring consistent date range handling across the package.
#'
#' @keywords internal
#'
#' @seealso get_edgar_params
get_year_qtr_table <- function(.from = NULL, .to = NULL) {
  params_ <- get_edgar_params(.from, .to)
  tidyr::expand_grid(
    Year = 1993:lubridate::year(Sys.Date()),
    Quarter = 1:4
  ) %>%
    dplyr::mutate(
      YearQuarter = as.numeric(paste0(Year, ".", Quarter)),
      YearQtrSave = paste0(Year, "-", Quarter)
    ) %>%
    dplyr::filter(dplyr::between(YearQuarter, params_$from, params_$to))
}

#' Print Verbose Messages
#'
#' @description
#' Prints status messages with consistent formatting if verbose mode is enabled.
#'
#' @param .msg Character string containing the message to print
#' @param .verbose Logical indicating whether to print the message
#' @param .line Character string specifying the line ending (default: "\\n")
#'
#' @keywords internal
print_verbose <- function(.msg, .verbose, .line = "\n") {
  if (.verbose) {
    cat(.line, paste0(.msg, paste(rep(" ", 40), collapse = "")))
  }
}

#' Validate and Process EDGAR Parameters
#'
#' @description
#' Internal helper function to validate and process input parameters for EDGAR functions
#'
#' @param .from Numeric value for start period (year.quarter)
#' @param .to Numeric value for end period (year.quarter)
#' @param .ciks Character vector of CIK numbers
#' @param .formtypes Character vector of document types
#' @param .doctypes Character vector of document types to filter
#'
#' @return A list containing processed parameters:
#' \itemize{
#'   \item from: Start period (defaults to 1993.1)
#'   \item to: End period (defaults to current quarter)
#'   \item ciks: Processed CIK numbers
#'   \item formtypes: Processed FormTypes
#'   \item formtypes: Processed DocumentTypes
#' }
#'
#' @keywords internal
get_edgar_params <- function(.from = NULL, .to = NULL, .ciks = NULL, .formtypes = NULL, .doctypes = NULL) {
  # Process time periods
  year_current_ <- lubridate::year(Sys.Date())
  qtr_current_ <- lubridate::quarter(Sys.Date()) - 1L
  current_period_ <- as.numeric(paste0(year_current_, ".", qtr_current_))

  if (!is.null(.doctypes)) {
    tab_doc_types_ <- tibble::tibble(DocTypeMod = .doctypes) %>%
      dplyr::left_join(get("Table_DocTypesRaw"), by = "DocTypeMod")
    nas_doc_types_ <- dplyr::filter(tab_doc_types_, is.na(DocTypeMod))
    if (nrow(nas_doc_types_) > 0) {
      miss_ <- paste(sort(unique(nas_doc_types_$DocTypeMod)), collapse = ", ")
      msg_ <- paste0("Following Requested DocTypes do not exist: ", miss_, " (Check 'Table_DocTypesRaw')")
      stop(msg_, call. = FALSE)
    }
    doc_types_ <- unique(tab_doc_types_$DocTypeRaw)
  } else {
    doc_types_ <- NA_character_
  }

  # Create output list
  list(
    from = ifelse(is.null(.from), 1993.1, .from),
    to = ifelse(is.null(.to), current_period_, .to),
    ciks = if (is.null(.ciks)) NA_character_ else .ciks,
    formtypes = if (is.null(.formtypes)) NA_character_ else .formtypes,
    doctypes = doc_types_
  )
}
