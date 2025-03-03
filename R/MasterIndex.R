
# Main Functions ------------------------------------------------------------------------------
#' Download SEC EDGAR Master Index Files
#'
#' @description
#' Downloads and processes the SEC EDGAR master index files for specified time periods.
#' These files contain metadata about all SEC filings submitted to the EDGAR system,
#' providing a comprehensive index to locate specific documents.
#'
#' @param .dir
#' Character string specifying the directory where the downloaded data will be stored.
#' This directory will be created if it doesn't exist, along with the necessary subdir structure.
#' @param .user
#' Character string specifying the user agent to be used in HTTP requests to the SEC EDGAR server.
#' This should typically be an email address to comply with SEC's fair access policy.
#' @param .from
#' Numeric value specifying the start year and quarter in format YYYY.Q  (e.g., 2020.1 for Q1 2020).
#' If NULL, defaults to 1993.1 (the earliest available data).
#' @param .to
#' Numeric value specifying the end year and quarter in format YYYY.Q.
#' If NULL, defaults to the current quarter.
#' @param .verbose
#' Logical indicating whether to print progress messages to the console during processing.
#' Default is TRUE.
#'
#' @details
#' The SEC EDGAR master index provides a comprehensive listing of all filings submitted
#' to the SEC. This function automates the process of downloading and standardizing
#' this data for further analysis.
#'
#' The master index includes these key fields:
#' \itemize{
#'   \item CIK (Central Index Key): Unique identifier for EDGAR filers
#'   \item Company Name: Name of the filing entity
#'   \item Form Type: SEC form type (e.g., 10-K, 10-Q, 8-K)
#'   \item Date Filed: Submission date
#'   \item URL: Link to the full text filing
#'   \item Additional metadata fields added during processing
#' }
#'
#' The function implements error handling and logging to gracefully handle network issues
#' or malformed data, ensuring robust operation even when processing large numbers of files.
#'
#' @return No return value, called for side effects (downloading and saving data files).
#'
#' @export
edgar_get_master_index <- function(.dir, .user, .from = NULL, .to = NULL, .verbose = TRUE) {
  # Get directory structure
  lp_ <- get_directories(.dir)
  plog_ <- lp_$MasterIndex$FilLog


  tab_urls_ <- get_year_qtr_table(.from, .to) %>%
    dplyr::mutate(
      UrlMasterIndex = "https://www.sec.gov/Archives/edgar/full-index",
      UrlMasterIndex = file.path(UrlMasterIndex, Year, paste0("QTR", Quarter), "master.idx"),
      PathOut = file.path(lp_$MasterIndex$DirParquet, paste0(YearQtrSave, ".parquet")),
      Print = paste0("Downloading Year-Quarter: ", Year, "-", Quarter)
    )
  use_urls_ <- dplyr::filter(tab_urls_, !file.exists(PathOut))
  min_yq_ <- min(tab_urls_$YearQuarter)
  max_yq_ <- max(tab_urls_$YearQuarter)

  error_logging(plog_, "INFO", paste0("Starting Download (", min_yq_, " to ", max_yq_, ")"))

  if (nrow(use_urls_) == 0) {
    error_logging(plog_, "INFO", "No new files to download")
    return(print_verbose("Master Index Complete", TRUE, "\r"))
  }

  # Log files to download
  error_logging(plog_, "INFO", paste0("Found ", nrow(use_urls_), " files to download"))

  for (i in seq_len(nrow(use_urls_))) {
    print_verbose(use_urls_$Print[i], .verbose, "\r")

    # Get Call
    result_ <- make_get_request(use_urls_$UrlMasterIndex[i], .user)
    if (!check_error_request(result_, .verbose, plog_)) next

    # Parse Content
    content_ <- parse_content(result_, .type = "text")
    if (!check_error_content(content_, .verbose, plog_)) next

    # Convert to Dataframe
    out_ <- content_to_dataframe(content_, use_urls_$Year[i], use_urls_$Quarter[i])
    if (!check_error_master_output(out_, .verbose, plog_)) next

    # Write Output
    arrow::write_parquet(out_, use_urls_$PathOut[i])

    msg_out_ <- paste0("Downloaded Year-Quarter: ", use_urls_$Year[i], "-", use_urls_$Quarter[i])
    det_out_ <- paste0("Rows: ", nrow(out_))
    error_logging(plog_, "INFO", msg_out_, det_out_)
  }

}


# Helper Functions ----------------------------------------------------------------------------
#' Convert SEC EDGAR Master Index Content to DataFrame
#'
#' @description
#' Parses and transforms raw SEC EDGAR master index content into a structured data frame
#' with standardized fields and additional metadata. This function handles the specific
#' format of the SEC EDGAR master.idx files, performing data cleaning and enrichment.
#'
#' @param .content
#' Character string containing the raw content of a master.idx filefrom the SEC EDGAR database.
#' @param .year Integer representing the year of the content (e.g., 2023)
#' @param .qtr Integer representing the quarter of the content (1-4)
#'
#' @details
#' The function includes error handling to manage cases where the input content
#' doesn't match the expected structure. In these cases, an empty tibble is returned.
#'
#' @return A tibble with the following columns:
#' \itemize{
#'   \item HashIndex: Unique identifier for the filing (digest of UrlIndexPage)
#'   \item Year: Filing year
#'   \item Quarter: Filing quarter (1-4)
#'   \item YearQuarter: Combined year and quarter as numeric (e.g., 2023.1)
#'   \item CIK: Central Index Key, zero-padded to 10 digits
#'   \item CompanyName: Name of the filing company
#'   \item FormType: SEC form type (e.g., 10-K, 10-Q)
#'   \item DateFiled: Filing date as a Date object
#'   \item UrlFullText: URL to the complete filing text
#'   \item UrlIndexPage: URL to the filing index page
#' }
#'
#' If the content does not match the expected format, returns an empty tibble.
#' @keywords internal
content_to_dataframe <- function(.content, .year, .qtr) {
  tab_ <- tibble::tibble(text = stringi::stri_split_lines1(.content)) %>%
    dplyr::slice(-(1:9)) %>%
    dplyr::mutate(text = stringi::stri_split_fixed(text, "|")) %>%
    tidyr::unnest_wider(text, names_sep = "_") %>%
    janitor::row_to_names(1) %>%
    janitor::clean_names(case = "big_camel") %>%
    dplyr::slice(-1)

  col_ <- c("Cik", "CompanyName", "FormType", "DateFiled", "Filename")
  if (!all(colnames(tab_) == col_)) {
    out_ <- tibble::tibble()
  } else {
    out_ <- tab_ %>%
      dplyr::rename(UrlFullText = Filename, CIK = Cik) %>%
      dplyr::mutate(
        UrlFullText = rvest::url_absolute(UrlFullText, "https://www.sec.gov/Archives/"),
        UrlIndexPage = paste0(fs::path_ext_remove(UrlFullText), "-index.html"),
        CIK = stringi::stri_pad_left(CIK, 10, "0"),
        DateFiled = lubridate::as_date(DateFiled),
        Year = .year,
        Quarter = .qtr,
        YearQuarter = as.numeric(paste0(.year, ".", .qtr)),
        HashIndex = purrr::map_chr(UrlIndexPage, digest::digest)
      ) %>%
      dplyr::select(HashIndex, Year, Quarter, YearQuarter, dplyr::everything())
  }

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

  edgar_get_master_index(
    .dir = dir_debug,
    .user = user,
    .from = NULL,
    .to = NULL,
    .verbose = TRUE
  )

  .dir = dir_debug
  .user = user
  .from = NULL
  .to = NULL
  .verbose = TRUE
}

