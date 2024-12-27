#' SEC EDGAR Form Types Dataset
#'
#' A dataset containing SEC EDGAR form types, their standardized versions, descriptions,
#' and filing statistics. This dataset combines information from the SEC EDGAR master index
#' with form type descriptions from secfilingdata.com.
#'
#' @format A tibble with 733 rows and 4 variables:
#' \describe{
#'   \item{FormTypeOriginal}{character. The original form type as it appears in the SEC EDGAR database}
#'   \item{FormTypeStandard}{character. Standardized version of the form type, with some corrections
#'                          (e.g., "10KSB" standardized to "10-KSB")}
#'   \item{Description}{character. Description of the form type and its purpose. NA if no description
#'                     is available}
#'   \item{TotalFilings}{integer. The total number of filings for this form type. NA if no count
#'                      is available}
#' }
#'
#' @source
#' \describe{
#'   \item{SEC EDGAR Master Index}{Data extracted from SEC EDGAR master index files}
#'   \item{Form Type Descriptions}{Scraped from https://www.secfilingdata.com/sec-form-type}
#' }
#'
#' @details
#' This dataset is created by combining form types from the SEC EDGAR master index with
#' descriptions from secfilingdata.com. The process includes:
#' 1. Extracting unique form types from the SEC EDGAR master index
#' 2. Standardizing certain form type formats
#' 3. Joining with scraped descriptions from secfilingdata.com
#'
#' The dataset is particularly useful for:
#' * Understanding different SEC filing types
#' * Standardizing form type representations
#' * Analyzing filing frequencies
#' * Supporting SEC EDGAR data analysis workflows
#' @keywords datasets SEC EDGAR filings
"FormTypes"
