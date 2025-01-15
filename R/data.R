#' SEC EDGAR Form Types Dataset
#'
#' A dataset containing all unique form types found in SEC EDGAR filings, including
#' both the raw form type strings and their standardized versions. This dataset
#' serves as a reference for understanding and categorizing SEC filing types.
#'
#' @format A tibble with 3 variables:
#' \describe{
#'   \item{FormTypeRaw}{character. The original form type as it appears in SEC EDGAR}
#'   \item{FormTypeMod}{character. Modified/standardized version of the form type
#'                      (currently identical to FormTypeRaw, placeholder for future standardization)}
#'   \item{nFormTypes}{integer. Count of occurrences of this form type in the database}
#' }
#'
#' @source SEC EDGAR database master index files
#' @keywords datasets SEC EDGAR filings forms
"FormTypes"

#' SEC EDGAR Document Types Dataset
#'
#' A dataset containing all unique document types found within SEC EDGAR filings.
#' Document types represent the specific components or attachments within SEC filings.
#'
#' @format A tibble with 3 variables:
#' \describe{
#'   \item{DocTypeRaw}{character. The original document type as it appears in SEC EDGAR}
#'   \item{DocTypeMod}{character. Modified/standardized version of the document type
#'                     (currently identical to DocTypeRaw, placeholder for future standardization)}
#'   \item{nDocTypes}{integer. Count of occurrences of this document type in the database}
#' }
#'
#' @source SEC EDGAR database document indices
#' @keywords datasets SEC EDGAR filings documents
"DocTypes"

#' SEC EDGAR Raw Form-Document Type Combinations Dataset
#'
#' A dataset containing all observed combinations of form types and document types
#' in their original form as they appear in SEC EDGAR filings. This dataset helps
#' understand which document types typically appear in which filing forms.
#'
#' @format A tibble with 3 variables:
#' \describe{
#'   \item{FormTypeRaw}{character. The original form type from the filing}
#'   \item{DocTypeRaw}{character. The original document type from the filing}
#'   \item{nRawTypes}{integer. Count of occurrences of this form type and document type combination}
#' }
#'
#' @details
#' This dataset is created by joining the master index with document links using
#' HashIndex as the key. It represents the raw, unmodified combinations as they
#' appear in the SEC EDGAR database.
#'
#' @source SEC EDGAR database master index and document indices
#' @keywords datasets SEC EDGAR filings documents combinations
"RawTypes"

#' SEC EDGAR Modified Form-Document Type Combinations Dataset
#'
#' A dataset containing all observed combinations of standardized form types and
#' document types from SEC EDGAR filings. This dataset is similar to RawTypes but
#' uses the modified/standardized versions of both form and document types.
#'
#' @format A tibble with 3 variables:
#' \describe{
#'   \item{FormTypeMod}{character. The modified/standardized form type}
#'   \item{DocTypeMod}{character. The modified/standardized document type}
#'   \item{nModTypes}{integer. Count of occurrences of this combination}
#' }
#'
#' @details
#' This dataset is created by joining the master index with document links using
#' HashIndex as the key, using the modified versions of both form and document types.
#' Currently, the modified versions are identical to the raw versions, serving as
#' placeholders for future standardization efforts.
#'
#' @source SEC EDGAR database master index and document indices
#' @keywords datasets SEC EDGAR filings documents combinations standardized
"ModTypes"
