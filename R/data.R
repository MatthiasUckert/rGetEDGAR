#' SEC EDGAR Form Types Dataset (Raw)
#'
#' A comprehensive dataset containing SEC EDGAR form types with their raw and standardized
#' versions, descriptions, and classifications.
#'
#' @format A tibble with 5 variables:
#' \describe{
#'   \item{FormClass}{character. Classification category of the form (e.g., Annual Reports, Registration Forms)}
#'   \item{FormTypeRaw}{character. The original form type as it appears in SEC EDGAR}
#'   \item{FormTypeMod}{character. Standardized version of the form type}
#'   \item{nDocs}{integer. Count of occurrences of this form type in the database}
#'   \item{Description}{character. Detailed description of the form type's purpose}
#' }
#'
#' @source SEC EDGAR database master index files
#' @keywords datasets SEC EDGAR filings forms
"Table_FormTypesRaw"

#' SEC EDGAR Form Types Dataset (Modified)
#'
#' A summarized dataset of SEC EDGAR form types grouped by their classifications
#' and standardized versions.
#'
#' @format A tibble with 4 variables:
#' \describe{
#'   \item{FormClass}{character. Classification category of the form}
#'   \item{FormTypeMod}{character. Standardized version of the form type}
#'   \item{nDocs}{integer. Aggregated count of occurrences}
#'   \item{Description}{character. Detailed description of the form type's purpose}
#' }
#'
#' @source SEC EDGAR database master index files
#' @keywords datasets SEC EDGAR filings forms standardized
"Table_FormTypesMod"

#' SEC EDGAR Document Types Dataset (Raw)
#'
#' A detailed dataset containing all unique document types found within SEC EDGAR filings,
#' including exhibits, main forms, and other document categories.
#'
#' @format A tibble with 5 variables:
#' \describe{
#'   \item{DocClass}{character. Classification category of the document (e.g., Exhibits, Main Form Types)}
#'   \item{DocTypeRaw}{character. The original document type as it appears in SEC EDGAR}
#'   \item{DocTypeMod}{character. Standardized version of the document type}
#'   \item{nDocs}{integer. Count of occurrences of this document type}
#'   \item{Description}{character. Detailed description of the document type's purpose}
#' }
#'
#' @source SEC EDGAR database document indices
#' @keywords datasets SEC EDGAR filings documents exhibits
"Table_DocTypesRaw"

#' SEC EDGAR Document Types Dataset (Modified)
#'
#' A summarized dataset of SEC EDGAR document types grouped by their classifications
#' and standardized versions.
#'
#' @format A tibble with 4 variables:
#' \describe{
#'   \item{DocClass}{character. Classification category of the document}
#'   \item{DocTypeMod}{character. Standardized version of the document type}
#'   \item{nDocs}{integer. Aggregated count of occurrences}
#'   \item{Description}{character. Detailed description of the document type's purpose}
#' }
#'
#' @source SEC EDGAR database document indices
#' @keywords datasets SEC EDGAR filings documents exhibits standardized
"Table_DocTypesMod"
