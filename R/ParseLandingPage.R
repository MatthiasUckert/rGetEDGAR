# Function: edgar_parse_landingpage() --------------------------------------------------------------
if (FALSE) {
  "4ba9652551addc2fdda3d99993b6052c"
  .path_landing <- "../_package_debug/rGetEDGAR/LandingPage/LandingPage"
  .hash_idx <- "f18135a76836adda8e2b0bc2b6f3cf52"
  .html <- arrow::open_dataset(.path_landing) %>%
    dplyr::filter(HashIndex == .hash_idx) %>%
    dplyr::collect() %>%
    dplyr::pull(HTML)

  .html_read <- rvest::read_html(.html, options = c("HUGE", "RECOVER"))
  .type <- ".identInfo"
}


#' Parse EDGAR SEC Filing Landing Page
#'
#' @description
#' Extracts and structures information from an EDGAR SEC filing landing page HTML.
#' The function parses various elements including company information, filing details,
#' and accession numbers into a tidy data format.
#'
#' @param .hash_idx Character string. Hash of the Landing Page (Master Index) URL
#' @param .html Character string. The HTML content of an EDGAR SEC filing landing page
#'
#' @return A tibble with one row containing:
#'   \item{HashIndex}{Original hash index of the landing page}
#'   \item{Error}{Integer. 0 if parsing successful, 1 if HTML is NA}
#'   \item{ErrMsg}{Character. Error message if Error = 1, NA otherwise}
#'   \item{Additional columns}{Various parsed fields from the landing page}
#'
#' @export
edgar_parse_landingpage <- function(.hash_idx, .html) {
  if (is.na(.html) | length(.html) == 0) {
    return(tibble::tibble(HashIndex = .hash_idx, Error = 1L, Message = "Error: No HTML Supplied"))
  }

  html_ <- rvest::read_html(.html, options = c("HUGE", "RECOVER"))
  purrr::map(
    .x = c(".formGrouping", "#formName", "#secNum", ".mailer", ".companyName", ".identInfo"),
    .f = ~ landing_extract_elements(html_, .x)
  ) %>%
    dplyr::bind_rows() %>%
    dplyr::mutate(
      dplyr::across(c(Name, Info), trimws),
      Name = snakecase::to_upper_camel_case(trimws(gsub("\\.|\\:", "", Name))),
    ) %>%
    tidyr::pivot_wider(names_from = Name, values_from = Info) %>%
    tidyr::fill(dplyr::everything(), .direction = "down") %>%
    dplyr::mutate(
      Error = 0L,
      Message = "Success: All Elements Parsed",
      HashIndex = .hash_idx
    ) %>%
    dplyr::select(HashIndex, dplyr::everything())
}

#' Assign Entry Numbers to Landing Page Elements
#'
#' @description
#' Helper function that adds sequential entry numbers to parsed landing page elements
#' within each Name group.
#'
#' @param .tab A tibble containing parsed landing page elements with at least 'Name' column
#'
#' @return A tibble with an additional 'EntryNum' column containing sequential numbers
#'   within each Name group
#'
#' @keywords internal
landing_assign_entry_number <- function(.tab) {
  .tab %>%
    dplyr::group_by(Name) %>%
    dplyr::mutate(EntryNum = dplyr::row_number(), .before = Name) %>%
    dplyr::ungroup()
}

#' Extract Specific Elements from EDGAR Landing Page HTML
#'
#' @description
#' Internal helper function that extracts and formats specific types of information
#' from an EDGAR landing page HTML document.
#'
#' @param .html_read An HTML document read using rvest::read_html()
#' @param .type
#' Character string. The type of element to extract. One of:
#' c(".formGrouping", "#formName", "#secNum", ".mailer", ".companyName", ".identInfo")
#'
#' @return A tibble containing:
#'   \item{EntryNum}{Sequential number for each entry}
#'   \item{Name}{Name/type of the extracted information}
#'   \item{Info}{The extracted information value}
#'
#' @details
#' Different .type values extract different information:
#' - '.formGrouping': Filing Date, Accepted, Documents
#' - '#formName': Current Report
#' - '#secNum': Accession Number
#' - '.mailer': Mailing Address, Business Address
#' - '.companyName': Company Name, CIK
#' - '.identInfo': IRS No., State of Incorp., Fiscal Year End, etc.
#'
#' @keywords internal
landing_extract_elements <- function(.html_read, .type = c(".formGrouping", "#formName", "#secNum", ".mailer", ".companyName", ".identInfo")) {
  if (.type == ".formGrouping") {
    # Includes: Filing Date, Accepted, Documents, ...
    dplyr::bind_rows(purrr::map(
      .x = rvest::html_elements(.html_read, ".formGrouping"),
      .f = ~ tibble::tibble(
        Name = rvest::html_text2(rvest::html_elements(.x, ".infoHead")),
        Info = rvest::html_text2(rvest::html_elements(.x, ".info")),
      )
    )) %>%
      dplyr::filter(!is.na(Name), !is.na(Info)) %>%
      dplyr::mutate(Info = gsub("00:00:00", "", Info)) %>%
      landing_assign_entry_number()
  } else if (.type == "#formName") {
    # Includes: Current Report, ...
    tibble::tibble(
      Name = "Current Report",
      Info = rvest::html_text2(rvest::html_elements(.html_read, "#formName")),
    ) %>%
      landing_assign_entry_number()
  } else if (.type == "#secNum") {
    # Includes: Accession Number, ...
    tibble::tibble(
      Name = "Accession Number",
      Info = rvest::html_text2(rvest::html_elements(.html_read, "#secNum")),
    ) %>%
      dplyr::mutate(Info = trimws(gsub("SEC Accession No.", "", Info))) %>%
      landing_assign_entry_number()
  } else if (.type == ".mailer") {
    # Includes: Mailing Address, Business Address, ...
    tibble::tibble(tmp = rvest::html_text2(rvest::html_elements(.html_read, ".mailer"))) %>%
      dplyr::mutate(
        tmp = gsub("Address", "Address |", tmp, fixed = TRUE),
        tmp = stringi::stri_split_fixed(tmp, "|")
      ) %>%
      tidyr::unnest_wider(tmp, names_sep = "") %>%
      dplyr::mutate(dplyr::across(c(tmp1, tmp2), trimws)) %>%
      dplyr::rename(Name = tmp1, Info = tmp2) %>%
      landing_assign_entry_number()
  } else if (.type == ".companyName") {
    # Includes: CompanyName, CIK, ...
    tibble::tibble(tmp = rvest::html_text2(rvest::html_elements(.html_read, ".companyName"))) %>%
      dplyr::mutate(
        tmp = gsub("CIK:", "CIK |", tmp, fixed = TRUE),
        tmp = stringi::stri_split_fixed(tmp, "|")
      ) %>%
      dplyr::mutate(EntryNum = dplyr::row_number(), .before = tmp) %>%
      tidyr::unnest_wider(tmp, names_sep = "") %>%
      dplyr::mutate(dplyr::across(dplyr::starts_with("tmp"), trimws)) %>%
      dplyr::rename(CompanyName = tmp1, CIK = tmp2) %>%
      tidyr::pivot_longer(c(CompanyName, CIK), names_to = "Name", values_to = "Info") %>%
      dplyr::mutate(
        Info = gsub("\\(Filer\\).+", "", Info),
        Info = gsub("(see all company filings)", "", Info, fixed = TRUE),
        dplyr::mutate(dplyr::across(c(Name, Info), trimws))
      )
  } else if (.type == ".identInfo") {
    # Includes: IRS No., State of Incorp., Fiscal Year End, Type, Act, File No., Film No., SIC, ...
    tibble::tibble(tmp = rvest::html_text2(rvest::html_elements(.html_read, ".identInfo"))) %>%
      dplyr::mutate(tmp = stringi::stri_split_fixed(tmp, "|")) %>%
      dplyr::mutate(EntryNum = dplyr::row_number(), .before = tmp) %>%
      tidyr::unnest_wider(tmp, names_sep = "") %>%
      dplyr::mutate(dplyr::across(dplyr::starts_with("tmp"), trimws)) %>%
      tidyr::pivot_longer(dplyr::starts_with("tmp"), names_to = NULL, values_to = "tmp") %>%
      dplyr::mutate(tmp = stringi::stri_split_regex(tmp, "\n")) %>%
      tidyr::unnest(tmp) %>%
      dplyr::filter(!is.na(tmp)) %>%
      dplyr::mutate(tmp = dplyr::case_when(
        grepl("(?i)SIC", tmp) ~ stringi::stri_extract_first_regex(tmp, "(?i)SIC\\s*:\\s*\\d+"),
        TRUE ~ tmp
      )) %>%
      tidyr::separate_wider_delim(
        cols = tmp,
        delim = ":",
        names = c("Name", "Info"),
        too_few = "align_start",
        too_many = "merge"
      ) %>%
      dplyr::mutate(dplyr::across(c(Name, Info), trimws))
  }
}
