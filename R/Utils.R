# Utils ---------------------------------------------------------------------------------------
#' Set Up Directory Structure
#'
#' @description
#' Creates and returns the directory structure for storing EDGAR data.
#'
#' @param .dir Character string specifying the base directory
#'
#' @return A list containing paths for MasterIndex, DocumentLinks, and DocumentData
#'
#' @keywords internal
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
    DocumentData = fs::dir_create(file.path(dir_, "DocumentData")),
    Temporary = list(
      DocumentLinks = file.path(dir_temp_, "TemporaryDocumentLinks.parquet"),
      DocumentData = file.path(dir_temp_, "TemporaryDocumentData.parquet")
    ),
    Logs = list(
      MasterIndex = file.path(dir_log_, "LogMasterIndex.csv"),
      DocumentLinks = file.path(dir_log_, "LogDocumentLinks.csv"),
      DocumentData = file.path(dir_log_, "LogDocumentData.csv")
    )
  )
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
      YearQuarter = as.numeric(paste0(Year, ".", Quarter))
    ) %>%
    dplyr::filter(dplyr::between(YearQuarter, params_$from, params_$to))
}

#' Format Numbers for Display
#'
#' @description
#' Formats numbers as either comma-separated values or percentages.
#'
#' @param .num Numeric value to format
#' @param .type Character string specifying format type: "c" for comma or "p" for percentage
#'
#' @return Formatted character string
#'
#' @keywords internal
format_number <- function(.num, .type = c("c", "p")) {
  # Match first argument of .type
  .type <- match.arg(.type)

  if (.type == "c") {
    # Format as number with comma separator
    return(trimws(format(.num, big.mark = ",", scientific = FALSE)))
  } else {
    # Format as percentage with 4 decimal places
    return(trimws(paste0(format(round(.num * 100, 4), nsmall = 4), "%")))
  }
}

#' Format Time Status Message
#'
#' @description
#' Creates a formatted status message showing elapsed time and estimated time remaining.
#'
#' @param .t0 POSIXct start time
#' @param .i Current iteration number
#' @param .n Total number of iterations
#'
#' @return Formatted status message
#'
#' @keywords internal
format_time <- function(.t0, .i, .n) {
  ela_ <- difftime(Sys.time(), .t0, units = "secs")
  eta_ <- (ela_ / .i) * (.n - .i)
  ela_ <- dplyr::case_when(
    ela_ > 60 * 60 * 24 ~ sprintf("%.2fd", ela_ / (60 * 60 * 24)),
    ela_ > 60 * 60 ~ sprintf("%.2fh", ela_ / (60 * 60)),
    ela_ > 60 ~ sprintf("%.2fm", ela_ / 60),
    TRUE ~ sprintf("%.2fs", ela_)
  )
  eta_ <- dplyr::case_when(
    eta_ > 60 * 60 * 24 ~ sprintf("%.2fd", eta_ / (60 * 60 * 24)),
    eta_ > 60 * 60 ~ sprintf("%.2fh", eta_ / (60 * 60)),
    eta_ > 60 ~ sprintf("%.2fm", eta_ / 60),
    TRUE ~ sprintf("%.2fs", eta_)
  )
  sprintf("Tot: %s | ETA: %s", ela_, eta_)
}


#' Format Loop Progress Message
#'
#' @description
#' Creates a formatted string showing the current progress of a parallel processing loop,
#' displaying the total number of queries processed versus the total expected.
#'
#' @param .i Current iteration number
#' @param .n Total number of iterations
#' @param .workers Number of parallel workers being used
#'
#' @return A character string in the format "Query: X/Y" where X is the current
#'         number of queries processed (.i * .workers) and Y is the total number
#'         of queries to process (.n * .workers)
#' @keywords internal
format_loop <- function(.i, .n, .workers) {
  paste0(format_number(.i * .workers), "/", format_number(.n * .workers))
}

#' Manage Rate Limiting Wait Time
#'
#' @description
#' Controls the timing between iterations in a parallel processing loop to maintain
#' a specified query rate limit. The function calculates and implements necessary
#' wait times based on the number of workers and desired queries per second.
#'
#' @param .last_time POSIXct timestamp from the previous iteration
#' @param .workers Number of parallel workers being used
#' @param .queries_per_second Maximum number of queries allowed per second (default: 10)
#'
#' @details
#' The function:
#' 1. Calculates required wait time based on workers and query limit (plus 0.1s buffer)
#' 2. Measures actual loop execution time
#' 3. Implements additional wait if needed
#' 4. Provides detailed timing information
#'
#' The needed wait time is calculated as: (workers / queries_per_second) + 0.1
#' This ensures the rate limit is maintained across parallel workers with a small
#' safety buffer.
#'
#' @return A list containing:
#'   \itemize{
#'     \item new_time: POSIXct timestamp after any implemented wait
#'     \item msg: Formatted string with timing details in the format
#'           "Loop: Xs + Ys = Zs (Target: Ws)" where:
#'           - X is the actual loop execution time
#'           - Y is the additional wait time
#'           - Z is the total time
#'           - W is the target time between iterations
#'   }
#' @keywords internal
loop_wait_time <- function(.last_time, .workers, .queries_per_second = 10) {
  # How many seconds we need between iterations, based on workers and QPS
  needed_wait <- (.workers / .queries_per_second) + .05

  # How long it has been since the last iteration
  Tloop <- as.numeric(difftime(Sys.time(), .last_time, units = "secs"))

  # Default no waiting needed
  Twait <- 0

  # If we haven't waited enough, sleep the difference
  if (Tloop < needed_wait) {
    Twait <- needed_wait - Tloop
    Sys.sleep(Twait)
  }

  # The total time for the iteration (loop time + wait time)
  Ttotal <- Tloop + Twait

  # The new time after any wait
  new_time <- Sys.time()

  # Construct your desired string message
  # msg <- paste0(sprintf("Loop: %.2fs + %.2fs = %.2fs", Tloop, Twait, Ttotal), " (Target: ", needed_wait, "s)")
  msg <- sprintf("Loop: %.2fs", Ttotal)

  # Return a list
  list(
    new_time = new_time,
    msg      = msg
  )
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


list_files <- function(.dirs, .reg = NULL, .id = "doc_id", .rec = FALSE) {
  purrr::map(
    .x = .dirs,
    .f = ~ tibble::tibble(path = list.files(.x, .reg, FALSE, TRUE, .rec))
  ) %>%
    dplyr::bind_rows() %>%
    dplyr::mutate(
      ID = fs::path_ext_remove(basename(path)),
      path = purrr::set_names(path, !!dplyr::sym(.id))
    ) %>%
    dplyr::select(ID, path) %>%
    purrr::set_names(c(.id, "path"))

}

list_data <- function(.dir) {
  lp_ <- get_directories(.dir)
  list_files(
    .dirs = c(lp_$MasterIndex$Parquet, lp_$DocumentLinks$Parquet, lp_$LandingPage$Parquet)
  ) %>%
    tidyr::separate(doc_id, c("Type", "YearQuarter"), sep = "_") %>%
    tidyr::pivot_wider(names_from = Type, values_from = path)
}

utils_showHtml <- function(.html) {
  tmp_ <- tempfile(fileext = ".html")
  write(.html, tmp_)
  utils::browseURL(tmp_)
}
