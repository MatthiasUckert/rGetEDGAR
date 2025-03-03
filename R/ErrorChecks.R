#' Log Error or Information Messages to File
#'
#' @description
#' Records standardized log entries to a CSV file, creating the file if it doesn't exist.
#' This function provides a consistent method for logging errors, warnings, and
#' informational messages throughout the package to facilitate debugging and provide
#' audit trails of package operations.
#'
#' @param .path_log
#' Character string specifying the path to the log file.
#' The parent directory will be created if it doesn't exist.
#' @param .level
#' Character string specifying the log level. Common values include:
#'  \itemize{
#'    \item "ERROR" - For error conditions that prevented successful completion
#'    \item "WARNING" - For potential issues that didn't prevent completion
#'    \item "INFO" - For general information about successful operations
#'    \item "DEBUG" - For detailed debugging information
#'  }
#' Default is "ERROR".
#' @param .message
#' Character string containing the log message.
#' This should be concise but descriptive of the event being logged.
#' @param .details
#' Optional character string containing additional details about the event.
#' This can include technical information like error messages, row counts, or file paths.
#' Default is NULL (no details).
#'
#' @details
#' The function creates a standardized log entry with the following fields:
#' \itemize{
#'   \item Timestamp: Current system time when log entry is created
#'   \item Level: Log level as specified in the .level parameter
#'   \item Message: Main description of the event
#'   \item Details: Additional technical details (if provided)
#' }
#'
#' The log file is a CSV format for easy importing and analysis in tools like
#' R, Excel, or database systems. If the file doesn't exist, it will be created
#' with appropriate column headers.
#'
#' @return Invisibly returns TRUE indicating successful logging.
#' @export
error_logging <- function(.path_log, .level = "ERROR", .message, .details = NULL) {
  # Create directory if needed
  dir_log <- dirname(.path_log)
  if (!dir.exists(dir_log)) {
    dir.create(dir_log, recursive = TRUE)
  }

  # Create log entry
  entry <- data.frame(
    Timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    Level = .level,
    Message = .message,
    Details = ifelse(is.null(.details), NA_character_, as.character(.details)),
    stringsAsFactors = FALSE
  )

  # Check if file exists
  file_exists <- file.exists(.path_log)

  # Write to file
  utils::write.table(
    entry,
    .path_log,
    append = file_exists,
    col.names = !file_exists,
    row.names = FALSE,
    sep = ",",
    quote = TRUE
  )

  return(invisible(TRUE))
}

#' Check and Log HTTP Request Errors
#'
#' @description
#' Validates an HTTP response object and logs any errors encountered. This function
#' checks both try-error objects and HTTP status codes to determine if a request
#' was successful.
#'
#' @param .obj HTTP response object from httr or try-error if the request failed
#' @param .verbose
#' Logical indicating whether to print error messages to the console. Default is TRUE.
#' @param .path_log
#' Optional character string specifying the path to a log file.
#' If provided, errors will be logged to this file. Default is NULL (no logging).
#'
#' @details
#' The function performs the following checks:
#' \itemize{
#'   \item Checks if the object is a try-error, indicating that the request failed to execute
#'   \item For valid responses, checks if the HTTP status is in the 200-299 range (i.e. success)
#' }
#'
#' If an error is detected and .path_log is provided, the function will automatically
#' log the error details using error_logging(). If .verbose is TRUE, error messages
#' will also be printed to the console using print_verbose().
#'
#' @return Logical value:
#'   \itemize{
#'     \item TRUE if the request was successful (no errors detected)
#'     \item FALSE if any errors were detected
#'   }
#' @seealso \code{\link{error_logging}} for the underlying logging function
#'
#' @keywords internal
check_error_request <- function(.obj, .verbose, .path_log = NULL) {
  if (inherits(.obj, "try-error")) {
    err_ <- paste(as.character(.obj), collapse = ", ")
    msg_ <- paste0("Error Occurred: ", err_, ", continuing ...")

    # Log error if path provided
    if (!is.null(.path_log)) {
      error_logging(.path_log, "ERROR", msg_, err_)
    }

    print_verbose(msg_, .verbose, "\r")
    return(FALSE)
  }

  if (!httr::status_code(.obj) %in% 200:299) {
    status_ <- httr::status_code(.obj)
    err_ <- paste0("Status Code (", status_, ")")
    msg_ <- paste0("Error Occurred: ", err_, ", continuing ...")

    # Log error if path provided
    if (!is.null(.path_log)) {
      error_logging(.path_log, "ERROR", msg_, err_)
    }

    Sys.sleep(60)
    print_verbose(msg_, .verbose, "\r")
    return(FALSE)
  }

  return(TRUE)
}

#' Check and Log Content Parsing Errors
#'
#' @description
#' Validates parsed content and logs any errors encountered. This function checks
#' for common problems in parsed content like empty or NA values.
#'
#' @param .obj Content object resulting from parsing (e.g., from parse_content())
#' @param .verbose
#' Logical indicating whether to print error messages to the console. Default is TRUE.
#' @param .path_log
#' Optional character string specifying the path to a log file.
#' If provided, errors will be logged to this file. Default is NULL (no logging).
#'
#' @details
#' The function performs the following checks:
#' \itemize{
#'   \item Checks if the object is a try-error, indicating that parsing failed
#'   \item Checks if the content is NA, empty, or contains only whitespace
#' }
#'
#' If an error is detected and .path_log is provided, the function will automatically
#' log the error details using error_logging(). If .verbose is TRUE, error messages
#' will also be printed to the console using print_verbose().
#'
#' @return Logical value:
#'   \itemize{
#'     \item TRUE if the content is valid (no errors detected)
#'     \item FALSE if any errors were detected
#'   }
#' @seealso \code{\link{error_logging}} for the underlying logging function
#'
#' @keywords internal
# check_error_content <- function(.obj, .verbose, .path_log = NULL) {
#   if (inherits(.obj, "try-error")) {
#     err_ <- paste(as.character(.obj), collapse = ", ")
#     msg_ <- paste0("Error Occurred: ", err_, ", continuing ...")
#
#     # Log error if path provided
#     if (!is.null(.path_log)) {
#       error_logging(.path_log, "ERROR", msg_, err_)
#     }
#
#     print_verbose(msg_, .verbose, "\r")
#     return(FALSE)
#   }
#
#   if (is.na(.obj) || (is.character(.obj) && length(.obj) == 0) || (is.character(.obj) && trimws(.obj) == "")) {
#     msg_ <- paste0("Error Occurred: ", "Content is Empty", ", continuing ...")
#
#     # Log error if path provided
#     if (!is.null(.path_log)) {
#       error_logging(.path_log, "ERROR", msg_, "Empty content")
#     }
#
#     print_verbose(msg_, .verbose, "\r")
#     return(FALSE)
#   }
#
#   return(TRUE)
# }

check_error_content <- function(.obj, .verbose, .path_log = NULL) {
  if (inherits(.obj, "try-error")) {
    err_ <- paste(as.character(.obj), collapse = ", ")
    msg_ <- paste0("Error Occurred: ", err_, ", continuing ...")
    # Log error if path provided
    if (!is.null(.path_log)) {
      error_logging(.path_log, "ERROR", msg_, err_)
    }
    print_verbose(msg_, .verbose, "\r")
    return(FALSE)
  }

  # Handle vector inputs correctly
  if (length(.obj) == 0 || all(is.na(.obj)) ||  (is.character(.obj) && all(trimws(.obj) == ""))) {
    msg_ <- paste0("Error Occurred: ", "Content is Empty", ", continuing ...")
    # Log error if path provided
    if (!is.null(.path_log)) {
      error_logging(.path_log, "ERROR", msg_, "Empty content")
    }
    print_verbose(msg_, .verbose, "\r")
    return(FALSE)
  }
  return(TRUE)
}

#' Check and Log Master Output Errors
#'
#' @description
#' Validates master index output data and logs any errors encountered. This function
#' checks for common problems in data frames like empty tables or error objects.
#'
#' @param .obj Data frame or tibble containing the processed output data
#' @param .verbose
#' Logical indicating whether to print error messages to the console. Default is TRUE.
#' @param .path_log
#' Optional character string specifying the path to a log file.
#' If provided, errors will be logged to this file. Default is NULL (no logging).
#'
#' @details
#' The function performs the following checks:
#' \itemize{
#'   \item Checks if the object is a try-error, indicating that processing failed
#'   \item Checks if the data frame has 0 rows, indicating that no data was found
#' }
#'
#' If an error is detected and .path_log is provided, the function will automatically
#' log the error details using error_logging(). If .verbose is TRUE, error messages
#' will also be printed to the console using print_verbose().
#'
#' @return Logical value:
#'   \itemize{
#'     \item TRUE if the output is valid (no errors detected)
#'     \item FALSE if any errors were detected
#'   }
#' @seealso \code{\link{error_logging}} for the underlying logging function
#'
#' @keywords internal
check_error_master_output <- function(.obj, .verbose, .path_log = NULL) {
  if (inherits(.obj, "try-error")) {
    err_ <- paste(as.character(.obj), collapse = ", ")
    msg_ <- paste0("Error Occurred: ", err_, ", continuing ...")

    # Log error if path provided
    if (!is.null(.path_log)) {
      error_logging(.path_log, "ERROR", msg_, err_)
    }

    print_verbose(msg_, .verbose, "\r")
    return(FALSE)
  }

  if (nrow(.obj) == 0) {
    msg_ <- paste0("Error Occurred: ", "Output Table has 0 Rows", ", continuing ...")

    # Log error if path provided
    if (!is.null(.path_log)) {
      error_logging(.path_log, "ERROR", msg_, "Empty table")
    }

    print_verbose(msg_, .verbose, "\r")
    return(FALSE)
  }

  return(TRUE)
}
