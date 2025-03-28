% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/Helpers-Internal.R
\name{parse_content}
\alias{parse_content}
\title{Parse HTTP Response Content}
\usage{
parse_content(.result, .type = c("text", "raw"))
}
\arguments{
\item{.result}{httr response object}
}
\value{
Parsed content as text
}
\description{
Parses HTTP response content with UTF-8 encoding.
}
\keyword{internal}
