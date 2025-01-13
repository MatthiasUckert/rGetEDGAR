## code to prepare `DATASET` dataset goes here
.url_form_type <- "https://www.secfilingdata.com/sec-form-type"
.tab_sec_form_type <- rvest::read_html(.url_form_type) %>%
  rvest::html_elements("table") %>%
  rvest::html_table()

tab_sec_form_type <- .tab_sec_form_type[[1]] %>%
  janitor::clean_names(case = "big_camel") %>%
  dplyr::rename(FormTypeStandard = FormType)

FormTypes <- arrow::open_dataset("../_package_debug/rGetEDGAR/MasterIndex/MasterIndex.parquet") %>%
  dplyr::distinct(FormType) %>%
  dplyr::collect() %>%
  dplyr::rename(FormTypeOriginal = FormType) %>%
  dplyr::mutate(
    FormTypeStandard = FormTypeOriginal,
    FormTypeStandard = gsub("10KSB", "10-KSB", FormTypeStandard),
  ) %>%
  dplyr::left_join(tab_sec_form_type, by = dplyr::join_by(FormTypeStandard)) %>%
  dplyr::arrange(FormTypeStandard)

usethis::use_data(FormTypes, overwrite = TRUE)
