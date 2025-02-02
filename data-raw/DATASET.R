## code to prepare `DATASET` dataset goes here

devtools::load_all(".")
lp_ <- get_directories(fs::dir_create("../_package_debug/rGetEDGAR"))

Table_FormTypesRaw <- arrow::open_dataset(lp_$MasterIndex$Parquet) %>%
  dplyr::count(FormType, sort = TRUE) %>%
  dplyr::collect() %>%
  dplyr::left_join(
    y = tibble::as_tibble(openxlsx::read.xlsx("inst/extdata/LookUpTable-Types.xlsx", 2)),
    by = "FormType"
    ) %>%
  dplyr::select(FormClass, FormTypeRaw = FormType, FormTypeMod, nDocs = n, Description)

sort(Table_FormTypesRaw$FormTypeRaw[is.na(Table_FormTypesRaw$FormTypeMod)])

Table_FormTypesMod <- Table_FormTypesRaw %>%
  dplyr::group_by(FormClass, FormTypeMod, Description) %>%
  dplyr::summarise(nDocs = sum(nDocs), .groups = "drop") %>%
  dplyr::select(FormClass, FormTypeMod, nDocs, Description) %>%
  dplyr::arrange(-nDocs)

Table_DocTypesRaw <- arrow::open_dataset(lp_$DocumentLinks$Parquet) %>%
  dplyr::mutate(
    Type = dplyr::if_else(is.na(Type) & Description == "Complete submission text file", "Full Submission Text File", Type)
    ) %>%
  dplyr::filter(Error == 0) %>%
  dplyr::count(Type, sort = TRUE) %>%
  dplyr::collect() %>%
  tidyr::expand_grid(
    tibble::as_tibble(openxlsx::read.xlsx("inst/extdata/LookUpTable-Types.xlsx", 1))
  ) %>%
  dplyr::mutate(Check = stringi::stri_detect_regex(Type, RegEx)) %>%
  dplyr::arrange(-Check) %>%
  dplyr::group_by(Type) %>%
  dplyr::mutate(nHit = sum(Check)) %>%
  dplyr::ungroup() %>%
  dplyr::distinct(Type, .keep_all = TRUE) %>%
  dplyr::arrange(nHit)  %>%
  dplyr::select(DocClass, DocTypeRaw = Type, DocTypeMod, nDocs = n, Description)

Table_DocTypesMod <- Table_DocTypesRaw %>%
  dplyr::group_by(DocClass, DocTypeMod, Description) %>%
  dplyr::summarise(nDocs = sum(nDocs), .groups = "drop") %>%
  dplyr::select(DocClass, DocTypeMod, nDocs, Description) %>%
  dplyr::arrange(-nDocs)


usethis::use_data(Table_DocTypesRaw, Table_DocTypesMod, Table_FormTypesRaw, Table_FormTypesMod, overwrite = TRUE)

list(
  Table_DocTypesRaw = Table_DocTypesRaw,
  Table_DocTypesMod = Table_DocTypesMod,
  Table_FormTypesRaw = Table_FormTypesRaw,
  Table_FormTypesMod = Table_FormTypesMod
) %>% openxlsx::write.xlsx("_debug/FormAndDocType.xlsx", TRUE, TRUE)
