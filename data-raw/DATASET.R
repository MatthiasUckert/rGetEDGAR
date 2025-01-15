## code to prepare `DATASET` dataset goes here

FormTypes <- edgar_read_master_index(
  .dir = fs::dir_create("../_package_debug/rGetEDGAR"),
  .from = NULL,
  .to = NULL,
  .ciks = NULL,
  .formtypes = NULL,
  .collect = FALSE
) %>%
  dplyr::count(FormType, sort = TRUE) %>%
  dplyr::collect() %>%
  dplyr::mutate(FormTypeMod = FormType) %>%
  dplyr::select(FormTypeRaw = FormType, FormTypeMod, nFormTypes = n)

DocTypes <- edgar_read_document_links(
  .dir = fs::dir_create("../_package_debug/rGetEDGAR"),
  .from = NULL,
  .to = NULL,
  .ciks = NULL,
  .formtypes = NULL,
  .doctypes = NULL,
  .collect = FALSE
) %>%
  dplyr::count(Type, sort = TRUE) %>%
  dplyr::collect() %>%
  dplyr::mutate(DocTypeMod = Type) %>%
  dplyr::select(DocTypeRaw = Type, DocTypeMod, nDocTypes = n)


RawTypes <- edgar_read_master_index(
  .dir = fs::dir_create("../_package_debug/rGetEDGAR"),
  .from = NULL,
  .to = NULL,
  .ciks = NULL,
  .formtypes = NULL,
  .collect = FALSE
) %>%
  dplyr::select(HashIndex, FormTypeRaw = FormType) %>%
  dplyr::inner_join(
    y = edgar_read_document_links(
      .dir = fs::dir_create("../_package_debug/rGetEDGAR"),
      .from = NULL,
      .to = NULL,
      .ciks = NULL,
      .formtypes = NULL,
      .doctypes = NULL,
      .collect = FALSE
    ) %>% dplyr::select(HashIndex, HashDocument, DocTypeRaw = Type)
  ) %>%
  dplyr::count(FormTypeRaw, DocTypeRaw, sort = TRUE) %>%
  dplyr::collect() %>%
  dplyr::rename(nRawTypes = n)

ModTypes <- edgar_read_master_index(
  .dir = fs::dir_create("../_package_debug/rGetEDGAR"),
  .from = NULL,
  .to = NULL,
  .ciks = NULL,
  .formtypes = NULL,
  .collect = FALSE
) %>%
  dplyr::select(HashIndex, FormTypeMod = FormType) %>%
  dplyr::inner_join(
    y = edgar_read_document_links(
      .dir = fs::dir_create("../_package_debug/rGetEDGAR"),
      .from = NULL,
      .to = NULL,
      .ciks = NULL,
      .formtypes = NULL,
      .doctypes = NULL,
      .collect = FALSE
    ) %>% dplyr::select(HashIndex, HashDocument, DocTypeMod = Type)
  ) %>%
  dplyr::count(FormTypeMod, DocTypeMod, sort = TRUE) %>%
  dplyr::collect() %>%
  dplyr::rename(nModTypes = n)


usethis::use_data(FormTypes, DocTypes, RawTypes, ModTypes, overwrite = TRUE)
