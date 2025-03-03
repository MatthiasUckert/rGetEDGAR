utils::globalVariables(c(
  # content_to_dataframe
  "text", "Filename", "Cik", "UrlFullText", "CIK", "DateFiled",
  "UrlIndexPage", "HashIndex", "Year", "Quarter", "YearQuarter",

  # edgar_download_document
  "dir_tmp", "dir_out", "YearQuarter", "DocType", "Seq", "DocID", "UrlDocument",

  # edgar_get_document_links
  "YearQuarter", "Seq", "UrlIndexPage", "HashIndex", "CIK", "HTML", "DocID",

  # edgar_get_master_index
  "UrlMasterIndex", "Year", "Quarter", "YearQtrSave", "PathOut",

  # edgar_parse_documents
  "filename", "DocID", "PathZIP", "dir_out", "FileExt",

  # get_doclinks
  "UrlDocument", "Seq", "HashDocument", "CIK", "Size", "Description",
  "Document", "Type", "HashIndex", "DocID", "YearQuarter",

  # get_edgar_params
  "DocTypeMod",

  # get_tbp_docids
  "filename", "DocID", "UrlDocument", "DocExt", "DocTypeRaw", "DocTypeMod",
  "YearQuarter", "TypeSave", "YqSave", "CIK", "Seq", "dir_tmp", "dir_out", "DocType",

  # get_tbp_hashindex
  "HashIndex", "YearQuarter", "CIK", "Seq", "UrlIndexPage",

  # get_year_qtr_table
  "Year", "Quarter", "YearQuarter",

  # list_files
  "path", "doc_id",

  # parse_files
  "HTML", "TextRaw", "TextMod"
))
