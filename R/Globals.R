utils::globalVariables(c(
  # Index and Identification Variables
  "CIK", "Cik", "HashIndex", "HashDocument", "ID", "DocID", "doc_id",

  # Date and Period Variables
  "Year", "Quarter", "YearQuarter",

  # Document Type and Classification Variables
  "Type", "FormType", "DocClass", "DocTypeRaw", "DocTypeMod",

  # File and Path Variables
  "path", "PathOut", "PathZIP", "DirTMP", "DirOut",
  "FileZIP", "filename", "DocFile", "Ext", "FileExt", "OutExt",

  # Document Content Variables
  "HTML", "TextRaw", "TextMod",
  "nWords", "nChars",

  # URL Variables
  "UrlMasterIndex", "UrlFullText", "UrlIndexPage", "UrlDocument",

  # Document Metadata Variables
  "Seq", "Description", "Document", "Size",
  "DateFiled",

  # Processing Status Variables
  "Error", "Split", "Print", "data",
  "AllRow", "AllTot", "AllPer",
  "DocNum", "nDocs", "ext",

  # Dataset Type Variables
  "DocumentLinks", "MasterIndex",

  # Text Processing Variables
  "text", "Filename", "PathTmp", "nam_out"
))
