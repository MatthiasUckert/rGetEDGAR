## code to prepare `DATASET` dataset goes here

devtools::load_all(".")

FormTypesRaw <- edgar_read_master_index(
  .dir = fs::dir_create("../_package_debug/rGetEDGAR"),
  .from = NULL,
  .to = NULL,
  .ciks = NULL,
  .formtypes = NULL,
  .collect = FALSE
) %>%
  dplyr::count(FormType, sort = TRUE) %>%
  dplyr::collect() %>%
  dplyr::mutate(FormTypeMod = dplyr::case_when(
    # Annual Reports (10-K family)
    grepl("^10-?K\\b", FormType) ~ "10-K (Annual Report)",
    grepl("^10-?K/A\\b", FormType) ~ "10-K/A (Amended Annual Report)",
    grepl("^10-?KSB\\b", FormType) ~ "10-KSB (Annual Report for Small Business)",
    grepl("^10-?KSB/A\\b", FormType) ~ "10-KSB/A (Amended Annual Report for Small Business)",
    grepl("^10-?K405\\b", FormType) ~ "10-K405 (Annual Report S-K Item 405)",
    grepl("^10-?K405/A\\b", FormType) ~ "10-K405/A (Amended Annual Report S-K Item 405)",
    grepl("^10-?KSB40\\b", FormType) ~ "10-KSB40 (Annual and Transition Reports)",
    grepl("^10-?KSB40/A\\b", FormType) ~ "10-KSB40/A (Amended Annual and Transition Reports)",
    grepl("^10-?KT\\b", FormType) ~ "10-KT (Transition Report)",
    grepl("^10-?KT/A\\b", FormType) ~ "10-KT/A (Amended Transition Report)",

    # Quarterly Reports (10-Q family)
    grepl("^10-?Q\\b", FormType) ~ "10-Q (Quarterly Report)",
    grepl("^10-?Q/A\\b", FormType) ~ "10-Q/A (Amended Quarterly Report)",
    grepl("^10-?QSB\\b", FormType) ~ "10-QSB (Quarterly Report for Small Business)",
    grepl("^10-?QSB/A\\b", FormType) ~ "10-QSB/A (Amended Quarterly Report for Small Business)",
    grepl("^10-?QT\\b", FormType) ~ "10-QT (Transition Quarterly Report)",
    grepl("^10-?QT/A\\b", FormType) ~ "10-QT/A (Amended Transition Quarterly Report)",

    # Current Reports (8-K family)
    grepl("^8-?K\\b", FormType) ~ "8-K (Current Report, Unscheduled Material Events)",
    grepl("^8-?K/A\\b", FormType) ~ "8-K/A (Amended Current Report)",
    grepl("^8-?K12G3\\b", FormType) ~ "8-K12G3 (Notification of Registration)",
    grepl("^8-?K12B\\b", FormType) ~ "8-K12B (Registration Notification)",
    grepl("^8-?K15D5\\b", FormType) ~ "8-K15D5 (Notification of Duty to Report)",

    # Foreign Issuer Reports
    grepl("^20-?F\\b", FormType) ~ "20-F (Foreign Private Issuer Annual Report)",
    grepl("^20-?F/A\\b", FormType) ~ "20-F/A (Amended Foreign Private Issuer Annual Report)",
    grepl("^20FR12[BG]\\b", FormType) ~ "20FR12 (Foreign Private Issuer Registration)",
    grepl("^6-?K\\b", FormType) ~ "6-K (Foreign Issuer Report)",
    grepl("^6-?K/A\\b", FormType) ~ "6-K/A (Amended Foreign Issuer Report)",

    # Other Annual Reports
    grepl("^11-?K\\b", FormType) ~ "11-K (Employee Stock Plan Annual Report)",
    grepl("^11-?K/A\\b", FormType) ~ "11-K/A (Amended Employee Stock Plan Annual Report)",
    grepl("^18-?K\\b", FormType) ~ "18-K (Annual Report for Foreign Governments)",

    # Registration and Securities
    grepl("^S-?1\\b", FormType) ~ "S-1 (IPO Registration Statement)",
    grepl("^S-?3\\b", FormType) ~ "S-3 (Simplified Registration Statement)",
    grepl("^S-?4\\b", FormType) ~ "S-4 (Business Combination Registration)",
    grepl("^S-?8\\b", FormType) ~ "S-8 (Employee Benefit Plan Registration)",
    grepl("^F-?1\\b", FormType) ~ "F-1 (Foreign Issuer Registration)",
    grepl("^F-?3\\b", FormType) ~ "F-3 (Foreign Issuer Simplified Registration)",

    # Proxy Materials
    grepl("^DEF ?14A\\b", FormType) ~ "DEF 14A (Definitive Proxy Statement)",
    grepl("^PRE ?14A\\b", FormType) ~ "PRE 14A (Preliminary Proxy Statement)",
    grepl("^DEFA14A\\b", FormType) ~ "DEFA14A (Additional Definitive Proxy Materials)",
    grepl("^DEFM14A\\b", FormType) ~ "DEFM14A (Merger Proxy Statement)",

    # Late Filing Notices
    grepl("^NT ?10-K\\b", FormType) ~ "NT 10-K (Notice of Late Annual Filing)",
    grepl("^NT ?10-Q\\b", FormType) ~ "NT 10-Q (Notice of Late Quarterly Filing)",
    grepl("^NT ?20-F\\b", FormType) ~ "NT 20-F (Notice of Late Foreign Annual Filing)",

    # Investment Company Forms
    grepl("^N-?CSR\\b", FormType) ~ "N-CSR (Investment Company Annual/Semi-Annual Report)",
    grepl("^N-?Q\\b", FormType) ~ "N-Q (Investment Company Quarterly Schedule)",
    grepl("^N-?PORT-P\\b", FormType) ~ "N-PORT-P (Monthly Portfolio Holdings)",

    # Schedule Forms
    grepl("^SC ?13D\\b", FormType) ~ "SC 13D (Beneficial Ownership Report)",
    grepl("^SC ?13G\\b", FormType) ~ "SC 13G (Simplified Beneficial Ownership Report)",
    grepl("^SC ?14D9\\b", FormType) ~ "SC 14D9 (Tender Offer Recommendation)",

    # Other Common Forms
    grepl("^424B[1-5]\\b", FormType) ~ "424B (Prospectus)",
    grepl("^ABS-EE\\b", FormType) ~ "ABS-EE (Asset-Backed Securities Report)",
    grepl("^CORRESP\\b", FormType) ~ "CORRESP (SEC Correspondence)",
    grepl("^D\\b", FormType) ~ "D (Notice of Exempt Offering)",

    # TRUE ~ FormType  # Default case
  )) %>%
  tidyr::separate(FormTypeMod, c("FormTypeMod", "Description"), sep = " \\(") %>%
  dplyr::mutate(Description = trimws(gsub("\\)$", "", Description))) %>%
  dplyr::mutate(FormClass = "TBD") %>%
  dplyr::select(FormClass, FormTypeRaw = FormType, FormTypeMod, nDocs = n, Description)


FormTypesMod <- FormTypesRaw %>%
  dplyr::group_by(FormClass, FormTypeMod, Description) %>%
  dplyr::summarise(nDocs = sum(nDocs), .groups = "drop") %>%
  dplyr::select(FormClass, FormTypeMod, nDocs, Description) %>%
  dplyr::arrange(-nDocs)


DocTypesRaw <- edgar_read_document_links(
  .dir = fs::dir_create("../_package_debug/rGetEDGAR"),
  .from = NULL,
  .to = NULL,
  .ciks = NULL,
  .formtypes = NULL,
  .doctypes = NULL,
  .collect = FALSE
) %>%
  dplyr::mutate(
    Type = dplyr::if_else(is.na(Type) & Description == "Complete submission text file", "Full Submission Text File", Type)
    ) %>%
  dplyr::filter(Error == 0) %>%
  dplyr::count(Type, sort = TRUE) %>%
  dplyr::collect() %>%
  dplyr::mutate(DocTypeMod = dplyr::case_when(
    grepl("^EX-1\\b", Type) ~ "Exhibit 01 (Underwriting agreement) - Exhibits",
    grepl("^EX-2\\b", Type) ~ "Exhibit 02 (Plan of acquisition, reorganization, arrangement, liquidation or succession) - Exhibits",
    grepl("^EX-3\\b", Type) ~ "Exhibit 03 (Articles of incorporation and Bylaws) - Exhibits",
    grepl("^EX-4\\b", Type) ~ "Exhibit 04 (Instruments defining the rights of securities holders, including indentures) - Exhibits",
    grepl("^EX-5\\b", Type) ~ "Exhibit 05 (Opinion re legality) - Exhibits",
    grepl("^EX-6\\b", Type) ~ "Exhibit 06 ([Reserved]) - Exhibits",
    grepl("^EX-7\\b", Type) ~ "Exhibit 07 (Correspondence from an independent accountant regarding non-reliance) - Exhibits",
    grepl("^EX-8\\b", Type) ~ "Exhibit 08 (Opinion re tax matters) - Exhibits",
    grepl("^EX-9\\b", Type) ~ "Exhibit 09 (Voting trust agreement) - Exhibits",
    grepl("^EX-10\\b", Type) ~ "Exhibit 10 (Material contracts) - Exhibits",
    grepl("^EX-11\\b|^EX-12\\b", Type) ~ "Exhibits 11-12 ([Reserved]) - Exhibits",
    grepl("^EX-13\\b", Type) ~ "Exhibit 13 (Annual report to security holders) - Exhibits",
    grepl("^EX-14\\b", Type) ~ "Exhibit 14 (Code of Ethics) - Exhibits",
    grepl("^EX-15\\b", Type) ~ "Exhibit 15 (Letter re unaudited interim financial information) - Exhibits",
    grepl("^EX-16\\b", Type) ~ "Exhibit 16 (Letter re change in certifying accountant) - Exhibits",
    grepl("^EX-17\\b", Type) ~ "Exhibit 17 (Correspondence on departure of director) - Exhibits",
    grepl("^EX-18\\b", Type) ~ "Exhibit 18 (Letter re change in accounting principles) - Exhibits",
    grepl("^EX-19\\b", Type) ~ "Exhibit 19 (Insider trading policies and procedures) - Exhibits",
    grepl("^EX-20\\b", Type) ~ "Exhibit 20 (Other documents or statements to security holders) - Exhibits",
    grepl("^EX-21\\b", Type) ~ "Exhibit 21 (Subsidiaries of the registrant) - Exhibits",
    grepl("^EX-22\\b", Type) ~ "Exhibit 22 (Subsidiary guarantors and issuers of guaranteed securities) - Exhibits",
    grepl("^EX-23\\b", Type) ~ "Exhibit 23 (Consents of experts and counsel) - Exhibits",
    grepl("^EX-24\\b", Type) ~ "Exhibit 24 (Power of attorney) - Exhibits",
    grepl("^EX-25\\b", Type) ~ "Exhibit 25 (Statement of eligibility of trustee) - Exhibits",
    grepl("^EX-(2[6-9]|30)\\b", Type) ~ "Exhibits 26-30 ([Reserved]) - Exhibits",
    grepl("^EX-31\\b", Type) ~ "Exhibit 31 (Rule 13a-14a/15d-14a Certifications) - Exhibits",
    grepl("^EX-32\\b", Type) ~ "Exhibit 32 (Section 1350 Certifications) - Exhibits",
    grepl("^EX-33\\b", Type) ~ "Exhibit 33 (Report on assessment of compliance with servicing criteria) - Exhibits",
    grepl("^EX-34\\b", Type) ~ "Exhibit 34 (Attestation report on assessment of compliance) - Exhibits",
    grepl("^EX-35\\b", Type) ~ "Exhibit 35 (Servicer compliance statement) - Exhibits",
    grepl("^EX-36\\b", Type) ~ "Exhibit 36 (Depositor Certification for shelf offerings) - Exhibits",
    grepl("^EX-(3[7-9]|[4-8][0-9]|9[0-4])\\b", Type) ~ "Exhibits 37-94 ([Reserved]) - Exhibits",
    grepl("^EX-95\\b", Type) ~ "Exhibit 95 (Mine Safety Disclosure Exhibit) - Exhibits",
    grepl("^EX-96\\b", Type) ~ "Exhibit 96 (Technical report summary) - Exhibits",
    grepl("^EX-97\\b", Type) ~ "Exhibit 97 (Policy Relating to Recovery of Erroneously Awarded Compensation) - Exhibits",
    grepl("^EX-98\\b", Type) ~ "Exhibit 98 (Reports, opinions, or appraisals in de-SPAC transactions) - Exhibits",
    grepl("^EX-99\\b", Type) ~ "Exhibit 99 (Additional exhibits) - Exhibits",
    grepl("^EX-100\\b", Type) ~ "Exhibit 100 ([Reserved]) - Exhibits",

    grepl("^EX-101.LAB\\b", Type) ~ "Exhibit 101.LAB (XBRL Labels) - Exhibits",
    grepl("^EX-101.PRE\\b", Type) ~ "Exhibit 101.PRE (XBRL Presentation) - Exhibits",
    grepl("^EX-101.SCH\\b", Type) ~ "Exhibit 101.SCH (XBRL Taxonomy Schema) - Exhibits",
    grepl("^EX-101.CAL\\b", Type) ~ "Exhibit 101.CAL (XBRL Calculation) - Exhibits",
    grepl("^EX-101.DEF\\b", Type) ~ "Exhibit 101.DEF (XBRL Definition) - Exhibits",
    grepl("^EX-101.INS\\b", Type) ~ "Exhibit 101.INS (XBRL Instance Document) - Exhibits",

    grepl("^EX-102\\b", Type) ~ "Exhibit 102 (Asset Data File) - Exhibits",
    grepl("^EX-103\\b", Type) ~ "Exhibit 103 (Asset Related Documents) - Exhibits",
    grepl("^EX-104\\b", Type) ~ "Exhibit 104 (Cover Page Interactive Data File) - Exhibits",
    grepl("^EX-105\\b", Type) ~ "Exhibit 105 ([Reserved]) - Exhibits",
    grepl("^EX-106\\b", Type) ~ "Exhibit 106 (Static Pool PDF) - Exhibits",
    grepl("^EX-107\\b", Type) ~ "Exhibit 107 (Filing Fee Table) - Exhibits",

    # Main form types with consistent pattern for amendments
    grepl("^10-?K\\b", Type) ~ "10-K (Annual Report) - Main Form Types",
    grepl("^10-?K/A\\b", Type) ~ "10-K/A (Amended Annual Report) - Main Form Types",
    grepl("^10-?KSB\\b", Type) ~ "10-KSB (Annual Report for Small Business) - Main Form Types",
    grepl("^10-?KSB/A\\b", Type) ~ "10-KSB/A (Amended Annual Report for Small Business) - Main Form Types",
    grepl("^10-?K405\\b", Type) ~ "10-K405 (Annual Report S-K Item 405) - Main Form Types",
    grepl("^10-?K405/A\\b", Type) ~ "10-K405/A (Amended Annual Report S-K Item 405) - Main Form Types",
    grepl("^10-?KSB40\\b", Type) ~ "10-KSB40 (Annual and Transition Reports) - Main Form Types",
    grepl("^10-?KSB40/A\\b", Type) ~ "10-KSB40/A (Amended Annual and Transition Reports) - Main Form Types",

    grepl("^10-?Q\\b", Type) ~ "10-Q (Quarterly Report) - Main Form Types",
    grepl("^10-?Q/A\\b", Type) ~ "10-Q/A (Amended Quarterly Report) - Main Form Types",
    grepl("^10-?QSB\\b", Type) ~ "10-Q (Quarterly Report for Small Business) - Main Form Types",
    grepl("^10-?QSB/A\\b", Type) ~ "10-Q/A (Amended Quarterly Report for Small Business) - Main Form Types",

    grepl("^8-?K\\b", Type) ~ "8-K (Current Report, Unscheduled Material Events) - Main Form Types",
    grepl("^8-?K/A\\b", Type) ~ "8-K/A (Amended Current Report, Unscheduled Material Events) - Main Form Types",

    grepl("^20-?F\\b", Type) ~ "20-F (Foreign Private Issuer Annual Report) - Main Form Types",
    grepl("^20-?F/A\\b", Type) ~ "20-F/A (Amended Foreign Private Issuer Annual Report) - Main Form Types",

    grepl("^11-?K\\b", Type) ~ "11-K (Employee Stock Plan Annual Report) - Main Form Types",
    grepl("^11-?K/A\\b", Type) ~ "11-K/A (Amended Employee Stock Plan Annual Report) - Main Form Types",

    grepl("^6-?K\\b", Type) ~ "6-K (Foreign Issuer Report) - Main Form Types",
    grepl("^6-?K/A\\b", Type) ~ "6-K/A (Amended Foreign Issuer Report) - Main Form Types",

    grepl("^8-?K12G3\\b", Type) ~ "8-K12G3 (Notification that a class of securities of successor issuer is deemed to be registered pursuant to Section 12g) - Main Form Types",

    grepl("^18-?K\\b", Type) ~ "18-K (Annual report for foreign governments) - Main Form Types",


    # Registration and other forms
    grepl("^20FR12[BG]$", Type) ~ "20FR12 (Foreign Private Issuer Initial Registration) - Registration Forms",
    grepl("^NT[N]* 10-[KQ]", Type) ~ "NT (Notice of Late Filing) - Registration Forms",
    grepl("^(8-A12B|10-12B)\\b", Type) ~ "12B (Securities Registration) - Registration Forms",
    grepl("^DEF 14A\\b", Type) ~ "DEF 14A (Definitive Proxy Statement) - Registration Forms",
    grepl("^NTN\\s?-?10Q", Type) ~ "NTN 10Q (Filing NTN 10Q) - Registration Forms",

    # Other document types
    grepl("^GRAPHIC[.0-9A-Z]*$", Type) ~ "GRAPHIC (Graphic File) - Other Documents",
    grepl("^XML\\b", Type) ~ "XML (XML File) - Other Documents",
    grepl("^COVER\\b", Type) ~ "COVER (Cover File) - Other Documents",
    grepl("^CORRESP\\b", Type) ~ "CORRESP (S.E.C. Correspondence Letter, CORRESP documents are letters or requests filed by the SEC directed toward a company) - Other Documents",
    grepl("^ARS\\b", Type) ~ "ARS (Annual Report Summary) - Other Documents",
    grepl("^IRANNOTICE\\b", Type) ~ "IRANNOTICE (Exchange Act Annual Disclosure Report) - Other Documents",
    grepl("^U-3A-2\\b", Type) ~ "U-3A-2 (Public Utility Filing) - Other Documents",

    Type == "Full Submission Text File" ~ "Full Submission Text File (Full Submission Text File) - Main Form Types"

  )) %>%
  tidyr::separate(DocTypeMod, c("DocTypeMod", "Description"), sep = " \\(") %>%
  tidyr::separate(Description, c("Description", "DocClass"), sep = "\\) - ") %>%
  dplyr::mutate(Description = trimws(gsub("\\)$", "", Description))) %>%
  dplyr::select(DocClass, DocTypeRaw = Type, DocTypeMod, nDocs = n, Description)


DocTypesMod <- DocTypesRaw %>%
  dplyr::group_by(DocClass, DocTypeMod, Description) %>%
  dplyr::summarise(nDocs = sum(nDocs), .groups = "drop") %>%
  dplyr::select(DocClass, DocTypeMod, nDocs, Description) %>%
  dplyr::arrange(-nDocs)


usethis::use_data(DocTypesRaw, DocTypesMod, FormTypesRaw, FormTypesMod, overwrite = TRUE)

list(
  DocTypesRaw = DocTypesRaw,
  DocTypesMod = DocTypesMod,
  FormTypesRaw = FormTypesRaw,
  FormTypesMod = FormTypesMod
) %>% openxlsx::write.xlsx("_debug/FormAndDocType.xlsx", TRUE, TRUE)
