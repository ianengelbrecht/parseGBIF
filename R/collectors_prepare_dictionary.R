#' @title Prepare the list with the last name of the main collector
#' @name collectors_prepare_dictionary
#'
#' @description
#' Returns the list with the last name of the main collector associated with the unique key recordedBy.
#' A necessary step for parsing duplicate records is generating a robust key for each unique collecting event
#' (aka 'gathering') that will support the recognition of duplicate records. For this purpose we generate a string
#' combining the plant family name + first collector's surname + the collection number.
#' It is therefore essential to consistently record the collector surname and for this purpose we provide a collector
#' dictionary. To extract the surname of the main collector based on the recordedBy field and assemble a list relating
#' the last name of the main collector and the raw data from the recordedBy, use the collectors_prepare_dictionary function.
#'
#' It is recommended to check the main collector's last name in the nameRecordedBy_Standard field.
#' Our goal is to standardize the main collector's last name, which is automatically extracted from the recordedBy field.
#' We do so by standardizing the text string so that it begins with an uppercase character and to replace non-ASCII
#' characters, so that collector responsible for a collection event is always recorded using the same string of characters.
#' If the searched recordedBy entry is present in the collector's dictionary, the function retrieves the last name
#' of the main collector with reference to the recordedBy field (in which case the CollectorDictionary field will be
#' flagged as 'checked'), otherwise, the function will return the last name of the main collector, extracted
#' automatically from the recordedBy field.
#'
#' Once verified, the collector's dictionary can be reused in the future.
#'
#' @param occ
#' Data frame. GBIF occurrence table with selected columns as returned by `select_gbif_fields(columns = 'standard')`.
#'
#' @param collectorDictionary_file
#' Character. Path to a collector dictionary file on your local disk. 
#' Must be provided if `collectorDictionary` data frame is not supplied.
#'
#' @param collectorDictionary
#' Data frame. Pre-loaded collector dictionary data. If provided, uses this data instead of loading from a file.
#'
#' @param silence
#' Logical. If `TRUE`, does not display progress messages. Default is `TRUE`.
#'
#' @param surname_selection_type
#' Character. Allows you to select two types of results for the main collector's last name:
#' - `"largest_string"`: word with the largest number of characters (default)
#' - `"last_name"`: literally the last name of the main collector, with more than two characters.
#'
#' @param max_words_name
#' Integer. Maximum words in the name. Default is 6.
#'
#' @param maximum_characters_in_name
#' Integer. Maximum characters in name. Default is 3.
#'
#' @details
#' ## Workflow Description:
#'
#' If recordedBy is present in the collector's dictionary, it returns the checked name; if not,
#' it returns the last name of the main collector extracted from the recordedBy field.
#'
#' It is recommended to curate the main collector's surname automatically extracted from the recordedBy field.
#' The objective is to standardize the last name of the main collector so that the primary botanical
#' collector of a sample is always recognized by the same last name, standardized in capital letters
#' and with non-ASCII characters replaced.
#'
#' ## Technical Implementation:
#'
#' 1. **Data Ingestion**: Loads dictionary from local file or provided data frame.
#' 2. **Data Extraction**: Extracts unique recordedBy values from occurrence data.
#' 3. **Name Processing**: Applies `collectors_get_name()` to extract surnames using specified selection method.
#' 4. **Dictionary Integration**: Merges results with existing collector dictionary using optimized relational joins.
#' 5. **Verification Flagging**: Marks dictionary-verified entries as "checked".
#'
#' @return
#' Returns a data frame with the following columns:
#' - `Ctrl_nameRecordedBy_Standard`: Standardized collector surname
#' - `Ctrl_recordedBy`: Original recordedBy field content
#' - `Ctrl_notes`: Additional notes from dictionary
#' - `collectorDictionary`: Verification status ("checked" if verified)
#' - `Ctrl_update`: Update information
#' - `collectorName`: Full collector name
#' - `Ctrl_fullName`: Alternative full name representation
#' - `Ctrl_fullNameII`: Secondary name representation
#' - `CVStarrVirtualHerbarium_PersonDetails`: Additional person details
#' - `parseGBIF_collector_record_count`: Number of occurrences associated with this exact recordedBy string
#'
#' @author
#' Pablo Hendrigo Alves de Melo,
#' Nadia Bystriakova &
#' Alexandre Monro
#' (Optimized via data.table for performance)
#'
#' @seealso
#' [`collectors_get_name()`] for extracting collector names from recordedBy fields,
#' [`generate_collection_event_key()`] for creating unique collection event identifiers
#'
#' @importFrom data.table fread as.data.table setDT setnames setDF fifelse
#' @importFrom rscopus replace_non_ascii
#' @export
collectors_prepare_dictionary <- function(occ = NA,
                                          collectorDictionary_file = '',
                                          collectorDictionary = NULL,
                                          silence = TRUE,
                                          surname_selection_type = 'largest_string',
                                          max_words_name = 6,
                                          maximum_characters_in_name = 3) {

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required for this fast implementation.")
  }

  stage_msg <- function(msg) if (!isTRUE(silence)) print(paste0("[parseGBIF] ", msg))

  # ---- 1. Data Ingestion (Fixed logic) ----
  stage_msg('Loading collectorDictionary...')

  if (!is.null(collectorDictionary)) {
    if (data.table::is.data.table(collectorDictionary)) {
      # Safely copy to prevent altering the user's original table by reference
      dictDT <- data.table::copy(collectorDictionary)
    } else {
      # Converts a standard data.frame into a data.table
      dictDT <- data.table::as.data.table(collectorDictionary)
    }
  } else if (collectorDictionary_file != '' && !is.na(collectorDictionary_file)) {
    if (!file.exists(collectorDictionary_file)) {
      stop("File does not exist: ", collectorDictionary_file)
    }
    dictDT <- data.table::fread(collectorDictionary_file, encoding = 'UTF-8')
  } else {
    stop("You must provide either 'collectorDictionary' (data frame) or 'collectorDictionary_file' (path).")
  }

  # ---- 2. Split Validation Checks ----
  if (NROW(dictDT) == 0) {
    stop("Validation Error: collectorDictionary is empty (contains 0 rows).")
  }

  expected_cols <- c('Ctrl_nameRecordedBy_Standard', 'Ctrl_recordedBy', 
                     'Ctrl_notes', 'collectorDictionary', 'Ctrl_update', 
                     'collectorName', 'Ctrl_fullName', 'Ctrl_fullNameII', 
                     'CVStarrVirtualHerbarium_PersonDetails')
  
  if (!any(colnames(dictDT) %in% expected_cols)) {
    stop("Validation Error: collectorDictionary does not contain any recognized dictionary column names.")
  }

  if (NROW(occ) == 0 || is.na(occ)[1]) {
    stop("Occurrence dataset is empty!")
  }

  # ---- 3. Dictionary Prep ----
  dictDT[, Ctrl_recordedBy := toupper(as.character(Ctrl_recordedBy))]
  if ("Ctrl_nameRecordedBy_Standard" %in% names(dictDT)) {
    data.table::setnames(dictDT, "Ctrl_nameRecordedBy_Standard", "Ctrl_nameRecordedBy_Standard_x")
  }

  data.table::setkey(dictDT, Ctrl_recordedBy)

  # ---- 4. Process Occurrences ----
  stage_msg("Extracting the main collector's surname....")
  
  occDT <- data.table::as.data.table(occ)

  # Instantly get unique recordedBy AND their exact frequency count in one pass
  occ_counts <- occDT[, .(parseGBIF_collector_record_count = .N), 
                      by = .(Ctrl_recordedBy = toupper(as.character(Ctrl_recordedBy)))]
  
  unique_recordedBy <- occ_counts$Ctrl_recordedBy

  # Process names
  extracted_names <- lapply(unique_recordedBy, function(x) {
    collectors_get_name(
      x,
      surname_selection_type = surname_selection_type,
      max_words_name = max_words_name,
      maximum_characters_in_name = maximum_characters_in_name
    )
  })

  # Extract the first column result safely and standardize strings
  extracted_vec <- sapply(extracted_names, function(res) as.character(res[1]))
  cleaned_names <- rscopus::replace_non_ascii(toupper(extracted_vec))

  resDT <- data.table::data.table(
    Ctrl_nameRecordedBy_Standard = cleaned_names,
    Ctrl_recordedBy = unique_recordedBy,
    parseGBIF_collector_record_count = occ_counts$parseGBIF_collector_record_count
  )

  data.table::setkey(resDT, Ctrl_recordedBy)

  # ---- 5. Relational Join (Replacing slow left_join + mutate) ----
  # Perform a native Left Join
  resDT <- dictDT[resDT, on = "Ctrl_recordedBy"]

  # Update flags and names via reference
  resDT[, collectorDictionary := data.table::fifelse(!is.na(Ctrl_nameRecordedBy_Standard_x), "checked", "")]
  resDT[collectorDictionary == "checked", Ctrl_nameRecordedBy_Standard := Ctrl_nameRecordedBy_Standard_x]

  # Ensure all expected columns exist (pad with NA if missing)
  for (col in expected_cols) {
    if (!col %in% names(resDT) && col != "Ctrl_nameRecordedBy_Standard") {
      resDT[, (col) := NA_character_]
    }
  }

  # Cast everything to character strictly as the original did
  for (col in expected_cols) {
    resDT[, (col) := as.character(get(col))]
  }

  # Select final columns and order
  final_cols <- c("Ctrl_nameRecordedBy_Standard", "Ctrl_recordedBy", "parseGBIF_collector_record_count", "Ctrl_notes",
                  "collectorDictionary", "Ctrl_update", "collectorName",
                  "Ctrl_fullName", "Ctrl_fullNameII", "CVStarrVirtualHerbarium_PersonDetails")
  
  resDT <- resDT[, ..final_cols]
  data.table::setorder(resDT, collectorDictionary, Ctrl_nameRecordedBy_Standard, Ctrl_recordedBy)
  data.table::setDF(resDT)
  
  return(resDT)
}