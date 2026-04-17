#' @title Generate Collection Event Key Version 2
#' @name generate_collection_event_key_v2
#'
#' @description
#' Generates standardized collection event keys for biological occurrence data using
#' optimized vectorized operations. Matches recorded collector names against collector
#' dictionaries and creates unique identifiers based on family, collector, record number,
#' and year for efficient duplicate grouping.
#'
#' @param occ
#' Data frame. GBIF occurrence data containing required columns:
#' `Ctrl_recordNumber`, `Ctrl_family`, `Ctrl_recordedBy`, `Ctrl_year`.
#'
#' @param collectorDictionary_checked_file
#' Character. Path to verified collector dictionary CSV file.
#'
#' @param collectorDictionary_checked
#' Data frame. Pre-loaded verified collector dictionary.
#'
#' @param collectorDictionary_file
#' Character. Path to base collector dictionary CSV file. If provided, loads
#' default dictionary from parseGBIF GitHub repository.
#'
#' @param collectorDictionary
#' Data frame. Pre-loaded base collector dictionary.
#'
#' @param silence
#' Logical. If `TRUE`, suppresses progress messages. Default is `TRUE`.
#'
#' @details
#' ## Key Improvements from Version 1:
#' - Vectorized operations replacing loops for better performance
#' - Efficient data.table joins for large datasets
#' - Memory-optimized dictionary loading
#' - Batch processing of collector name matching
#'
#' ## Processing Steps:
#' 1. Loads and validates collector dictionaries
#' 2. Performs vectorized matching of collector names
#' 3. Standardizes record numbers (numeric extraction)
#' 4. Generates unique collection event keys
#' 5. Identifies new collectors for dictionary updates
#'
#' @return
#' A list with three components:
#' - `occ_collectorsDictionary`: Occurrence data with standardized names and collection keys
#' - `summary`: Frequency summary of collection keys
#' - `collectorsDictionary_add`: New collector entries for dictionary updates
#'
#' @author
#' Pablo Hendrigo Alves de Melo,
#' Nadia Bystriakova &
#' Alexandre Monro
#'
#' @encoding UTF-8
#'
#' @examples
#' \donttest{
#' # Generate collection event keys with optimized processing
#' result <- generate_collection_event_key_v2(
#'   occ = occ_data,
#'   collectorDictionary_checked_file = 'collectorDictionary_checked.csv',
#'   silence = FALSE
#' )
#'
#' # View optimized results
#' names(result)
#' head(result$occ_collectorsDictionary)
#' head(result$summary)
#' }
#'
#' @importFrom dplyr bind_rows mutate select rename distinct left_join coalesce
#' @importFrom dplyr if_else arrange desc count all_of case_when
#' @importFrom readr read_csv locale
#' @importFrom stringr str_replace_all
#' @importFrom data.table as.data.table
#' @importFrom utils rm

#' @export
generate_collection_event_key <- function(occ,
                                          collectorDictionary_checked,
                                          collectorDictionary = NULL,
                                          silence = TRUE) {

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required.")
  }

  stage <- function(msg) if (!isTRUE(silence)) message("[parseGBIF] ", msg)

  # ---- Validate and COPY inputs (Memory Safety)
  if (is.null(occ) || !is.data.frame(occ) || nrow(occ) == 0) stop("occ is empty!")
  occDT <- data.table::copy(data.table::as.data.table(occ))

  req_occ <- c("Ctrl_recordNumber", "Ctrl_family", "Ctrl_recordedBy", "Ctrl_year")
  miss_occ <- setdiff(req_occ, names(occDT))
  if (length(miss_occ)) stop("occ missing required columns: ", paste(miss_occ, collapse = ", "))

  if (is.null(collectorDictionary_checked) || !is.data.frame(collectorDictionary_checked) || nrow(collectorDictionary_checked) == 0) {
    stop("collectorDictionary_checked is empty!")
  }
  checkedDT <- data.table::copy(data.table::as.data.table(collectorDictionary_checked))

  req_dict <- c("Ctrl_recordedBy", "Ctrl_nameRecordedBy_Standard")
  miss_dict <- setdiff(req_dict, names(checkedDT))
  if (length(miss_dict)) stop("collectorDictionary_checked missing required columns: ", paste(miss_dict, collapse = ", "))

  # Optional base dictionary
  baseDT <- NULL
  if (!is.null(collectorDictionary)) {
    baseDT <- data.table::copy(data.table::as.data.table(collectorDictionary))
    miss_base <- setdiff(req_dict, names(baseDT))
    if (length(miss_base)) stop("collectorDictionary missing required columns: ", paste(miss_base, collapse = ", "))
  }

  # ---- Normalize casing for join key
  stage("Normalizing collector names for join")
  occDT[, Ctrl_recordedBy := toupper(Ctrl_recordedBy)]
  checkedDT[, Ctrl_recordedBy := toupper(Ctrl_recordedBy)]
  if (!is.null(baseDT)) baseDT[, Ctrl_recordedBy := toupper(Ctrl_recordedBy)]

  # ---- collectorsDictionary_add (new collectors)
  stage("Computing collectorsDictionary_add")
  if (!is.null(baseDT)) {
    # Anti-join to find new collectors
    collectorsDictionary_add <- checkedDT[!baseDT, on = "Ctrl_recordedBy", .(Ctrl_recordedBy, Ctrl_nameRecordedBy_Standard)]
  } else {
    collectorsDictionary_add <- checkedDT[0, .(Ctrl_recordedBy, Ctrl_nameRecordedBy_Standard)]
  }

  # ---- Build lookup (dedupe by Ctrl_recordedBy)
  stage("Preparing collector lookup")
  lookup <- unique(checkedDT[, .(Ctrl_recordedBy, Ctrl_nameRecordedBy_Standard)], by = "Ctrl_recordedBy")

  # ---- High-Speed Update Join
  stage("Joining collector standardized names")

  # Pre-fill with the "Not Found" default
  occDT[, Ctrl_nameRecordedBy_Standard := "NOT-FOUND-COLLECTOR"]

  # Map the standardized names over seamlessly
  occDT[lookup, on = "Ctrl_recordedBy", Ctrl_nameRecordedBy_Standard := i.Ctrl_nameRecordedBy_Standard]

  # ---- Standardize record numbers and build keys
  stage("Standardizing record numbers and generating keys")

  occDT[, Ctrl_recordNumber_Standard := gsub("[^0-9]", "", as.character(Ctrl_recordNumber))]
  occDT[is.na(Ctrl_recordNumber_Standard) | Ctrl_recordNumber_Standard == "", Ctrl_recordNumber_Standard := ""]

  # Strip leading zeros by converting to integer, then back to character
  occDT[Ctrl_recordNumber_Standard != "", Ctrl_recordNumber_Standard := as.character(as.integer(Ctrl_recordNumber_Standard))]
  occDT[is.na(Ctrl_recordNumber_Standard), Ctrl_recordNumber_Standard := ""]

  occDT[, Ctrl_key_family_recordedBy_recordNumber := paste(
    toupper(trimws(Ctrl_family)),
    Ctrl_nameRecordedBy_Standard,
    Ctrl_recordNumber_Standard,
    sep = "_"
  )]

  occDT[, Ctrl_key_year_recordedBy_recordNumber := paste(
    data.table::fifelse(is.na(Ctrl_year), "noYear", as.character(Ctrl_year)),
    Ctrl_nameRecordedBy_Standard,
    Ctrl_recordNumber_Standard,
    sep = "_"
  )]

  # ---- Summary
  stage("Computing summary")
  summary_dt <- occDT[, .(numberOfRecords = .N), by = .(Ctrl_key_family_recordedBy_recordNumber)][order(-numberOfRecords)]

  # ---- Return
  stage("Preparing outputs")
  result_cols <- c(
    "Ctrl_nameRecordedBy_Standard",
    "Ctrl_recordNumber_Standard",
    "Ctrl_key_family_recordedBy_recordNumber",
    "Ctrl_key_year_recordedBy_recordNumber"
  )

  return(list(
    occ_collectorsDictionary = as.data.frame(occDT[, ..result_cols]),
    summary = as.data.frame(summary_dt),
    collectorsDictionary_add = as.data.frame(collectorsDictionary_add)
  ))
}
