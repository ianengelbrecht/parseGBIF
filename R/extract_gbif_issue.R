#' @title Extract and Tabulate GBIF Data Quality Issues
#' @name extract_gbif_issue
#'
#' @description
#' Extracts and tabulates GBIF validation rules for occurrence records, creating
#' individual columns for each issue type with TRUE/FALSE flags indicating whether
#' each issue applies to each record.
#'
#' @param occ
#' Data frame. GBIF occurrence table with selected columns as returned by
#' `select_gbif_fields(columns = 'standard')`. Must contain a `Ctrl_issue` column.
#'
#' @param enumOccurrenceIssue
#' Data frame. An enumeration of validation rules for single occurrence records.
#' If `NA` (default), uses the built-in `EnumOccurrenceIssue` dataset.
#'
#' @details
#' GBIF recognizes and documents several issues relating to data fields for individual records.
#' The `Ctrl_issue` field stores terms representing an enumeration of GBIF validation rules.
#' These issues can indicate data quality problems or processing alterations made by GBIF.
#'
#' This function:
#' 1. Uses the `EnumOccurrenceIssue` dataset as a reference for known GBIF issues
#' 2. Creates individual columns for each issue type
#' 3. Flags each record with TRUE/FALSE for each applicable issue
#' 4. Provides a summary of issue frequencies across the dataset
#'
#' Not all issues indicate bad data - some flag that GBIF has altered values during processing.
#'
#' @return
#' A list with two data frames:
#' - `occ_gbif_issue`: Original occurrence data with additional columns for each GBIF issue,
#'   containing TRUE/FALSE values indicating whether the issue applies to each record
#' - `summary`: Summary data frame showing the frequency of each issue across all records,
#'   sorted by most frequent issues first
#'
#' @author
#' Pablo Hendrigo Alves de Melo,
#' Nadia Bystriakova &
#' Alexandre Monro
#'
#' @seealso
#' [`prepare_gbif_occurrence_data()`] for preparing GBIF occurrence data,
#' [`select_gbif_fields()`] for selecting relevant GBIF fields,
#' [`EnumOccurrenceIssue`] for the GBIF issue enumeration dataset
#'
#' @examples
#' \donttest{
#' library(parseGBIF)
#'
#' # Load sample data
#' occ_file <- 'https://raw.githubusercontent.com/pablopains/parseGBIF/main/dataGBIF/Achatocarpaceae/occurrence.txt'
#'
#' occ <- prepare_gbif_occurrence_data(
#'   gbif_occurrece_file = occ_file,
#'   columns = 'standard'
#' )
#'
#' # Extract GBIF issues
#' occ_gbif_issue <- extract_gbif_issue(occ = occ)
#'
#' # View results
#' names(occ_gbif_issue)
#'
#' # Summary of issues
#' head(occ_gbif_issue$summary)
#'
#' # Issue flags for each record
#' colnames(occ_gbif_issue$occ_gbif_issue)
#' head(occ_gbif_issue$occ_gbif_issue)
#' }
#'

#' Fast rewrite using data.table

#' @export
extract_gbif_issue <- function(occ = NA,
                               enumOccurrenceIssue = NA) {

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required for this implementation.")
  }

  # Load EnumOccurrenceIssue if not supplied
  if (length(enumOccurrenceIssue) == 1 && is.na(enumOccurrenceIssue)) {
    data("EnumOccurrenceIssue", envir = environment())
  } else {
    EnumOccurrenceIssue <- enumOccurrenceIssue
  }

  if (is.null(occ) || nrow(occ) == 0) {
    stop("occ is empty!")
  }

  # Cast safely without duplicating memory if it's already a data.table
  occDT <- data.table::as.data.table(occ)

  if (!"Ctrl_issue" %in% names(occDT)) {
    stop("occ must contain column Ctrl_issue")
  }

  issue_key <- as.character(EnumOccurrenceIssue$constant)
  issue_key <- issue_key[!is.na(issue_key) & issue_key != ""]

  n <- nrow(occDT)

  # Pre-allocate the issue table natively in data.table
  issue_table <- data.table::setDF(
    lapply(issue_key, function(x) rep(FALSE, n))
  )
  names(issue_table) <- issue_key
  data.table::setDT(issue_table)

  # Normalize Ctrl_issue to character; treat NA as blank
  x <- as.character(occDT$Ctrl_issue)
  x[is.na(x)] <- ""

  # THE FIX: Run strsplit exactly once and store it to save massive amounts of RAM
  split_issues <- strsplit(x, "[;,]", perl = TRUE)

  # Create long table mapping rows to their specific issues
  long <- data.table::data.table(
    row_id = rep.int(seq_len(n), lengths(split_issues)),
    issue  = trimws(unlist(split_issues, use.names = FALSE))
  )

  # Keep only valid issues
  long <- long[issue %chin% issue_key]

  if (nrow(long) > 0) {
    # Deduplicate in case an issue appears twice in one row string
    long <- unique(long, by = c("row_id", "issue"))

    # Fast assignment: Map the TRUE values directly into the table
    issue_list <- split(long$row_id, long$issue)
    for (k in names(issue_list)) {
      data.table::set(issue_table, i = issue_list[[k]], j = k, value = TRUE)
    }
  }

  # Generate Summary
  counts <- colSums(issue_table, na.rm = TRUE)
  issue_result <- data.frame(
    issue = names(counts),
    n_occ = as.integer(counts),
    stringsAsFactors = FALSE
  )

  # Sort summary natively
  issue_result <- issue_result[order(-issue_result$n_occ), ]
  rownames(issue_result) <- NULL

  return(list(
    occ_gbif_issue = as.data.frame(issue_table),
    summary = issue_result
  ))
}
