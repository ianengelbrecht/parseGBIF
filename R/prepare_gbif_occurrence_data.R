# File: R/prepare_gbif_occurrence_data.R
# Returns: data.table (fast, zero-copy-friendly downstream)

#' @title Preparing occurrence data downloaded from GBIF for use by parseGBIF
#'
#' @name prepare_gbif_occurrence_data
#'
#' @description Prepare occurrence data downloaded from GBIF to be used by parseGBIF functions
#'
#' @param gbif_occurrece_file The name of the file with occurrence data downloaded from GBIF
#' @param columns Character vector of strings to indicate column names of the GBIF occurrence file.
#' Use 'standard' to select basic columns for use in the package, 'all' to select all available columns.
#' The default is 'standard'
#'
#' @details Select data fields and rename field names prefixed with "Ctrl_"
#'
#' @return data.table with renamed selected fields with prefix "Ctrl_"
#'
#' @importFrom data.table fread setnames as.data.table
#' @export
prepare_gbif_occurrence_data <- function(gbif_occurrece_file = "",
                                         columns = "standard") {

  if (gbif_occurrece_file == "") {
    stop("Inform the file name!")
  }

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required (this version returns a data.table).")
  }

  col_sel <- select_gbif_fields(columns = columns)

  # Read only needed columns; keep as data.table
  occ <- data.table::fread(
    file = gbif_occurrece_file,
    sep = "\t",
    encoding = "UTF-8",
    showProgress = FALSE,
    select = col_sel,
    data.table = TRUE
  )

  # Prefix column names in-place (no copy)
  data.table::setnames(occ, names(occ), paste0("Ctrl_", names(occ)))

  # Normalize Ctrl_hasCoordinate if present: NA -> FALSE
  if ("Ctrl_hasCoordinate" %in% names(occ)) {

    # If it's not logical, coerce common GBIF encodings
    if (!is.logical(occ[["Ctrl_hasCoordinate"]])) {
      x <- as.character(occ[["Ctrl_hasCoordinate"]])
      occ[, Ctrl_hasCoordinate := x %in% c("TRUE", "True", "true", "1")]
    }

    # NA -> FALSE
    occ[is.na(Ctrl_hasCoordinate), Ctrl_hasCoordinate := FALSE]
  }

  return(occ)
}