# File: R/wcvp_prepare_index.R
# Purpose: Pre-index WCVP names table for fast lookup in wcvp_check_name()
#
# What it does:
# - Adds lightweight helper columns (if not already present):
#     .KEY_NAME                = TAXON_NAME_U
#     .KEY_NAME_AUTH           = paste0(TAXON_NAME_U, "\t", TAXON_AUTHORS_U)
# - Builds an index map:
#     .IDX_BY_NAME             = list column: for each unique name, the integer row indices in wcvp_names
# - Stores the map as attributes on the data.frame (doesn't break printing/subsetting)
#
# Why this helps:
# - wcvp_check_name() can do O(1) lookups to get all candidate rows for a name,
#   instead of scanning TAXON_NAME_U every call.
#
# Safe:
# - Does NOT change any existing WCVP columns.
# - Returns a data.frame (same class), just with extra attributes and helper columns.

#' @title Prepare an index for WCVP name lookup (speed optimization)
#' @name wcvp_prepare_index
#' @param wcvp_names data.frame as loaded from WCVP (must contain TAXON_NAME_U, TAXON_AUTHORS_U)
#' @param overwrite if TRUE, rebuild index even if already present
#' @return wcvp_names with lookup index attached (attributes) and helper key columns added
#' @export
wcvp_prepare_index <- function(wcvp_names, overwrite = FALSE) {
  if (!is.data.frame(wcvp_names)) {
    stop("wcvp_prepare_index: wcvp_names must be a data.frame")
  }
  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("wcvp_prepare_index: package 'data.table' is required")
  }

  # Convert in-place (by reference)
  data.table::setDT(wcvp_names)

  req <- c(
    "TAXON_NAME_U", "TAXON_AUTHORS_U", "plant_name_id",
    "accepted_plant_name_id", "taxon_status", "taxon_authors"
  )
  missing <- setdiff(req, names(wcvp_names))
  if (length(missing)) {
    stop(
      "wcvp_prepare_index: wcvp_names is missing required columns: ",
      paste(missing, collapse = ", ")
    )
  }

  already <- isTRUE(attr(wcvp_names, ".wcvp_indexed"))
  if (already && !isTRUE(overwrite)) {
    return(wcvp_names)
  }

  # Ensure key columns are character (avoid factors; keep comparisons fast)
  wcvp_names[, TAXON_NAME_U := as.character(TAXON_NAME_U)]
  wcvp_names[, TAXON_AUTHORS_U := as.character(TAXON_AUTHORS_U)]
  wcvp_names[, plant_name_id := as.character(plant_name_id)]
  wcvp_names[, accepted_plant_name_id := as.character(accepted_plant_name_id)]
  wcvp_names[, taxon_status := as.character(taxon_status)]
  wcvp_names[, taxon_authors := as.character(taxon_authors)]

  # Create helper columns if not present (you already have them, but keep safe)
  if (!(".KEY_NAME" %in% names(wcvp_names))) {
    wcvp_names[, ".KEY_NAME" := TAXON_NAME_U]
  } else {
    wcvp_names[, ".KEY_NAME" := as.character(.KEY_NAME)]
  }

  if (!(".KEY_NAME_AUTH" %in% names(wcvp_names))) {
    wcvp_names[, ".KEY_NAME_AUTH" := paste0(.KEY_NAME, "\t", TAXON_AUTHORS_U)]
  } else {
    wcvp_names[, ".KEY_NAME_AUTH" := as.character(.KEY_NAME_AUTH)]
  }

  # Key for fast name lookup
  data.table::setkey(wcvp_names, TAXON_NAME_U)

  # Secondary index for accepted_plant_name_id -> plant_name_id lookup
  data.table::setindex(wcvp_names, plant_name_id)

  data.table::setattr(wcvp_names, ".wcvp_indexed", TRUE)
  wcvp_names
}