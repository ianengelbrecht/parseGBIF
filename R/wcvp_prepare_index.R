#' @title Prepare an index for WCVP name lookup (speed optimization)
#' @name wcvp_prepare_index
#' 
#' @description 
#' Pre-indexes the WCVP names table for lightning-fast, O(1) lookups in the 
#' \code{\link[parseGBIF]{wcvp_check_name}} function.
#'
#' @param wcvp_names Data frame. As loaded from WCVP (must contain `TAXON_NAME_U` and `TAXON_AUTHORS_U`).
#' @param overwrite Logical. If `TRUE`, rebuilds the index even if it is already present. Default is `FALSE`.
#'
#' @details
#' **What it does:**
#' \itemize{
#'   \item Adds lightweight helper columns (if not already present): `.KEY_NAME` and `.KEY_NAME_AUTH`.
#'   \item Builds an index map using native `data.table` keys (`setkey` and `setindex`).
#'   \item Stores the map state as attributes on the data frame so it doesn't break standard printing or subsetting.
#' }
#' 
#' **Why this helps:**
#' `wcvp_check_name()` can execute `O(1)` binary lookups to get all candidate rows for a name, 
#' rather than performing a slow, linear scan of `TAXON_NAME_U` on every single function call.
#' 
#' **Safety:**
#' This function safely modifies the table by reference. It does NOT change or overwrite any 
#' existing WCVP columns.
#'
#' @return 
#' Returns the `wcvp_names` data frame (as a `data.table`) with lookup indices attached 
#' via attributes and helper key columns added.
#'
#' @author 
#' Pablo Hendrigo Alves de Melo,
#' Nadia Bystriakova &
#' Alexandre Monro
#' (Optimized via data.table for performance)
#' 
#' @seealso \code{\link[parseGBIF]{wcvp_check_name}}, \code{\link[parseGBIF]{wcvp_get_data}}
#'
#' @examples
#' \donttest{
#' # Load WCVP data from local directory
#' wcvp_data <- wcvp_get_data(path_data = "C:/parseGBIF/dataWCVP")
#' wcvp_names <- wcvp_data$wcvp_names
#' 
#' # Prepare the index
#' wcvp_names_indexed <- wcvp_prepare_index(wcvp_names)
#' 
#' # Verify the attribute was attached
#' attr(wcvp_names_indexed, ".wcvp_indexed")
#' }
#'
#' @importFrom data.table setDT := setkey setindex setattr
#'
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