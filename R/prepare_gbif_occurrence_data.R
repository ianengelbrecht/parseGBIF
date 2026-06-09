#' @title Preparing occurrence data downloaded from GBIF for use by parseGBIF
#' @name prepare_gbif_occurrence_data
#'
#' @description 
#' Prepare occurrence data downloaded from GBIF to be used by parseGBIF functions. 
#' Uses high-speed file reading and memory-efficient column selection.
#'
#' @param gbif_occurrece_file 
#' Character. The name or path of the file with occurrence data downloaded from GBIF.
#' 
#' @param columns 
#' Character string or vector. Indicates which columns to select from the GBIF occurrence file.
#' Use `'standard'` to select basic columns needed for the package, or `'all'` to select all available columns.
#' Default is `'standard'`.
#'
#' @details 
#' This function performs the initial data ingestion step. It selectively reads only the required 
#' data fields directly from the disk to minimize memory usage, and immediately prefixes all 
#' selected column names with "Ctrl_" for downstream pipeline standardization. It also explicitly 
#' normalizes missing coordinate flags to `FALSE`.
#'
#' @return 
#' A `data.table` containing the selected GBIF occurrence records with field names prefixed by "Ctrl_".
#'
#' @author
#' Pablo Hendrigo Alves de Melo,
#' Nadia Bystriakova &
#' Alexandre Monro
#' (Optimized via data.table for performance)
#' 
#' @seealso \code{\link[parseGBIF]{select_gbif_fields}}
#'
#' @examples
#' \donttest{
#' # Assuming 'occurrence.txt' is a downloaded GBIF dataset in your working directory
#' occ_data <- prepare_gbif_occurrence_data(
#'   gbif_occurrece_file = "occurrence.txt",
#'   columns = "standard"
#' )
#' 
#' head(occ_data)
#' }
#'
#' @importFrom data.table fread setnames as.data.table :=
#'
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