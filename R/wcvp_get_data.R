#' @title Get local WCVP database
#'
#' @name wcvp_get_data
#'
#' @description Load the World Checklist of Vascular Plants (WCVP) database from a local directory or zip file.
#'
#' @param path_data Character string. The path to the local directory containing the unzipped WCVP CSV files, or the path to the `wcvp.zip` file.
#' @param load_distribution Logical. TRUE to also load the file with geographical distribution of species.
#' @param silence Logical. TRUE to suppress progress messages.
#'
#' @details
#' To maximize performance and reliability, this function is designed to read the WCVP dataset from a local download rather than pulling directly from the KEW SFTP server during execution. You can download the latest version from: http://sftp.kew.org/pub/data-repositories/WCVP/
#'
#' This space contains data resources publicly accessible to the user 'anonymous'. No password required for access. Use of data made available via this site may be subject to legal and licensing restrictions. The README in the top-level directory for each data resource provides specific information about its terms of use.
#'
#' @return list with two data frames:
#' - `wcvp_names`: taxonomic names database
#' - `wcvp_distribution`: geographical distribution data (if load_distribution = TRUE)
#'
#' @author Pablo Hendrigo Alves de Melo,
#'         Nadia Bystriakova &
#'         Alexandre Monro
#'
#' @seealso \code{\link[parseGBIF]{wcvp_check_name}}, \code{\link[parseGBIF]{wcvp_check_name_batch}}
#'
#' @examples
#' \donttest{
#' # load package
#' library(parseGBIF)
#'
#' help(wcvp_get_data)
#'
#' # Point to the directory where you extracted the WCVP download
#' path_data <- "C:/parseGBIF/dataWCVP"
#'
#' wcvp <- wcvp_get_data(path_data = path_data,
#'                       load_distribution = TRUE)
#'
#' names(wcvp)
#'
#' head(wcvp$wcvp_names)
#' colnames(wcvp$wcvp_names)
#'
#' head(wcvp$wcvp_distribution)
#' colnames(wcvp$wcvp_distribution)
#' }
#'
#' @importFrom data.table fread as.data.table := setDF
#' @importFrom utils unzip
#' @export
wcvp_get_data <- function(path_data,
                          load_distribution = FALSE,
                          silence = FALSE) {

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required for this fast implementation.")
  }

  stage_msg <- function(msg) if (!isTRUE(silence)) message("[parseGBIF] ", msg)

  # Check if path_data exists
  if (!file.exists(path_data)) {
    stop("The path provided does not exist: ", path_data)
  }

  # If a zip file is provided, extract it to a temporary directory
  if (grepl("\\.zip$", path_data, ignore.case = TRUE)) {
    stage_msg("Zip file detected. Extracting to temporary directory...")
    temp_dir <- tempfile()
    dir.create(temp_dir)
    utils::unzip(path_data, exdir = temp_dir)
    target_dir <- temp_dir
  } else {
    target_dir <- path_data
  }

  # 1. Load the Names dataset
  file_names <- file.path(target_dir, "wcvp_names.csv")

  if (!file.exists(file_names)) {
    stop("Could not find 'wcvp_names.csv' in the specified location: ", target_dir)
  }

  stage_msg(paste0("Loading wcvp_names from: ", file_names))

  # data.table::fread handles the massive file size efficiently
  wcvp_names <- data.table::fread(file_names, sep = "|", quote = "", fill = TRUE, encoding = "UTF-8")

  # Optimize string operations in place
  stage_msg("Generating standardized taxonomy columns...")
  wcvp_names[, TAXON_NAME_U := toupper(taxon_name)]
  wcvp_names[, TAXON_AUTHORS_U := gsub("\\s+", "", toupper(taxon_authors))]

  stage_msg(paste0("wcvp_names loaded: ", nrow(wcvp_names), " rows"))

  # 2. Load the Distribution dataset (if requested)
  if (isTRUE(load_distribution)) {
    file_dist <- file.path(target_dir, "wcvp_distribution.csv")

    if (!file.exists(file_dist)) {
      warning("load_distribution is TRUE, but 'wcvp_distribution.csv' was not found.")
      wcvp_distribution <- NA
    } else {
      stage_msg(paste0("Loading wcvp_distribution from: ", file_dist))
      wcvp_distribution <- data.table::fread(file_dist, sep = "|", quote = "", fill = TRUE, encoding = "UTF-8")

      # Convert to base data.frame for output compatibility
      data.table::setDF(wcvp_distribution)
      stage_msg(paste0("wcvp_distribution loaded: ", nrow(wcvp_distribution), " rows"))
    }
  } else {
    wcvp_distribution <- NA
  }

  # Clean up temp directory if a zip was extracted
  if (grepl("\\.zip$", path_data, ignore.case = TRUE)) {
    unlink(target_dir, recursive = TRUE)
  }

  # Convert wcvp_names to base data.frame for output compatibility
  data.table::setDF(wcvp_names)

  return(list(
    wcvp_names = wcvp_names,
    wcvp_distribution = wcvp_distribution
  ))
}
