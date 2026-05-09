#' @title Download and combine the default Collector Dictionary
#' @name collectors_download_dictionary
#'
#' @description
#' Dynamically scans the parseGBIF GitHub repository for all Collector Dictionary CSV files,
#' downloads them, combines them into a single dataset, and saves it locally. This allows you to run the
#' \code{\link[parseGBIF]{collectors_prepare_dictionary}} function offline and significantly
#' increases pipeline speed.
#'
#' @param dest_dir Character. The directory where the dictionary should be saved. Defaults to the R temporary directory `tempdir()`.
#' @param file_name Character. The name of the saved file. Defaults to `'CollectorsDictionary.csv'`.
#' @param silence Logical. If `TRUE`, suppresses progress messages. Default is `FALSE`.
#'
#' @return A character string containing the full file path to the downloaded and saved CSV file.
#'
#' @author
#' Pablo Hendrigo Alves de Melo
#' (Modified for local data caching and dynamic fetching)
#'
#' @seealso
#' \code{\link[parseGBIF]{collectors_prepare_dictionary}}
#'
#' @examples
#' \donttest{
#' # Download the dictionary to your working directory
#' local_dict_path <- collectors_download_dictionary(dest_dir = getwd())
#'
#' # Print the path to verify
#' print(local_dict_path)
#' }
#'
#' @importFrom data.table fread rbindlist fwrite
#' @importFrom jsonlite fromJSON
#' @export
collectors_download_dictionary <- function(dest_dir = tempdir(),
                                           file_name = "CollectorsDictionary.csv",
                                           silence = FALSE) {

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required for this fast implementation.")
  }
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    stop("Package 'jsonlite' is required to dynamically read the GitHub repository.")
  }

  stage_msg <- function(msg) {
    if (!isTRUE(silence)) message("[parseGBIF] ", msg)
  }

  # 1. Ensure destination directory exists
  if (!dir.exists(dest_dir)) {
    stage_msg("Creating destination directory...")
    dir.create(dest_dir, recursive = TRUE)
  }

  # 2. Query GitHub API for the folder contents
  stage_msg("Scanning GitHub repository for dictionary files...")
  api_url <- "https://api.github.com/repos/pablopains/parseGBIF/contents/collectorDictionary"

  # Fetch directory info
  repo_contents <- tryCatch({
    jsonlite::fromJSON(api_url)
  }, error = function(e) {
    stop("Failed to connect to GitHub API. Check your internet connection or GitHub rate limits.")
  })

  # 3. Filter for files that end in .csv
  csv_files <- repo_contents[grepl("\\.csv$", repo_contents$name, ignore.case = TRUE), ]

  if (nrow(csv_files) == 0) {
    stop("No CSV files found in the remote repository.")
  }

  stage_msg(paste0("Found ", nrow(csv_files), " dictionary file(s). Starting download..."))

  # 4. Download and read all files dynamically
  dict_list <- list()

  for (i in seq_len(nrow(csv_files))) {
    file_url <- csv_files$download_url[i]
    file_name_remote <- csv_files$name[i]

    stage_msg(paste0("Downloading: ", file_name_remote, " (", i, "/", nrow(csv_files), ")"))

    # Read directly into a list
    dict_list[[i]] <- data.table::fread(file_url, encoding = 'UTF-8', showProgress = FALSE)
  }

  # 5. Combine them all
  stage_msg("Combining dictionaries...")
  dict_combined <- data.table::rbindlist(dict_list, fill = TRUE)

  # 6. Define output path and save
  output_path <- file.path(dest_dir, file_name)

  stage_msg(paste0("Saving combined dictionary to: ", output_path))
  data.table::fwrite(dict_combined, file = output_path, quote = TRUE)

  stage_msg("Download complete!")

  return(output_path)
}
