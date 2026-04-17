#' @title Check species names against World Checklist of Vascular Plants (WCVP) database
#'
#' @name wcvp_check_name
#'
#' @description Use the [World Checklist of Vascular Plants WCVP](https://powo.science.kew.org/)
#' [database](http://sftp.kew.org/pub/data-repositories/WCVP/) to check accepted names and update synonyms.
#'
#' The World Checklist of Vascular Plants (WCVP) database is available from the
#' [Royal Botanic Gardens, Kew](https://powo.science.kew.org/about-wcvp).
#' It can be downloaded to a folder of the user's choice or into memory using the get_wcvp function. The output has 33 columns.
#'
#' @param searchedName scientific name, with or without author
#' @param wcvp_names WCVP table, wcvp_names.csv file from http://sftp.kew.org/pub/data-repositories/WCVP/
#' If NA, automatically load the latest version of the database by the function
#' parseGBIF::wcvp_get_data(read_only_to_memory = TRUE)$wcvp_names.
#' @param if_author_fails_try_without_combinations option for partial verification of the authorship
#' of the species. Remove the authors of combinations, in parentheses
#'
#' @details About the World Checklist of Vascular Plants https://powo.science.kew.org/about-wcvp
#' searchNotes values:
#'
#' * Accepted - When only one authorless scientific name is present in the list of TAXON_name with
#' and TAXON_STATUS equal to "Accepted", verified_speciesName = 100.
#' * Accepted among homonyms - When more than one authorless scientific name is present in the
#' TAXON_name list, but only one of the homonyms displays TAXON_STATUS equal to "Accepted",
#' verified_speciesName = number of matches/100.
#' * Homonyms - When more than one authorless scientific name is present in the TAXON_name list
#' and more than one, or none among the homonyms, display TAXON_STATUS equal to "Accepted",
#' verified_speciesName = number of matches/100.
#' Before searching for homonyms, there was a failure in trying to find the matching match between
#' authorless scientific name in TAXON_name and author in TAXON_AUTHORS, in these cases
#' verified_author equal to 0 (zero),
#' * Not Found: When the authorless scientific name is not present in the TAXON_NAME LIST
#' * Unplaced: When only one authorless scientific name is present in the list of TAXON_name with and TAXON_STATUS = "Unplaced"
#' * Updated: When only one authorless scientific name is present in the list of TAXON_name and ACCEPTED_PLANT_NAME_ID
#' are not empty (and ACCEPTED_PLANT_NAME_ID is different from the ID of the species consulted)
#' taxon_status_of_searchedName, plant_name_id_of_searchedName and taxon_authors_of_searchedName values:
#'
#'    * When searchNotes equals "Updated" - The fields record the information of the scientific name originally consulted.
#'    * When searchNotes equals "Homonyms" - Fields record the information of homonymous synonyms separated by "|".
#'
#' * verified_author values:
#'
#'    * When value equal to 100 - when there is matched match between authorless scientific name in TAXON_name and author in TAXON_AUTHORS.
#'    * When value equal to 50 - when there is combined correspondence between authorless scientific name in TAXON_name and author, without (combination), in TAXON_AUTHORS.
#'    * When value equal to 0 - regardless of the correspondence between authorless scientific name in TAXON_name, author is not present in TAXON_AUTHORS.
#'
#' @author Pablo Hendrigo Alves de Melo,
#'         Nadia Bystriakova &
#'         Alexandre Monro
#'
#' @seealso \code{\link[parseGBIF]{wcvp_check_name_batch}}, \code{\link[parseGBIF]{wcvp_get_data}}
#'
#' @return Data frame with WCVP fields prefixed with 'wcvp_'
#'
#' @examples
#' # These examples take >10 seconds to run and require 'parseGBIF::wcvp_get_data()'
#' \donttest{
#' library(parseGBIF)
#'
#' help(wcvp_check_name)
#'
#' wcvp_names <- wcvp_get_data(read_only_to_memory = TRUE)$wcvp_names
#'
#' # 1) Updated
#' wcvp_check_name(searchedName = 'Hemistylus brasiliensis Wedd.',
#'                wcvp_names = wcvp_names,
#'                if_author_fails_try_without_combinations = TRUE)
#'
#' # 2) Accepted
#' wcvp_check_name(searchedName = 'Hemistylus boehmerioides Wedd. ex Warm.',
#'                wcvp_names = wcvp_names,
#'                if_author_fails_try_without_combinations = TRUE)
#'
#' # 3) Unplaced - taxon_status = Unplaced
#' wcvp_check_name(searchedName = 'Leucosyke australis Unruh',
#'                wcvp_names = wcvp_names,
#'                if_author_fails_try_without_combinations = TRUE)
#'
#' # 4) Accepted among homonyms - When author is not informed. In this case, one of the homonyms, taxon_status is accepted
#' wcvp_check_name(searchedName = 'Parietaria cretica',
#'                wcvp_names = wcvp_names,
#'                if_author_fails_try_without_combinations = TRUE)
#'
#' # When author is informed
#' wcvp_check_name(searchedName = 'Parietaria cretica L.',
#'                wcvp_names = wcvp_names,
#'                if_author_fails_try_without_combinations = TRUE)
#'
#' # When author is informed
#' wcvp_check_name(searchedName = 'Parietaria cretica Moris',
#'                wcvp_names = wcvp_names,
#'                if_author_fails_try_without_combinations = TRUE)
#'
#' # 5) Homonyms - When author is not informed. In this case, none of the homonyms, taxon_status is Accepted
#' wcvp_check_name(searchedName = 'Laportea peltata',
#'                wcvp_names = wcvp_names,
#'                if_author_fails_try_without_combinations = TRUE)
#'
#' # When author is informed
#' wcvp_check_name(searchedName = 'Laportea peltata Gaudich. & Decne.',
#'                wcvp_names = wcvp_names,
#'                if_author_fails_try_without_combinations = TRUE)
#'
#' # When author is informed
#' wcvp_check_name(searchedName = 'Laportea peltata (Blume) Gaudich.',
#'                wcvp_names = wcvp_names,
#'                if_author_fails_try_without_combinations = TRUE)
#' }

#' Purpose: Faster wcvp_check_name() with identical output format
#' Update: Uses wcvp_prepare_index() attributes/keys when present for O(1) name candidate retrieval

#'
#' @importFrom dplyr add_row mutate
#' @importFrom stringr str_c
#' @export
wcvp_check_name <- function(searchedName = "Hemistylus brasiliensis Wedd.",
                            wcvp_names = "",
                            if_author_fails_try_without_combinations = TRUE) {

  if (!is.data.frame(wcvp_names)) {
    stop("wcvp_names:  Inform wcvp_names data frame!")
  }
  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("wcvp_check_name: package 'data.table' is required")
  }

  data.table::setDT(wcvp_names)
  if (!isTRUE(attr(wcvp_names, ".wcvp_indexed"))) {
    wcvp_names <- wcvp_prepare_index(wcvp_names, overwrite = FALSE)
  }

  # Helper: Lightning-fast 1-row NA table matching wcvp_names column classes
  empty_like <- function() {
    out <- data.table::as.data.table(lapply(wcvp_names, function(col) col[NA_integer_]))
    out
  }

  if (length(searchedName) != 1L) searchedName <- as.character(searchedName[1])
  if (is.na(searchedName)) searchedName <- ""

  if (searchedName == "") {
    x <- empty_like()
    x[, searchedName := searchedName]
    x[, taxon_status_of_searchedName := NA_character_]
    x[, plant_name_id_of_searchedName := NA_character_]
    x[, taxon_authors_of_searchedName := NA_character_]
    x[, verified_author := 0L]
    x[, verified_speciesName := 0]
    x[, searchNotes := "Not found"]
    data.table::setnames(x, names(x), paste0("wcvp_", names(x)))
    return(as.data.frame(x))
  }

  sp_wcvp <- standardize_scientificName(searchedName)

  norm_name <- toupper(sp_wcvp$standardizeName)
  norm_auth_full <- toupper(gsub("\\s+", "", sp_wcvp$taxonAuthors))
  norm_auth_last <- toupper(gsub("\\s+", "", sp_wcvp$taxonAuthors_last))
  has_author <- !is.null(sp_wcvp$taxonAuthors) && sp_wcvp$taxonAuthors != ""

  # Candidates by name (fast join)
  cand <- wcvp_names[.(norm_name), on = "TAXON_NAME_U", nomatch = 0L]

  index_author <- 0L
  if (has_author) {
    index_author <- 100L
    cand2 <- cand[TAXON_AUTHORS_U == norm_auth_full]

    if (nrow(cand2) == 0L && isTRUE(if_author_fails_try_without_combinations)) {
      index_author <- 50L
      cand2 <- cand[TAXON_AUTHORS_U == norm_auth_last]
    }

    if (nrow(cand2) == 0L) {
      index_author <- 0L
      cand2 <- cand
    }
    cand <- cand2
  }

  ntaxa <- nrow(cand)

  # Not found
  if (ntaxa == 0L) {
    x <- empty_like()
    x[, searchedName := sp_wcvp$standardizeName]
    x[, taxon_status_of_searchedName := NA_character_]
    x[, plant_name_id_of_searchedName := NA_character_]
    x[, taxon_authors_of_searchedName := NA_character_]
    x[, verified_author := index_author]
    x[, verified_speciesName := 0]
    x[, searchNotes := "Not found"]
    data.table::setnames(x, names(x), paste0("wcvp_", names(x)))
    return(as.data.frame(x))
  }

  # Single match
  if (ntaxa == 1L) {
    verified_speciesName <- 100
    # USE COPY to prevent reference bleeding into the master database
    irow <- data.table::copy(cand[1])

    acc_id <- as.character(irow$accepted_plant_name_id)
    plant_id <- as.character(irow$plant_name_id)
    acc_missing <- is.na(acc_id) || acc_id == ""

    if (!acc_missing && !is.na(plant_id) && plant_id != acc_id) {
      taxon_status_of_searchedName <- as.character(irow$taxon_status)
      plant_name_id_of_searchedName <- plant_id
      taxon_authors_of_searchedName <- as.character(irow$taxon_authors)

      # THE FIX: Native data.table lookup instead of massive vectorization
      target_id <- if (is.numeric(wcvp_names$plant_name_id)) {
        as.integer(acc_id)
      } else {
        as.character(acc_id)
      }

      acc_row <- wcvp_names[.(target_id), on = "plant_name_id", nomatch = NULL]

      if (nrow(acc_row) > 0L) {
        x <- data.table::copy(acc_row[1])
      } else {
        x <- data.table::copy(irow)
      }

      x[, searchedName := sp_wcvp$standardizeName]
      x[, taxon_status_of_searchedName := taxon_status_of_searchedName]
      x[, plant_name_id_of_searchedName := plant_name_id_of_searchedName]
      x[, taxon_authors_of_searchedName := taxon_authors_of_searchedName]
      x[, verified_author := index_author]
      x[, verified_speciesName := verified_speciesName]
      x[, searchNotes := "Updated"]

      data.table::setnames(x, names(x), paste0("wcvp_", names(x)))
      return(as.data.frame(x))
    }

    x <- data.table::copy(irow)
    x[, searchedName := sp_wcvp$standardizeName]
    x[, taxon_status_of_searchedName := NA_character_]
    x[, plant_name_id_of_searchedName := NA_character_]
    x[, taxon_authors_of_searchedName := NA_character_]
    x[, verified_author := index_author]
    x[, verified_speciesName := verified_speciesName]
    x[, searchNotes := ""]
    data.table::setnames(x, names(x), paste0("wcvp_", names(x)))
    return(as.data.frame(x))
  }

  # Multiple matches
  taxon_status_of_searchedName <- paste(as.character(cand$taxon_status), collapse = "|")
  plant_name_id_of_searchedName <- paste(as.character(cand$plant_name_id), collapse = "|")
  taxon_authors_of_searchedName <- paste(as.character(cand$taxon_authors), collapse = "|")

  acc <- cand[taxon_status %in% "Accepted"]
  if (nrow(acc) == 1L) {
    x <- data.table::copy(acc[1])
    x[, searchedName := sp_wcvp$standardizeName]
    x[, taxon_status_of_searchedName := taxon_status_of_searchedName]
    x[, plant_name_id_of_searchedName := plant_name_id_of_searchedName]
    x[, taxon_authors_of_searchedName := taxon_authors_of_searchedName]
    x[, verified_author := index_author]
    x[, verified_speciesName := 100 / ntaxa]
    x[, searchNotes := "Accepted among homonyms"]
    data.table::setnames(x, names(x), paste0("wcvp_", names(x)))
    return(as.data.frame(x))
  }

  # Homonyms
  x <- empty_like()
  x[, searchedName := toupper(sp_wcvp$standardizeName)]
  x[, taxon_status_of_searchedName := taxon_status_of_searchedName]
  x[, plant_name_id_of_searchedName := plant_name_id_of_searchedName]
  x[, taxon_authors_of_searchedName := taxon_authors_of_searchedName]
  x[, verified_author := index_author]
  x[, verified_speciesName := 100 / ntaxa]
  x[, searchNotes := "Homonyms"]
  data.table::setnames(x, names(x), paste0("wcvp_", names(x)))
  as.data.frame(x)
}
