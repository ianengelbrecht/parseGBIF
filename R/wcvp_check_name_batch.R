# File: R/wcvp_check_name_batch.R
# Purpose: Faster wcvp_check_name_batch() with identical result format
# Key optimizations:
# - Avoid repeated scanning of occ_all for each searchedName (no `%in%` update loop).
# - Compute WCVP checks once per unique name, then join back (vectorized).
# - Use data.table for fast joins (keeps dependencies minimal and optional).
# - Preserve return structure: list(occ_wcvp_check_name = <df>, summary = <df>)
# - Preserve column set and naming exactly as before (colunas_wcvp_sel).

#' @title In batch, use the WCVP database to check accepted names and update synonyms
#'
#' @name wcvp_check_name_batch
#'
#' @description Species' names can be checked against WCVP database one by one, or in a batch mode.
#' To verify individual names, the function wcvp_check_name is used.
#'
#' @param occ GBIF occurrence table with selected columns as select_gbif_fields(columns = 'standard')
#' @param wcvp_names get data frame in parseGBIF::wcvp_get_data(read_only_to_memory = TRUE)$wcvp_names
#' @param if_author_fails_try_without_combinations option for partial verification of the authorship of the species.
#' @param wcvp_selected_fields WCVP fields selected as return, 'standard' basic columns, 'all' all available columns.
#' @param silence if TRUE does not display progress messages
#'
#' @return list with two data frames:
#' - `summary`: summary of name checking results
#' - `occ_wcvp_check_name`: occurrence data with WCVP fields
#'
#' @export
wcvp_check_name_batch <- function(occ = NA,
                                  wcvp_names = "",
                                  if_author_fails_try_without_combinations = TRUE,
                                  wcvp_selected_fields = "standard",
                                  silence = TRUE) {

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("wcvp_check_name_batch: package 'data.table' is required")
  }

  stage <- function(msg) if (!isTRUE(silence)) message("[parseGBIF] ", msg)

  if (is.null(occ) || !is.data.frame(occ) || nrow(occ) == 0) stop("occ is empty!")
  if (!is.data.frame(wcvp_names)) stop("wcvp_names: Inform wcvp_names data frame!")
  if (!wcvp_selected_fields %in% c("standard", "all")) stop("wcvp_selected_fields: standard or all!")

  # Define output columns
  if (wcvp_selected_fields == "standard") {
    colunas_wcvp_sel <- c("wcvp_plant_name_id", "wcvp_taxon_rank", "wcvp_taxon_status",
                          "wcvp_family", "wcvp_taxon_name", "wcvp_taxon_authors",
                          "wcvp_accepted_plant_name_id", "wcvp_reviewed", "wcvp_searchedName",
                          "wcvp_taxon_status_of_searchedName", "wcvp_plant_name_id_of_searchedName",
                          "wcvp_taxon_authors_of_searchedName", "wcvp_verified_author",
                          "wcvp_verified_speciesName", "wcvp_searchNotes")
  } else {
    colunas_wcvp_sel <- c("wcvp_plant_name_id", "wcvp_ipni_id", "wcvp_taxon_rank",
                          "wcvp_taxon_status", "wcvp_family", "wcvp_genus_hybrid", "wcvp_genus",
                          "wcvp_species_hybrid", "wcvp_species", "wcvp_infraspecific_rank",
                          "wcvp_infraspecies", "wcvp_parenthetical_author", "wcvp_primary_author",
                          "wcvp_publication_author", "wcvp_place_of_publication",
                          "wcvp_volume_and_page", "wcvp_first_published",
                          "wcvp_nomenclatural_remarks", "wcvp_geographic_area",
                          "wcvp_lifeform_description", "wcvp_climate_description",
                          "wcvp_taxon_name", "wcvp_taxon_authors", "wcvp_accepted_plant_name_id",
                          "wcvp_basionym_plant_name_id", "wcvp_replaced_synonym_author",
                          "wcvp_homotypic_synonym", "wcvp_parent_plant_name_id", "wcvp_powo_id",
                          "wcvp_hybrid_formula", "wcvp_reviewed", "wcvp_searchedName",
                          "wcvp_taxon_status_of_searchedName", "wcvp_plant_name_id_of_searchedName",
                          "wcvp_taxon_authors_of_searchedName", "wcvp_verified_author",
                          "wcvp_verified_speciesName", "wcvp_searchNotes")
  }

  stage("Preparing WCVP indexes")
  wcvp_names <- wcvp_prepare_index(wcvp_names, overwrite = FALSE)

  out <- data.table::copy(occ)
  data.table::setDT(out)

  if (!"Ctrl_taxonRank" %in% names(out)) stop("occ must contain Ctrl_taxonRank column")

  idx_rank <- toupper(as.character(out$Ctrl_taxonRank)) %in% toupper(c("SPECIES", "VARIETY", "SUBSPECIES", "FORM"))
  names_to_check <- sort(unique(out$Ctrl_scientificName[idx_rank == TRUE]))

  stage(sprintf("Checking %d unique names against WCVP", length(names_to_check)))

  res_list <- vector("list", length(names_to_check))
  for (i in seq_along(names_to_check)) {
    nm <- names_to_check[i]

    if (!isTRUE(silence) && (i %% 200 == 0)) {
      message("[parseGBIF] ", i, "/", length(names_to_check), "  ", nm)
    }

    one <- data.table::as.data.table(
      wcvp_check_name(
        searchedName = nm,
        wcvp_names = wcvp_names,
        if_author_fails_try_without_combinations = if_author_fails_try_without_combinations
      )
    )

    missing_cols <- setdiff(colunas_wcvp_sel, names(one))
    if (length(missing_cols)) {
      for (cn in missing_cols) data.table::set(one, j = cn, value = NA)
    }

    extracted <- one[, ..colunas_wcvp_sel]
    # Add our join anchor
    extracted[, raw_search_name := nm]
    res_list[[i]] <- extracted
  }

  stage("Applying results back onto occurrences")

  if (length(res_list) > 0) {
    summary_dt <- data.table::rbindlist(res_list, use.names = TRUE, fill = TRUE)
    join_dt <- unique(summary_dt, by = "raw_search_name")

    # Rename the anchor to seamlessly match the occurrence data
    data.table::setnames(join_dt, "raw_search_name", "Ctrl_scientificName")

    # THE FIX: Native Left Join. Keeps all original rows untouched, brings in strongly-typed wcvp columns natively.
    out <- join_dt[out, on = "Ctrl_scientificName"]

    # Remove the temporary key from summary to match original output perfectly
    summary_dt[, Ctrl_scientificName := NULL]
  } else {
    summary_dt <- data.table::data.table()
  }

  # Unchecked rows need wcvp_searchedName to default to Ctrl_scientificName
  out[is.na(wcvp_searchedName), wcvp_searchedName := as.character(Ctrl_scientificName)]

  # Only pad with NA if the columns missed the join entirely
  missing_after_join <- setdiff(colunas_wcvp_sel, names(out))
  if (length(missing_after_join) > 0) {
    for (cn in missing_after_join) {
      data.table::set(out, j = cn, value = NA)
    }
  }

  stage("Done")

  return(list(
    occ_wcvp_check_name = as.data.frame(out[, ..colunas_wcvp_sel]),
    summary = as.data.frame(summary_dt)
  ))
}
