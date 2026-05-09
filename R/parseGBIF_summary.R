#' @title Generating summary statistics for parseGBIF data
#' @name parseGBIF_summary
#'
#' @description Generates comprehensive summary statistics for parseGBIF processed data
#'
#' @param parseGBIF_all_data Data frame containing parseGBIF processed data
#' @param file.parseGBIF_all_data Character string with path to CSV file containing parseGBIF data
#' @param fields_to_merge Character vector of field names used for merging collection events
#' @param fields_to_compare Character vector of field names to compare content frequency (currently unused)
#' @param fields_to_parse Character vector of all field names (currently unused)
#' @param silence Logical, if TRUE does not display progress messages
#'
#' @details This function generates multiple summary statistics for parseGBIF processed data,
#' including general counts, taxonomic diversity metrics, data quality assessments,
#' and field merging statistics.
#'
#' @return A list with four data frames:
#' * `parseGBIF_general_summary`: General summary statistics
#' * `parseGBIF_merge_fields_summary`: Summary of field merging operations
#' * `parseGBIF_merge_fields_summary_useable_data`: Field merging for usable data only
#' * `parseGBIF_merge_fields_summary_unusable_data`: Field merging for unusable data only
#'
#' @author Pablo Hendrigo Alves de Melo,
#'         Nadia Bystriakova &
#'         Alexandre Monro
#'         (Optimized via data.table for performance)
#'
#' @seealso \code{\link[parseGBIF]{batch_checkName_wcvp}}, \code{\link[parseGBIF]{extract_gbif_issue}}
#'
#' @examples
#' \donttest{
#' results <- parseGBIF_summary(parseGBIF_all_data = your_data)
#' names(results)
#' head(results$parseGBIF_general_summary)
#' }
#'
#' @importFrom data.table fread as.data.table setDF
#' @export
parseGBIF_summary <- function(parseGBIF_all_data = NA,
                              file.parseGBIF_all_data = '',
                              fields_to_merge = c('Ctrl_fieldNotes', 'Ctrl_year', 'Ctrl_stateProvince',
                                                  'Ctrl_municipality', 'Ctrl_locality', 'Ctrl_countryCode',
                                                  'Ctrl_eventDate', 'Ctrl_habitat', 'Ctrl_level0Name',
                                                  'Ctrl_level1Name', 'Ctrl_level2Name', 'Ctrl_level3Name'),
                              fields_to_compare = NULL,
                              fields_to_parse = NULL,
                              silence = FALSE) {

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required for this fast implementation.")
  }

  stage_msg <- function(msg) if (!isTRUE(silence)) print(paste0("[parseGBIF] ", msg))

  if (is.na(file.parseGBIF_all_data)) file.parseGBIF_all_data <- ''

  stage_msg('Loading data for summary...')

  # 1. High-speed data loading
  if (file.parseGBIF_all_data != '') {
    occ_tmp <- data.table::fread(file.parseGBIF_all_data, encoding = "UTF-8")
  } else {
    occ_tmp <- data.table::as.data.table(parseGBIF_all_data)
  }

  stage_msg('Calculating statistics...')

  # Initialize vectors to build the summary table efficiently
  q_vec <- character()
  v_vec <- character()
  c_vec <- character()

  add_stat <- function(question, value, condition) {
    q_vec <<- c(q_vec, question)
    v_vec <<- c(v_vec, as.character(value))
    c_vec <<- c(c_vec, condition)
  }

  # High-speed string matching to replace slow JSON parsing loop
  fast_freq_merged <- function(fields, dt_subset) {
    merged_strs <- dt_subset[parseGBIF_merged == TRUE, parseGBIF_merged_fields]

    if (length(merged_strs) == 0) {
      return(data.frame(id = fields, val = 0, stringsAsFactors = FALSE))
    }

    vals <- numeric(length(fields))
    for (ic in seq_along(fields)) {
      # Look for the exact key pattern in the JSON string instantly
      pattern <- paste0('"', fields[ic], '":')
      vals[ic] <- sum(grepl(pattern, merged_strs, fixed = TRUE))
    }

    res <- data.frame(id = fields, val = vals, stringsAsFactors = FALSE)
    res <- res[order(-res$val), ]
    return(res)
  }

  # --- General Statistics ---
  add_stat('total number of records', nrow(occ_tmp), 'all lines')
  add_stat('total number of unique collection events', sum(occ_tmp$parseGBIF_digital_voucher == TRUE, na.rm = TRUE), "where parseGBIF_digital_voucher = TRUE")
  add_stat('total number of duplicates records of unique collection events', sum(occ_tmp$parseGBIF_dataset_result == 'duplicate', na.rm = TRUE), "where parseGBIF_dataset_result = 'duplicate'")

  # Useable Data Stats
  add_stat('total number of useable records', sum(occ_tmp$parseGBIF_dataset_result == 'useable', na.rm = TRUE), "where parseGBIF_dataset_result = 'useable'")
  add_stat('total number of useable records / consensus on identification', sum(occ_tmp$parseGBIF_dataset_result == 'useable' & occ_tmp$parseGBIF_sample_taxon_name_status == 'identified', na.rm = TRUE), "where parseGBIF_dataset_result = 'useable' AND parseGBIF_sample_taxon_name_status = 'identified'")
  add_stat('total number of useable records / divergent identifications', sum(occ_tmp$parseGBIF_dataset_result == 'useable' & occ_tmp$parseGBIF_sample_taxon_name_status == 'divergent identifications', na.rm = TRUE), "where parseGBIF_dataset_result = 'useable' AND parseGBIF_sample_taxon_name_status = 'divergent identifications'")

  # Coordinates
  add_stat('total number of useable records / coordinate status success', sum(occ_tmp$parseGBIF_dataset_result == 'useable' & occ_tmp$parseGBIF_coordinate_status == "success", na.rm = TRUE), "where parseGBIF_dataset_result = 'useable' AND parseGBIF_coordinate_status = 'success'")
  add_stat('total number of useable records / coordinate status warning', sum(occ_tmp$parseGBIF_dataset_result == 'useable' & occ_tmp$parseGBIF_coordinate_status == "warning", na.rm = TRUE), "where parseGBIF_dataset_result = 'useable' AND parseGBIF_coordinate_status = 'warning'")
  add_stat('total number of useable records / coordinate status danger', sum(occ_tmp$parseGBIF_dataset_result == 'useable' & occ_tmp$parseGBIF_coordinate_status == "danger", na.rm = TRUE), "where parseGBIF_dataset_result = 'useable' AND parseGBIF_coordinate_status = 'danger'")

  # Unusable Data Stats
  add_stat('total number of unusable records', sum(occ_tmp$parseGBIF_dataset_result == 'unusable', na.rm = TRUE), "where parseGBIF_dataset_result = 'unusable'")
  add_stat('total number of unusable records / unidentified', sum(occ_tmp$parseGBIF_dataset_result == 'unusable' & occ_tmp$parseGBIF_unidentified_sample == TRUE, na.rm = TRUE), "where parseGBIF_dataset_result = 'unusable' AND parseGBIF_unidentified_sample = TRUE")
  add_stat('total number of unusable records / not suitable for geospatial analysis', sum(occ_tmp$parseGBIF_dataset_result == 'unusable' & occ_tmp$parseGBIF_useful_for_spatial_analysis == FALSE, na.rm = TRUE), "where parseGBIF_dataset_result = 'unusable' AND parseGBIF_useful_for_spatial_analysis = FALSE")

  # Merges
  merge_condition <- ifelse("merge_unusable_data" %in% names(occ_tmp), '', ' (unusable data not included)')
  add_stat(paste0('total unique collection events containing merged fields', merge_condition), sum(occ_tmp$parseGBIF_merged == TRUE, na.rm = TRUE), "where parseGBIF_merged = TRUE")

  add_stat('----------', '----------', '----------')

  # --- Taxonomic Diversity ---
  add_stat('Taxonomic diversity, based on GBIF taxonomy, from GBIF scientificName', length(unique(occ_tmp$Ctrl_scientificName[occ_tmp$Ctrl_taxonRank %in% c('SPECIES', 'SUBSPECIES', 'VARIETY')])), "count scientificName where Ctrl_taxonRank = 'SPECIES' OR 'SUBSPECIES' OR 'VARIETY'")
  add_stat('Taxonomic diversity, based on GBIF taxonomy, from GBIF scientificName / suitable for geospatial analysis', length(unique(occ_tmp$Ctrl_scientificName[occ_tmp$Ctrl_taxonRank %in% c('SPECIES', 'SUBSPECIES', 'VARIETY') & occ_tmp$parseGBIF_useful_for_spatial_analysis == TRUE])), "count scientificName where (Ctrl_taxonRank = 'SPECIES' OR 'SUBSPECIES' OR 'VARIETY') AND (parseGBIF_useful_for_spatial_analysis = TRUE)")

  add_stat('Taxonomic diversity, based on GBIF taxonomy, from standardized GBIF scientificName', length(unique(occ_tmp$wcvp_searchedName)), "count wcvp_searchedName ")
  add_stat('Taxonomic diversity, based on GBIF taxonomy, from standardized GBIF scientificName / suitable for geospatial analysis', length(unique(occ_tmp$wcvp_searchedName[occ_tmp$parseGBIF_useful_for_spatial_analysis == TRUE])), "count wcvp_searchedName where (parseGBIF_useful_for_spatial_analysis = TRUE)")

  add_stat('Taxonomic diversity, based on WCVP taxonomy', length(unique(occ_tmp$wcvp_taxon_name)), "count wcvp_taxon_name ")
  add_stat('Taxonomic diversity, based on WCVP taxonomy / suitable for geospatial analysis', length(unique(occ_tmp$wcvp_taxon_name[occ_tmp$parseGBIF_useful_for_spatial_analysis == TRUE])), "count wcvp_taxon_name where (parseGBIF_useful_for_spatial_analysis = TRUE)")

  add_stat('Taxonomic diversity, based on data cleaned in parseGBIF workflow', length(unique(occ_tmp$parseGBIF_sample_taxon_name)), "count parseGBIF_sample_taxon_name ")
  add_stat('Taxonomic diversity, based on data cleaned in parseGBIF workflow / suitable for geospatial analysis', length(unique(occ_tmp$parseGBIF_sample_taxon_name[occ_tmp$parseGBIF_useful_for_spatial_analysis == TRUE])), "count parseGBIF_sample_taxon_name where (parseGBIF_useful_for_spatial_analysis = TRUE)")

  add_stat('----------', '----------', '----------')

  # --- Data Quality Maps ---
  geo_freq <- as.data.frame(table(occ_tmp$Ctrl_geospatial_quality, useNA = "no"))
  geo_freq <- geo_freq[order(-geo_freq$Freq), ]
  for (i in seq_len(nrow(geo_freq))) {
    add_stat(paste0('Data quality map based on frequency of Impact of the issue for the use of geospatial information (', geo_freq$Var1[i], ')'), geo_freq$Freq[i], 'frequency of selection_score')
  }

  add_stat('----------', '----------', '----------')

  verb_freq <- as.data.frame(table(occ_tmp$Ctrl_verbatim_quality, useNA = "no"))
  verb_freq <- verb_freq[order(-verb_freq$Freq), ]
  for (i in seq_len(nrow(verb_freq))) {
    add_stat(paste0('Data quality map based on record completeness (', verb_freq$Var1[i], ')'), verb_freq$Freq[i], 'frequency of Ctrl_verbatim_quality')
  }

  # Assemble Base General Summary
  parseGBIF_general_summary <- data.frame(question = q_vec, value = v_vec, condition = c_vec, stringsAsFactors = FALSE)

  # --- Merged Fields Summaries ---
  parseGBIF_merge_fields_summary <- data.frame(question = character(), value = character(), condition = character(), stringsAsFactors = FALSE)
  parseGBIF_merge_fields_summary_complete <- data.frame(question = character(), value = character(), condition = character(), stringsAsFactors = FALSE)
  parseGBIF_merge_fields_summary_incomplete <- data.frame(question = character(), value = character(), condition = character(), stringsAsFactors = FALSE)

  if (!all(is.na(occ_tmp$parseGBIF_merged_fields)) && any(occ_tmp$parseGBIF_merged_fields != '')) {

    # Total Merged
    x_freq <- fast_freq_merged(fields_to_merge, occ_tmp)
    if(nrow(x_freq) > 0) {
      parseGBIF_merge_fields_summary <- data.frame(
        question = paste0(x_freq$id, ' : total merge actions'),
        value = as.character(x_freq$val),
        condition = rep('frequency of Ctrl_verbatim_quality', nrow(x_freq)),
        stringsAsFactors = FALSE
      )
    }

    # Useable Merged
    occ_useable <- occ_tmp[parseGBIF_dataset_result == 'useable']
    if (nrow(occ_useable) > 0) {
      x_freq_c <- fast_freq_merged(fields_to_merge, occ_useable)
      if(nrow(x_freq_c) > 0) {
        parseGBIF_merge_fields_summary_complete <- data.frame(
          question = paste0(x_freq_c$id, ' : merge actions '),
          value = as.character(x_freq_c$val),
          condition = rep('frequency of Ctrl_verbatim_quality', nrow(x_freq_c)),
          stringsAsFactors = FALSE
        )
      }
    }

    # Unusable Merged
    occ_unusable <- occ_tmp[parseGBIF_dataset_result == 'unusable']
    if (nrow(occ_unusable) > 0) {
      x_freq_inc <- fast_freq_merged(fields_to_merge, occ_unusable)
      if(nrow(x_freq_inc) > 0) {
        parseGBIF_merge_fields_summary_incomplete <- data.frame(
          question = paste0(x_freq_inc$id, ' : merge actions '),
          value = as.character(x_freq_inc$val),
          condition = rep('frequency of Ctrl_verbatim_quality', nrow(x_freq_inc)),
          stringsAsFactors = FALSE
        )
      }
    }
  }

  stage_msg('Summary generation complete.')

  return(list(
    parseGBIF_general_summary = parseGBIF_general_summary,
    parseGBIF_merge_fields_summary = parseGBIF_merge_fields_summary,
    parseGBIF_merge_fields_summary_useable_data = parseGBIF_merge_fields_summary_complete,
    parseGBIF_merge_fields_summary_unusable_data = parseGBIF_merge_fields_summary_incomplete
  ))
}
