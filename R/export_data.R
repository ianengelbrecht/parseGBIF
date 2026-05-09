#' @title Export Parsed GBIF Data Results
#' @name export_data
#'
#' @description
#' Processes and exports results from parsed GBIF occurrence data, merging information
#' from duplicate records to create unique collection event records. For each unique
#' collection event key (complete or incomplete), this function combines information
#' from duplicate records and generates a single unique collection event record.
#'
#' @param occ_digital_voucher_file
#' Character. Path to CSV file result from `select_digital_voucher()$occ_digital_voucher`.
#'
#' @param occ_digital_voucher
#' Data frame. Result from `select_digital_voucher()$occ_digital_voucher`.
#'
#' @param merge_unusable_data
#' Logical. If `TRUE`, includes incomplete unique collection events in merge processing.
#' Default is `FALSE`.
#'
#' @param fields_to_merge
#' Character vector. Fields to merge from duplicates. Default includes:
#' `Ctrl_fieldNotes`, `Ctrl_year`, `Ctrl_stateProvince`, `Ctrl_municipality`,
#' `Ctrl_locality`, `Ctrl_countryCode`, `Ctrl_eventDate`, `Ctrl_habitat`,
#' `Ctrl_level0Name`, `Ctrl_level1Name`, `Ctrl_level2Name`, `Ctrl_level3Name`.
#'
#' @param fields_to_compare
#' Character vector. Fields to compare content frequency across duplicates.
#'
#' @param fields_to_parse
#' Character vector. All fields to include in output.
#'
#' @param silence
#' Logical. If `TRUE`, does not display progress messages. Default is `TRUE`.
#'
#' @details
#' ## Taxonomic Identification Selection:
#' For complete unique collection event keys, the accepted taxon name is selected as:
#' 1. The most frequently applied name at or below species rank among duplicates
#' 2. If equal frequency, uses alphabetical order
#' 3. If no species-level identification, marked as unidentified
#'
#' ## Geospatial Information:
#' If the master voucher lacks coordinates, coordinates are sought from duplicate records.
#'
#' ## Output Datasets:
#' - **useable_data**: Unique collection events with taxonomic identification and coordinates
#' - **unusable_data**: Unique collection events without identification and/or coordinates
#' - **duplicates**: All duplicate records of unique collection events
#'
#' ## Field Merging:
#' For complete unique collection events, empty fields in the digital voucher record
#' are populated with data from duplicates during content merging.
#'
#' @return
#' A list with 6 data frames:
#' - `all_data`: All processed records (merged unique collection events and duplicates)
#' - `useable_data_merge`: Merged complete unique collection events
#' - `useable_data_raw`: Raw complete unique collection events
#' - `duplicates`: Duplicates of unique collection events
#' - `unusable_data_merge`: Merged incomplete unique collection events (NA if merge_unusable_data=FALSE)
#' - `unusable_data_raw`: Raw incomplete unique collection events
#'
#' @author
#' Pablo Hendrigo Alves de Melo,
#' Nadia Bystriakova &
#' Alexandre Monro
#' (Optimized via data.table for performance)
#'
#' @seealso
#' [`select_digital_voucher()`] for selecting digital vouchers,
#' [`batch_checkName_wcvp()`] for taxonomic name checking,
#' [`extract_gbif_issue()`] for GBIF data quality issues
#'
#' @importFrom data.table fread setDT setkey := as.data.table setDF
#' @importFrom jsonlite fromJSON
#' @importFrom jsonify to_json
#' @importFrom stringr str_count
#' @importFrom dplyr arrange desc add_row
#' @export
export_data <- function(occ_digital_voucher_file = '',
                        occ_digital_voucher = NA,
                        merge_unusable_data = FALSE,
                        fields_to_merge = c('Ctrl_fieldNotes', 'Ctrl_year', 'Ctrl_stateProvince',
                                            'Ctrl_municipality', 'Ctrl_locality', 'Ctrl_countryCode',
                                            'Ctrl_eventDate', 'Ctrl_habitat', 'Ctrl_level0Name',
                                            'Ctrl_level1Name', 'Ctrl_level2Name', 'Ctrl_level3Name'),
                        fields_to_compare = c('Ctrl_gbifID', 'Ctrl_scientificName', 'Ctrl_recordedBy',
                                              'Ctrl_recordNumber', 'Ctrl_identifiedBy', 'Ctrl_dateIdentified',
                                              'Ctrl_institutionCode', 'Ctrl_collectionCode', 'Ctrl_datasetName',
                                              'Ctrl_language', "wcvp_plant_name_id", "wcvp_taxon_rank",
                                              "wcvp_taxon_status", "wcvp_family", "wcvp_taxon_name",
                                              "wcvp_taxon_authors", "wcvp_searchNotes"),
                        fields_to_parse = c('Ctrl_gbifID', 'Ctrl_bibliographicCitation', 'Ctrl_language',
                                            'Ctrl_institutionCode', 'Ctrl_collectionCode', 'Ctrl_datasetName',
                                            'Ctrl_basisOfRecord', 'Ctrl_catalogNumber', 'Ctrl_recordNumber',
                                            'Ctrl_recordedBy', 'Ctrl_occurrenceStatus', 'Ctrl_eventDate',
                                            'Ctrl_year', 'Ctrl_month', 'Ctrl_day', 'Ctrl_habitat',
                                            'Ctrl_fieldNotes', 'Ctrl_eventRemarks', 'Ctrl_countryCode',
                                            'Ctrl_stateProvince', 'Ctrl_municipality', 'Ctrl_county',
                                            'Ctrl_locality', 'Ctrl_issue', 'Ctrl_level0Name',
                                            'Ctrl_level1Name', 'Ctrl_level2Name', 'Ctrl_level3Name',
                                            'Ctrl_identifiedBy', 'Ctrl_dateIdentified', 'Ctrl_scientificName',
                                            'Ctrl_taxonRank', 'Ctrl_decimalLatitude', 'Ctrl_decimalLongitude',
                                            'Ctrl_nameRecordedBy_Standard', 'Ctrl_recordNumber_Standard',
                                            'Ctrl_key_family_recordedBy_recordNumber', 'Ctrl_geospatial_quality',
                                            'Ctrl_verbatim_quality', 'Ctrl_moreInformativeRecord',
                                            'Ctrl_coordinates_validated_by_gbif_issue', "wcvp_plant_name_id",
                                            "wcvp_taxon_rank", "wcvp_taxon_status", "wcvp_family",
                                            "wcvp_taxon_name", "wcvp_taxon_authors", "wcvp_searchedName",
                                            "wcvp_searchNotes", 'parseGBIF_digital_voucher',
                                            'parseGBIF_duplicates', 'parseGBIF_num_duplicates',
                                            'parseGBIF_non_groupable_duplicates', 'parseGBIF_duplicates_grouping_status',
                                            'parseGBIF_unidentified_sample', 'parseGBIF_sample_taxon_name',
                                            'parseGBIF_sample_taxon_name_status', 'parseGBIF_number_taxon_names',
                                            'parseGBIF_useful_for_spatial_analysis', 'parseGBIF_decimalLatitude',
                                            'parseGBIF_decimalLongitude', 'parseGBIF_wcvp_plant_name_id',
                                            'parseGBIF_wcvp_taxon_rank', 'parseGBIF_wcvp_taxon_status',
                                            'parseGBIF_wcvp_family', 'parseGBIF_wcvp_taxon_name',
                                            'parseGBIF_wcvp_taxon_authors', 'parseGBIF_wcvp_reviewed',
                                            'parseGBIF_dataset_result'),
                        silence=TRUE) {

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required for this fast implementation.")
  }

  stage_msg <- function(msg) {
    if (!isTRUE(silence)) print(paste0("[parseGBIF] ", msg))
  }

  stage_msg('Loading occurrence data...')

  if (is.na(occ_digital_voucher_file)) {
    occ_digital_voucher_file <- ''
  }

  # 1. High-speed file reading and data conversion
  if (occ_digital_voucher_file != '') {
    if (!file.exists(occ_digital_voucher_file)) stop("Invalid occurrence file!")
    occ_tmp <- data.table::fread(occ_digital_voucher_file, encoding = "UTF-8", select = fields_to_parse)
  } else {
    if (is.null(occ_digital_voucher) || NROW(occ_digital_voucher) == 0) stop("Empty occurrence data frame!")
    occ_tmp <- data.table::as.data.table(occ_digital_voucher)[, ..fields_to_parse]
  }

  stage_msg('Preparing and Subsetting Data...')

  # Add tracking columns by reference
  occ_tmp[, `:=`(
    parseGBIF_freq_duplicate_or_missing_data = '',
    parseGBIF_duplicates_map = '',
    parseGBIF_merged_fields = '',
    parseGBIF_merged = FALSE
  )]

  # Split datasets
  occ_in <- occ_tmp[parseGBIF_dataset_result == 'useable']
  occ_dup <- occ_tmp[parseGBIF_dataset_result == 'duplicate']
  occ_out_to_recover <- occ_tmp[parseGBIF_dataset_result == 'unusable']

  stage_msg('Merging Fields (High-Speed Indexed Loop)...')

  if(isTRUE(merge_unusable_data)) {
    occ_res_full <- data.table::rbindlist(list(occ_in, occ_out_to_recover), use.names = TRUE, fill = TRUE)
  } else {
    occ_res_full <- data.table::copy(occ_in)
  }

  # Set keys for extremely fast binary search inside the loop
  data.table::setkey(occ_res_full, Ctrl_key_family_recordedBy_recordNumber)
  data.table::setkey(occ_dup, Ctrl_key_family_recordedBy_recordNumber)

  key_vals <- unique(occ_res_full$Ctrl_key_family_recordedBy_recordNumber)
  tot <- length(key_vals)
  fields_to_all <- c(fields_to_compare, fields_to_merge)

  # Pre-format characters
  occ_res_full[, Ctrl_eventDate := as.character(Ctrl_eventDate)]
  occ_dup[, Ctrl_eventDate := as.character(Ctrl_eventDate)]

  for(i in seq_len(tot)) {
    current_key <- key_vals[i]

    # Binary search subset - instantaneous instead of scanning all rows
    res_idx <- occ_res_full[.(current_key), which = TRUE, nomatch = NULL]

    if(length(res_idx) == 0 || !isTRUE(occ_res_full$parseGBIF_duplicates[res_idx[1]])) next

    # Get duplicates instantly
    dup_rows <- occ_dup[.(current_key), nomatch = NULL]
    if(nrow(dup_rows) == 0) next

    if(!silence && i %% 1000 == 0) {
      print(paste0("Processed ", i, " of ", tot, " keys..."))
    }

    x_jonsom_full <- ""
    parseGBIF_merged_fields_str <- ""
    freq_data_col_full <- ""

    for (ic in seq_along(fields_to_all)) {
      field_name <- fields_to_all[ic]

      data_col <- occ_res_full[[field_name]][res_idx[1]]
      if(is.na(data_col)) data_col <- ""
      data_col <- gsub('\\{|\\}|\\[|\\]|\\(|\\)|\\\\|\\*', '', data_col)
      x_data_col <- toupper(data_col)

      data_col_dup <- dup_rows[[field_name]]

      # Calculate Frequency
      freq_vals <- c(occ_res_full[[field_name]][res_idx[1]], data_col_dup)
      freq_data_col <- as.data.frame(table(freq_vals, useNA = "no"))

      if(nrow(freq_data_col) == 0) {
        freq_data_col <- data.frame(value = 'empty', freq = dup_rows$parseGBIF_num_duplicates[1])
      } else {
        colnames(freq_data_col) <- c('value','freq')
        freq_data_col <- freq_data_col[order(-freq_data_col$freq), ]
        freq_tmp <- dup_rows$parseGBIF_num_duplicates[1] - sum(freq_data_col$freq)
        if(freq_tmp > 0) {
          freq_data_col <- dplyr::add_row(freq_data_col, value = 'empty', freq = freq_tmp)
        }
      }

      x <- paste0('"',field_name,'":', jsonify::to_json(freq_data_col))
      freq_data_col_full <- paste0(freq_data_col_full, ifelse(freq_data_col_full == "", "", ","), x)

      x_jonsom <- ""

      for(ix in seq_len(nrow(dup_rows))) {
        val_dup <- data_col_dup[ix]
        if(is.na(val_dup) || nchar(val_dup) > 10000) next

        data_col_dup_ix <- gsub('\\{|\\}|\\[|\\]|\\(|\\)|\\\\|\\*', '', val_dup)
        x_Ctrl_gbifID_dup <- dup_rows$Ctrl_gbifID[ix]
        x_data_col_dup <- toupper(data_col_dup_ix)

        if(x_data_col == x_data_col_dup) next

        if (x_jonsom != "") {
          x_jonsom_test <- paste0('{',x_jonsom, ']}')
          fromJSON_flag2 <- FALSE
          try({
            test_json <- jsonlite::fromJSON(x_jonsom_test)
            if(!is.na(data_col_dup_ix) && data_col_dup_ix %in% test_json[[field_name]]) {
              fromJSON_flag2 <- TRUE
            }
          }, silent = TRUE)
          if(isTRUE(fromJSON_flag2)) next
        }

        if(x_jonsom == "") {
          x_jonsom <- paste0('"',field_name,'":[', '"', gsub('"','',data_col), '"')
        }

        x_data_col_dup_ix <- paste0("{\"",field_name,"\":[\"",data_col_dup_ix,"\"]}")
        fromJSON_flag <- FALSE
        try({
          jsonlite::fromJSON(x_data_col_dup_ix)
          fromJSON_flag <- TRUE
        }, silent = TRUE)
        if(!fromJSON_flag) next

        if(substr(x_jonsom, nchar(x_jonsom), nchar(x_jonsom)) == '[') {
          x_jonsom <- paste0(x_jonsom, '"', gsub('"','',data_col_dup_ix), '"')
        } else {
          x_jonsom <- paste0(x_jonsom, ",", '"', gsub('"','',data_col_dup_ix), '"')
        }

        # Merge action tracking
        if(x_data_col == "" && field_name %in% fields_to_merge) {
          # Update by reference
          data.table::set(occ_res_full, i = res_idx[1], j = field_name, value = data_col_dup_ix)

          if(parseGBIF_merged_fields_str == "") {
            parseGBIF_merged_fields_str <- paste0('"', field_name,'":["', x_Ctrl_gbifID_dup,'"]')
          } else {
            parseGBIF_merged_fields_str <- paste0(parseGBIF_merged_fields_str, ",", '"', field_name,'":["', x_Ctrl_gbifID_dup,'"]')
          }
        }
      }

      if (x_jonsom != "") {
        x_jonsom <- paste0(x_jonsom, ']')
        if (x_jonsom_full == "") {
          x_jonsom_full <- x_jonsom
        } else {
          x_jonsom_full <- paste0(x_jonsom_full,',',x_jonsom)
        }
      }
    }

    if(x_jonsom_full != "") {
      data.table::set(occ_res_full, i = res_idx[1], j = "parseGBIF_duplicates_map", value = paste0('{',x_jonsom_full,'}'))
    }

    if (parseGBIF_merged_fields_str != "") {
      data.table::set(occ_res_full, i = res_idx[1], j = "parseGBIF_merged_fields", value = paste0('{',parseGBIF_merged_fields_str,'}'))
      data.table::set(occ_res_full, i = res_idx[1], j = "parseGBIF_merged", value = TRUE)
    }

    if (freq_data_col_full != "") {
      data.table::set(occ_res_full, i = res_idx[1], j = "parseGBIF_freq_duplicate_or_missing_data", value = paste0('{',freq_data_col_full,'}'))
    }
  }

  stage_msg('Finalizing Outputs...')

  if(isTRUE(merge_unusable_data)) {
    occ_out_to_recover_merge <- occ_res_full[parseGBIF_dataset_result == 'unusable']
    occ_res_useable <- occ_res_full[parseGBIF_dataset_result == 'useable']
    occ_all <- data.table::rbindlist(list(occ_res_useable, occ_out_to_recover_merge, occ_dup), fill = TRUE)

    data.table::setDF(occ_all); data.table::setDF(occ_res_useable); data.table::setDF(occ_in)
    data.table::setDF(occ_dup); data.table::setDF(occ_out_to_recover_merge); data.table::setDF(occ_out_to_recover)

    return(list(all_data = occ_all, useable_data_merge = occ_res_useable, useable_data_raw = occ_in,
                duplicates = occ_dup, unusable_data_merge = occ_out_to_recover_merge, unusable_data_raw = occ_out_to_recover))
  } else {
    occ_all <- data.table::rbindlist(list(occ_res_full, occ_out_to_recover, occ_dup), fill = TRUE)

    data.table::setDF(occ_all); data.table::setDF(occ_res_full); data.table::setDF(occ_in)
    data.table::setDF(occ_dup); data.table::setDF(occ_out_to_recover)

    return(list(all_data = occ_all, useable_data_merge = occ_res_full, useable_data_raw = occ_in,
                duplicates = occ_dup, unusable_data_merge = NA, unusable_data_raw = occ_out_to_recover))
  }
}
