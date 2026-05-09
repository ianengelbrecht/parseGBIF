#' @title Export Parsed GBIF Data Results
#' @name export_data
#'
#' @description
#' Processes and exports results from parsed GBIF occurrence data, merging information
#' from duplicate records to create unique collection event records. For each unique
#' collection event key (complete or incomplete), this function combines information
#' from duplicate records and generates a single unique collection event record.
#'
#' @param occ_digital_voucher_file Character. Path to CSV file.
#' @param occ_digital_voucher Data frame. Result from `select_digital_voucher()`.
#' @param merge_unusable_data Logical.
#' @param fields_to_merge Character vector.
#' @param fields_to_compare Character vector.
#' @param fields_to_parse Character vector.
#' @param silence Logical. If `FALSE`, displays progress messages and a progress bar.
#'
#' @details
#' (Documentation maintained from original)
#'
#' @author
#' Pablo Hendrigo Alves de Melo,
#' Nadia Bystriakova &
#' Alexandre Monro
#' (Fully Refactored for Relational Vectorization and Type Safety)
#'
#' @seealso \code{\link[parseGBIF]{select_digital_voucher}}
#'
#' @importFrom data.table fread as.data.table setDT := setDF rbindlist setorder fcase data.table
#' @importFrom utils txtProgressBar setTxtProgressBar close
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

  if (!requireNamespace("data.table", quietly = TRUE)) stop("data.table is required.")
  stage_msg <- function(msg) { if (!isTRUE(silence)) cat(paste0("\n[parseGBIF] ", msg, "\n")) }

  stage_msg('Loading occurrence data...')
  if (is.na(occ_digital_voucher_file)) occ_digital_voucher_file <- ''

  if (occ_digital_voucher_file != '') {
    occ_tmp <- data.table::fread(occ_digital_voucher_file, encoding = "UTF-8", select = fields_to_parse)
  } else {
    occ_tmp <- data.table::as.data.table(occ_digital_voucher)[, ..fields_to_parse]
  }

  stage_msg('Preparing Data Architecture...')
  occ_tmp[, `:=`(parseGBIF_freq_duplicate_or_missing_data = '', parseGBIF_duplicates_map = '',
                 parseGBIF_merged_fields = '', parseGBIF_merged = FALSE)]

  occ_in <- occ_tmp[parseGBIF_dataset_result == 'useable']
  occ_dup <- occ_tmp[parseGBIF_dataset_result == 'duplicate']
  occ_out_to_recover <- occ_tmp[parseGBIF_dataset_result == 'unusable']

  if(isTRUE(merge_unusable_data)) {
    occ_res_full <- data.table::rbindlist(list(occ_in, occ_out_to_recover), use.names = TRUE, fill = TRUE)
  } else {
    occ_res_full <- data.table::copy(occ_in)
  }

  occ_res_full[, Ctrl_eventDate := as.character(Ctrl_eventDate)]
  occ_dup[, Ctrl_eventDate := as.character(Ctrl_eventDate)]

  fields_to_all <- unique(c(fields_to_compare, fields_to_merge))
  keys_with_dups <- occ_res_full[parseGBIF_duplicates == TRUE, Ctrl_key_family_recordedBy_recordNumber]

  dt_M <- occ_res_full[Ctrl_key_family_recordedBy_recordNumber %in% keys_with_dups]

  # --- USE HERMETIC JOIN_KEY TO PREVENT NAMESPACE COLLISIONS ---
  dt_M[, `:=`(join_key = Ctrl_key_family_recordedBy_recordNumber,
              map_str = "", merge_str = "", freq_str = "")]

  stage_msg('Executing Vectorized Relational Joins...')

  tot_cols <- length(fields_to_all)

  for (idx in seq_along(fields_to_all)) {
    col <- fields_to_all[idx]

    if (!isTRUE(silence)) {
      cat(sprintf("\r  -> Processing column %02d/%02d: %-30s", idx, tot_cols, col))
      flush.console()
    }

    if (!col %in% names(dt_M) || !col %in% names(occ_dup)) next

    M <- dt_M[, .(join_key, gbifID = as.character(Ctrl_gbifID), val = as.character(get(col)), is_master = TRUE)]
    D <- occ_dup[Ctrl_key_family_recordedBy_recordNumber %in% keys_with_dups, .(
      join_key = Ctrl_key_family_recordedBy_recordNumber, gbifID = as.character(Ctrl_gbifID),
      val = as.character(get(col)), num_dups = parseGBIF_num_duplicates, is_master = FALSE)]

    comb <- data.table::rbindlist(list(M, D[, .(join_key, gbifID, val, is_master)]), fill=TRUE)
    comb[is.na(val), val := ""]
    comb[, val_clean := gsub('\\{|\\}|\\[|\\]|\\(|\\)|\\\\|\\*|\"', '', val)]
    comb[, val_upper := toupper(val_clean)]

    k_dups <- D[, .(num_dups = num_dups[1]), by = "join_key"]

    # VECTORIZED FREQUENCY
    f_counts <- comb[val_clean != "", .N, by = .(join_key, val_clean)]
    if(nrow(f_counts) > 0) {
      data.table::setorder(f_counts, join_key, -N)
      f_counts <- k_dups[f_counts, on="join_key"]
      f_counts[, piece := paste0('{"value":"', val_clean, '","freq":', N, '}')]
      f_agg <- f_counts[, .(sum_n = sum(N), num_dups = num_dups[1], pieces = paste(piece, collapse=",")), by = "join_key"]
      f_agg[, empty_n := num_dups - sum_n]
      f_agg[empty_n > 0, empty_piece := paste0(',{"value":"empty","freq":', empty_n, '}')]
      f_agg[is.na(empty_piece) | empty_n <= 0, empty_piece := ""]
    } else {
      f_agg <- data.table::data.table(join_key = character(), pieces = character(), empty_piece = character())
    }

    all_empty_keys <- k_dups$join_key[!k_dups$join_key %in% f_agg$join_key]
    if(length(all_empty_keys) > 0) {
      empty_add <- data.table::data.table(join_key = all_empty_keys, pieces = "",
                                          empty_piece = paste0('{"value":"empty","freq":', k_dups[join_key %in% all_empty_keys, num_dups], '}'))
      f_agg <- data.table::rbindlist(list(f_agg, empty_add), fill=TRUE, use.names=TRUE)
    }

    if (nrow(f_agg) > 0) {
      f_agg[, final_pieces := paste0(pieces, empty_piece)]
      f_agg[substr(final_pieces, 1, 1) == ",", final_pieces := substr(final_pieces, 2, nchar(final_pieces))]
      f_agg[, col_json := paste0('"', col, '":[', final_pieces, ']')]
      dt_M[f_agg, freq_str := data.table::fcase(freq_str == "", i.col_json, default = paste0(freq_str, ",", i.col_json)), on="join_key"]
    }

    # VECTORIZED DUPLICATE MAP
    map_dt <- comb[val_clean != ""]
    if(nrow(map_dt) > 0) {
      data.table::setorder(map_dt, join_key, -is_master)
      map_dt <- map_dt[, .SD[1], by = .(join_key, val_upper)]
      m_agg <- map_dt[, .(col_json = paste0('"', col, '":[', paste0('"', val_clean, '"', collapse=","), ']')), by = "join_key"]
      dt_M[m_agg, map_str := data.table::fcase(map_str == "", i.col_json, default = paste0(map_str, ",", i.col_json)), on="join_key"]
    }

    # VECTORIZED MERGING (WITH TYPE CASTING)
    if (col %in% fields_to_merge) {
      empty_keys <- M[val == "", join_key]
      if (length(empty_keys) > 0) {
        valid_dups <- D[join_key %in% empty_keys & val != ""]
        valid_dups[, val_clean := gsub('\\{|\\}|\\[|\\]|\\(|\\)|\\\\|\\*|\"', '', val)]
        valid_dups <- valid_dups[val_clean != ""]

        if (nrow(valid_dups) > 0) {
          first_dups <- valid_dups[, .SD[1], by = "join_key"]

          target_class <- class(dt_M[[col]])[1]

          suppressWarnings({
            if (target_class == "integer") {
              first_dups[, val_typed := as.integer(val_clean)]
            } else if (target_class %in% c("numeric", "double")) {
              first_dups[, val_typed := as.numeric(val_clean)]
            } else if (target_class == "logical") {
              first_dups[, val_typed := as.logical(val_clean)]
            } else {
              first_dups[, val_typed := val_clean]
            }
          })

          dt_M[first_dups, (col) := i.val_typed, on="join_key"]

          first_dups[, col_json := paste0('"', col, '":["', gbifID, '"]')]
          dt_M[first_dups, merge_str := data.table::fcase(merge_str == "", i.col_json, default = paste0(merge_str, ",", i.col_json)), on="join_key"]
        }
      }
    }
  }

  if (!isTRUE(silence)) cat("\n")

  stage_msg('Formatting Final Output...')
  dt_M[map_str != "", parseGBIF_duplicates_map := paste0('{', map_str, '}')]
  dt_M[merge_str != "", `:=`(parseGBIF_merged_fields = paste0('{', merge_str, '}'), parseGBIF_merged = TRUE)]
  dt_M[freq_str != "", parseGBIF_freq_duplicate_or_missing_data := paste0('{', freq_str, '}')]

  cols_to_update <- c("parseGBIF_duplicates_map", "parseGBIF_merged_fields", "parseGBIF_merged", "parseGBIF_freq_duplicate_or_missing_data", fields_to_merge)
  occ_res_full[dt_M, (cols_to_update) := mget(paste0("i.", cols_to_update)), on=.(Ctrl_key_family_recordedBy_recordNumber = join_key)]

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
