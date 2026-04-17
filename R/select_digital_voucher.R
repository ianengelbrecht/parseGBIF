# File: R/select_digital_voucher.R
# Purpose: Fast rewrite of select_digital_voucher() using data.table (grouped/vectorized)
# Adds: simple stage messages (no progress bar)

#' @title Selecting the master digital voucher (FAST)
#' @name select_digital_voucher
#'
#' @description
#' Fast rewrite of the original function. Groups duplicates by Ctrl_key_family_recordedBy_recordNumber,
#' computes record completeness + geospatial quality, selects a master digital voucher per group,
#' sets sample identification fields, and chooses coordinates for spatial analysis.
#'
#' @param occ GBIF occurrence table with selected columns as select_gbif_fields(columns = 'standard')
#' @param occ_gbif_issue result of function extract_gbif_issue()$occ_gbif_issue
#' @param occ_wcvp_check_name result of function batch_checkName_wcvp()$occ_wcvp_check_name
#' @param occ_collectorsDictionary result of function update_collectorsDictionary()$occ_collectorsDictionary
#' @param enumOccurrenceIssue An enumeration of validation rules for single occurrence records by GBIF file, if NA, will be used, data(EnumOccurrenceIssue)
#' @param silence if TRUE does not display progress messages
#'
#' @return list with two data frames:
#' - occ_digital_voucher: full output dataset (occ_all) with parseGBIF_* fields, plus WCVP sample columns
#' - occ_results: only processing/result fields
#'
#' @importFrom stats na.omit
#' @export
select_digital_voucher <- function(occ = NA,
                                   occ_gbif_issue = NA,
                                   occ_wcvp_check_name = NA,
                                   occ_collectorsDictionary = NA,
                                   enumOccurrenceIssue = NA,
                                   silence = TRUE) {

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required for this fast implementation.")
  }

  library(data.table)

  stages <- c(
    "Load EnumOccurrenceIssue",
    "Combine inputs",
    "Score GBIF issues",
    "Score verbatim completeness",
    "Key cleanup + group flags",
    "Voucher selection",
    "Taxon aggregation",
    "Coordinate selection",
    "Build outputs + WCVP join",
    "Finalize column order"
  )

  stage_msg <- function(i) {
    if (!isTRUE(silence)) {
      message(sprintf("[parseGBIF] Stage %d/%d: %s", i, length(stages), stages[i]))
    }
  }

  # ---- Stage 1: Load EnumOccurrenceIssue ----
  stage_msg(1)
  if (length(enumOccurrenceIssue) == 1 && is.na(enumOccurrenceIssue)) {
    data("EnumOccurrenceIssue", envir = environment())
  } else {
    EnumOccurrenceIssue <- enumOccurrenceIssue
  }

  # ---- Stage 2: Combine once (Smart Column Injection) ----
  stage_msg(2)

  # Start with the base occurrence dataset
  DT <- data.table::as.data.table(occ)

  # Helper function to slot ONLY the new columns from each pipeline step into DT
  add_new_cols <- function(source_df) {
    if (is.data.frame(source_df) && nrow(source_df) > 0) {
      # Find columns that exist in the new DF but aren't in DT yet
      new_cols <- setdiff(names(source_df), names(DT))
      if (length(new_cols) > 0) {
        # Cast to base data.frame just for the column extraction to avoid data.table scope errors
        DT[, (new_cols) := as.data.frame(source_df)[, new_cols, drop = FALSE]]
      }
    }
  }

  # Inject the generated columns without duplicating the base columns
  add_new_cols(occ_gbif_issue)
  add_new_cols(occ_wcvp_check_name)
  add_new_cols(occ_collectorsDictionary)

  # Normalize WCVP columns safely
  if ("wcvp_taxon_rank" %in% names(DT)) {
    DT[, wcvp_taxon_rank := as.character(wcvp_taxon_rank)]
    DT[is.na(wcvp_taxon_rank), wcvp_taxon_rank := ""]
  }
  if ("wcvp_taxon_status" %in% names(DT)) {
    DT[, wcvp_taxon_status := as.character(wcvp_taxon_status)]
    DT[is.na(wcvp_taxon_status), wcvp_taxon_status := ""]
  }

  # ---- Stage 3: Score GBIF issues ----
  stage_msg(3)
  idx1 <- which(EnumOccurrenceIssue$score == 1 & EnumOccurrenceIssue$type == "geospatial")
  idx2 <- which(EnumOccurrenceIssue$score == 2 & EnumOccurrenceIssue$type == "geospatial")
  idx3 <- which(EnumOccurrenceIssue$score == 3 & EnumOccurrenceIssue$type == "geospatial")

  cols1 <- EnumOccurrenceIssue$constant[idx1]
  cols2 <- EnumOccurrenceIssue$constant[idx2]
  cols3 <- EnumOccurrenceIssue$constant[idx3]

  cols1 <- cols1[cols1 %in% names(DT)]
  cols2 <- cols2[cols2 %in% names(DT)]
  cols3 <- cols3[cols3 %in% names(DT)]

  rs1 <- if (length(cols1)) rowSums(as.matrix(DT[, ..cols1]), na.rm = TRUE) else rep(0L, nrow(DT))
  rs2 <- if (length(cols2)) rowSums(as.matrix(DT[, ..cols2]), na.rm = TRUE) else rep(0L, nrow(DT))
  rs3 <- if (length(cols3)) rowSums(as.matrix(DT[, ..cols3]), na.rm = TRUE) else rep(0L, nrow(DT))

  DT[, Ctrl_coordinates_validated_by_gbif_issue :=
       (rs3 == 0) &
       (Ctrl_hasCoordinate == TRUE) &
       (Ctrl_decimalLatitude != 0) &
       (Ctrl_decimalLongitude != 0)
  ]
  DT[is.na(Ctrl_coordinates_validated_by_gbif_issue), Ctrl_coordinates_validated_by_gbif_issue := FALSE]

  DT[, Ctrl_geospatial_quality :=
       fifelse(rs3 > 0, -9L,
               fifelse(rs2 > 0, -3L,
                       fifelse(rs1 > 0, -1L, 0L)))]
  DT[Ctrl_hasCoordinate == FALSE, Ctrl_geospatial_quality := -9L]

  # ---- Stage 4: Verbatim completeness flags ----
  stage_msg(4)
  DT[, temAnoColeta       := !is.na(Ctrl_year) & Ctrl_year != "" & Ctrl_year != 0 & Ctrl_year > 10]
  DT[, temCodigoInstituicao := !is.na(Ctrl_institutionCode) & Ctrl_institutionCode != ""]
  DT[, temNumeroCatalogo  := !is.na(Ctrl_catalogNumber) & Ctrl_catalogNumber != ""]
  DT[, temColetor         := !is.na(Ctrl_recordedBy) & Ctrl_recordedBy != ""]
  DT[, temNumeroColeta    := !is.na(Ctrl_recordNumber) & Ctrl_recordNumber != ""]
  DT[, temPais            := !(COUNTRY_INVALID == TRUE)]
  DT[, temUF              := !is.na(Ctrl_stateProvince) & Ctrl_stateProvince != ""]
  DT[, temMunicipio       := !is.na(Ctrl_municipality) & Ctrl_municipality != ""]
  DT[, temLocalidade      := !is.na(Ctrl_locality) & Ctrl_locality != ""]
  DT[, temNotas           := !is.na(Ctrl_fieldNotes) & Ctrl_fieldNotes != ""]

  DT[, Ctrl_verbatim_quality :=
       temColetor + temNumeroColeta + temAnoColeta + temCodigoInstituicao +
       temNumeroCatalogo + temLocalidade + temMunicipio + temUF + temPais + temNotas
  ]
  DT[, Ctrl_moreInformativeRecord := Ctrl_geospatial_quality + Ctrl_verbatim_quality]

  # Remove temporary completeness columns to free up memory
  cols_to_drop <- c("temAnoColeta", "temCodigoInstituicao", "temNumeroCatalogo", "temColetor",
                    "temNumeroColeta", "temPais", "temUF", "temMunicipio", "temLocalidade", "temNotas")
  DT[, (cols_to_drop) := NULL]

  # ---- Stage 5: Key cleanup + group flags ----
  stage_msg(5)
  keycol <- "Ctrl_key_family_recordedBy_recordNumber"
  if (!keycol %in% names(DT)) stop("Expected column missing: Ctrl_key_family_recordedBy_recordNumber")

  DT[, (keycol) := sub("_NA$", "", get(keycol))]
  DT[, key := get(keycol)]

  # Generate keys logic efficiently
  keys <- unique(DT[, .(key)])
  keys[, `:=`(
    has_unknown = grepl("UNKNOWN-COLLECTOR", key, fixed = TRUE),
    has_double  = grepl("__", key, fixed = TRUE),
    ends_uscore = grepl("_$", key)
  )]

  keys[, FAMILY__ := grepl("__$", key)]
  keys[, FAMILY_recordedBy_ := (!FAMILY__) & (has_unknown | (has_double & !grepl("__$", key)))]
  keys[, FAMILY__recordNumber := (!FAMILY__) & (!FAMILY_recordedBy_) & ends_uscore & !grepl("__$", key)]
  keys[, non_groupable := FAMILY__ | FAMILY__recordNumber | FAMILY_recordedBy_]

  keys[, parseGBIF_duplicates_grouping_status :=
         fifelse(FAMILY__, "not groupable: no recordedBy and no recordNumber",
                 fifelse(FAMILY__recordNumber, "not groupable: no recordNumber ",
                         fifelse(FAMILY_recordedBy_, "not groupable: no recordedBy", "groupable")))]

  # Update DT by reference (avoids creating a massive duplicate copy in RAM)
  DT[keys, on = "key", `:=`(
    non_groupable = i.non_groupable,
    parseGBIF_duplicates_grouping_status = i.parseGBIF_duplicates_grouping_status
  )]

  # Initialize result fields
  DT[, `:=`(
    parseGBIF_digital_voucher = FALSE,
    parseGBIF_duplicates = FALSE,
    parseGBIF_non_groupable_duplicates = FALSE,

    parseGBIF_unidentified_sample = TRUE,
    parseGBIF_wcvp_plant_name_id = "",
    parseGBIF_sample_taxon_name = "",
    parseGBIF_sample_taxon_name_status = "",
    parseGBIF_number_taxon_names = 0L,
    parseGBIF_useful_for_spatial_analysis = FALSE,
    parseGBIF_decimalLatitude = as.numeric(NA),
    parseGBIF_decimalLongitude = as.numeric(NA)
  )]

  DT[, parseGBIF_num_duplicates := .N, by = key]

  # ---- Stage 6: Voucher selection ----
  stage_msg(6)
  DT[non_groupable == TRUE, `:=`(
    parseGBIF_non_groupable_duplicates = TRUE,
    parseGBIF_digital_voucher = TRUE,
    parseGBIF_num_duplicates = 1L,
    parseGBIF_duplicates = FALSE
  )]

  DT[non_groupable == FALSE, parseGBIF_duplicates := (parseGBIF_num_duplicates > 1L)]

  # Optimized voucher selection: Sort by informative score, then pick the first per key
  data.table::setorder(DT, key, -Ctrl_moreInformativeRecord)
  DT[non_groupable == FALSE, parseGBIF_digital_voucher := (seq_len(.N) == 1L), by = key]

  # ---- Stage 7: Taxon aggregation (Optimized to remove grouped table() loops) ----
  stage_msg(7)
  DT[, wcvp_taxon_name_and_wcvp_plant_name_id := paste0(wcvp_taxon_name, ";", wcvp_plant_name_id)]

  # Non-groupable logic remains simple
  DT[non_groupable == TRUE, `:=`(
    parseGBIF_wcvp_plant_name_id = fifelse(wcvp_taxon_status == "Accepted", as.character(wcvp_plant_name_id), ""),
    parseGBIF_sample_taxon_name  = fifelse(wcvp_taxon_status == "Accepted", as.character(wcvp_taxon_name), ""),
    parseGBIF_unidentified_sample = !(wcvp_taxon_status == "Accepted" & !is.na(wcvp_taxon_name) & wcvp_taxon_name != ""),
    parseGBIF_number_taxon_names = fifelse(wcvp_taxon_status == "Accepted" & !is.na(wcvp_taxon_name) & wcvp_taxon_name != "", 1L, 0L),
    parseGBIF_sample_taxon_name_status =
      fifelse(wcvp_taxon_status == "Accepted" & !is.na(wcvp_taxon_name) & wcvp_taxon_name != "", "identified", "unidentified")
  )]

  # Groupable logic: Calculate summaries globally instead of by-group to preserve memory/time
  tax_dt <- DT[non_groupable == FALSE & !is.na(wcvp_taxon_name) & wcvp_taxon_name != "",
               .(Freq = .N),
               by = .(key, wcvp_taxon_name_and_wcvp_plant_name_id, wcvp_taxon_status)]

  if (nrow(tax_dt) > 0) {
    tax_counts <- tax_dt[, .(num_tax = uniqueN(wcvp_taxon_name_and_wcvp_plant_name_id)), by = key]

    # Update total taxon counts
    DT[tax_counts, on = "key", parseGBIF_number_taxon_names := i.num_tax]

    # Find most frequent accepted name per key
    acc_dt <- tax_dt[wcvp_taxon_status == "Accepted"]
    data.table::setorder(acc_dt, key, -Freq, wcvp_taxon_name_and_wcvp_plant_name_id)
    best_acc <- acc_dt[, .SD[1], by = key]

    # Update DT where an accepted name exists
    DT[best_acc, on = "key", c("parseGBIF_sample_taxon_name", "parseGBIF_wcvp_plant_name_id") :=
         data.table::tstrsplit(i.wcvp_taxon_name_and_wcvp_plant_name_id, ";", fixed = TRUE)]

    DT[best_acc, on = "key", `:=`(
      parseGBIF_sample_taxon_name_status = fifelse(parseGBIF_number_taxon_names == 1L, "identified", "divergent identifications"),
      parseGBIF_unidentified_sample = FALSE
    )]
  }

  # Ensure blanks default correctly for groupables missing accepted names
  DT[non_groupable == FALSE & is.na(parseGBIF_sample_taxon_name_status),
     parseGBIF_sample_taxon_name_status := "unidentified"]

  # ---- Stage 8: Coordinate selection (Optimized sorting) ----
  stage_msg(8)
  DT[non_groupable == TRUE, `:=`(
    parseGBIF_decimalLatitude  = fifelse(Ctrl_coordinates_validated_by_gbif_issue, Ctrl_decimalLatitude, as.numeric(NA)),
    parseGBIF_decimalLongitude = fifelse(Ctrl_coordinates_validated_by_gbif_issue, Ctrl_decimalLongitude, as.numeric(NA)),
    parseGBIF_useful_for_spatial_analysis = Ctrl_coordinates_validated_by_gbif_issue
  )]

  # Find best coordinates globally by sorting preferences, then applying to the master DT
  good_coords <- DT[non_groupable == FALSE & Ctrl_coordinates_validated_by_gbif_issue == TRUE]

  if (nrow(good_coords) > 0) {
    data.table::setorder(good_coords, key, -parseGBIF_digital_voucher, -Ctrl_geospatial_quality)
    best_coords <- good_coords[, .SD[1], by = key]

    DT[best_coords, on = "key", `:=`(
      parseGBIF_decimalLatitude = i.Ctrl_decimalLatitude,
      parseGBIF_decimalLongitude = i.Ctrl_decimalLongitude,
      parseGBIF_useful_for_spatial_analysis = TRUE
    )]
  }

  DT[, parseGBIF_dataset_result := fifelse(
    parseGBIF_digital_voucher == TRUE &
      parseGBIF_unidentified_sample == FALSE &
      parseGBIF_useful_for_spatial_analysis == TRUE,
    "useable",
    fifelse(parseGBIF_digital_voucher == FALSE, "duplicate", "unusable")
  )]

  # ---- Stage 9: Build outputs + WCVP join ----
  stage_msg(9)
  occ_results_cols <- c(
    "Ctrl_geospatial_quality", "Ctrl_verbatim_quality", "Ctrl_moreInformativeRecord",
    "parseGBIF_digital_voucher", "parseGBIF_duplicates", "parseGBIF_num_duplicates",
    "parseGBIF_non_groupable_duplicates", "parseGBIF_duplicates_grouping_status",
    "Ctrl_coordinates_validated_by_gbif_issue", "parseGBIF_unidentified_sample",
    "parseGBIF_wcvp_plant_name_id", "parseGBIF_sample_taxon_name",
    "parseGBIF_sample_taxon_name_status", "parseGBIF_number_taxon_names",
    "parseGBIF_useful_for_spatial_analysis", "parseGBIF_decimalLatitude",
    "parseGBIF_decimalLongitude"
  )

  # No need to cbind again! DT already has all the columns from Stage 2.
  occ_results <- as.data.frame(DT[, ..occ_results_cols])

  # Efficient WCVP join using update-by-reference
  xn <- as.data.table(occ_wcvp_check_name)
  xn[, wcvp_plant_name_id := as.character(wcvp_plant_name_id)]
  xn <- unique(xn[, .(
    wcvp_plant_name_id, wcvp_taxon_rank, wcvp_taxon_status,
    wcvp_family, wcvp_taxon_name, wcvp_taxon_authors, wcvp_reviewed
  )])
  data.table::setnames(xn, names(xn), paste0("parseGBIF_", names(xn)))

  DT[, parseGBIF_wcvp_plant_name_id := as.character(parseGBIF_wcvp_plant_name_id)]

  # Join new WCVP columns into DT without duplicating the dataset
  new_cols <- setdiff(names(xn), "parseGBIF_wcvp_plant_name_id")
  DT[xn, on = "parseGBIF_wcvp_plant_name_id", (new_cols) := mget(paste0("i.", new_cols))]

  # ---- Stage 10: Final column order ----
  stage_msg(10)
  final_cols <- c(
    "Ctrl_gbifID", "Ctrl_bibliographicCitation", "Ctrl_language", "Ctrl_institutionCode",
    "Ctrl_collectionCode", "Ctrl_datasetName", "Ctrl_basisOfRecord", "Ctrl_catalogNumber",
    "Ctrl_recordNumber", "Ctrl_recordedBy", "Ctrl_georeferenceVerificationStatus",
    "Ctrl_occurrenceStatus", "Ctrl_eventDate", "Ctrl_year", "Ctrl_month", "Ctrl_day",
    "Ctrl_habitat", "Ctrl_fieldNotes", "Ctrl_eventRemarks", "Ctrl_locationID",
    "Ctrl_higherGeography", "Ctrl_islandGroup", "Ctrl_island", "Ctrl_countryCode",
    "Ctrl_stateProvince", "Ctrl_municipality", "Ctrl_county", "Ctrl_locality",
    "Ctrl_verbatimLocality", "Ctrl_locationRemarks", "Ctrl_level0Name", "Ctrl_level1Name",
    "Ctrl_level2Name", "Ctrl_level3Name", "Ctrl_identifiedBy", "Ctrl_dateIdentified",
    "Ctrl_scientificName", "Ctrl_decimalLatitude", "Ctrl_decimalLongitude",
    "Ctrl_identificationQualifier", "Ctrl_typeStatus", "Ctrl_family", "Ctrl_taxonRank",
    "Ctrl_issue", "Ctrl_nameRecordedBy_Standard", "Ctrl_recordNumber_Standard",
    "Ctrl_key_family_recordedBy_recordNumber", "Ctrl_geospatial_quality",
    "Ctrl_verbatim_quality", "Ctrl_moreInformativeRecord",
    "Ctrl_coordinates_validated_by_gbif_issue", "wcvp_plant_name_id", "wcvp_taxon_rank",
    "wcvp_taxon_status", "wcvp_family", "wcvp_taxon_name", "wcvp_taxon_authors",
    "wcvp_reviewed", "wcvp_searchedName", "wcvp_searchNotes", "parseGBIF_digital_voucher",
    "parseGBIF_duplicates", "parseGBIF_num_duplicates", "parseGBIF_non_groupable_duplicates",
    "parseGBIF_duplicates_grouping_status", "parseGBIF_unidentified_sample",
    "parseGBIF_sample_taxon_name", "parseGBIF_sample_taxon_name_status",
    "parseGBIF_number_taxon_names", "parseGBIF_useful_for_spatial_analysis",
    "parseGBIF_decimalLatitude", "parseGBIF_decimalLongitude", "parseGBIF_dataset_result",
    "parseGBIF_wcvp_plant_name_id", "parseGBIF_wcvp_taxon_rank", "parseGBIF_wcvp_taxon_status",
    "parseGBIF_wcvp_family", "parseGBIF_wcvp_taxon_name", "parseGBIF_wcvp_taxon_authors",
    "parseGBIF_wcvp_reviewed"
  )

  final_cols <- final_cols[final_cols %in% names(DT)]
  data.table::setcolorder(DT, final_cols)

  return(list(
    occ_digital_voucher = as.data.frame(DT),
    occ_results = occ_results
  ))
}
