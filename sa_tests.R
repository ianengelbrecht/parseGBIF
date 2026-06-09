# parseGBIF revision output comparisons
# Author: Google Gemini via Ian Engelbrecht (assisting SANBI)

### PREPPED DATASETS ###
dim(prepped_orig) == dim(prepped_revised)
identical(names(prepped_orig), names(prepped_revised))
all.equal(as.data.frame(prepped_orig), 
          as.data.frame(prepped_revised), 
          check.attributes = FALSE)

### ISSUES ### 
identical(names(issues_orig), names(issues_revised))
dim(issues_orig$occ_gbif_issue) == dim(issues_revised$occ_gbif_issue)
dim(issues_orig$summary) == dim(issues_revised$summary)
all.equal(as.data.frame(issues_orig$occ_gbif_issue), 
          as.data.frame(issues_revised$occ_gbif_issue),
          check.attributes = FALSE)

all.equal(as.data.frame(issues_orig$summary), 
          as.data.frame(issues_revised$summary),
          check.attributes = FALSE)

all.equal(
  issues_orig$summary[order(issues_orig$summary$issue), ],
  issues_revised$summary[order(issues_revised$summary$issue), ],
  check.attributes = FALSE
)

### NAMES CHECKED ###
# note that the dims are now different so we do this a little differently
identical(names(names.checked_orig), names(names.checked_revised))

orig_compare <- names.checked_orig$occ_wcvp_check_name[, common_cols]
revised_compare <- as.data.frame(names.checked_revised$occ_wcvp_check_name)[, common_cols]

# Temporarily cast the revised IDs back to numeric just for the test
id_cols <- c("wcvp_plant_name_id", "wcvp_accepted_plant_name_id", "wcvp_plant_name_id_of_searchedName")
for(col in id_cols) {
  revised_compare[[col]] <- as.numeric(revised_compare[[col]])
  orig_compare[[col]] <- as.numeric(orig_compare[[col]])
}

# Run the check
all.equal(orig_compare, revised_compare, check.attributes = FALSE)

### EVENT KEYS ###

# ==========================================
# 1. Dimension check
# ==========================================
# All three should return TRUE TRUE
print("Checking Dimensions:")
dim(event_keys_orig$occ_collectorsDictionary) == dim(event_keys_revised$occ_collectorsDictionary)
dim(event_keys_orig$summary) == dim(event_keys_revised$summary)
dim(event_keys_orig$collectorsDictionary_add) == dim(event_keys_revised$collectorsDictionary_add)

# ==========================================
# 2. Main occurrence keys check
# ==========================================
# This checks if every single row got the exact same 4 generated keys
print("Checking Main Keys:")
all.equal(
  as.data.frame(event_keys_orig$occ_collectorsDictionary),
  as.data.frame(event_keys_revised$occ_collectorsDictionary),
  check.attributes = FALSE
)

# ==========================================
# 3. Summary table check
# ==========================================
# We sort alphabetically by the generated key to prevent false tie-breaker errors
print("Checking Summary:")
orig_sum <- as.data.frame(event_keys_orig$summary)
rev_sum <- as.data.frame(event_keys_revised$summary)

# Sort both
orig_sum <- orig_sum[order(orig_sum$Ctrl_key_family_recordedBy_recordNumber), ]
rev_sum <- rev_sum[order(rev_sum$Ctrl_key_family_recordedBy_recordNumber), ]
rownames(orig_sum) <- NULL
rownames(rev_sum) <- NULL

all.equal(orig_sum, rev_sum, check.attributes = FALSE)

# ==========================================
# 4. New collectors dictionary check
# ==========================================
# Sort alphabetically by the raw collector name
print("Checking Dictionary Additions:")
orig_add <- as.data.frame(event_keys_orig$collectorsDictionary_add)
rev_add <- as.data.frame(event_keys_revised$collectorsDictionary_add)

orig_add <- orig_add[order(orig_add$Ctrl_recordedBy), ]
rev_add <- rev_add[order(rev_add$Ctrl_recordedBy), ]
rownames(orig_add) <- NULL
rownames(rev_add) <- NULL

all.equal(orig_add, rev_add, check.attributes = FALSE)


### SELECTED VOUCHERS ###

# 1. Align rows perfectly by ID
orig_final <- merged_records_orig$occ_digital_voucher[order(as.numeric(merged_records_orig$occ_digital_voucher$Ctrl_gbifID)), ]
revised_final <- merged_records_revised$occ_digital_voucher[order(as.numeric(merged_records_revised$occ_digital_voucher$Ctrl_gbifID)), ]

# 2. Check if the row counts even match!
print(paste("Rows match?", nrow(orig_final) == nrow(revised_final)))

# 3. Test just the vital logic columns we care about
vital_cols <- c("parseGBIF_digital_voucher", "parseGBIF_duplicates", 
                "parseGBIF_num_duplicates", "parseGBIF_non_groupable_duplicates", 
                "parseGBIF_duplicates_grouping_status")

# 4. Checking it all worked (we need check.attributes to avoid the unecessary row index comparison)...
all.equal(orig_final[, vital_cols], revised_final[, vital_cols], check.attributes = FALSE)
