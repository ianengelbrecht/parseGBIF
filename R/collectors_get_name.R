#' @title Get the last name of the main collector
#' @name collectors_get_name
#'
#' @description
#' Extracts and standardizes the main collector's surname from the recordedBy field in GBIF data.
#' Handles various formats, special characters, abbreviations, and multiple collector scenarios.
#'
#' @param x Character string. The recordedBy field content from GBIF occurrence data.
#' @param surname_selection_type Character. Method for selecting the main collector's surname:
#' - `"largest_string"`: Selects the word with the largest number of characters (default)
#' - `"last_name"`: Selects the last valid name component with more than two characters
#' @param max_words_name Integer. Maximum number of words to consider in a name. Default is 6.
#' @param maximum_characters_in_name Integer. Minimum character length for valid surnames. Default is 3.
#'
#' @details
#' ## Processing Steps:
#' 1. Cleans and standardizes input text (strips bounding brackets, special characters, abbreviations)
#' 2. Handles multiple collectors (uses first collector before "&", "AND", ";")
#' 3. Converts diacritics and special characters to ASCII equivalents
#' 4. Applies selection method to extract main surname
#' 5. Validates extracted name against exclusion list and preserves valid hyphenated names
#'
#' ## Common Abbreviations Handled:
#' - Team/expedition references: "TEAM", "EXPED", "STAFF", "CLUB"
#' - Academic references: "UNIVERSITY", "DEPARTMENT", "HERBARIUM"
#' - Relationship suffixes: "JR", "FILHO", "NETO", "SOBRINHO"
#' - Connectors: "ET", "AND", "WITH", "FROM"
#'
#' @return
#' Character string. Standardized surname of the main collector, or:
#' - `"UNKNOWN-COLLECTOR"` for empty, invalid, or unrecognizable inputs
#' - `NA` if extracted name doesn't meet minimum character requirements
#'
#' @author
#' Pablo Hendrigo Alves de Melo,
#' Nadia Bystriakova &
#' Alexandre Monro
#' (Optimized via Base R for performance)
#'
#' @seealso
#' [`collectors_prepare_dictionary()`] for creating collector dictionaries
#'
#' @examples
#' # Basic usage
#' collectors_get_name('Melo, P.H.A & Monro, A.')
#' collectors_get_name('Monro, A. & Melo, P.H.A')
#'
#' # Hyphenated names are preserved
#' collectors_get_name('Smith-Jones, John') # Returns "SMITH-JONES"
#'
#' # Bounding brackets are stripped
#' collectors_get_name('[Smith, John]') # Returns "SMITH"
#'
#' @export
collectors_get_name <- function(x = NA,
                                surname_selection_type = 'largest_string',
                                max_words_name = 6,
                                maximum_characters_in_name = 3) {

  if (is.na(x) || x == "?., ?." || x == "?" || nchar(trimws(x)) == 0) {
    return('UNKNOWN-COLLECTOR')
  }

  no_name <- c('AL', 'ALLI', 'JR', 'ET', 'TEAM', 'JUNIOR', 'FILHO', 'NETO',
               'SOBRINHO', 'RESEARCH', 'BY', 'IN', 'FROM', 'DE', 'STAFF',
               'EXPED', 'EXP', 'DEPARTMENT', 'OF', 'CENTER', 'COLLECTION',
               'UNIVERSITY', 'COLLEGE', 'EX', 'HERB', 'SCHOOL', 'TECH',
               'DEPT', 'II', 'III', 'AND', 'COL', 'COLL', 'WITH', 'DEN',
               'VAN', 'CLUB', 'BOTANICAL', 'GARDEN', 'GARDENS', 'SOCIETY',
               'HERBARIUM', 'SECTION', 'FIELD', 'TRIP', 'CLASS', 'BOTANY',
               'EXPEDITION', 'TRANSECT', 'COLLECTOR')

  # Fast Base R helper to check if a string is a date, numeric, or in exclusion list
  check_date_num <- function(x_t, no_name) {
    x_r <- !is.na(suppressWarnings(as.Date(x_t, tryFormats = c("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y"))))
    
    if (!x_r) x_r <- !is.na(suppressWarnings(as.numeric(x_t)))
    
    if (!x_r) {
      x_t_clean <- gsub("[^A-Z]", "", x_t)
      x_r <- is.na(x_t_clean) || x_t_clean == '' || x_t %in% no_name
    }
    return(x_r)
  }

  # ---- Cleanup string ----
  
  # STRIP SURROUNDING BRACKETS EARLY: Removes outer (), [], or {} wrapping the whole string
  x <- gsub("^\\s*[\\(\\[\\{]+|[\\)\\]\\}]+\\s*$", "", x)
  
  x <- gsub("\\], \\[", "", x)
  if (substr(x, 1, 1) == "&") x <- sub("\\&", '', x)

  x <- gsub("[?.]", " ", x)
  if (nchar(trimws(x)) == 0) return('UNKNOWN-COLLECTOR')

  if (grepl("\\|", x)) x <- strsplit(x, "\\|")[[1]][1]

  x <- toupper(x)
  
  # Strip common verbose preambles
  preambles <- c("COLLECTOR\\(S\\)\\: ", "COLLECTOR\\(S\\)\\:", "COLLECTORS\\: ",
                 "COLLECTORS\\:", "COLLECTORS:", "COLLABORATION; ", "COLLABORATION;",
                 "\\[\\]\\, ", "\\[AND\\]", "PROJETO FLORA CRISTALINO; ",
                 "PROJETO FLORA CRISTALINO;", "ET AL.; ", "ET AL.;")
  for (p in preambles) x <- gsub(p, "", x)

  # Strip internal parentheses contents
  x_s <- regexpr("(", x, fixed = TRUE)[1]
  x_e <- regexpr(")", x, fixed = TRUE)[1]
  if (x_e < 0) x_e <- regexpr("]", x, fixed = TRUE)[1] 

  if (x_s > 1 && x_e > x_s) {
    x <- substr(x, 1, x_s - 1)
    if (substr(x, nchar(x), nchar(x)) == ' ') {
      x <- substr(x, 1, nchar(x) - 1)
    }
  }

  # Fast Diacritic replacement
  x <- chartr("ÁÀÂÃÄÓÒÔÕÖÍÌÎÏÚÙÛÜÉÈÊËÑÇĆŚŁČŇŃŘŠŻŰĖĚØĂĘŤĽĀŮŹÅŸ", 
              "AAAAAOOOOOIIIIUUUUEEEENCCSLCZNRZUEEOAETLAUZAAY", x)
  x <- gsub("¡", "I", x)
  x <- gsub("¢", "O", x)
  x <- gsub("Ē", "E", x)
  x <- gsub("Ņ", "N", x)
  x <- gsub("[`'´’]", "", x)
  x <- gsub("[(){}\\[\\]\"]", " ", x)
  x <- gsub("\\s+", " ", x)

  if (nchar(trimws(x)) == 0) return('UNKNOWN-COLLECTOR')

  # ---- Isolate First Collector ----
  delims <- c("&", " AND ", " WITH ", " ET ", " TO ", " IN ", " , ")
  for (d in delims) {
    if (grepl(d, x, fixed = TRUE)) x <- strsplit(x, d, fixed = TRUE)[[1]][1]
  }

  if (x == ';') return('UNKNOWN-COLLECTOR')

  if (grepl(";", x)) {
    parts <- strsplit(x, ";")[[1]]
    x_t <- parts[1]
    if (nchar(trimws(x_t)) == 0 && length(parts) > 1) {
      x_t <- parts[2]
      if (is.na(x_t)) x_t <- ""
    }
    x <- if (nchar(trimws(x_t)) > 0) x_t else ""
  }

  vl <- grep(",| ", x)

  if (length(vl) > 0) {
    xx <- strsplit(x, ",")[[1]][1]
    xx <- strsplit(xx, " ")[[1]]
    xx <- xx[xx != ""]

    if (length(xx) > 0 && max(nchar(xx)) > 2) {
      if (surname_selection_type == 'largest_string') {
        vll <- which(nchar(xx) == max(nchar(xx)))
        if (length(vll) > 1) vll <- vll[length(vll)]
        sobren <- xx[vll]
      } else {
        sobren <- ''
        
        hyphen_counts <- nchar(xx) - nchar(gsub("-", "", xx, fixed = TRUE))
        
        ind_name <- (xx != no_name) & (nchar(xx) > 1) & grepl('A|E|I|O|U|Y', xx) & (hyphen_counts < 2)

        if (sum(ind_name) > 0) {
          valid_xx <- xx[ind_name]
          for (i2 in length(valid_xx):1) {
            if (i2 > max_words_name) next
            if (nchar(valid_xx[i2]) >= maximum_characters_in_name && !check_date_num(valid_xx[i2], no_name)) {
              sobren <- valid_xx[i2]
              break
            }
          }
        }
      }
    } else if (length(xx) > 0) {
      sb <- strsplit(x, ",")[[1]]
      sb <- trimws(sb)
      nsb <- nchar(sb)
      sbvl <- which(nsb == max(nsb))
      if (length(sbvl) > 1) sbvl <- sbvl[length(sbvl)]
      sobren <- sb[sbvl]
    } else {
      sobren <- ""
    }

  } else {
    xx <- strsplit(x, " ")[[1]]

    if (surname_selection_type == 'largest_string') {
      sobren <- xx[length(xx)]
    } else {
      sobren <- ''
      hyphen_counts <- nchar(xx) - nchar(gsub("-", "", xx, fixed = TRUE))
      ind_name <- (xx != no_name) & (nchar(xx) > 1) & grepl('A|E|I|O|U|Y', xx) & (hyphen_counts < 2)

      if (sum(ind_name) > 0) {
        valid_xx <- xx[ind_name]
        for (i2 in length(valid_xx):1) {
          if (i2 > max_words_name) next
          if (nchar(valid_xx[i2]) >= maximum_characters_in_name && !check_date_num(valid_xx[i2], no_name)) {
            sobren <- valid_xx[i2]
            break
          }
        }
      }
    }
  }

  sobren <- trimws(sobren)
  sobren <- toupper(sobren)

  if (length(sobren) == 1 && !is.na(sobren) && nchar(sobren) >= maximum_characters_in_name) {

    # Clean stray edge hyphens
    sobren <- sub("^-+", "", sobren)  
    sobren <- sub("-+$", "", sobren)  

    # PRESERVE FULL HYPHENATED NAMES (e.g. SMITH-JONES) while stripping suffixes (e.g. SILVA-JR)
    if (grepl("-", sobren, fixed = TRUE)) {
      parts <- strsplit(sobren, "-", fixed = TRUE)[[1]]
      keep <- !parts %in% c("JUNIOR", "JR", "FILHO", "NETO", "SOBRINHO")
      
      if (sum(keep) > 0) {
        sobren <- paste(parts[keep], collapse = "-")
      } else {
        sobren <- ""
      }
    }

    if (nchar(sobren) >= maximum_characters_in_name) {
      return(sobren)
    } else {
      return(NA)
    }
  } else {
    return(NA)
  }
}