#' @title Standardize scientific names for WCVP compatibility
#' @name standardize_scientificName
#'
#' @description Standardize binomial name, variety, subspecies, form and hybrids, authorship
#' to allow comparison with names of taxa in the World Checklist of Vascular Plants (WCVP) database
#'
#' @param searchedName scientific name, with or without author
#'
#' @details Standardize scientific name according to WCVP format.
#' Separate generic epithet, specific epithet, variety, subspecies, form, hybrid and author, in the scientific name, if any.
#' Standardize, according to WCVP, abbreviation of infrataxon, if any:
#' variety to var.,
#' subspecies to subsp.,
#' FORM to f.,
#' hybrid separator separate x from the specific epithet.
#'
#' @return
#' A list with four elements:
#' - `searchedName`: original scientific name
#' - `standardizeName`: standardized scientific name without authors
#' - `taxonAuthors`: full author string
#' - `taxonAuthors_last`: last author(s) after parentheses
#'
#' @author Pablo Hendrigo Alves de Melo,
#'         Nadia Bystriakova &
#'         Alexandre Monro
#'
#' @seealso \code{\link[parseGBIF]{get_wcvp}}, \code{\link[parseGBIF]{checkName_wcvp}}
#'
#' @examples
#' \donttest{
#' standardize_scientificName('Leucanthemum ×superbum (Bergmans ex J.W.Ingram) D.H.Kent')
#' standardize_scientificName('Alomia angustata (Gardner) Benth. ex Baker')
#' standardize_scientificName('Centaurea ×aemiliae Font Quer')
#' }
#'
#' @importFrom stringr str_split str_detect str_sub str_locate
#' @export
standardize_scientificName <- function(searchedName = 'Alomia angustata (Gardner) Benth. ex Baker') {

  if (is.na(searchedName) || searchedName == "") {
    return(list(searchedName = "", standardizeName = "", taxonAuthors = "", taxonAuthors_last = ""))
  }

  # 1. Base R split (Massively faster than stringr)
  sp <- strsplit(searchedName, " ", fixed = TRUE)[[1]]
  if (length(sp) == 0) return(list(searchedName = searchedName, standardizeName = searchedName, taxonAuthors = "", taxonAuthors_last = ""))

  padrao_s <- c("var.", "nothosubsp.", "subsp.", "f.")
  has_hybrid <- grepl("×", searchedName, fixed = TRUE)

  std_name <- ""
  infrataxa <- ""

  # THE FIX: Find the exact marker instantly without a 12-line if/else ladder
  marker_idx <- which(sp %in% padrao_s)[1]

  if (!is.na(marker_idx)) {
    if (length(sp) > marker_idx) infrataxa <- sp[marker_idx + 1]
    marker <- sp[marker_idx]

    if (has_hybrid) {
      std_name <- paste0(sp[1], " × ", sp[3], if (infrataxa != "") paste0(" ", marker, " ", infrataxa) else "")
    } else {
      std_name <- paste0(sp[1], " ", sp[2], " ", marker, " ", infrataxa)
    }
  } else {
    
    if (has_hybrid) {
      if (length(sp) > 1 && grepl("×", sp[2], fixed = TRUE)) {
         fixed_name <- sub("×", "× ", searchedName, fixed = TRUE)
         sp <- strsplit(fixed_name, " ", fixed = TRUE)[[1]]
      }
      std_name <- if (length(sp) >= 3) paste0(sp[1], " × ", sp[3]) else paste0(sp[1], " ×")

      # Handle genus starting with ×
      if (substr(sp[1], 1, 1) == "×") {
        clean_genus <- substr(sp[1], 2, nchar(sp[1]))
        std_name <- if (length(sp) > 1) paste0(clean_genus, " ", sp[2]) else clean_genus
      }
    } else {
      # Check if the second word is capitalized or starts with '('
      if (length(sp) > 1) {
        first_char <- substr(sp[2], 1, 1)
        if (first_char == toupper(first_char) || first_char == "(") {
          std_name <- sp[1]
        } else {
          std_name <- paste0(sp[1], " ", sp[2])
        }
      } else {
        std_name <- sp[1]
      }
    }
  }

  # 2. Extract Author cleanly (THE FIX: Replaced the slow try() block with fast regex)
  sp2 <- strsplit(std_name, " ", fixed = TRUE)[[1]]
  taxon_authors <- ""

  if (length(sp2) > 0) {
    last_word <- sp2[length(sp2)]
    # Escape any special characters in the last word just in case
    esc_last_word <- gsub("([.|()\\^{}+$*?]|\\[|\\])", "\\\\\\1", last_word)
    
    # Grab everything after the last standardized word
    regex_pattern <- paste0(".*?\\b", esc_last_word, "\\b\\s*(.*)")

    if (grepl(esc_last_word, searchedName)) {
       extracted <- sub(regex_pattern, "\\1", searchedName, perl = TRUE)
       if (extracted != searchedName) taxon_authors <- extracted
    }
  }

  if (length(sp2) == 4 && taxon_authors != "" && paste0(sp2[3], " ", sp2[4]) == taxon_authors) {
    taxon_authors <- ""
  }

  # 3. Extract Author Last cleanly
  taxonAuthors_last <- ""
  if (nchar(taxon_authors) > 0 && substr(taxon_authors, 1, 1) == "(") {
     paren_pos <- regexpr(")", taxon_authors, fixed = TRUE)[1]
     if (paren_pos > 0 && paren_pos < nchar(taxon_authors)) {
       taxonAuthors_last <- trimws(substr(taxon_authors, paren_pos + 1, nchar(taxon_authors)))
     }
  }

  return(list(
    searchedName = searchedName,
    standardizeName = trimws(std_name),
    taxonAuthors = trimws(taxon_authors),
    taxonAuthors_last = taxonAuthors_last
  ))
}

