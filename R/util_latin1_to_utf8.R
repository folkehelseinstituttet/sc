#' latin1_to_utf8
#' Takes a data.frame and converts it to utf8
#' @param df data.frame to convert
#' @export
latin1_to_utf8 <- function(df) {
  setDT(df)
  for (i in names(df)) {
    try(if ("UTF-8" %in% Encoding(df[[i]])) df[, (i) := iconv(get(i), from = "latin1", to = "utf8")], TRUE)
  }
  return(df)
}
