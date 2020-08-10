#' mv
#' @param from a
#' @param to a
#' @export
mv <- function(from, to){
  system2(
    "cp",
    c(
      glue::glue(from),
      glue::glue(to)
    )
  )
}
