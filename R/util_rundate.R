#' update_rundate
#' Updates the rundate db tables
#' @param task a
#' @export
update_rundate <- function(task){
  if(is.null(config$schemas$rundate$conn)) config$schemas$rundate$db_connect()
  to_upload <- data.table(
    task = task,
    date = lubridate::today(),
    datetime = as.character(lubridate::now())
  )
  config$schemas$rundate$db_upsert_data(to_upload)
}

#' get_rundate
#' Gets the rundate db table
#' @param task a
#' @export
get_rundate <- function(task=NULL) {
  if(is.null(config$schemas$rundate$conn)) config$schemas$rundate$db_connect()

  x_task <- task
  if(!is.null(task)){
    temp <- config$schemas$rundate$dplyr_tbl() %>%
      dplyr::filter(task == x_task) %>%
      dplyr::collect() %>%
      latin1_to_utf8()
  }else{
    temp <- config$schemas$rundate$dplyr_tbl() %>%
      dplyr::collect() %>%
      latin1_to_utf8()
  }
  return(temp)
}

# greater_than_rundate
# Checks to see if the rundate exists fora particular package
exists_rundate <- function(pkg) {
  if(is.null(config$schemas$rundate$conn)) config$schemas$rundate$db_connect()

  rd <- get_rundate()
  if (pkg %in% rd$package) {
    return(TRUE)
  }
  return(FALSE)
}
