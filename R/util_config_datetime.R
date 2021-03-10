#' update_config_datetime
#' Updates the config_datetime db tables
#' @param type a
#' @param tag a
#' @export
update_config_datetime <- function(type, tag, date = NULL, datetime = NULL){
  stopifnot(type %in% c("task","data"))
  if(is.null(config$schemas$config_datetime$conn)) config$schemas$config_datetime$db_connect()

  if(!is.null(datetime)) datetime <- as.character(datetime)

  if(is.null(date) & is.null(datetime)){
    date <- lubridate::today()
    datetime <- as.character(lubridate::now())
  }
  if(is.null(date) & !is.null(datetime)){
    date <- stringr::str_sub(datetime, 1,10)
  }
  if(!is.null(date) & is.null(datetime)){
    datetime <- paste0(date, " 00:01:00")
  }
  to_upload <- data.table(
    type = type,
    tag = tag,
    date = date,
    datetime = datetime
  )
  config$schemas$config_datetime$db_upsert_load_data_infile(to_upload)
}

#' get_config_datetime
#' Gets the config_datetime db table
#' @param type a
#' @param tag a
#' @export
get_config_datetime <- function(type=NULL, tag=NULL) {
  if(is.null(config$schemas$config_datetime$conn)) config$schemas$config_datetime$db_connect()

  if(!is.null(tag)){
    temp <- config$schemas$config_datetime$dplyr_tbl() %>%
      dplyr::filter(tag == !!tag) %>%
      dplyr::collect() %>%
      latin1_to_utf8()
  } else{
    temp <- config$schemas$config_datetime$dplyr_tbl() %>%
      dplyr::collect() %>%
      latin1_to_utf8()
  }
  if(!is.null(type)){
    x_type <- type
    temp <- temp[type==x_type]
  }
  return(temp)
}

# greater_than_config_datetime
# Checks to see if the config_datetime exists fora particular package
exists_config_datetime <- function(pkg) {
  if(is.null(config$schemas$config_datetime$conn)) config$schemas$config_datetime$db_connect()

  rd <- get_config_datetime()
  if (pkg %in% rd$package) {
    return(TRUE)
  }
  return(FALSE)
}
