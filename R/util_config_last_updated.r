update_config_last_updated_internal <- function(type, tag, date = NULL, datetime = NULL){
  stopifnot(type %in% c("task","data"))
  if(is.null(config$schemas$config_last_updated$conn)) config$schemas$config_last_updated$connect()

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
  config$schemas$config_last_updated$upsert_data(to_upload)
}

#' update_config_last_updated
#' Updates the config_last_updated db tables
#' @param type a
#' @param tag a
#' @param date Date to set in config_last_updated
#' @param datetime Datetime to set in config_last_updated
#' @export
update_config_last_updated <- function(type, tag, date = NULL, datetime = NULL){
  if(!stringr::str_detect(tag, "^tmp") & !tag %in% c("config_datetime", "config_last_updated")) update_config_last_updated_internal(type = type, tag = tag)
}

#' get_config_last_updated
#' Gets the config_last_updated db table
#' @param type a
#' @param tag a
#' @export
get_config_last_updated <- function(type=NULL, tag=NULL) {
  if(is.null(config$schemas$config_last_updated$conn)) config$schemas$config_last_updated$connect()

  if(!is.null(tag)){
    temp <- config$schemas$config_last_updated$tbl() %>%
      dplyr::filter(tag == !!tag) %>%
      dplyr::collect() %>%
      as.data.table()
  } else{
    temp <- config$schemas$config_last_updated$tbl() %>%
      dplyr::collect() %>%
      as.data.table()
  }
  if(!is.null(type)){
    x_type <- type
    temp <- temp[type==x_type]
  }
  return(temp)
}


# DELETE UNDER -----
update_config_datetime_internal <- function(type, tag, date = NULL, datetime = NULL){
  stopifnot(type %in% c("task","data"))
  if(is.null(config$schemas$config_datetime$conn)) config$schemas$config_datetime$connect()

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
  config$schemas$config_datetime$upsert_data(to_upload)
}

#' update_config_datetime
#' Updates the config_datetime db tables
#' @param type a
#' @param tag a
#' @param date Date to set in config_datetime
#' @param datetime Datetime to set in config_datetime
#' @export
update_config_datetime <- function(type, tag, date = NULL, datetime = NULL){
  .Deprecated("update_config_last_updated")
  if(!stringr::str_detect(tag, "^tmp") & !tag %in% c("config_datetime", "config_last_updated")) update_config_datetime_internal(type = type, tag = tag)
}

#' get_config_datetime
#' Gets the config_datetime db table
#' @param type a
#' @param tag a
#' @export
get_config_datetime <- function(type=NULL, tag=NULL) {
  .Deprecated("get_config_last_updated")
  if(is.null(config$schemas$config_datetime$conn)) config$schemas$config_datetime$connect()

  if(!is.null(tag)){
    temp <- config$schemas$config_datetime$tbl() %>%
      dplyr::filter(tag == !!tag) %>%
      dplyr::collect() %>%
      as.data.table()
  } else{
    temp <- config$schemas$config_datetime$tbl() %>%
      dplyr::collect() %>%
      as.data.table()
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
  if(is.null(config$schemas$config_datetime$conn)) config$schemas$config_datetime$connect()

  rd <- get_config_datetime()
  if (pkg %in% rd$package) {
    return(TRUE)
  }
  return(FALSE)
}

#' update_rundate
#' Updates the rundate db tables
#' @param task a
#' @export
update_rundate <- function(task){
  if(is.null(config$schemas$rundate$conn)) config$schemas$rundate$connect()
  to_upload <- data.table(
    task = task,
    date = lubridate::today(),
    datetime = as.character(lubridate::now())
  )
  config$schemas$rundate$upsert_data(to_upload)
}

#' get_rundate
#' Gets the rundate db table
#' @param task a
#' @export
get_rundate <- function(task=NULL) {
  if(is.null(config$schemas$rundate$conn)) config$schemas$rundate$connect()

  x_task <- task
  if(!is.null(task)){
    temp <- config$schemas$rundate$tbl() %>%
      dplyr::filter(task == x_task) %>%
      dplyr::collect() %>%
      latin1_to_utf8()
  }else{
    temp <- config$schemas$rundate$tbl() %>%
      dplyr::collect() %>%
      latin1_to_utf8()
  }
  return(temp)
}

# greater_than_rundate
# Checks to see if the rundate exists fora particular package
exists_rundate <- function(pkg) {
  if(is.null(config$schemas$rundate$conn)) config$schemas$rundate$connect()

  rd <- get_rundate()
  if (pkg %in% rd$package) {
    return(TRUE)
  }
  return(FALSE)
}
