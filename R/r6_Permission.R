#' add permission
#' @param name the name of the permission
#' @param permission a Permission R6 class
#' @export
add_permission <- function(name, permission){
  config$permissions[[name]] <- permission
}

#' Permission
#' @import R6
#' @export
Permission <- R6::R6Class(
  "Permission",
  portable = FALSE,
  cloneable = FALSE,
  public = list(
    key = NULL,
    value = NULL,
    dev_always_performs = FALSE,
    production_days = c(1:7),
    db_schema = NULL,
    initialize = function(
      key,
      value,
      production_days = c(1:7)
      ) {
      value <<- as.character(value)
      production_days <<- production_days

      if (is_final()) {
        key <<- glue::glue("FINAL_{key}")
      } else {
        key <<- glue::glue("PRELIM_{key}")
      }

      field_types <- c(
        "xkey" = "TEXT",
        "value" = "TEXT"
      )

      keys <- c(
        "xkey"
      )

      db_schema <<- Schema_v8$new(
        db_config = config$db_config,
        db_table = "permission",
        db_field_types = field_types,
        db_load_folder = tempdir(),
        keys = keys
      )
    },
    has_permission = function() {
      permission <- TRUE

      old_value <- current_value()
      if (length(old_value) > 0) {
        if (value == old_value) {
          permission <- FALSE
        }
      }

      if (!config$is_production) {
        permission <- TRUE
      }

      return(permission)
    },
    revoke_permission = function() {
      db_schema$connect()

      to_upload <- data.table(
        xkey = key,
        value = as.character(value)
      )

      db_schema$upset_data(to_upload)
      db_schema$disconnect()
    },
    grant_permission = function() {
      db_schema$connect()

      to_upload <- data.table(
        xkey = key,
        value = uuid::UUIDgenerate()
      )

      db_schema$upset_data(to_upload)
      db_schema$disconnect()
    },
    is_final = function() {
      today <- lubridate::wday(lubridate::today(), week_start = 1)
      return(today %in% production_days)
    },
    current_value = function() {
      if(!"permission" %in% list_tables()){
        db_schema$connect()
        db_schema$disconnect()
      }
      temp <- tbl("permission") %>%
        dplyr::collect() %>%
        latin1_to_utf8()

      return(temp[xkey == key, value])
    }
  )
)
