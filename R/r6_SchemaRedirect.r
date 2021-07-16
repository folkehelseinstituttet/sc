#' censor 0-4 function factory
#' If you use granularity_geo\* and granularity_time\* together, then they
#' will be treated as AND
#' @param column_name_to_be_censored Name of the column to be censored
#' @param column_name_value Name of the column whose value is determining if something should be censored
#' @param censored_value The value that censored data will be set to
#' @param granularity_time Which granularity_times to use this function on
#' @param granularity_time_not Which granularity_times to not use this function on
#' @param granularity_geo Which granularity_geos to use this function on
#' @param granularity_geo_not Which granularity_geos to not use this function on
#' @export
censor_function_factory_nothing <- function(
  column_name_to_be_censored,
  column_name_value = column_name_to_be_censored,
  censored_value = 0,
  granularity_time = NULL,
  granularity_time_not = NULL,
  granularity_geo = NULL,
  granularity_geo_not = NULL
){
  force(column_name_to_be_censored)
  force(column_name_value)
  force(granularity_time)
  force(granularity_time_not)
  force(granularity_geo)
  force(granularity_geo_not)
  if(!is.null(granularity_time) & !is.null(granularity_time_not)) stop("you can't use both granularity_time and granularity_time_not")
  if(!is.null(granularity_geo) & !is.null(granularity_geo_not)) stop("you can't use both granularity_geo and granularity_geo_not")
  function(d){
    censored_column_name <- paste0(column_name_to_be_censored, "_censored")
    if(!censored_column_name %in% names(d)) d[, (censored_column_name) := FALSE]
  }
}

#' censor 0-4 function factory
#' If you use granularity_geo\* and granularity_time\* together, then they
#' will be treated as AND
#' @param column_name_to_be_censored Name of the column to be censored
#' @param column_name_value Name of the column whose value is determining if something should be censored
#' @param censored_value The value that censored data will be set to
#' @param granularity_time Which granularity_times to use this function on
#' @param granularity_time_not Which granularity_times to not use this function on
#' @param granularity_geo Which granularity_geos to use this function on
#' @param granularity_geo_not Which granularity_geos to not use this function on
#' @export
censor_function_factory_everything <- function(
  column_name_to_be_censored,
  column_name_value = column_name_to_be_censored,
  censored_value = 0,
  granularity_time = NULL,
  granularity_time_not = NULL,
  granularity_geo = NULL,
  granularity_geo_not = NULL
){
  force(column_name_to_be_censored)
  force(column_name_value)
  force(granularity_geo)
  force(granularity_geo_not)
  force(granularity_time)
  force(granularity_time_not)
  if(!is.null(granularity_time) & !is.null(granularity_time_not)) stop("you can't use both granularity_time and granularity_time_not")
  if(!is.null(granularity_geo) & !is.null(granularity_geo_not)) stop("you can't use both granularity_geo and granularity_geo_not")
  function(d){
    censored_column_name <- paste0(column_name_to_be_censored, "_censored")
    if(!censored_column_name %in% names(d)) d[, (censored_column_name) := FALSE]

    if(is.null(granularity_time) & is.null(granularity_time_not)){
      x_granularity_time <- unique(d$granularity_time)
    } else if(!is.null(granularity_time)){
      x_granularity_time <- granularity_time
    } else if(!is.null(granularity_time_not)){
      x_granularity_time <- unique(d$granularity_time)[!unique(d$granularity_time) %in% granularity_time_not]
    }

    if(is.null(granularity_geo) & is.null(granularity_geo_not)){
      x_granularity_geo <- unique(d$granularity_geo)
    } else if(!is.null(granularity_geo)){
      x_granularity_geo <- granularity_geo
    } else if(!is.null(granularity_geo_not)){
      x_granularity_geo <- unique(d$granularity_geo)[!unique(d$granularity_geo) %in% granularity_geo_not]
    }

    d[
      granularity_time %in% x_granularity_time &
      granularity_geo %in% x_granularity_geo,
      c(
        censored_column_name,
        column_name_to_be_censored
      ) := list(
        TRUE,
        censored_value
      )
    ]
  }
}

#' censor 0-4 function factory
#' @param column_name_to_be_censored Name of the column to be censored
#' @param column_name_value Name of the column whose value is determining if something should be censored
#' @param granularity_time Which granularity_times to use this function on
#' @param granularity_time_not Which granularity_times to not use this function on
#' @param granularity_geo Which granularity_geos to use this function on
#' @param granularity_geo_not Which granularity_geos to not use this function on
#' @export
censor_function_factory_values_0_4 <- function(
  column_name_to_be_censored,
  column_name_value = column_name_to_be_censored,
  censored_value = 0,
  granularity_time = NULL,
  granularity_time_not = NULL,
  granularity_geo = NULL,
  granularity_geo_not = NULL
){
  force(column_name_to_be_censored)
  force(column_name_value)
  force(granularity_geo)
  force(granularity_geo_not)
  force(granularity_time)
  force(granularity_time_not)
  if(!is.null(granularity_time) & !is.null(granularity_time_not)) stop("you can't use both granularity_time and granularity_time_not")
  if(!is.null(granularity_geo) & !is.null(granularity_geo_not)) stop("you can't use both granularity_geo and granularity_geo_not")
  function(d){
    censored_column_name <- paste0(column_name_to_be_censored, "_censored")
    if(!censored_column_name %in% names(d)) d[, (censored_column_name) := FALSE]

    if(is.null(granularity_time) & is.null(granularity_time_not)){
      x_granularity_time <- unique(d$granularity_time)
    } else if(!is.null(granularity_time)){
      x_granularity_time <- granularity_time
    } else if(!is.null(granularity_time_not)){
      x_granularity_time <- unique(d$granularity_time)[!unique(d$granularity_time) %in% granularity_time_not]
    }

    if(is.null(granularity_geo) & is.null(granularity_geo_not)){
      x_granularity_geo <- unique(d$granularity_geo)
    } else if(!is.null(granularity_geo)){
      x_granularity_geo <- granularity_geo
    } else if(!is.null(granularity_geo_not)){
      x_granularity_geo <- unique(d$granularity_geo)[!unique(d$granularity_geo) %in% granularity_geo_not]
    }

    d[
      granularity_time %in% x_granularity_time &
        granularity_geo %in% x_granularity_geo &
        get(column_name_value) >= 0 & get(column_name_value) <= 4,
      c(
        censored_column_name,
        column_name_to_be_censored
      ) := list(
        TRUE,
        censored_value
      )
    ]
  }
}

#' censor 0-4 function factory
#' @param list_of_censors List of censors
#' @export
censor_list_function_factory <- function(
  list_of_censors
){
  force(list_of_censors)
  function(d){
    for(i in list_of_censors) i(d)
  }
}

#' SchemaRedirect class description
#'
#' @import data.table
#' @import R6
#' @export SchemaRedirect_v8
SchemaRedirect_v8 <- R6Class(
  "SchemaRedirect_v8",

  public = list(
    table_names = NULL,
    table_accesses = NULL,
    preferred_table_name = NULL,
    censors = NULL,
    schemas = NULL,

    initialize = function(
      name_access = NULL,
      name_grouping = NULL,
      name_variant = NULL,
      db_configs = NULL,
      field_types,
      keys,
      censors = NULL,
      indexes=NULL,
      validator_field_types=validator_field_types_blank,
      validator_field_contents=validator_field_contents_blank,
      info = NULL
    ) {
      force(name_access)
      force(name_grouping)
      force(name_variant)
      force(db_configs)
      force(field_types)
      force(keys)
      force(censors)
      force(indexes)
      force(validator_field_types)
      force(validator_field_contents)
      force(info)

      self$censors <- censors
      self$schemas <- list()

      if(sum(!names(censors) %in% name_access)>0) stop("censors are not listed in name_access")
      if(sum(!name_access %in% names(censors))>0) stop("missing censors")
      for(i in seq_along(name_access)) if(sum(!names(censors[[i]]) %in% names(field_types))>0) stop("censor[[",i,"]] has columns that are not listed in field_types")

      self$table_names <- c()
      self$table_accesses <- c()
      for(i in seq_along(name_access)){
        stopifnot(name_access[i] %in% c("restr", "anon", "config"))
        table_name <- paste0(c(name_access[i], name_grouping, name_variant), collapse = "_")
        force(table_name)

        censored_field_names <- paste0(names(censors[[i]]),"_censored")
        if(censored_field_names[1] != "_censored"){
          censored_field_types <- rep("BOOLEAN", length = length(censored_field_names))
          names(censored_field_types) <- censored_field_names

          field_types_with_censoring <- c(
            field_types,
            censored_field_types
          )
        } else {
          field_types_with_censoring <- c(
            field_types
          )
        }
        force(field_types_with_censoring)
        schema <- sc::Schema_v8$new(
          db_config = db_configs[[name_access[i]]],
          table_name = table_name,
          field_types = field_types_with_censoring,
          keys = keys,
          censors = censors[[name_access[i]]],
          indexes = indexes,
          validator_field_types = validator_field_types,
          validator_field_contents = validator_field_contents,
          info = info
        )

        config$schemas[[table_name]] <- schema
        self$schemas[[table_name]] <- schema$clone(deep = TRUE)
        self$table_names <- c(self$table_names, table_name)
        self$table_accesses <- c(self$table_accesses, name_access[i])

        if(name_access[i] == config$db_config_preferred) self$preferred_table_name <- schema$table_name
      }
    },
    #' @description
    #' Connect to a db
    connect = function() {
      for(i in self$table_names) self$schemas[[i]]$connect()
    },

    #' @description
    #' Disconnect from a db
    disconnect = function() {
      for(i in self$table_names) self$schemas[[i]]$disconnect()
    },

    #' @description
    #' Create db table
    create_table = function() {
      for(i in self$table_names) self$schemas[[i]]$create_table()
    },

    drop_table = function() {
      for(i in self$table_names) self$schemas[[i]]$drop_table()
    },

    #' @description
    #' Inserts data into db table
    insert_data = function(newdata, verbose = TRUE) {
      for(i in seq_along(self$table_names)){
        table_name <- self$table_names[i]
        table_access <- self$table_accesses[i]
        self$schemas[[table_name]]$insert_data(
          newdata = newdata,
          verbose = verbose
        )
      }
    },

    #' @description
    #' Upserts data into db table
    upsert_data = function(newdata, drop_indexes = names(self$indexes), verbose = TRUE) {
      for(i in seq_along(self$table_names)){
        table_name <- self$table_names[i]
        table_access <- self$table_accesses[i]

        self$schemas[[table_name]]$upsert_data(
          newdata = newdata,
          drop_indexes = drop_indexes,
          verbose = verbose
        )
      }
    },

    drop_all_rows = function() {
      for(i in self$table_names) self$schemas[[i]]$drop_all_rows()
    },

    drop_rows_where = function(condition, cores = 1){
      stopifnot(cores == 1)
      if(cores == 1){
        for(i in self$table_names) self$schemas[[i]]$drop_rows_where(condition = condition)
      } else {
        parallel::mclapply(self$schemas, function(x, condition) drop_rows_where(condition = condition), mc.cores = 3, condition = condition)
      }
    },

    keep_rows_where = function(condition, cores = 1){
      stopifnot(cores == 1)
      if(cores == 1){
        for(i in self$table_names) self$schemas[[i]]$keep_rows_where(condition = condition)
      } else {
        parallel::mclapply(self$schemas, function(x, condition) keep_rows_where(condition = condition), mc.cores = 3, condition = condition)
      }
    },

    drop_all_rows_and_then_upsert_data =  function(newdata, drop_indexes = names(self$indexes), verbose = TRUE) {
      for(i in seq_along(self$table_names)){
        table_name <- self$table_names[i]
        table_access <- self$table_accesses[i]

        self$schemas[[table_name]]$drop_all_rows_and_then_upsert_data(
          newdata = censored_data,
          drop_indexes = drop_indexes,
          verbose = verbose
        )
      }
    },

    drop_all_rows_and_then_insert_data =  function(newdata, verbose = TRUE) {
      for(i in seq_along(self$table_names)){
        table_name <- self$table_names[i]
        table_access <- self$table_accesses[i]

        self$schemas[[table_name]]$drop_all_rows_and_then_insert_data(
          newdata = censored_data,
          verbose = verbose
        )
      }
    },

    tbl = function() {
      self$schemas[[self$preferred_table_name]]$tbl()
    },

    print_dplyr_select = function() {
      self$schemas[[self$preferred_table_name]]$print_dplyr_select()
    },

    list_indexes_db = function(){
      self$schemas[[self$preferred_table_name]]$list_indexes_db()
    },

    add_indexes = function() {
      for(i in self$table_names) self$schemas[[i]]$add_indexes()
    },

    drop_indexes = function() {
      for(i in self$table_names) self$schemas[[i]]$drop_indexes()
    }
  ),
  private = list(
    finalize = function() {
      # self$db_disconnect()
    }
  )
)
