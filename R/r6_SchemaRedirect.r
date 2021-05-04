#' censor 0-4 function factory
#' @param column_name_to_be_censored Name of the column to be censored
#' @param column_name_value Name of the column whose value is determining if something should be censored
censor_function_factory_nothing <- function(column_name_to_be_censored, column_name_value = column_name_to_be_censored){
  force(column_name_to_be_censored)
  force(column_name_value)
  function(d){
    censored_column_name <- paste0(column_name_to_be_censored, "_censored")
    d[, (censored_column_name) := FALSE]
  }
}

#' censor 0-4 function factory
#' @param column_name_to_be_censored Name of the column to be censored
#' @param column_name_value Name of the column whose value is determining if something should be censored
censor_function_factory_values_0_4 <- function(column_name_to_be_censored, column_name_value = column_name_to_be_censored){
  force(column_name_to_be_censored)
  force(column_name_value)
  function(d){
    censored_column_name <- paste0(column_name_to_be_censored, "_censored")
    d[, (censored_column_name) := FALSE]
    d[get(column_name_value) >= 0 & get(column_name_value) <= 4, c(censored_column_name, column_name_to_be_censored) := list(TRUE, 0)]
    print(d)
  }
}

#' SchemaRedirect class description
#'
#' @import data.table
#' @import R6
#' @export SchemaRedirect_v3
SchemaRedirect_v3 <- R6Class(
  "SchemaRedirect_v3",

  public = list(
    table_names = NULL,
    table_accesses = NULL,
    preferred_table_name = NULL,
    censors = NULL,

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

      if(sum(!names(censors) %in% name_access)>0) stop("censors are not listed in name_access")
      if(sum(!name_access %in% names(censors))>0) stop("missing censors")
      for(i in seq_along(name_access)) if(sum(!names(censors[[i]]) %in% names(field_types))>0) stop("censor[[",i,"]] has columns that are not listed in field_types")

      self$table_names <- c()
      self$table_accesses <- c()
      for(i in seq_along(name_access)){
        stopifnot(name_access[i] %in% c("restr", "anon"))
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
          indexes = indexes,
          validator_field_types = validator_field_types,
          validator_field_contents = validator_field_contents,
          info = info
        )

        config$schemas[[table_name]] <- schema
        self$table_names <- c(self$table_names, table_name)
        self$table_accesses <- c(self$table_accesses, name_access[i])

        if(name_access[i] == config$db_config_preferred) self$preferred_table_name <- schema$table_name
      }
    },
    #' @description
    #' Connect to a db
    connect = function() {
      for(i in self$table_names) config$schemas[[i]]$connect()
    },

    #' @description
    #' Disconnect from a db
    disconnect = function() {
      for(i in self$table_names) config$schemas[[i]]$disconnect()
    },

    #' @description
    #' Create db table
    create_table = function() {
      for(i in self$table_names) config$schemas[[i]]$create_table()
    },

    drop_table = function() {
      for(i in self$table_names) config$schemas[[i]]$drop_table()
    },

    #' @description
    #' Inserts data into db table
    insert_data = function(newdata, verbose = TRUE) {
      for(i in seq_along(self$table_names)){
        table_name <- self$table_names[i]
        table_access <- self$table_accesses[i]

        censored_data <- private$make_censored_data(newdata, table_access)
        config$schemas[[table_name]]$insert_data(
          newdata = censored_data,
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

        censored_data <- private$make_censored_data(newdata, table_access)
        config$schemas[[table_name]]$upsert_data(
          newdata = censored_data,
          drop_indexes = drop_indexes,
          verbose = verbose
        )
      }
    },

    drop_all_rows = function() {
      for(i in self$table_names) config$schemas[[i]]$drop_all_rows()
    },

    drop_rows_where = function(condition){
      for(i in self$table_names) config$schemas[[i]]$drop_rows_where(condition = condition)
    },

    keep_rows_where = function(condition){
      for(i in self$table_names) config$schemas[[i]]$keep_rows_where(condition = condition)
    },

    drop_all_rows_and_then_upsert_data =  function(newdata, drop_indexes = names(self$indexes), verbose = TRUE) {
      for(i in seq_along(self$table_names)){
        table_name <- self$table_names[i]
        table_access <- self$table_accesses[i]

        censored_data <- private$make_censored_data(newdata, table_access)
        config$schemas[[table_name]]$drop_all_rows_and_then_upsert_data(
          newdata = censored_data,
          drop_indexes = drop_indexes,
          verbose = verbose
        )
      }
    },

    tbl = function() {
      config$schemas[[self$preferred_table_name]]$tbl()
    },

    list_indexes_db = function(){
      config$schemas[[self$preferred_table_name]]$list_indexes_db()
    },

    add_indexes = function() {
      for(i in self$table_names) config$schemas[[i]]$add_indexes()
    },

    drop_indexes = function() {
      for(i in self$table_names) config$schemas[[i]]$drop_indexes()
    }
  ),
  private = list(
    make_censored_data = function(newdata, access){
      d <- copy(newdata)
      for(i in seq_along(self$censors[[access]])){
        self$censors[[access]][[i]](d)
      }
      return(d)
    },

    finalize = function() {
      # self$db_disconnect()
    }
  )
)
