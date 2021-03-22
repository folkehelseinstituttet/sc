#' Blank field_types validator
#' @param db_field_types db_field_types passed to schema
#' @export
validator_field_types_blank <- function(db_field_types){
  return(TRUE)
}

#' Blank data validator
#' @param data data passed to schema
#' @export
validator_field_contents_blank <- function(data){
  return(TRUE)
}

#' validator_field_types_sykdomspulsen
#' An example (schema) validator of field_types used in Sykdomspulsen
#' @param db_field_types db_field_types passed to schema
#' @export
validator_field_types_sykdomspulsen <- function(db_field_types){
  if(!inherits(db_field_types,"character")) return(FALSE)
  if(!length(db_field_types) >= 12) return(FALSE)
  if(!(
    identical(
      db_field_types[1:12],
      c(
        "granularity_time" = "TEXT",
        "granularity_geo" = "TEXT",
        "location_code" = "TEXT",
        "border" = "INTEGER",
        "age" = "TEXT",
        "sex" = "TEXT",
        "year" = "INTEGER",
        "week" = "INTEGER",
        "yrwk" = "TEXT",
        "season" = "TEXT",
        "x" = "DOUBLE",
        "date" = "DATE"
      )
    ) |
    identical(
      db_field_types[1:12],
      c(
        "granularity_time" = "TEXT",
        "granularity_geo" = "TEXT",
        "location_code" = "TEXT",
        "border" = "INTEGER",
        "age" = "TEXT",
        "sex" = "TEXT",
        "isoyear" = "INTEGER",
        "isoweek" = "INTEGER",
        "isoyearweek" = "TEXT",
        "season" = "TEXT",
        "seasonweek" = "DOUBLE",
        "date" = "DATE"
      )
    )
  )) return(FALSE)

  return(TRUE)
}

#' validator_field_contents_sykdomspulsen
#' An example (schema) validator of database data used in Sykdomspulsen
#' @param data data passed to schema
#' @export
validator_field_contents_sykdomspulsen <- function(data){
  if(sum(!unique(data$granularity_time) %in% c(
    "total",
    "isoyear",
    "calyear",
    "year",
    "season",
    "month",
    "isoweek",
    "week",
    "day",
    "hour",
    "minute"
  ))>0){
    retval <- FALSE
    attr(retval, "var") <- "granularity_time"
    return(retval)
  }

  if(sum(!unique(data$granularity_geo) %in% c(
    "nation",
    "region",
    "hospitaldistrict",
    "county",
    "municip",
    "wardoslo",
    "extrawardoslo",
    "wardbergen",
    "wardtrondheim",
    "wardstavanger",
    "missingwardoslo",
    "missingwardbergen",
    "missingwardtrondheim",
    "missingwardstavanger",
    "ward",
    "station",
    "baregion",
    "missingcounty",
    "missingmunicip",
    "notmainlandcounty",
    "notmainlandmunicip"
  ))>0){
    retval <- FALSE
    attr(retval, "var") <- "granularity_geo"
    return(retval)
  }

  if(sum(!unique(data$border) %in% c(
    "2020"
  ))>0){
    retval <- FALSE
    attr(retval, "var") <- "border"
    return(retval)
  }

  if(sum(!unique(data$sex) %in% c(
    "male",
    "female",
    "total"
  ))>0){
    retval <- FALSE
    attr(retval, "var") <- "sex"
    return(retval)
  }

  if(!inherits(data$date,"Date")){
    retval <- FALSE
    attr(retval, "var") <- "date"
    return(retval)
  }

  return(TRUE)
}

#' add schema
#' @param name the name of the schema
#' @param schema a Schema R6 class
#' @export
add_schema <- function(name=NULL, schema){
  if(is.null(name)) name <- schema$db_table
  config$schemas[[schema$db_table]] <- schema
}

#' schema class description
#'
#' @import data.table
#' @import R6
#' @export Schema
Schema <- R6Class("Schema",
                  public = list(
                    dt = NULL,
                    conn = NULL,
                    db_config = NULL,
                    db_table = NULL,
                    db_field_types = NULL,
                    db_load_folder = NULL,
                    keys = NULL,
                    keys_with_length = NULL,
                    indexes = NULL,
                    validator_field_contents = NULL,
                    info = "No information given in schema definition",
                    initialize = function(
                      dt = NULL,
                      conn = NULL,
                      db_config = NULL,
                      db_table,
                      db_field_types,
                      db_load_folder,
                      keys,
                      indexes=NULL,
                      validator_field_types=validator_field_types_blank,
                      validator_field_contents=validator_field_contents_blank,
                      info = NULL
                    ) {
                      self$dt <- dt
                      self$conn <- conn
                      self$db_config <- db_config
                      self$db_table <- db_table
                      self$db_field_types <- db_field_types
                      self$db_load_folder <- db_load_folder
                      self$keys <- keys
                      self$keys_with_length <- keys
                      self$indexes <- indexes

                      # validators
                      if(!is.null(validator_field_types)) if(!validator_field_types(self$db_field_types)) stop(glue::glue("db_field_types not validated in {db_table}"))
                      self$validator_field_contents <- validator_field_contents

                      # info
                      if(!is.null(info)) self$info <- info

                      # fixing indexes
                      ind <- self$db_field_types[self$keys] == "TEXT"
                      ind_text_with_specific_length <- stringr::str_detect(self$db_field_types[self$keys], "TEXT")
                      ind_text_with_specific_length[ind] <- FALSE
                      if (sum(ind) > 0) {
                        self$keys_with_length[ind] <- paste0(self$keys_with_length[ind], " (50)")
                      }
                      if (sum(ind_text_with_specific_length) > 0) {
                        lengths <- stringr::str_extract(self$db_field_types[self$keys][ind_text_with_specific_length], "\\([0-9]*\\)")
                        #self$keys_with_length[ind_text_with_specific_length] <- paste0(self$keys_with_length[ind_text_with_specific_length], " ", lengths)
                      }
                      if (!is.null(self$conn)) self$db_create_table()
                    },
                    db_connect = function(db_config = self$db_config) {
                      self$conn <- get_db_connection(db_config=db_config)
                      use_db(self$conn, db_config$db)
                      self$db_create_table()
                    },
                    db_disconnect = function() {
                      if (!is.null(self$conn)) if(DBI::dbIsValid(self$conn)){
                        DBI::dbDisconnect(self$conn)
                      }
                    },
                    db_add_constraint = function(){
                      add_constraint(
                        conn = self$conn,
                        table = self$db_table,
                        keys = self$keys
                      )
                    },
                    db_drop_constraint = function(){
                      drop_constraint(
                        conn = self$conn,
                        table = self$db_table
                      )
                    },
                    db_create_table = function() {
                      create_tab <- TRUE
                      if (DBI::dbExistsTable(self$conn, self$db_table)) {
                        if (!self$db_check_fields_match()) {
                          message(glue::glue("Dropping table {self$db_table} because fields dont match"))
                          self$db_drop_table()
                        } else {
                          create_tab <- FALSE
                        }
                      }

                      if (create_tab) {
                        message(glue::glue("Creating table {self$db_table}"))
                        create_table(self$conn, self$db_table, self$db_field_types, self$keys)
                        self$db_add_constraint()
                      }
                    },
                    db_drop_table = function() {
                      if (DBI::dbExistsTable(self$conn, self$db_table)) {
                        DBI::dbRemoveTable(self$conn, self$db_table)
                      }
                    },
                    db_check_fields_match = function() {
                      fields <- DBI::dbListFields(self$conn, self$db_table)
                      retval <- identical(fields, names(self$db_field_types))
                      if(retval == FALSE){
                        message(glue::glue(
                          "given fields: {paste0(names(self$db_field_types),collapse=', ')}\n",
                          "db fields: {paste0(fields,collapse=', ')}"
                        ))
                      }
                      return(retval)
                    },
                    db_insert_data = function(newdata, verbose = TRUE) {
                      if(nrow(newdata)==0) return()

                      validated <- self$validator_field_contents(newdata)
                      if(!validated) stop(glue::glue("db_load_data_infile not validated in {self$db_table}. {attr(validated,'var')}"))

                      infile <- random_file(self$db_load_folder)
                      load_data_infile(
                        conn = self$conn,
                        db_config = self$db_config,
                        table = self$db_table,
                        dt = newdata,
                        file = infile
                      )
                    },
                    db_upsert_data = function(newdata, drop_indexes = names(self$indexes), verbose = TRUE) {
                      if(nrow(newdata)==0) return()

                      validated <- self$validator_field_contents(newdata)
                      if(!validated) stop(glue::glue("db_upsert_load_data_infile not validated in {self$db_table}. {attr(validated,'var')}"))

                      infile <- random_file(self$db_load_folder)
                      upsert_load_data_infile(
                        # conn = self$conn,
                        db_config = self$db_config,
                        table = self$db_table,
                        dt = newdata[, names(self$db_field_types), with = F],
                        file = infile,
                        fields = names(self$db_field_types),
                        keys = self$keys,
                        drop_indexes = drop_indexes
                      )
                    },
                    db_load_data_infile = function(newdata, verbose = TRUE){
                      .Deprecated(new="db_insert_data", old = "db_load_data_infile")
                      self$db_insert_data(newdata = newdata, verbose = verbose)
                    },
                    db_upsert_load_data_infile = function(newdata, verbose = TRUE){
                      .Deprecated(new="db_upsert_data", old = "db_upsert_load_data_infile")
                      self$db_upsert_data(newdata = newdata, verbose = verbose)
                    },
                    db_drop_all_rows = function() {
                      drop_all_rows(self$conn, self$db_table)
                    },
                    db_drop_rows_where = function(condition){
                      drop_rows_where(self$conn, self$db_table, condition)
                    },
                    db_keep_rows_where = function(condition){
                      keep_rows_where(self$conn, self$db_table, condition)
                      self$db_add_constraint()
                    },
                    db_drop_all_rows_and_then_upsert_data =  function(newdata, drop_indexes = names(self$indexes), verbose = TRUE) {
                      self$db_drop_all_rows()
                      self$db_upsert_data(
                        newdata = newdata,
                        drop_indexes = drop_indexes,
                        verbose = verbose
                      )
                    },
                    get_data = function(...) {
                      dots <- dplyr::quos(...)
                      params <- c()

                      for (i in seq_along(dots)) {
                        temp <- rlang::quo_text(dots[[i]])
                        temp <- stringr::str_extract(temp, "^[a-zA-Z0-9]+")
                        params <- c(params, temp)
                      }

                      if (length(params) > length(keys)) {
                        stop("Too many requests")
                      }
                      if (sum(!params %in% keys)) {
                        stop("names(...) not in keys")
                      }
                      if (nrow(self$dt) > 0 | ncol(self$dt) > 0) {
                        x <- self$get_data_dt(...)
                      } else {
                        x <- self$get_data_db(...)
                      }
                      return(x)
                    },
                    get_data_dt = function(...) {
                      dots <- dplyr::quos(...)
                      txt <- c()
                      for (i in seq_along(dots)) {
                        txt <- c(txt, rlang::quo_text(dots[[i]]))
                      }
                      if (length(txt) == 0) {
                        return(self$dt)
                      } else {
                        txt <- paste0(txt, collapse = "&")
                        return(self$dt[eval(parse(text = txt))])
                      }
                    },
                    get_data_db = function(...) {
                      dots <- dplyr::quos(...)
                      retval <- self$conn %>%
                        dplyr::tbl(self$db_table) %>%
                        dplyr::filter(!!!dots) %>%
                        dplyr::collect()
                      setDT(retval)
                      return(retval)
                    },
                    dplyr_tbl = function() {
                      retval <- self$conn %>%
                        dplyr::tbl(self$db_table)
                      return(retval)
                    },

                    list_indexes_db = function(){
                      list_indexes(
                        conn = self$conn,
                        table = self$db_table
                      )
                    },

                    add_indexes = function() {
                      for(i in names(self$indexes)){
                        message(glue::glue("Adding index {i}"))

                        add_index(
                          conn = self$conn,
                          table = self$db_table,
                          index = i,
                          keys = self$indexes[[i]]
                        )
                      }
                    },

                    drop_indexes = function() {
                      for(i in names(self$indexes)){
                        message(glue::glue("Dropping index {i}"))
                        drop_index(
                          conn= self$conn,
                          table = self$db_table,
                          index = i
                        )
                      }
                    },

                    identify_dt_that_exists_in_db = function() {
                      setkeyv(self$dt, self$keys)
                      from_db <- self$get_data_db()
                      setkeyv(from_db, self$keys)
                      self$dt[, exists_in_db := FALSE]
                      self$dt[from_db, exists_in_db := TRUE]
                    }
                  ),
                  private = list(
                    finalize = function() {
                      # self$db_disconnect()
                    }
                  )
)
