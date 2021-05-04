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
    # identical(
    #   db_field_types[1:16],
    #   c(
    #     "granularity_time" = "TEXT",
    #     "granularity_geo" = "TEXT",
    #     "country_iso3" = "TEXT",
    #     "location_code" = "TEXT",
    #     "border" = "INTEGER",
    #     "age" = "TEXT",
    #     "sex" = "TEXT",
    #
    #     "date" = "DATE",
    #
    #     "isoyear" = "INTEGER",
    #     "isoweek" = "INTEGER",
    #     "isoyearweek" = "TEXT",
    #     "season" = "TEXT",
    #     "seasonweek" = "DOUBLE",
    #
    #     "calyear" = "INTEGER",
    #     "calmonth" = "INTEGER",
    #     "calyearmonth" = "TEXT"
    #   )
    # ) |
    identical(
      db_field_types[1:12],
      c(
        "granularity_time" = "TEXT",
        "granularity_geo" = "TEXT",
        "country_iso3" = "TEXT",
        "location_code" = "TEXT",
        "border" = "INTEGER",
        "age" = "TEXT",
        "sex" = "TEXT",

        "date" = "DATE",

        "isoyear" = "INTEGER",
        "isoweek" = "INTEGER",
        "isoyearweek" = "TEXT",
        "season" = "TEXT"
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
        "year" = "INTEGER",
        "week" = "INTEGER",
        "yrwk" = "TEXT",
        "season" = "TEXT",
        "x" = "DOUBLE",
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

# schema_v8 ----
#' schema class description
#'
#' @import data.table
#' @import R6
#' @export Schema_v8
Schema_v8 <- R6Class(
  "Schema_v8",

  # public ----
  public = list(
    #' @field conn db connection
    conn = NULL,
    db_config = NULL,
    table_name = NULL,
    field_types = NULL,
    field_types_with_length = NULL,
    keys = NULL,
    keys_with_length = NULL,
    indexes = NULL,
    validator_field_contents = NULL,
    info = "No information given in schema definition",
    load_folder = tempdir(),

    #' @description
    #' Create a new Schema_v3 object.
    #' @param conn Name.
    #' @param db_config Hair color.
    #' @return A new `Schema_v3` object.
    initialize = function(
      conn = NULL,
      db_config = NULL,
      table_name,
      field_types,
      keys,
      indexes=NULL,
      validator_field_types=validator_field_types_blank,
      validator_field_contents=validator_field_contents_blank,
      info = NULL
    ) {

      force(conn)
      self$conn <- conn

      force(db_config)
      self$db_config <- db_config

      force(table_name)
      self$table_name <- table_name

      force(field_types)
      self$field_types <- field_types
      self$field_types_with_length <- field_types

      force(keys)
      self$keys <- keys
      self$keys_with_length <- keys

      force(indexes)
      self$indexes <- indexes

      # validators
      if(!is.null(validator_field_types)) if(!validator_field_types(self$field_types)) stop(glue::glue("field_types not validated in {table_name}"))
      self$validator_field_contents <- validator_field_contents

      # info
      if(!is.null(info)) self$info <- info

      # db_field_types_with_lengths
      ind <- self$field_types == "TEXT"
      ind_text_with_specific_length <- stringr::str_detect(self$field_types, "TEXT")
      ind_text_with_specific_length[ind] <- FALSE
      if (sum(ind) > 0) {
        self$field_types_with_length[ind] <- paste0(self$field_types_with_length[ind], " (100)")
      }
      if (sum(ind_text_with_specific_length) > 0) {
        lengths <- stringr::str_extract(self$field_types[ind_text_with_specific_length], "\\([0-9]*\\)")
        self$field_types_with_length[ind_text_with_specific_length] <- paste0(self$field_types_with_length[ind_text_with_specific_length], " ", lengths)
      }

      # remove numbers from field_types
      naming <- names(self$field_types)
      self$field_types <- stringr::str_remove(self$field_types, " \\([0-9]*\\)")
      names(self$field_types) <- naming
      # fixing indexes
      self$keys_with_length <- self$field_types_with_length[self$keys]
      if (!is.null(self$conn)) self$create_table()
    },

    #' @description
    #' Connect to a db
    #' @param db_config db_config
    connect = function(db_config = self$db_config) {
      self$conn <- get_db_connection(db_config = db_config)
      use_db(self$conn, db_config$db)
      self$create_table()
    },

    #' @description
    #' Disconnect from a db
    disconnect = function() {
      if (!is.null(self$conn)) if(DBI::dbIsValid(self$conn)){
        DBI::dbDisconnect(self$conn)
      }
    },

    #' @description
    #' Create db table
    create_table = function() {
      create_tab <- TRUE
      if (DBI::dbExistsTable(self$conn, self$table_name)) {
        if (!private$check_fields_match()) {
          message(glue::glue("Dropping table {self$table_name} because fields dont match"))
          self$drop_table()
        } else {
          create_tab <- FALSE
        }
      }
      if (create_tab) {
        message(glue::glue("Creating table {self$table_name}"))
        create_table(self$conn, self$table_name, self$field_types, self$keys)
        private$add_constraint()
      }
    },

    drop_table = function() {
      if (DBI::dbExistsTable(self$conn, self$table_name)) {
        message(glue::glue("Dropping table {self$table_name}"))
        DBI::dbRemoveTable(self$conn, self$table_name)
      }
    },

    #' @description
    #' Inserts data into db table
    insert_data = function(newdata, verbose = TRUE) {
      if(nrow(newdata)==0) return()

      validated <- self$validator_field_contents(newdata)
      if(!validated) stop(glue::glue("load_data_infile not validated in {self$table_name}. {attr(validated,'var')}"))

      infile <- random_file(self$load_folder)
      load_data_infile(
        conn = self$conn,
        db_config = self$db_config,
        table = self$table_name,
        dt = newdata,
        file = infile
      )
    },

    #' @description
    #' Upserts data into db table
    upsert_data = function(newdata, drop_indexes = names(self$indexes), verbose = TRUE) {
      if(nrow(newdata)==0) return()

      validated <- self$validator_field_contents(newdata)
      if(!validated) stop(glue::glue("upsert_load_data_infile not validated in {self$table_name}. {attr(validated,'var')}"))

      infile <- random_file(self$load_folder)
      upsert_load_data_infile(
        # conn = self$conn,
        db_config = self$db_config,
        table = self$table_name,
        dt = newdata[, names(self$field_types), with = F],
        file = infile,
        fields = names(self$field_types),
        keys = self$keys,
        drop_indexes = drop_indexes
      )
    },

    drop_all_rows = function() {
      drop_all_rows(self$conn, self$table_name)
    },

    drop_rows_where = function(condition){
      drop_rows_where(self$conn, self$table_name, condition)
    },

    keep_rows_where = function(condition){
      keep_rows_where(self$conn, self$table_name, condition)
      private$add_constraint()
    },

    drop_all_rows_and_then_upsert_data =  function(newdata, drop_indexes = names(self$indexes), verbose = TRUE) {
      self$drop_all_rows()
      self$upsert_data(
        newdata = newdata,
        drop_indexes = drop_indexes,
        verbose = verbose
      )
    },

    tbl = function() {
      retval <- self$conn %>%
        dplyr::tbl(self$table_name)
      return(retval)
    },

    list_indexes_db = function(){
      list_indexes(
        conn = self$conn,
        table = self$table_name
      )
    },

    add_indexes = function() {
      for(i in names(self$indexes)){
        message(glue::glue("Adding index {i}"))

        add_index(
          conn = self$conn,
          table = self$table_name,
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
          table = self$table_name,
          index = i
        )
      }
    },

    # to be deleted soon ----
    db_connect = function(db_config = self$db_config) {
      .Deprecated("connect")
      self$connect(db_config)
    },
    db_disconnect = function() {
      .Deprecated("disconnect")
      self$disconnect()
    }
  ),

  # private ----
  private = list(
    check_fields_match = function() {
      fields <- DBI::dbListFields(self$conn, self$table_name)
      retval <- identical(fields, names(self$field_types))
      if(retval == FALSE){
        message(glue::glue(
          "given fields: {paste0(names(self$field_types),collapse=', ')}\n",
          "db fields: {paste0(fields,collapse=', ')}"
        ))
      }
      return(retval)
    },

    #' @description
    #' Add constraint to a db table
    add_constraint = function(){
      add_constraint(
        conn = self$conn,
        table = self$table_name,
        keys = self$keys
      )
    },

    #' @description
    #' Drop constraint from a db table
    drop_constraint = function(){
      drop_constraint(
        conn = self$conn,
        table = self$table_name
      )
    },

    finalize = function() {
      # self$db_disconnect()
    }
  )
)


# schema ----
#' schema class description
#'
#' @import data.table
#' @import R6
#' @export Schema
Schema <- R6Class(
  "Schema",
  public = list(
    dt = NULL,
    conn = NULL,
    db_config = NULL,
    db_table = NULL,
    db_field_types = NULL,
    db_field_types_with_length = NULL,
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
      self$db_field_types_with_length <- db_field_types
      self$db_load_folder <- db_load_folder
      self$keys <- keys
      self$keys_with_length <- keys
      self$indexes <- indexes

      # validators
      if(!is.null(validator_field_types)) if(!validator_field_types(self$db_field_types)) stop(glue::glue("db_field_types not validated in {db_table}"))
      self$validator_field_contents <- validator_field_contents

      # info
      if(!is.null(info)) self$info <- info

      # db_field_types_with_lengths
      ind <- self$db_field_types == "TEXT"
      ind_text_with_specific_length <- stringr::str_detect(self$db_field_types, "TEXT")
      ind_text_with_specific_length[ind] <- FALSE
      if (sum(ind) > 0) {
        self$db_field_types_with_length[ind] <- paste0(self$db_field_types_with_length[ind], " (100)")
      }
      if (sum(ind_text_with_specific_length) > 0) {
        lengths <- stringr::str_extract(self$db_field_types[ind_text_with_specific_length], "\\([0-9]*\\)")
        self$db_field_types_with_length[ind_text_with_specific_length] <- paste0(self$db_field_types_with_length[ind_text_with_specific_length], " ", lengths)
      }

      # remove numbers from db_field_types
      naming <- names(self$db_field_types)
      self$db_field_types <- stringr::str_remove(self$db_field_types, " \\([0-9]*\\)")
      names(self$db_field_types) <- naming
      # fixing indexes
      self$keys_with_length <- self$db_field_types_with_length[self$keys]
      if (!is.null(self$conn)) self$db_create_table()
    },







    #' @description
    #' Connect to a db
    #' @param db_config db_config
    connect = function(db_config = self$db_config) {
      self$db_connect(db_config)
    },

    #' @description
    #' Disconnect from a db
    disconnect = function() {
      self$db_disconnect()
    },

    #' @description
    #' Create db table
    create_table = function() {
      self$db_create_table()
    },

    drop_table = function() {
      self$db_drop_table()
    },

    #' @description
    #' Inserts data into db table
    insert_data = function(newdata, verbose = TRUE) {
      self$db_insert_data(newdata, verbose)
    },

    #' @description
    #' Upserts data into db table
    upsert_data = function(newdata, drop_indexes = names(self$indexes), verbose = TRUE) {
      self$db_upsert_data(newdata, drop_indexes, verbose)
    },

    drop_all_rows = function() {
      self$db_drop_all_rows()
    },

    drop_rows_where = function(condition){
      self$db_drop_rows_where(condition)
    },

    keep_rows_where = function(condition){
      self$db_keep_rows_where(condition)
    },

    drop_all_rows_and_then_upsert_data =  function(newdata, drop_indexes = names(self$indexes), verbose = TRUE) {
      self$db_drop_all_rows_and_then_upsert_data(newdata, drop_indexes, verbose)
    },

    tbl = function() {
      self$dplyr_tbl()
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
