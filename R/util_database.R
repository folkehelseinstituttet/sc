is_mssql <- function(conn){
  return(conn@info$dbms.name=="Microsoft SQL Server")
}

#' use_db
#' @param conn a
#' @param db a
#' @export
use_db <- function(conn, db) {
  tryCatch({
    a <- DBI::dbExecute(conn, glue::glue({
      "USE {db};"
    }))
  }, error = function(e){

    a <- DBI::dbExecute(conn, glue::glue({
      "CREATE DATABASE {db};"
    }))
    a <- DBI::dbExecute(conn, glue::glue({
      "USE {db};"
    }))
  })
}

set_db_recovery_simple <- function(conn = get_db_connection(), db = config$db_config$db, i_am_sure_i_want_to_do_this = FALSE) {
  stopifnot(i_am_sure_i_want_to_do_this==T)

  a <- DBI::dbExecute(conn, glue::glue({
    "ALTER DATABASE  {db} SET RECOVERY SIMPLE;"
  }))
}

#' get_field_types
#' @param conn a
#' @param dt a
get_field_types <- function(conn, dt) {
  field_types <- vapply(dt, DBI::dbDataType,
    dbObj = conn,
    FUN.VALUE = character(1)
  )
  return(field_types)
}

random_uuid <- function() {
  x <- uuid::UUIDgenerate(F)
  x <- gsub("-", "", x)
  x <- paste0("a", x)
  x
}

random_file <- function(folder, extension = ".csv") {
  fs::path(folder, paste0(random_uuid(), extension))
}

write_data_infile <- function(
  dt,
  file = paste0(tempfile(),".csv"),
  colnames=T,
  eol="\n",
  quote = "auto",
  na = "\\N",
  sep=","
  ) {
  # infinites and NANs get written as text
  # which destroys the upload
  # we need to set them to NA
  for(i in names(dt)){
    dt[is.infinite(get(i)), (i) := NA]
    dt[is.nan(get(i)), (i) := NA]
  }
  fwrite(dt,
    file = file,
    logical01 = T,
    na = na,
    col.names=colnames,
    eol=eol,
    quote = quote,
    sep = sep
  )
}

load_data_infile <- function(
  conn,
  db_config,
  table,
  dt,
  file
) UseMethod("load_data_infile")

load_data_infile.default <- function(conn = NULL, db_config = NULL, table, dt = NULL, file = "/xtmp/x123.csv") {
  if(is.null(dt)) return()

  t0 <- Sys.time()


  if (is.null(conn) & is.null(db_config)) {
    stop("conn and db_config both have error")
  } else if (is.null(conn) & !is.null(db_config)) {
    conn <- get_db_connection(db_config = db_config)
    use_db(conn, db_config$db)
    on.exit(DBI::dbDisconnect(conn))
  }

  correct_order <- DBI::dbListFields(conn, table)
  if(length(correct_order)>0) dt <- dt[,correct_order,with=F]
  write_data_infile(dt = dt, file = file)
  on.exit(fs::file_delete(file), add = T)

  sep <- ","
  eol <- "\n"
  quote <- '"'
  skip <- 0
  header <- T
  path <- normalizePath(file, winslash = "/", mustWork = TRUE)

  sql <- paste0(
    "LOAD DATA INFILE ", DBI::dbQuoteString(conn, path), "\n",
    "INTO TABLE ", DBI::dbQuoteIdentifier(conn, table), "\n",
    "CHARACTER SET utf8", "\n",
    "FIELDS TERMINATED BY ", DBI::dbQuoteString(conn, sep), "\n",
    "OPTIONALLY ENCLOSED BY ", DBI::dbQuoteString(conn, quote), "\n",
    "LINES TERMINATED BY ", DBI::dbQuoteString(conn, eol), "\n",
    "IGNORE ", skip + as.integer(header), " LINES \n",
    "(", paste0(correct_order, collapse = ","), ")"
  )
  DBI::dbExecute(conn, sql)

  t1 <- Sys.time()
  dif <- round(as.numeric(difftime(t1, t0, units = "secs")), 1)
  if(config$verbose) message(glue::glue("Uploaded {nrow(dt)} rows in {dif} seconds to {table}"))

  invisible()
}

`load_data_infile.Microsoft SQL Server` <- function(
  conn = NULL,
  db_config = NULL,
  table,
  dt,
  file = tempfile()
  ) {
  if(is.null(dt)) return()
  if(is.null(conn)){
    conn <- get_db_connection()
    on.exit(DBI::dbDisconnect(conn))
  } else if(!DBI::dbIsValid(conn)){
    conn <- get_db_connection()
    on.exit(DBI::dbDisconnect(conn))
  }

  a <- Sys.time()

  if (is.null(conn) & is.null(db_config)) {
    stop("conn and db_config both have error")
  } else if (is.null(conn) & !is.null(db_config)) {
    conn <- get_db_connection(db_config = db_config)
    use_db(conn, db_config$db)
    on.exit(DBI::dbDisconnect(conn))
  }

  correct_order <- DBI::dbListFields(conn, table)
  if(length(correct_order)>0) dt <- dt[,correct_order,with=F]
  write_data_infile(
    dt = dt,
    file = file,
    colnames=F,
    eol = "\n",
    quote = FALSE,
    na="",
    sep="\t")
  on.exit(fs::file_delete(file), add = T)

  format_file <- tempfile()
  on.exit(fs::file_delete(format_file), add = T)

  args <- c(
    table,
    "format" ,
    "nul",
    "-q",
    "-c",
    #"-t,",
    "-f",
    format_file,
    "-S",
    db_config$server,
    "-d",
    db_config$db,
    "-U",
    db_config$user,
    "-P",
    db_config$password
  )
  if(db_config$trusted_connection=="yes"){
    args <- c(args,"-T")
  }
  system2(
    "bcp",
    args=args,
    stdout=NULL
  )

  args <- c(
    table,
    "in" ,
    file,
    "-S",
    db_config$server,
    "-d",
    db_config$db,
    "-U",
    db_config$user,
    "-P",
    db_config$password,
    "-f",
    format_file
  )
  if(db_config$trusted_connection=="yes"){
    args <- c(args,"-T")
  }
  system2(
    "bcp",
    args=args,
    stdout=NULL
  )

  b <- Sys.time()
  dif <- round(as.numeric(difftime(b, a, units = "secs")), 1)
  if(config$verbose) message(glue::glue("Uploaded {nrow(dt)} rows in {dif} seconds to {table}"))

  update_config_datetime(type = "data", tag = table)

  invisible()
}

######### upsert_load_data_infile

upsert_load_data_infile <- function(
  conn = NULL,
  db_config,
  table,
  dt,
  file,
  fields,
  keys,
  drop_indexes
){
  if (is.null(conn) & is.null(db_config)) {
    stop("conn and db_config both have error")
  } else if (is.null(conn) & !is.null(db_config)) {
    conn <- get_db_connection(db_config = db_config)
    use_db(conn, db_config$db)
    on.exit(DBI::dbDisconnect(conn))
  }

  upsert_load_data_infile_internal(
    conn = conn,
    db_config = db_config,
    table = table,
    dt = dt,
    file = file,
    fields = fields,
    keys = keys,
    drop_indexes = drop_indexes
  )

}

upsert_load_data_infile_internal <- function(
  conn,
  db_config,
  table,
  dt,
  file,
  fields,
  keys,
  drop_indexes
  ) UseMethod("upsert_load_data_infile_internal")

upsert_load_data_infile_internal.default <- function(
  conn = NULL,
  db_config = NULL,
  table,
  dt,
  file = "/tmp/x123.csv",
  fields,
  keys = NULL,
  drop_indexes = NULL
) {
  temp_name <- random_uuid()
  # ensure that the table is removed **FIRST** (before deleting the connection)
  on.exit(DBI::dbRemoveTable(conn, temp_name), add = TRUE, after = FALSE)

  sql <- glue::glue("CREATE TEMPORARY TABLE {temp_name} LIKE {table};")
  DBI::dbExecute(conn, sql)

  # TO SPEED UP EFFICIENCY DROP ALL INDEXES HERE
  if (!is.null(drop_indexes)) {
    for (i in drop_indexes) {
      try(
        DBI::dbExecute(
          conn,
          glue::glue("ALTER TABLE `{temp_name}` DROP INDEX `{i}`")
        ),
        TRUE
      )
    }
  }

  load_data_infile(
    conn = conn,
    db_config = db_config,
    table = temp_name,
    dt = dt,
    file = file
    )

  t0 <- Sys.time()

  vals_fields <- glue::glue_collapse(fields, sep = ", ")
  vals <- glue::glue("{fields} = VALUES({fields})")
  vals <- glue::glue_collapse(vals, sep = ", ")

  sql <- glue::glue("
    INSERT INTO {table} SELECT {vals_fields} FROM {temp_name}
    ON DUPLICATE KEY UPDATE {vals};
    ")
  DBI::dbExecute(conn, sql)

  t1 <- Sys.time()
  dif <- round(as.numeric(difftime(t1, t0, units = "secs")), 1)
  if(config$verbose) message(glue::glue("Upserted {nrow(dt)} rows in {dif} seconds from {temp_name} to {table}"))

  invisible()
}

`upsert_load_data_infile_internal.Microsoft SQL Server` <- function(
  conn = NULL,
  db_config = NULL,
  table,
  dt,
  file = tempfile(),
  fields,
  keys,
  drop_indexes = NULL
) {
  # conn <- schema$output$conn
  # db_config <- config$db_config
  # table <- schema$output$db_table
  # dt <- data_clean
  # file <- tempfile()
  # fields <- schema$output$db_fields
  # keys <- schema$output$keys
  # drop_indexes <- NULL
  if(is.null(conn)){
    conn <- get_db_connection()
    on.exit(DBI::dbDisconnect(conn))
  } else if(!DBI::dbIsValid(conn)){
    conn <- get_db_connection()
    on.exit(DBI::dbDisconnect(conn))
  }

  temp_name <- paste0("tmp",random_uuid())

  # ensure that the table is removed **FIRST** (before deleting the connection)
  on.exit(DBI::dbRemoveTable(conn, temp_name), add = TRUE, after = FALSE)

  sql <- glue::glue("SELECT * INTO {temp_name} FROM {table} WHERE 1 = 0;")
  DBI::dbExecute(conn, sql)

  load_data_infile(
    conn = conn,
    db_config = db_config,
    table = temp_name,
    dt = dt,
    file = file
    )

  a <- Sys.time()
  add_index(
    conn = conn,
    table = temp_name,
    keys = keys
  )

  vals_fields <- glue::glue_collapse(fields, sep = ", ")
  vals <- glue::glue("{fields} = VALUES({fields})")
  vals <- glue::glue_collapse(vals, sep = ", ")

  sql_on_keys <- glue::glue("{t} = {s}", t=paste0("t.",keys), s=paste0("s.",keys))
  sql_on_keys <- paste0(sql_on_keys, collapse = " and ")

  sql_update_set <- glue::glue("{t} = {s}", t=paste0("t.",fields), s=paste0("s.",fields))
  sql_update_set <- paste0(sql_update_set, collapse = ", ")

  sql_insert_fields <- paste0(fields, collapse=", ")
  sql_insert_s_fields <- paste0(paste0("s.",fields), collapse=", ")

  sql <- glue::glue("
  MERGE {table} t
  USING {temp_name} s
  ON ({sql_on_keys})
  WHEN MATCHED
  THEN UPDATE SET
    {sql_update_set}
  WHEN NOT MATCHED BY TARGET
  THEN INSERT ({sql_insert_fields})
    VALUES ({sql_insert_s_fields});
  ")

  DBI::dbExecute(conn, sql)

  b <- Sys.time()
  dif <- round(as.numeric(difftime(b, a, units = "secs")), 1)
  if(config$verbose) message(glue::glue("Upserted {nrow(dt)} rows in {dif} seconds from {temp_name} to {table}"))

  update_config_datetime(type = "data", tag = table)
  invisible()
}

######### create_table
create_table <- function(conn, table, fields, keys) UseMethod("create_table")

create_table.default <- function(conn, table, fields, keys=NULL) {
  fields_new <- fields
  fields_new[fields == "TEXT"] <- "TEXT CHARACTER SET utf8 COLLATE utf8_unicode_ci"

  sql <- DBI::sqlCreateTable(conn, table, fields_new,
                             row.names = F, temporary = F
  )
  DBI::dbExecute(conn, sql)
}

`create_table.Microsoft SQL Server` <- function(conn, table, fields, keys=NULL) {
  fields_new <- fields
  fields_new[fields == "TEXT"] <- "NVARCHAR (50)"
  fields_new[fields == "DOUBLE"] <- "FLOAT"
  fields_new[fields == "BOOLEAN"] <- "BIT"

  if(!is.null(keys)) fields_new[names(fields_new) %in% keys] <- paste0(fields_new[names(fields_new) %in% keys], " NOT NULL")
  sql <- DBI::sqlCreateTable(conn, table, fields_new,
                             row.names = F, temporary = F
  )
  DBI::dbExecute(conn, sql)
}

######### add_constraint
add_constraint <- function(conn, table, keys) UseMethod("add_constraint")

add_constraint.default <- function(conn, table, keys) {
  t0 <- Sys.time()

  primary_keys <- glue::glue_collapse(keys, sep = ", ")
  constraint <- glue::glue("PK_{table}")
  sql <- glue::glue("
          ALTER table {table}
          ADD CONSTRAINT {constraint} PRIMARY KEY CLUSTERED ({primary_keys});")
  #print(sql)
  a <- DBI::dbExecute(conn, sql)
  # DBI::dbExecute(conn, "SHOW INDEX FROM x");
  t1 <- Sys.time()
  dif <- round(as.numeric(difftime(t1, t0, units = "secs")), 1)
  if(config$verbose) message(glue::glue("Added constraint {constraint} in {dif} seconds to {table}"))
}

######### drop_constraint
drop_constraint <- function(conn, table) UseMethod("drop_constraint")

drop_constraint.default <- function(conn, table) {
  constraint <- glue::glue("PK_{table}")
  sql <- glue::glue("
          ALTER table {table}
          DROP CONSTRAINT {constraint};")
  #print(sql)
  try(a <- DBI::dbExecute(conn, sql),TRUE)
}

drop_index <- function(conn, table, index) UseMethod("drop_index")

drop_index.default <- function(conn, table, index){
  try(
    DBI::dbExecute(
      conn,
      glue::glue("ALTER TABLE `{table}` DROP INDEX `{index}`")
    ),
    TRUE
  )
}

`drop_index.Microsoft SQL Server` <- function(conn, table, index){
  try(
    DBI::dbExecute(
      conn,
      glue::glue("DROP INDEX {table}.{index}")
    ),
    TRUE
  )
}

add_index <- function(conn, table, index, keys) UseMethod("add_index")

add_index.default <- function(conn, table, keys, index) {
  keys <- glue::glue_collapse(keys, sep = ", ")

  sql <- glue::glue("
    ALTER TABLE `{table}` ADD INDEX `{index}` ({keys})
    ;")
  #print(sql)
  try(a <- DBI::dbExecute(conn, sql),T)
}

`add_index.Microsoft SQL Server` <- function(conn, table, keys, index){
  keys <- glue::glue_collapse(keys, sep = ", ")

  try(
    DBI::dbExecute(
      conn,
      glue::glue("CREATE INDEX {index} ON {table} ({keys});")
    ),
    T
  )
}

#' copy_into_new_table_where
#' Keeps the rows where the condition is met
#' @param conn A db connection
#' @param table_from Table name
#' @param table_to Table name
#' @param condition A string SQL condition
#' @export
copy_into_new_table_where <- function(
  conn=NULL,
  table_from,
  table_to,
  condition = "1=1"
  ) {
  if(is.null(conn)){
    conn <- get_db_connection()
    on.exit(DBI::dbDisconnect(conn))
  } else if(!DBI::dbIsValid(conn)){
    conn <- get_db_connection()
    on.exit(DBI::dbDisconnect(conn))
  }
  t0 <- Sys.time()
  temp_name <- paste0("tmp",random_uuid())

  sql <- glue::glue("SELECT * INTO {temp_name} FROM {table_from} WHERE {condition}")
  DBI::dbExecute(conn, sql)

  try(DBI::dbRemoveTable(conn, name = table_to), TRUE)

  sql <- glue::glue("EXEC sp_rename '{temp_name}', '{table_to}'")
  DBI::dbExecute(conn, sql)
  t1 <- Sys.time()
  dif <- round(as.numeric(difftime(t1, t0, units = "secs")), 1)
  if(config$verbose) message(glue::glue("Copied rows in {dif} seconds from {table_from} to {table_to}"))

  update_config_datetime(type = "data", tag = table_to)
}

#' drop_all_rows
#' Drops all rows
#' @param conn A db connection
#' @param table Table name
#' @export
drop_all_rows <- function(conn=NULL, table) {
  if(is.null(conn)){
    conn <- get_db_connection()
    on.exit(DBI::dbDisconnect(conn))
  } else if(!DBI::dbIsValid(conn)){
    conn <- get_db_connection()
    on.exit(DBI::dbDisconnect(conn))
  }
  a <- DBI::dbExecute(conn, glue::glue({
    "TRUNCATE TABLE {table};"
  }))

  update_config_datetime(type = "data", tag = table)
}

#' keep_rows_where
#' Keeps the rows where the condition is met
#' @param conn A db connection
#' @param table Table name
#' @param condition A string SQL condition
#' @export
keep_rows_where <- function(conn=NULL, table, condition) {
  if(is.null(conn)){
    conn <- get_db_connection()
    on.exit(DBI::dbDisconnect(conn))
  } else if(!DBI::dbIsValid(conn)){
    conn <- get_db_connection()
    on.exit(DBI::dbDisconnect(conn))
  }
  t0 <- Sys.time()
  temp_name <- paste0("tmp",random_uuid())

  sql <- glue::glue("SELECT * INTO {temp_name} FROM {table} WHERE {condition}")
  DBI::dbExecute(conn, sql)

  DBI::dbRemoveTable(conn, name = table)

  sql <- glue::glue("EXEC sp_rename '{temp_name}', '{table}'")
  DBI::dbExecute(conn, sql)
  t1 <- Sys.time()
  dif <- round(as.numeric(difftime(t1, t0, units = "secs")), 1)
  if(config$verbose) message(glue::glue("Kept rows in {dif} seconds from {table}"))

  update_config_datetime(type = "data", tag = table)
}

#' drop_rows_where
#' Drops the rows where the condition is met
#' @param conn A db connection
#' @param table Table name
#' @param condition A string SQL condition
#' @export
drop_rows_where <- function(conn=NULL, table, condition) {
  if(is.null(conn)){
    conn <- get_db_connection()
    on.exit(DBI::dbDisconnect(conn))
  }
  t0 <- Sys.time()
  a <- DBI::dbExecute(conn, glue::glue({
    "DELETE FROM {table} WHERE {condition};"
  }))
  t1 <- Sys.time()
  dif <- round(as.numeric(difftime(t1, t0, units = "secs")), 1)
  if(config$verbose) message(glue::glue("Deleted rows in {dif} seconds from {table}"))

  update_config_datetime(type = "data", tag = table)
}


#' get_db_connection
#' @param driver driver
#' @param server server
#' @param port port
#' @param user user
#' @param password password
#' @param db database
#' @param trusted_connection trusted connection yes/no
#' @param db_config A list containing driver, server, port, user, password
#' @export get_db_connection
get_db_connection <- function(
                              driver = NULL,
                              server = NULL,
                              port = NULL,
                              user = NULL,
                              password = NULL,
                              db = NULL,
                              trusted_connection = NULL,
                              db_config = config$db_config
                              ) {

  if(!is.null(db_config) & is.null(driver)){
    driver <- db_config$driver
  }
  if(!is.null(db_config) & is.null(server)){
    server <- db_config$server
  }
  if(!is.null(db_config) & is.null(port)){
    port <- db_config$port
  }
  if(!is.null(db_config) & is.null(user)){
    user <- db_config$user
  }
  if(!is.null(db_config) & is.null(password)){
    password <- db_config$password
  }
  if(!is.null(db_config) & is.null(db)){
    db <- db_config$db
  }

  if(!is.null(db_config) & is.null(trusted_connection)){
    trusted_connection <- db_config$trusted_connection
  }

  use_trusted <- FALSE
  if(!is.null(trusted_connection)) if(trusted_connection=="yes") use_trusted <- TRUE

  if(use_trusted & driver %in% c("ODBC Driver 17 for SQL Server")){
    conn <- DBI::dbConnect(
        odbc::odbc(),
        driver = driver,
        server = server,
        port = port,
        trusted_connection = "yes"
      )
  } else if(driver %in% c("ODBC Driver 17 for SQL Server")){
    conn <- DBI::dbConnect(
      odbc::odbc(),
      driver = driver,
      server = server,
      port = port,
      uid = user,
      pwd = password,
      encoding = "utf8"
    )
  } else {
    conn <- DBI::dbConnect(
      odbc::odbc(),
      driver = driver,
      server = server,
      port = port,
      user = user,
      password = password,
      encoding = "utf8"
    )
  }
  if(!is.null(db)) use_db(conn, db)
  return(conn)
}

#' tbl
#' @param table table
#' @param db db
#' @export
tbl <- function(table, db = config$db_config$db) {
  if (is.null(connections[[db]])) {
    connections[[db]] <- get_db_connection()
    use_db(connections[[db]], db)
  }
  return(dplyr::tbl(connections[[db]], table))
}

#' list_indexes
#' @param table tbl
#' @param conn conn
#' @export
list_indexes <- function(table, conn=NULL){
  if(is.null(conn)){
    db <- config$db_config$db
    if (is.null(connections[[db]])) {
      connections[[db]] <- get_db_connection(db = db)
      use_db(connections[[db]], db)
      conn <- connections
    }
  }
  retval <- DBI::dbGetQuery(
    conn,
    glue::glue("select * from sys.indexes where object_id = (select object_id from sys.objects where name = '{table}')")
  )
  return(retval)
}

#' list_tables
#' @param db db
#' @export
list_tables <- function(db = config$db_config$db) {
  if (is.null(connections[[db]])) {
    connections[[db]] <- get_db_connection(db = db)
    use_db(connections[[db]], db)
  }
  retval <- DBI::dbListTables(connections[[db]])
  last_val <- which(retval == "trace_xe_action_map") - 1
  retval <- retval[1:last_val]

  # remove airflow tables
  if (db == "Sykdomspulsen_surv") {
    retval <- retval[
      which(!retval %in% c(
        "alembic_version",
        "chart",
        "connection",
        "dag",
        "dag_pickle",
        "dag_run",
        "dag_tag",
        "import_error",
        "job",
        "known_event",
        "known_event_type",
        "kube_resource_version",
        "kube_worker_uuid",
        "log",
        "serialized_dag",
        "sla_miss",
        "slot_pool",
        "task_fail",
        "task_instance",
        "task_reschedule",
        "users",
        "variable",
        "xcom"
      ))
      ]
  }
  return(retval)
}


#' drop_table
#' @param table table
#' @param db db
#' @export
drop_table <- function(table, db = config$db_config$db) {
  if (is.null(connections[[db]])) {
    connections[[db]] <- get_db_connection()
    use_db(connections[[db]], db)
  }
  return(try(DBI::dbRemoveTable(connections[[db]], name = table), TRUE))
}


