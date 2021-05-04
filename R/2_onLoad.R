.onLoad <- function(libname, pkgname) {
  config$tasks <- TaskManager$new()

  check_env_vars()

  set_db()
  set_computer_type()
  set_progressr()
  set_path()

  invisible()
}

check_env_vars <- function(){
  needed <- c(
    "SYKDOMSPULSEN_DB_DRIVER",
    "SYKDOMSPULSEN_DB_SERVER",
    "SYKDOMSPULSEN_DB_PORT",
    "SYKDOMSPULSEN_DB_USER",
    "SYKDOMSPULSEN_DB_PASSWORD",
    "SYKDOMSPULSEN_DB_DB",
    "SYKDOMSPULSEN_DB_TRUSTED_CONNECTION",

    "SYKDOMSPULSEN_PRODUCTION",

    "SYKDOMSPULSEN_PATH_INPUT",
    "SYKDOMSPULSEN_PATH_OUTPUT"
  )

  for(i in needed){
    getval <- Sys.getenv(i)
    if(getval==""){
      packageStartupMessage(crayon::red(glue::glue("{i}=''")))
    } else {
      if(stringr::str_detect(i,"PASSWORD")) getval <- "*****"
      packageStartupMessage(crayon::blue(glue::glue("{i}='{getval}'")))
    }
  }
  packageStartupMessage(glue::glue("spulscore: {utils::packageVersion('sc')}"))

}

set_db <- function(){
  config$db_config <- list(
    driver = Sys.getenv("SYKDOMSPULSEN_DB_DRIVER", "MySQL"),
    server = Sys.getenv("SYKDOMSPULSEN_DB_SERVER", "db"),
    port = as.integer(Sys.getenv("SYKDOMSPULSEN_DB_PORT", 1433)),
    user = Sys.getenv("SYKDOMSPULSEN_DB_USER", "root"),
    password = Sys.getenv("SYKDOMSPULSEN_DB_PASSWORD", "example"),
    db = Sys.getenv("SYKDOMSPULSEN_DB_DB", "sykdomspuls"),
    trusted_connection = Sys.getenv("SYKDOMSPULSEN_DB_TRUSTED_CONNECTION")
  )

  config$db_configs <- list(
    "restr" = config$db_config,
    "anon" = config$db_config,
    "config" = config$db_config
  )
  config$db_config_preferred <- "restr"

  # config_last_updated ----
  add_schema(
    schema = Schema$new(
      db_config = config$db_config,
      db_table = "config_last_updated",
      db_field_types = c(
        "type" = "TEXT",
        "tag" = "TEXT",
        "date" = "DATE",
        "datetime" = "DATETIME"
      ),
      db_load_folder = tempdir(),
      keys = c(
        "type",
        "tag"
      )
    )
  )

  # config_structure_time ----
  add_schema(
    schema = Schema$new(
      db_config = config$db_config,
      db_table = "config_structure_time",
      db_field_types = c(
        "granularity_time" = "TEXT",
        "isoyear" = "TEXT",
        "isoyearweek" = "TEXT",
        "date" = "DATE"
      ),
      db_load_folder = tempdir(),
      keys = c(
        "type",
        "tag"
      )
    )
  )

  # rundate ----
  add_schema(
    name = "rundate",
    schema = Schema$new(
      db_config = config$db_config,
      db_table = "rundate",
      db_field_types = c(
        "task" = "TEXT",
        "date" = "DATE",
        "datetime" = "DATETIME"
      ),
      db_load_folder = tempdir(),
      keys = c(
        "task"
      )
    )
  )

  # config_datetime ----
  add_schema(
    name = "config_datetime",
    schema = Schema$new(
      db_config = config$db_config,
      db_table = "config_datetime",
      db_field_types = c(
        "type" = "TEXT",
        "tag" = "TEXT",
        "date" = "DATE",
        "datetime" = "DATETIME"
      ),
      db_load_folder = tempdir(),
      keys = c(
        "type",
        "tag"
      )
    )
  )
}

set_computer_type <- function() {
  if (Sys.getenv("SYKDOMSPULSEN_PRODUCTION") == "1") {
    config$is_production <- TRUE
  }
}

set_progressr <- function(){
  options("progressr.enable" = TRUE)
  progressr::handlers(
    progressr::handler_progress(
      format = "[:bar] :current/:total (:percent) in :elapsedfull, eta: :eta\n",
      clear = FALSE
    )
  )
}

set_path <- function(){
  config$path_input <- Sys.getenv("SYKDOMSPULSEN_PATH_INPUT")
  config$path_output <- Sys.getenv("SYKDOMSPULSEN_PATH_OUTPUT")
}
