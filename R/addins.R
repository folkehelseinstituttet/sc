addin_load_production <- function(){
  rstudioapi::insertText(
    'rstudioapi::restartSession("Sys.setenv(SYKDOMSPULSEN_PRODUCTION=1);devtools::load_all(\\".\\");Sys.setenv(SYKDOMSPULSEN_PRODUCTION=0)")'
  )
}

addin_task_inline_v1_copy_to_db <- function(){
  rstudioapi::insertText(
    '
# TASK_NAME ----
sc::add_task(
  sc::task_inline_v1(
    name = "TASK_NAME",
    action_fn = function(data, argset, schema){
      sc::copy_into_new_table_where(
        table_from = "TABLE",
        table_to = "web_TABLE",
        condition = "1=1"
      )
    }
  )
)
'
  )
}

addin_add_task_from_config_v8_basic <- function(){
  rstudioapi::insertText(
    '
# TASK_NAME ----
# tm_run_task("TASK_NAME")
sc::add_task_from_config_v8(
  name_grouping = "TASK_GROUPING",
  name_action = "TASK_ACTION",
  name_variant = "TASK_VARIANT",
  cores = 1,
  permission = NULL,
  plan_analysis_fn_name = NULL, # "PACKAGE::TASK_NAME_plan_analysis"
  for_each_plan = plnr::expand_list(
    x = 1
  ),
  for_each_analysis = NULL,
  universal_argset = NULL,
  upsert_at_end_of_each_plan = FALSE,
  insert_at_end_of_each_plan = FALSE,
  action_fn_name = "PACKAGE::TASK_NAME_action",
  data_selector_fn_name = "PACKAGE::TASK_NAME_data_selector",
  schema = list(
    # input
    "SCHEMA_NAME_1" = sc::config$schemas$SCHEMA_NAME_1,

    # output
    "SCHEMA_NAME_2" = sc::config$schemas$SCHEMA_NAME_2
  ),
  info = "This task does..."
)
'
  )
}

addin_db_schema_v8_anon <- function(){
  rstudioapi::insertText(
    '
# anon_GROUPING_VARIANT ----
sc::add_schema_v8(
  name_access = c("anon"),
  name_grouping = NULL,
  name_variant = NULL,
  db_configs = sc::config$db_configs,
  field_types =  c(
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
    "season" = "TEXT",
    "seasonweek" = "DOUBLE",

    "calyear" = "INTEGER",
    "calmonth" = "INTEGER",
    "calyearmonth" = "TEXT",

    "XXXX_n" = "INTEGER",
    "XXXX_pr" = "DOUBLE"
  ),
  keys = c(
    "granularity_time",
    "location_code",
    "date",
    "age",
    "sex"
  ),
  censors = list(
    anon = list(
      XXXX_n = sc::censor_function_factory_values_0_4(column_name_to_be_censored = "XXXX_n", column_name_value = "XXXX_n"),
      XXXX_pr = sc::censor_function_factory_values_0_4(column_name_to_be_censored = "XXXX_pr", column_name_value = "XXXX_n")
    )
  ),
  validator_field_types = sc::validator_field_types_sykdomspulsen,
  validator_field_contents = sc::validator_field_contents_sykdomspulsen,
  info = "This db table is used for..."
)
'
  )
}

addin_db_schema_v8_restr_anon <- function(){
  rstudioapi::insertText(
    '
# redirect_GROUPING_VARIANT ----
# restr_GROUPING_VARIANT ----
# anon_GROUPING_VARIANT ----
sc::add_schema_v8(
  name_access = c("restr", "anon"),
  name_grouping = NULL,
  name_variant = NULL,
  db_configs = sc::config$db_configs,
  field_types =  c(
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
    "season" = "TEXT",
    "seasonweek" = "DOUBLE",

    "calyear" = "INTEGER",
    "calmonth" = "INTEGER",
    "calyearmonth" = "TEXT",

    "XXXX_n" = "INTEGER",
    "XXXX_pr" = "DOUBLE"
  ),
  keys = c(
    "granularity_time",
    "location_code",
    "date",
    "age",
    "sex"
  ),
  censors = list(
    restr = list(
      XXXX_n = sc::censor_function_factory_nothing(column_name_to_be_censored = "XXXX_n"),
      XXXX_pr = sc::censor_function_factory_nothing(column_name_to_be_censored = "XXXX_pr")
    ),
    anon = list(
      XXXX_n = sc::censor_function_factory_values_0_4(column_name_to_be_censored = "XXXX_n", column_name_value = "XXXX_n"),
      XXXX_pr = sc::censor_function_factory_values_0_4(column_name_to_be_censored = "XXXX_pr", column_name_value = "XXXX_n")
    )
  ),
  validator_field_types = sc::validator_field_types_sykdomspulsen,
  validator_field_contents = sc::validator_field_contents_sykdomspulsen,
  info = "This db table is used for..."
)
'
  )
}

addin_action_and_data_selector <- function(){
  rstudioapi::insertText(
    '
# **** action **** ----
#\' TASK_NAME (action)
#\' @param data Data
#\' @param argset Argset
#\' @param schema DB Schema
#\' @export
TASK_NAME_action <- function(data, argset, schema) {
  # tm_run_task("TASK_NAME")

  if(plnr::is_run_directly()){
    # sc::tm_get_plans_argsets_as_dt("TASK_NAME")

    index_plan <- 1
    index_analysis <- 1

    data <- sc::tm_get_data("TASK_NAME", index_plan = index_plan)
    argset <- sc::tm_get_argset("TASK_NAME", index_plan = index_plan, index_analysis = index_analysis)
    schema <- sc::tm_get_schema("TASK_NAME")
  }

  # code goes here
  # special case that runs before everything
  if(argset$first_analysis == TRUE){

  }

  # put data in db table
  # sc::fill_in_missing_v8(d, border = config$border)
  # schema$SCHEMA_NAME$insert_data(d)
  # schema$SCHEMA_NAME$upsert_data(d)
  # schema$SCHEMA_NAME$drop_all_rows_and_then_insert_data(d)

  # special case that runs after everything
  # copy to anon_web?
  if(argset$last_analysis == TRUE){
    # sc::copy_into_new_table_where(
    #   table_from = "anon_X",
    #   table_to = "anon_webkht"
    # )
  }
}

# **** data_selector **** ----
#\' TASK_NAME (data selector)
#\' @param argset Argset
#\' @param schema DB Schema
#\' @export
TASK_NAME_data_selector = function(argset, schema){
  if(plnr::is_run_directly()){
    # sc::tm_get_plans_argsets_as_dt("TASK_NAME")

    index_plan <- 1

    argset <- sc::tm_get_argset("TASK_NAME", index_plan = index_plan)
    schema <- sc::tm_get_schema("TASK_NAME")
  }

  # The database schemas can be accessed here
  d <- schema$SCHEMA_NAME$tbl() %>%
    sc::mandatory_db_filter(
      granularity_time = NULL,
      granularity_time_not = NULL,
      granularity_geo = NULL,
      granularity_geo_not = NULL,
      country_iso3 = NULL,
      location_code = NULL,
      age = NULL,
      age_not = NULL,
      sex = NULL,
      sex_not = NULL
    ) %>%
    dplyr::select(
      granularity_time,
      granularity_geo,
      country_iso3,
      location_code,
      border,
      age,
      sex,

      date,

      isoyear,
      isoweek,
      isoyearweek,
      season,
      seasonweek,

      calyear,
      calmonth,
      calyearmonth
    ) %>%
    dplyr::collect() %>%
    as.data.table() %>%
    setorder(
      location_code,
      date
    )

  # The variable returned must be a named list
  retval <- list(
    "NAME" = d
  )
  retval
}

# **** plan_analysis **** ----
#\' TASK_NAME (plan/argset)
#\' This function can be deleted if you are not using "plan_analysis_fn_name"
#\' inside sc::task_from_config_v3
#\' @export
TASK_NAME_plan_analysis <- function(argset, schema) {
  if(plnr::is_run_directly()){
    argset <- sc::tm_get_argset("TASK_NAME")
    schema <- sc::tm_get_schema("TASK_NAME")
  }

  # code goes here
  for_each_plan <- plnr::expand_list(
    x = 1
  )

  for_each_analysis <- NULL

  retval <- list(
    for_each_plan = for_each_plan,
    for_each_analysis = for_each_analysis
  )
}

# **** functions **** ----




'
  )
}

