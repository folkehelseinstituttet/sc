# 2 cores, x=1:80, 1800 MB, 51s
# 2 cores, x=1:40, 630 MB, 24s
# sc:::set_test_task()
# sc::tm_run_task("sc_test")
# Rscript -e '.libPaths("~/R"); sc:::set_test_task(); sc::tm_run_task("sc_test")'
set_test_task <- function(){
  # tm_run_task("sc_test")
  add_task_from_config_v8(
    name_grouping = "sc_test",
    name_action = NULL,
    name_variant = NULL,
    cores = 2,
    plan_analysis_fn_name = NULL, # "PACKAGE::TASK_NAME_plan_analysis"
    for_each_plan = plnr::expand_list(
      x = 1:80
    ),
    for_each_analysis = NULL,
    universal_argset = NULL,
    upsert_at_end_of_each_plan = FALSE,
    insert_at_end_of_each_plan = FALSE,
    action_fn_name = "sc::sc_test_action",
    data_selector_fn_name = "sc::sc_test_data_selector",
    schema = list(
    ),
    info = "This task does..."
  )

  p <- plnr::Plan$new(use_foreach=T)
  for(i in 1:1){
    p$add_analysis(fn = function(data, argset, schema){Sys.sleep(1)})
  }
  sc::add_task(
    sc::Task$new(
      name = "sc_test_2",
      type = "analysis",
      plans = list(p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p),
      schema = c("output" = sc::config$schemas$results_normomo_standard),
      cores = 2
    )
  )

}

# **** action **** ----
#' sc_test_action (action)
#' @param data Data
#' @param argset Argset
#' @param schema DB Schema
#' @export
sc_test_action <- function(data, argset, schema) {
  # tm_run_task("sc_test")

  if(plnr::is_run_directly()){
    # sc::tm_get_plans_argsets_as_dt("sc_test")

    index_plan <- 1
    index_analysis <- 1

    data <- sc::tm_get_data("sc_test", index_plan = index_plan)
    argset <- sc::tm_get_argset("sc_test", index_plan = index_plan, index_analysis = index_analysis)
    schema <- sc::tm_get_schema("sc_test")
  }

  # code goes here
  # special case that runs before everything
  if(argset$first_argset == TRUE){

  }

  if(argset$index == 5) stop("this is my error")

  Sys.sleep(1)

  # put data in db table
  # sc::fill_in_missing_v8(d, border = config$border)
  # schema$SCHEMA_NAME$insert_data(d)
  # schema$SCHEMA_NAME$upsert_data(d)
  # schema$SCHEMA_NAME$drop_all_rows_and_then_upsert_data(d)

  # special case that runs after everything
  # copy to anon_web?
  if(argset$last_argset == TRUE){
    # sc::copy_into_new_table_where(
    #   table_from = "anon_X",
    #   table_to = "anon_webkht"
    # )
  }

  return(data)
}

# **** data_selector **** ----
#' sc_test (data selector)
#' @param argset Argset
#' @param schema DB Schema
#' @export
sc_test_data_selector = function(argset, schema){
  if(plnr::is_run_directly()){
    # sc::tm_get_plans_argsets_as_dt("sc_test")

    index_plan <- 1

    argset <- sc::tm_get_argset("sc_test", index_plan = index_plan)
    schema <- sc::tm_get_schema("sc_test")
  }

  # The database schemas can be accessed here
  # d <- schema$SCHEMA_NAME$tbl() %>%
  #   mandatory_db_filter(
  #     granularity_time = NULL,
  #     granularity_time_not = NULL,
  #     granularity_geo = NULL,
  #     granularity_geo_not = NULL,
  #     country_iso3 = NULL,
  #     location_code = NULL,
  #     age = NULL,
  #     age_not = NULL,
  #     sex = NULL,
  #     sex_not = NULL
  #   ) %>%
  #   dplyr::select(
  #     granularity_time,
  #     granularity_geo,
  #     country_iso3,
  #     location_code,
  #     border,
  #     age,
  #     sex,
  #
  #     date,
  #
  #     isoyear,
  #     isoweek,
  #     isoyearweek,
  #     season,
  #     seasonweek,
  #
  #     calyear,
  #     calmonth,
  #     calyearmonth
  #   )
  #   dplyr::collect() %>%
  #   as.data.table() %>%
  #   setorder(
  #     location_code,
  #     date
  #   )

  # The variable returned must be a named list
  retval <- list(
    "NAME" = 1
  )
  retval
}

# **** plan_analysis **** ----
#' sc_test (plan/analysis)
#' This function can be deleted if you are not using "plan_analysis_fn_name"
#' inside sc::task_from_config_v3
#' @param argset argset
#' @param schema schema
#' @export
sc_test_plan_analysis <- function(argset, schema) {
  if(plnr::is_run_directly()){
    argset <- sc::tm_get_argset("sc_test")
    schema <- sc::tm_get_schema("sc_test")
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




