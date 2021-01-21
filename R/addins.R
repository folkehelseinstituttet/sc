addin_task_from_config_v3_basic <- function(){
  rstudioapi::insertText(
    '
sc::add_task(
  task_from_config_v3(
    name = "TASK_NAME",
    cores = 1,
    for_each_plan = plnr::expand_list(
      x = 1
    ),
    for_each_argset = NULL,
    universal_argset = NULL,
    upsert_at_end_of_each_plan = FALSE,
    insert_at_end_of_each_plan = FALSE,
    action_fn_name = "PACKAGE::TASK_NAME_action",
    data_selector_fn_name = "PACKAGE::TASK_NAME_data_selector",
    schema = list(
      "SCHEMA_NAME" = sc::config$schemas$SCHEMA_NAME
    ),
    info = "This task does..."
  )
)
'
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

addin_db_schema <- function(){
  rstudioapi::insertText(
    '
# data_XXXXXX ----
sc::add_schema(
  name = "data_XXXXXX",
  schema = sc::Schema$new(
    db_table = "data_XXXXXX",
    db_config = sc::config$db_config,
    db_field_types =  c(
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
      "date" = "DATE",

      "XXXX" = "DOUBLE"
    ),
    db_load_folder = tempdir(),
    keys =  c(
      "granularity_time",
      "location_code",
      "date",
      "age",
      "sex",
      "date"
    ),
    validator_field_types = sc::validator_field_types_sykdomspulsen,
    validator_field_contents = sc::validator_field_contents_sykdomspulsen,
    info = "This db table is used for..."
  )
)
'
  )
}

addin_action_and_data_selector <- function(){
  rstudioapi::insertText(
    '
#\' TASK_NAME (action)
#\' @param data Data
#\' @param argset Argset
#\' @param schema DB Schema
#\' @export
TASK_NAME_action <- function(data, argset, schema) {
  # tm_run_task("TASK_NAME")

  if(plnr::is_run_directly()){
    index_plan <- 1
    index_argset <- 1

    data <- sc::tm_get_data("TASK_NAME", index_plan = index_plan)
    argset <- sc::tm_get_argset("TASK_NAME", index_plan = index_plan, index_argset = index_argset)
    schema <- sc::tm_get_schema("TASK_NAME")
  }

  # code goes here
}

#\' TASK_NAME (data selector)
#\' @param argset Argset
#\' @param schema DB Schema
#\' @export
TASK_NAME_data_selector = function(argset, schema){
  if(plnr::is_run_directly()){
    # inside here is just for testing/development
    argset <- sc::tm_get_argset("TASK_NAME", index_plan=1)
    schema <- sc::tm_get_schema("TASK_NAME", index_plan=1)
  }

  # The database schemas can be accessed here
  d <- schema$SCHEMA_NAME$dplyr_tbl() %>%
    dplyr::collect() %>%
    as.data.table()

  # The variable returned must be a named list
  retval <- list(
    "NAME" = d
  )
  retval
}
'
  )
}

