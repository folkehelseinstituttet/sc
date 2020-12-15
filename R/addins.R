addin_task_from_config_v2_basic <- function(){
  rstudioapi::insertText(
    '
# TASK_NAME ----
sc::add_task(
  sc::task_from_config_v2(
    name = "TASK_NAME",
    cores = 1,
    for_each_plan = plnr::expand_list(
      x = 1
    ),
    for_each_argset = NULL,
    universal_argset = NULL,
    upsert_at_end_of_each_plan = FALSE,
    insert_at_end_of_each_plan = FALSE,
    action_name = "PACKAGE_NAME::TASK_NAME",
    data_output_schemas = NULL,
    data_selector_schemas = NULL,
    data_selector_fn_default = NULL
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
      "date" = "DATE"

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
    validator_field_contents = sc::validator_field_contents_sykdomspulsen
  )
)
'
  )
}

addin_action <- function(){
  rstudioapi::insertText(
    '
#\' TASK_NAME
#\' @param data Data
#\' @param argset Argset
#\' @param schema DB Schema
#\' @export
TASK_NAME <- function(data, argset, schema) {
  # tm_run_task("TASK_NAME")

  if(plnr::is_run_directly()){
    index_plan <- 1

    data <- sc::tm_get_data("TASK_NAME", index_plan=index_plan)
    argset <- sc::tm_get_argset("TASK_NAME", index_plan=index_plan, index_argset = 1)
    schema <- sc::tm_get_schema("TASK_NAME")
  }

  # code goes here
}
'
  )
}

