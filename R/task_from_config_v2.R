generic_data_function_factory <- function(schema, argset, fn){
  force(schema)
  force(argset)
  force(fn)
  function() {
    schema$db_connect()
    on.exit(schema$db_disconnect())
    fn(schema, argset)
  }
}

#' Creating a task with DB as source
#'
#' This function is used to create a task when the
#' source is a database table.
#'
#' @param name Name of the task
#' @param cores Number of CPU cores
#' @param for_each_plan A list, where each unit corresponds to one data extraction. Generally recommended to use \code{plnr::expand_list}.
#' @param for_each_argset A list, where each unit corresponds to one analysis within a plan (data extraction). Generally recommended to use \code{plnr::expand_list}.
#' @param universal_argset A list, where these argsets are applied to all analyses univerally
#' @param upsert_at_end_of_each_plan Do you want to upsert your results automatically at the end of each plan?
#' @param insert_at_end_of_each_plan Do you want to insert your results automatically at the end of each plan?
#' @param action_name The name of the function that will be called for each analysis
#' @param data_output_schemas A named list that maps \code{config$schemas} for use in receiving the output
#' @param data_selector_schemas A named list that maps \code{config$schemas} for use in providing data to each plan
#' @param data_selector_fn_default A function with arguments \code{schema}, \code{argset} that is the default function to extract data for each unit in data_selector_schemas not covered by \code{data_selection_fn_specific}
#' @param data_selector_fn_specific A named list, where each unit contains a function with arguments \code{schema}, \code{argset} that is the default function to extract data for specific units in \code{data_selector_schemas}
#' @examples
#' \dontrun{
#' task_from_config_v2(
#'   # name of the task
#'   name = "ui_normomo_attrib_excel",
#'   # number of CPU cores
#'   cores = 1,
#'   # each unit of the plan corresponds to one data extraction
#'   for_each_plan = plnr::expand_list(
#'     location_code = "norge"
#'   ),
#'   for_each_argset = NULL,
#'   universal_argset = NULL,
#'   upsert_at_end_of_each_plan = FALSE,
#'   insert_at_end_of_each_plan = FALSE,
#'   action_name = "sykdomspulsen::ui_normomo_attrib_excel",
#'   data_output_schemas = NULL,
#'   # these are the database tables we are interested in
#'   data_selector_schemas = list(
#'     "results_normomo_attrib_standard" = sc::config$schemas$results_normomo_attrib_standard,
#'     "results_normomo_attrib_irr" = sc::config$schemas$results_normomo_attrib_irr
#'   ),
#'   # for each plan, this is the default for how the data will be extracted for each of the database tables listed above
#'   data_selector_fn_default = function(schema, argset){
#'     schema$dplyr_tbl() %>%
#'       mandatory_db_filter(
#'         granularity_time = "week",
#'         granularity_geo = NULL,
#'         age = "total",
#'         sex = "total"
#'       ) %>%
#'       dplyr::filter(location_code == !!argset$location_code) %>%
#'       dplyr::filter(year>=2020) %>%
#'       dplyr::collect() %>%
#'       as.data.table()
#'   },
#'   # for each plan, specific data extractor functions can be given for each database tables listed above
#'   data_selector_fn_specific = list(
#'   results_normomo_attrib_irr = function(schema, argset){
#'     schema$dplyr_tbl() %>%
#'       mandatory_db_filter(
#'         granularity_time = "season",
#'         granularity_geo = NULL,
#'         age = "total",
#'         sex = "total"
#'       ) %>%
#'       dplyr::filter(location_code == !!argset$location_code) %>%
#'       dplyr::filter(year>=2020) %>%
#'       dplyr::collect() %>%
#'       as.data.table()
#'     }
#'   )
#' )
#' }
#' @export
task_from_config_v2 <- function(
  name,
  cores = 1,
  for_each_plan,
  for_each_argset = NULL,
  universal_argset = NULL,
  upsert_at_end_of_each_plan = FALSE,
  insert_at_end_of_each_plan = FALSE,
  action_name,
  data_output_schemas = NULL,
  data_selector_schemas = NULL,
  data_selector_fn_default = NULL,
  data_selector_fn_specific = NULL
){

  index <- 1
  list_plan <- list()

  for(index_plan in seq_along(for_each_plan)){
    # create a new plan
    list_plan[[length(list_plan)+1]] <- plnr::Plan$new()

    # add data
    for(index_data in seq_along(data_selector_schemas)){
      schema_name <- names(data_selector_schemas)[[index_data]]
      schema <- data_selector_schemas[[index_data]]
      argset <- for_each_plan[[index_plan]]
      argset$today <- lubridate::today()

      if(schema_name %in% names(data_selector_fn_specific)){
        list_plan[[length(list_plan)]]$add_data(
          name = schema_name,
          fn=generic_data_function_factory(
            schema = schema,
            argset = argset,
            fn = data_selector_fn_specific[[schema_name]]
          )
        )
      } else {
        list_plan[[length(list_plan)]]$add_data(
          name = schema_name,
          fn=generic_data_function_factory(
            schema = schema,
            argset = argset,
            fn = data_selector_fn_default
          )
        )
      }
    }

    # add analyses
    if(is.null(for_each_argset)){
      for_each_argset <- list(NULL)
    }
    for(index_argset in seq_along(for_each_argset)){
      # add analysis
      argset <- c(
        for_each_plan[[index_plan]],
        for_each_argset[[index_argset]],
        universal_argset,
        index = index
      )
      argset$today <- lubridate::today()

      list_plan[[length(list_plan)]]$add_analysis_from_list(
        fn_name = action_name,
        l = list(argset)
      )

      index <- index + 1
    }
  }

  task <- Task$new(
    name = name,
    type = "analysis",
    plans = list_plan,
    schema = c(
      data_output_schemas,
      data_selector_schemas
    ),
    cores = cores,
    upsert_at_end_of_each_plan = upsert_at_end_of_each_plan,
    insert_at_end_of_each_plan = insert_at_end_of_each_plan
  )

  return(task)
}


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


