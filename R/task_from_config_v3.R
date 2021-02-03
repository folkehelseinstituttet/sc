generic_data_function_factory_v3 <- function(schema, argset, fn_name){
  force(schema)
  force(argset)
  force(fn_name)
  function() {
    for(i in schema) i$db_connect()
    on.exit(for(i in schema) i$db_disconnect())
    fn <- plnr::get_anything(fn_name)
    fn(argset, schema)
  }
}

#' Creating a task from config (v3)
#'
#' This function is used to easily create a task
#'
#' @param name Name of the task
#' @param cores Number of CPU cores
#' @param for_each_plan A list, where each unit corresponds to one data extraction. Generally recommended to use \code{plnr::expand_list}.
#' @param for_each_argset A list, where each unit corresponds to one analysis within a plan (data extraction). Generally recommended to use \code{plnr::expand_list}.
#' @param universal_argset A list, where these argsets are applied to all analyses univerally
#' @param upsert_at_end_of_each_plan Do you want to upsert your results automatically at the end of each plan?
#' @param insert_at_end_of_each_plan Do you want to insert your results automatically at the end of each plan?
#' @param action_fn_name The name of the function that will be called for each analysis with arguments \code{data}, \code{argset}, \code{schema}
#' @param data_selector_fn_name The name of a function that will be called to obtain the data for each analysis. The function must have the arguments \code{argset}, \code{schema} and must return a named list.
#' @param schema A named list that maps \code{sc::config$schemas} for use in \code{action_fn_name} and \code{data_selector_fn_name}
#' @param info Information for documentation
#' @export
task_from_config_v3 <- function(
  name,
  cores = 1,
  for_each_plan,
  for_each_argset = NULL,
  universal_argset = NULL,
  upsert_at_end_of_each_plan = FALSE,
  insert_at_end_of_each_plan = FALSE,
  action_fn_name,
  data_selector_fn_name = NULL,
  schema = NULL,
  info = NULL
){

  index <- 1
  list_plan <- list()

  for(index_plan in seq_along(for_each_plan)){
    # create a new plan
    list_plan[[length(list_plan)+1]] <- plnr::Plan$new()

    # add data
    argset <- c(
      "**universal**"="*",
      universal_argset,
      "**plan**"="*",
      for_each_plan[[index_plan]],
      "**automatic**"="*",
      index = index
    )
    argset$today <- lubridate::today()

    if(!is.null(data_selector_fn_name)){
      list_plan[[length(list_plan)]]$add_data(
        name = "data__________go_up_one_level",
        fn=generic_data_function_factory_v3(
          schema = schema,
          argset = argset,
          fn_name = data_selector_fn_name
        )
      )
    }

    # add analyses
    if(is.null(for_each_argset)){
      for_each_argset <- list(NULL)
    }
    for(index_argset in seq_along(for_each_argset)){
      # add analysis
      argset <- c(
        "**universal**"="*",
        universal_argset,
        "**plan**"="*",
        for_each_plan[[index_plan]],
        "**argset**"="*",
        for_each_argset[[index_argset]],
        "**automatic**"="*",
        index = index
      )
      argset$today <- lubridate::today()

      list_plan[[length(list_plan)]]$add_analysis_from_list(
        fn_name = action_fn_name,
        l = list(argset)
      )

      index <- index + 1
    }
  }

  task <- sc::Task$new(
    name = name,
    type = "analysis",
    plans = list_plan,
    schema = schema,
    cores = cores,
    upsert_at_end_of_each_plan = upsert_at_end_of_each_plan,
    insert_at_end_of_each_plan = insert_at_end_of_each_plan,
    info = info
  )

  return(task)
}


