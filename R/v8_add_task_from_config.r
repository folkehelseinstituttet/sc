generic_data_function_factory_v8 <- function(schema, argset, fn_name){
  force(schema)
  force(argset)
  force(fn_name)
  function() {
    for(i in schema) i$connect()
    on.exit(for(i in schema) i$disconnect())
    fn <- plnr::get_anything(fn_name)
    fn(argset, schema)
  }
}

generic_list_plan_function_factory_v8 <- function(
  universal_argset,
  schema,
  plan_argset_fn_name,
  action_fn_name,
  data_selector_fn_name
){

  force(universal_argset)
  force(schema)
  force(plan_argset_fn_name)
  force(action_fn_name)
  force(data_selector_fn_name)

  function() {
    for(i in schema) i$connect()
    on.exit(for(i in schema) i$disconnect())

    fn <- plnr::get_anything(plan_argset_fn_name)
    plan_argset <- fn(universal_argset, schema)
    list_plan <- task_from_config_v8_list_plan(
      for_each_plan = plan_argset$for_each_plan,
      for_each_argset = plan_argset$for_each_argset,
      universal_argset = universal_argset,
      action_fn_name = action_fn_name,
      data_selector_fn_name = data_selector_fn_name,
      schema = schema
    )
    return(list_plan)
  }
}

task_from_config_v8_list_plan <- function(
  for_each_plan,
  for_each_argset = NULL,
  universal_argset = NULL,
  action_fn_name,
  data_selector_fn_name,
  schema
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
    argset$yesterday <- lubridate::today() - 1

    if(!is.null(data_selector_fn_name)){
      list_plan[[length(list_plan)]]$add_data(
        name = "data__________go_up_one_level",
        fn=generic_data_function_factory_v8(
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
      argset$yesterday <- lubridate::today() - 1

      list_plan[[length(list_plan)]]$add_analysis_from_list(
        fn_name = action_fn_name,
        l = list(argset)
      )

      index <- index + 1
    }
  }

  return(list_plan)
}


task_from_config_v8 <- function(
  name = NULL,
  name_grouping = NULL,
  name_action= NULL,
  name_variant = NULL,
  cores = 1,
  plan_argset_fn_name = NULL,
  for_each_plan = NULL,
  for_each_argset = NULL,
  universal_argset = NULL,
  upsert_at_end_of_each_plan = FALSE,
  insert_at_end_of_each_plan = FALSE,
  action_fn_name,
  data_selector_fn_name = NULL,
  schema = NULL,
  info = NULL
){

  if(is.null(for_each_plan) & is.null(plan_argset_fn_name)) stop("You must provide at least one of for_each_plan or plan_argset_fn_name")
  stopifnot(!(is.null(name) & is.null(name_grouping) & is.null(name_action) & is.null(name_variant)))
  if(is.null(name_grouping) & is.null(name_action) & is.null(name_variant)){
    name_description <- NULL
  } else {
    name_description <- list(
      grouping = name_grouping,
      action = name_action,
      variant = name_variant
    )
  }

  if(!is.null(for_each_plan)){
    stopifnot(is.list(for_each_plan))
    list_plan <- task_from_config_v8_list_plan(
      for_each_plan = for_each_plan,
      for_each_argset = for_each_argset,
      universal_argset = universal_argset,
      action_fn_name = action_fn_name,
      data_selector_fn_name = data_selector_fn_name,
      schema = schema
    )
    update_plans_fn <- NULL
  } else if(!is.null(plan_argset_fn_name)){
    list_plan <- NULL
    update_plans_fn <- generic_list_plan_function_factory_v8(
      universal_argset = universal_argset,
      schema = schema,
      plan_argset_fn_name = plan_argset_fn_name,
      action_fn_name = action_fn_name,
      data_selector_fn_name = data_selector_fn_name
    )
  }

  task <- sc::Task$new(
    name = name,
    name_description = name_description,
    type = "analysis",
    plans = list_plan,
    update_plans_fn = update_plans_fn,
    schema = schema,
    cores = cores,
    upsert_at_end_of_each_plan = upsert_at_end_of_each_plan,
    insert_at_end_of_each_plan = insert_at_end_of_each_plan,
    info = info,
    info_plan_argset_fn_name = plan_argset_fn_name,
    info_action_fn_name = action_fn_name,
    info_data_selector_fn_name = data_selector_fn_name
  )

  return(task)
}

#' add_task_from_config_v8
#' @param name Name of the task (dont use)
#' @param name_grouping Name of the task (grouping)
#' @param name_action Name of the task (action)
#' @param name_variant Name of the task (variant)
#' @param cores Number of CPU cores
#' @param plan_argset_fn_name The name of a function that returns a named list \code{list(for_each_plan = list(), for_each_argset = NULL)}.
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
add_task_from_config_v8 <- function(
  name = NULL,
  name_grouping = NULL,
  name_action= NULL,
  name_variant = NULL,
  cores = 1,
  plan_argset_fn_name = NULL,
  for_each_plan = NULL,
  for_each_argset = NULL,
  universal_argset = NULL,
  upsert_at_end_of_each_plan = FALSE,
  insert_at_end_of_each_plan = FALSE,
  action_fn_name,
  data_selector_fn_name = NULL,
  schema = NULL,
  info = NULL
){
  force(name)
  force(name_grouping)
  force(name_action)
  force(name_variant)
  force(cores)
  force(plan_argset_fn_name)
  force(for_each_plan)
  force(for_each_argset)
  force(universal_argset)
  force(upsert_at_end_of_each_plan)
  force(insert_at_end_of_each_plan)
  force(action_fn_name)
  force(data_selector_fn_name)
  force(schema)
  force(info)

  add_task(
    task_from_config_v8(
      name = name,
      name_grouping = name_grouping,
      name_action = name_action,
      name_variant = name_variant,
      cores = cores,
      plan_argset_fn_name = plan_argset_fn_name,
      for_each_plan = for_each_plan,
      for_each_argset = for_each_argset,
      universal_argset = universal_argset,
      upsert_at_end_of_each_plan = upsert_at_end_of_each_plan,
      insert_at_end_of_each_plan = insert_at_end_of_each_plan,
      action_fn_name = action_fn_name,
      data_selector_fn_name = data_selector_fn_name,
      schema = schema,
      info = info
    )
  )
}
