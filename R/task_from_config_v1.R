#' task_from_config
#' @param name Name of task
#' @param type Type of task ("data","single", "analysis", "ui")
#' @param cores Number of cores to run the task on
#' @param db_table Database table
#' @param filter Filter for the database table
#' @param for_each_plan Create a plan for each value
#' @param for_each_argset Create an argset for each value
#' @param upsert_at_end_of_each_plan If TRUE, then schema$output is used to upsert at the end of each plan
#' @param insert_at_end_of_each_plan If TRUE, then schema$output is used to insert at the end of each plan
#' @param action Text
#' @param schema List of schema mappings
#' @param args List of args
#' @export
task_from_config <- function(
  name,
  type,
  cores = 1,
  db_table=NULL,
  filter = "",
  for_each_plan=NULL,
  for_each_argset=NULL,
  upsert_at_end_of_each_plan = FALSE,
  insert_at_end_of_each_plan = FALSE,
  action,
  schema=NULL,
  args=NULL
) {

  stopifnot(type %in% c("data","single", "analysis", "ui"))
  stopifnot(cores %in% 1:parallel::detectCores())
  stopifnot(upsert_at_end_of_each_plan %in% c(T,F))
  stopifnot(insert_at_end_of_each_plan %in% c(T,F))
  if(upsert_at_end_of_each_plan & insert_at_end_of_each_plan) stop("upsert_at_end_of_each_plan & insert_at_end_of_each_plan")

  plans <- list()

  task <- NULL
  if (type %in% c("data", "single")) {
    plan <- plnr::Plan$new()
    arguments <- list(
      fn_name = action,
      name = name,
      today = Sys.Date()
    )
    if (!is.null(args)) arguments <- c(arguments, args)
    do.call(plan$add_analysis, arguments)

    task <- Task$new(
      name = name,
      type = type,
      plans = list(plan),
      schema = schema,
      cores = cores,
      upsert_at_end_of_each_plan = upsert_at_end_of_each_plan,
      insert_at_end_of_each_plan = insert_at_end_of_each_plan
    )
  } else if (type %in% c("analysis", "ui")) {
    task <- Task$new(
      name = name,
      type = type,
      plans = plans,
      schema = schema,
      cores = cores,
      upsert_at_end_of_each_plan = upsert_at_end_of_each_plan,
      insert_at_end_of_each_plan = insert_at_end_of_each_plan
    )

    task$update_plans_fn <- function() {
      table_name <- db_table
      x_plans <- list()

      filters_plan <- get_filters(
        for_each = for_each_plan,
        table_name = table_name,
        filter = filter
      )
      filters_argset <- get_filters(
        for_each = for_each_argset,
        table_name = table_name,
        filter = filter
      )

      filters_plan <- do.call(tidyr::crossing, filters_plan)
      filters_argset <- do.call(tidyr::crossing, filters_argset)

      for (i in 1:nrow(filters_plan)) {
        current_plan <- plnr::Plan$new()
        fs <- c()
        arguments <- list(
          fn_name = action, name = glue::glue("{name}_{i}"),
          source_table = table_name,
          today = Sys.Date()
        )
        for (n in names(filters_plan)) {
          arguments[n] <- filters_plan[i, n]
          fs <- c(fs, glue::glue("{n}=='{filters_plan[i,n]}'"))
        }

        filter_x <- paste(fs, collapse = " & ")
        if (filter != "") {
          filter_x <- paste(filter_x, filter, sep = " & ")
        }
        current_plan$add_data(name = "data", fn = data_function_factory(table_name, filter_x))

        if (!is.null(args)) arguments <- c(arguments, args)

        if (nrow(filters_argset) == 0) {
          do.call(current_plan$add_analysis, arguments)
        } else {
          for (j in 1:nrow(filters_argset)) {
            for (n in names(filters_argset)) {
              arguments[n] <- filters_argset[j, n]
            }
            arguments$name <- glue::glue("{arguments$name}_{j}")
            do.call(current_plan$add_analysis, arguments)
          }
        }
        x_plans[[i]] <- current_plan
      }
      return(x_plans)
    }
  }
  return(task)
}
