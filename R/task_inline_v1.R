#' Creating an inline task
#'
#' This function is used to create a task when
#' you need to do a very simple task, very quickly
#'
#' @param name Name of the task
#' @param action_fn A function with arguments \code{schema}, \code{argset} that is the default function to extract data for each unit in data_selector_schemas not covered by \code{data_selection_fn_specific}
#' @examples
#' \dontrun{
#' task_inline_v1(
#'   # name of the task
#'   name = "ui_normomo_attrib_excel",
#'   # inline function that will run
#'   action_fn = function(data, argset, schema){
#'     print(1)
#'   }
#' )
#' }
#' @export
task_inline_v1 <- function(
  name,
  action_fn = function(data, argset, schema){
    print(1)
  }
){
  index <- 1
  list_plan <- list()
  list_plan[[1]] <- plnr::Plan$new()

  list_plan[[1]]$add_analysis_from_list(
    fn = action_fn,
    l = list(list(today = lubridate::today()))
  )

  task <- Task$new(
    name = name,
    type = "analysis",
    plans = list_plan,
    schema = NULL,
    cores = 1,
    upsert_at_end_of_each_plan = FALSE,
    insert_at_end_of_each_plan = FALSE
  )

  return(task)
}



