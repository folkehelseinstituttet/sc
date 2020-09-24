#' Shortcut to task
#' @param task_name Name of the task
#' @param index_plan Not used
#' @param index_argset Not used
#' @export
tm_shortcut_task <- function(task_name, index_plan = NULL, index_argset = NULL){
  config$tasks$task_get(task_name)
}

#' Shortcut to plan within task
#' @param task_name Name of the task
#' @param index_plan Plan within task
#' @param index_argset Not used
#' @export
tm_shortcut_plan <- function(task_name, index_plan = 1, index_argset = NULL){
  tm_shortcut_task(task_name = task_name)$list_plan[[index_plan]]
}

#' Shortcut to data within plan within task
#' @param task_name Name of the task
#' @param index_plan Plan within task
#' @param index_argset Not used
#' @export
tm_shortcut_data <- function(task_name, index_plan = 1, index_argset = NULL){
  tm_shortcut_plan(
    task_name = task_name,
    index_plan = index_plan
  )$data_get()
}

#' Shortcut to argset within plan within task
#' @param task_name Name of the task
#' @param index_plan Plan within task
#' @param index_argset Argset within plan
#' @export
tm_shortcut_argset <- function(task_name, index_plan = 1, index_argset = 1){
  tm_shortcut_plan(
    task_name = task_name,
    index_plan = index_plan
  )$argset_get(index_argset)
}

#' Shortcut to schema within task
#' @param task_name Name of the task
#' @param index_plan Not used
#' @param index_argset Not used
#' @export
tm_shortcut_schema <- function(task_name, index_plan = NULL, index_argset = NULL){
  tm_shortcut_task(
    task_name = task_name
  )$schema
}


