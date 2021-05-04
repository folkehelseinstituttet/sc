#' shortcut to get available task names
#' @export
tm_get_task_names <- function(){
  names(config$tasks$list_task)
}

#' Shortcut to task
#' @param task_name Name of the task
#' @param index_plan Not used
#' @param index_argset Not used
#' @export
tm_get_task <- function(task_name, index_plan = NULL, index_argset = NULL) {
  retval <- config$tasks$get_task(task_name)
  retval$update_plans()
  return(retval)
}

#' Shortcut to update plans for a task
#' @param task_name Name of the task
#' @param index_plan Not used
#' @param index_argset Not used
#' @export
tm_update_plans <- function(task_name, index_plan = NULL, index_argset = NULL) {
  task <- tm_get_task(
    task_name = task_name,
    index_plan = index_plan,
    index_argset = index_argset
  )
  task$update_plans()
}

#' Shortcut to run task
#' @param task_name Name of the task
#' @param index_plan Not used
#' @param index_argset Not used
#' @export
tm_run_task <- function(task_name, index_plan = NULL, index_argset = NULL) {
  # message(glue::glue("spulscore {utils::packageVersion('sc')}"))

  task <- tm_get_task(
    task_name = task_name,
    index_plan = index_plan,
    index_argset = index_argset
  )
  task$run(log=FALSE)
}

#' Shortcut to plan within task
#' @param task_name Name of the task
#' @param index_plan Not used
#' @param index_argset Not used
#' @export
tm_get_plans <- function(task_name, index_plan = NULL, index_argset = NULL) {
  tm_get_task(task_name = task_name)$plans
}

#' Shortcut to plan within task
#' @param task_name Name of the task
#' @param index_plan Plan within task
#' @param index_argset Not used
#' @export
tm_get_plan <- function(task_name, index_plan = 1, index_argset = NULL) {
  tm_get_task(task_name = task_name)$plans[[index_plan]]
}

#' Shortcut to data within plan within task
#' @param task_name Name of the task
#' @param index_plan Plan within task
#' @param index_argset Not used
#' @export
tm_get_data <- function(task_name, index_plan = 1, index_argset = NULL) {
  tm_get_plan(
    task_name = task_name,
    index_plan = index_plan
  )$get_data()
}

#' Shortcut to argset within plan within task
#' @param task_name Name of the task
#' @param index_plan Plan within task
#' @param index_argset Argset within plan
#' @export
tm_get_argset <- function(task_name, index_plan = 1, index_argset = 1) {
  tm_get_plan(
    task_name = task_name,
    index_plan = index_plan
  )$get_argset(index_argset)
}

#' Shortcut to schema within task
#' @param task_name Name of the task
#' @param index_plan Not used
#' @param index_argset Not used
#' @export
tm_get_schema <- function(task_name, index_plan = NULL, index_argset = NULL) {
  schema <- tm_get_task(
    task_name = task_name
  )$schema
  for(s in schema) s$connect()
  return(schema)
}

analyses_to_dt <- function(analyses){
  retval <- lapply(analyses, function(x){
    data.table(t(x$argset))
  })
  retval <- rbindlist(retval)
  retval[,index_argset := 1:.N]

  return(retval)
}

plans_to_dt <- function(plans){
  retval <- lapply(plans, function(x) analyses_to_dt(x$analyses))
  for(i in seq_along(retval)) retval[[i]][, index_plan := i]
  retval <- rbindlist(retval)
  setcolorder(retval, c("index_plan", "index_argset"))
  retval
}

#' Gets a data.table overview of index_plan and index_argset
#' @param task_name Name of the task
#' @param index_plan Not used
#' @param index_argset Not used
#' @export
tm_get_plans_argsets_as_dt <- function(task_name, index_plan = NULL, index_argset = NULL){
  p <- tm_get_plans(task_name)
  plans_to_dt(p)
}


#'
#'
#' TaskManager
#'
#' An R6 Action class contains a function called 'run' that takes three arguments:
#' - data (list)
#' - arg (list)
#' - schema (list)
#'
#' An action is (note: we dont explicitly create these):
#' - one R6 Action class
#' - A plnr::Plan that provide
#'   a) data (from the plan -- `get_data`)
#'   b) arguments (from the plan -- `get_argset`)
#' to the R6 action class
#'
#' A task is:
#' - one R6 Action class
#' - A plnr::Plan that provide
#'   a) data (from the plan -- `get_data`)
#'   b) arguments (from the plan -- `get_argset`)
#' to the R6 action class
#'
#' A TaskManager is:
#' - one R6 Action class
#' - A list of plnr::Plan's
#'
#' @import R6
#' @export TaskManager
TaskManager <- R6::R6Class(
  "TaskManager",
  portable = FALSE,
  cloneable = TRUE,
  public = list(
    list_task = list(),
    initialize = function() {
      # nothing
    },
    add_task = function(task) {
      list_task[[task$name]] <<- task
    },
    get_task = function(name) {
      list_task[[name]]
    },
    ## list_plan_get = function(task_name){
    ##   if(is.null(list_task[[task_name]]$list_plan) & is.null(list_task[[task_name]]$fn_plan)){
    ##     retval <- list(x=1)
    ##   } else if(!is.null(list_task[[task_name]]$list_plan) & is.null(list_task[[task_name]]$fn_plan)){
    ##     retval <- list_task[[task_name]]$list_plan
    ##   } else if(is.null(list_task[[task_name]]$list_plan) & !is.null(list_task[[task_name]]$fn_plan)){
    ##     retval <- list_task[[task_name]]$fn_plan()
    ##   }
    ##   return(retval)
    ## },


    run_all = function(log = TRUE){
      for(task in list_task){
        task_run(task$name, log=log)

      }

    },
    task_run = function(name, log = TRUE) {
      list_task[[name]]$run(log)
    },

    run = function() {
      stop("run must be implemented")
    }
  )
)
