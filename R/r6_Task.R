#' add task
#' @param task a Task R6 class
#' @export
add_task <- function(task){
  config$tasks$add_task(task)
}

#' task_from_config
#' @param name Name of task
#' @param type Type of task ("data","single", "analysis", "ui")
#' @param cores Number of cores to run the task on
#' @param db_table Database table
#' @param filter Filter for the database table
#' @param for_each_plan Create a plan for each value
#' @param for_each_argset Create an argset for each value
#' @param upsert_at_end_of_each_plan If TRUE, then schema$output is used to upsert at the end of each plan
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
  action,
  schema=NULL,
  args=NULL
) {

  stopifnot(type %in% c("data","single", "analysis", "ui"))
  stopifnot(cores %in% 1:parallel::detectCores())
  stopifnot(upsert_at_end_of_each_plan %in% c(T,F))
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
      upsert_at_end_of_each_plan = upsert_at_end_of_each_plan
    )
  } else if (type %in% c("analysis", "ui")) {
    task <- Task$new(
      name = name,
      type = type,
      plans = plans,
      schema = schema,
      cores = cores,
      upsert_at_end_of_each_plan = upsert_at_end_of_each_plan
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

#' Task
#'
#' @import R6
#' @import foreach
#' @export
Task <- R6::R6Class(
  "Task",
  portable = FALSE,
  cloneable = TRUE,
  public = list(
    type = NULL,
    permission = NULL,
    plans = list(),
    schema = list(),
    cores = 1,
    upsert_at_end_of_each_plan = FALSE,
    name = NULL,
    update_plans_fn = NULL,
    initialize = function(
                              name,
                              type,
                              permission = NULL,
                              plans = NULL,
                              update_plans_fn = NULL,
                              schema,
                              cores = 1,
                              upsert_at_end_of_each_plan = FALSE) {
      self$name <- name
      self$type <- type
      self$permission <- permission
      self$plans <- plans
      self$update_plans_fn <- update_plans_fn
      self$schema <- schema
      self$cores <- cores
      self$upsert_at_end_of_each_plan <- upsert_at_end_of_each_plan
    },
    update_plans = function() {
      if (!is.null(self$update_plans_fn)) {
        message(glue::glue("Updating plans..."))
        self$plans <- self$update_plans_fn()
      }
    },
    num_argsets = function() {
      retval <- 0
      for (i in seq_along(plans)) {
        retval <- retval + plans[[i]]$len()
      }
      return(retval)
    },
    run = function(log = TRUE, cores = self$cores) {
      # task <- tm_get_task("analysis_norsyss_qp_gastro")

      message(glue::glue("task: {self$name}"))
      if(!is.null(self$permission)) if(!self$permission$has_permission()) return(NULL)

      upsert_at_end_of_each_plan <- self$upsert_at_end_of_each_plan

      self$update_plans()

      message(glue::glue("Running task={self$name} with plans={length(self$plans)} and argsets={self$num_argsets()}"))

      if (cores != 1) {
        doFuture::registerDoFuture()

        if (length(self$plans) == 1) {
          # parallelize the inner loop
          future::plan(list(
            future::sequential,
            future::multisession,
            workers = cores,
            earlySignal = TRUE
          ))

          parallel <- "plans=sequential, argset=multisession"
        } else {
          # parallelize the outer loop
          future::plan(future::multisession, workers = cores)

          parallel <- "plans=multisession, argset=sequential"
        }
      } else {
        data.table::setDTthreads()

        parallel <- "plans=sequential, argset=sequential"
      }

      message(glue::glue("{parallel} with cores={cores}"))

      if (cores == 1) {
        # not running in parallel
        pb <- progress::progress_bar$new(
          format = "[:bar] :current/:total (:percent) in :elapsedfull, eta: :eta",
          clear = FALSE,
          total = self$num_argsets()
        )
        for (s in schema) s$db_connect()
        for (x in self$plans) {
          x$set_progress(pb)
          retval <- x$run_all(schema = schema)
          if (upsert_at_end_of_each_plan) {
            retval <- rbindlist(retval)
            schema$output$db_upsert_load_data_infile(retval, verbose = F)
          }
          rm("retval")
        }
        for (s in schema) s$db_disconnect()
      } else {
        # running in parallel
        message("\n***** REMEMBER TO INSTALL SYKDOMSPULSEN *****")
        message("***** OR ELSE THE PARALLEL PROCESSES WON'T HAVE ACCESS *****")
        message("***** TO THE NECESSARY FUNCTIONS *****\n")

        progressr::with_progress({
          pb <- progressr::progressor(steps = self$num_argsets())
          y <- foreach(x = self$plans) %dopar% {
            data.table::setDTthreads(1)

            for (s in schema) s$db_connect()
            x$set_progressor(pb)
            retval <- x$run_all(schema = schema)
            if (upsert_at_end_of_each_plan) {
              retval <- rbindlist(retval)
              schema$output$db_upsert_load_data_infile(retval, verbose = F)
            }
            rm("retval")
            for (s in schema) s$db_disconnect()

            # ***************************** #
            # NEVER DELETE gc()             #
            # IT CAUSES 2x SPEEDUP          #
            # AND 10x MEMORY EFFICIENCY     #
            gc() #
            # ***************************** #
            1
          }
        },
        delay_stdout = FALSE,
        delay_conditions = ""
        )
      }

      future::plan(future::sequential)
      foreach::registerDoSEQ()
      data.table::setDTthreads()

      update_rundate(task = self$name)
      if(!is.null(self$permission)) self$permission$revoke_permission()

    }
  )
)

data_function_factory <- function(table_name, filter) {
  force(table_name)
  force(filter)
  function() {
    if (is.na(filter)) {
      d <- tbl(table_name) %>%
        dplyr::collect() %>%
        latin1_to_utf8()
    } else {
      d <- tbl(table_name) %>%
        dplyr::filter(!!!rlang::parse_exprs(filter)) %>%
        dplyr::collect() %>%
        latin1_to_utf8()
    }
  }
}

get_filters <- function(for_each, table_name, filter = "") {
  retval <- list()
  for (t in names(for_each)) {
    message(glue::glue("{Sys.time()} Starting pulling plan data for {t} from {table_name}"))
    if (for_each[t] == "all") {
      table <- tbl(table_name)
      if (filter != "") {
        table <- table %>% dplyr::filter(!!!rlang::parse_exprs(filter))
      }
      options <- table %>%
        dplyr::distinct(!!as.symbol(t)) %>%
        dplyr::collect() %>%
        dplyr::pull(!!as.symbol(t))
    } else {
      options <- for_each[[t]]
    }
    retval[[t]] <- options
    message(glue::glue("{Sys.time()} Finished pulling plan data for {t} from {table_name}"))
  }
  return(retval)
}
