#' add task
#' @param task a Task R6 class
#' @export
add_task <- function(task){
  config$tasks$add_task(task)
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
    insert_at_end_of_each_plan = FALSE,
    name = NULL,
    name_description = list(),
    update_plans_fn = NULL,
    action_before_fn = NULL,
    action_after_fn = NULL,
    info = "No information given in task definition.",
    initialize = function(
                              name = NULL,
                              name_description = NULL,
                              type,
                              permission = NULL,
                              plans = NULL,
                              update_plans_fn = NULL,
                              schema,
                              cores = 1,
                              upsert_at_end_of_each_plan = FALSE,
                              insert_at_end_of_each_plan = FALSE,
                              action_before_fn = NULL,
                              action_after_fn = NULL,
                              info = NULL
                              ) {
      stopifnot(!(is.null(name) & is.null(name_description)))
      if(!is.null(name_description)){
        stopifnot(is.list(name_description))
        stopifnot(sum(c("grouping", "action", "variant") %in% names(name_description))==3)
        name <- paste0(unlist(name_description), collapse="_")
      } else {
        name_description <- list(
          grouping = NULL,
          action = NULL,
          variant = NULL
        )
      }
      self$name <- name
      self$name_description <- name_description
      self$type <- type
      self$permission <- permission
      self$plans <- plans
      self$update_plans_fn <- update_plans_fn
      self$schema <- schema
      self$cores <- cores
      self$upsert_at_end_of_each_plan <- upsert_at_end_of_each_plan
      self$insert_at_end_of_each_plan <- insert_at_end_of_each_plan
      self$action_before_fn <- action_before_fn
      self$action_after_fn <- action_after_fn
      if(!is.null(info)) self$info <- info
    },
    update_plans = function() {
      if (!is.null(self$update_plans_fn)) {
        message(glue::glue("Updating plans..."))
        # if("schema" %in% names(formals(self$update_plans_fn))){
        #   for (s in self$schema) s$db_connect()
        #   self$plans <- self$update_plans_fn(schema = self$schema)
        #   for (s in self$schema) s$db_disconnect()
        # } else {
        #   self$plans <- self$update_plans_fn()
        # }
        self$plans <- self$update_plans_fn()
        self$update_plans_fn <- NULL
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
      if(!is.null(self$action_before_fn)){
        message("Running action_before_fn")
        self$action_before_fn()
      }
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
          force = TRUE,
          total = self$num_argsets()
        )
        for (s in schema) s$db_connect()
        for (i in seq_along(self$plans)) {
          if(!interactive()){
            message(glue::glue("Plan {i}/{length(self$plans)}"))
          }

          self$plans[[i]]$set_progress(pb)
          retval <- self$plans[[i]]$run_all(schema = schema)

          if (upsert_at_end_of_each_plan) {
            retval <- rbindlist(retval)
            schema$output$db_upsert_load_data_infile(retval, verbose = F)
          }

          if (insert_at_end_of_each_plan) {
            retval <- rbindlist(retval)
            schema$output$db_load_data_infile(retval, verbose = F)
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

            if (insert_at_end_of_each_plan) {
              retval <- rbindlist(retval)
              schema$output$db_load_data_infile(retval, verbose = F)
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

      if(!is.null(self$action_after_fn)){
        message("Running action_after_fn")
        self$action_after_fn()
      }

      update_rundate(task = self$name)
      update_config_datetime(type = "task", tag = self$name)
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
