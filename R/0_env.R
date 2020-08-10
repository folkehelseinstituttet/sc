#' Environment to store db connections
#' @export connections
connections <- new.env()

#' Flags/values to be used in the 'dashboards' scene
#' @export config
config <- new.env()

config$is_production <- FALSE
config$verbose <- FALSE
config$schemas <- list()
config$permissions <- list()

