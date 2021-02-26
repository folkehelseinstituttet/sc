#' Describe all available schemas
#' @export
describe_schemas <- function(){
  retval <- vector("list", length = length(config$schemas))
  for(i in seq_along(config$schemas)){
    schema <- config$schemas[[i]]

    tab <- data.frame(schema$db_field_types)
    tab$variable <- row.names(tab)
    setDT(tab)
    tab[, key := ""]
    tab[variable %in% schema$keys, key := "*"]
    setcolorder(tab, c("variable", "key", "schema.db_field_types"))
    setnames(tab, c("Variable","Key", "Type"))
    tab[, Info := ""]

    retval[[i]] <- list(
      name = schema$db_table,
      info = schema$info,
      details = tab
    )
  }
  return(retval)
}

#' Describe all available tasks
#' @export
describe_tasks <- function(){
  retval <- vector("list", length = length(sc::config$tasks$list_task))
  for(i in seq_along(config$tasks$list_task)){
    task <- config$tasks$list_task[[i]]
    name <- config$tasks$list_task$name[[i]]
    name_description <- config$tasks$list_task[[i]]$name_description
    schemas <- names(task$schema)

    retval[[i]] <- list(
      name = name,
      name_grouping = name_description$grouping,
      name_action = name_description$action,
      name_variant = name_description$variant,
      info = task$info,
      schemas = schemas
    )
  }
  return(retval)
}
