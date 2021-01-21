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
      name = tab,
      info = schema$info,
      details = schema$db_table
    )
  }
  return(retval)
}

#' Describe all available tasks
#' @param data data passed to schema
#' @export
describe_tasks <- function(){
  retval <- vector("list", length = length(config$schemas))
  for(i in seq_along(config$tasks$list_task)){
    task <- config$tasks$list_task[[i]]
    name <- names(config$tasks$list_task)[i]
    schemas <- names(task$schema)

    retval[[i]] <- list(
      name = name,
      info = task$info,
      schemas = schemas
    )
  }
  return(retval)
}
