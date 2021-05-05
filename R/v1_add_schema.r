#' add schema
#' @param name the name of the schema
#' @param schema a Schema R6 class
#' @export
add_schema <- function(name=NULL, schema){
  if(is.null(name)) name <- schema$db_table
  config$schemas[[schema$db_table]] <- schema
}
