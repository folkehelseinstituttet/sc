#' add schema
#' @param name_access a
#' @param name_grouping a
#' @param name_variant a
#' @param db_configs a
#' @param field_types a
#' @param keys a
#' @param censors a
#' @param indexes a
#' @param validator_field_types a
#' @param validator_field_contents a
#' @param info a
#' @export
add_schema_v8 <- function(
  name_access = NULL,
  name_grouping = NULL,
  name_variant = NULL,
  db_configs = NULL,
  field_types,
  keys,
  censors = NULL,
  indexes = NULL,
  validator_field_types = validator_field_types_blank,
  validator_field_contents = validator_field_contents_blank,
  info = NULL
  ){

  force(name_access)
  force(name_grouping)
  force(name_variant)
  force(db_configs)
  force(field_types)
  force(keys)
  force(censors)
  force(indexes)
  force(validator_field_types)
  force(validator_field_contents)
  force(info)

  redirect <- SchemaRedirect_v8$new(
    name_access = name_access,
    name_grouping = name_grouping,
    name_variant = name_variant,
    db_configs = db_configs,
    field_types = field_types,
    keys = keys,
    censors = censors,
    indexes = indexes,
    validator_field_types = validator_field_types,
    validator_field_contents = validator_field_contents,
    info = info
  )
  if(length(name_access)>1){
    table_name <- paste0(c("redirect", name_grouping, name_variant), collapse = "_")
    config$schemas[[table_name]] <- redirect
  }
}


