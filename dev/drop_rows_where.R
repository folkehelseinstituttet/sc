library(data.table)
library(magrittr)
system("/bin/authenticate.sh")


# anon_GROUPING_VARIANT ----
sc::add_schema_v8(
  name_access = c("anon"),
  name_grouping = "test",
  name_variant = NULL,
  db_configs = sc::config$db_configs,
  field_types =  c(
    "uuid" = "INTEGER",
    "n" = "INTEGER"
  ),
  keys = c(
    "uuid"
  ),
  censors = list(
    anon = list(
    )
  ),
  validator_field_types = NULL,
  validator_field_contents = NULL,
  info = "This db table is used for..."
)

sc::config$schemas$anon_test

d = data.table(uuid = 1:10000)
d$n = 1

sc::config$schemas$anon_test$tbl()
sc::config$schemas$anon_test$insert_data(d)

sc::config$schemas$anon_test$tbl() %>%
  dplyr::collect()

drop_rows_where(table = "anon_test")
