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
  info = "This db table is used for..."
)

sc::config$schemas$anon_test

d = data.table(uuid = 1:10000000)
d$n = 1

sc::config$schemas$anon_test$tbl()
# setkey(d, uuid)
#setorder(d, -uuid)
a <- Sys.time()
sc::config$schemas$anon_test$drop_all_rows_and_then_insert_data(d)
b <- Sys.time()

b - a

sc::config$schemas$anon_test$tbl() %>% dplyr::summarize(n()) %>% dplyr::collect()
