library(data.table)
library(magrittr)
system("/bin/authenticate.sh")


sc::add_schema_v8(
  name_access = c("restr", "anon"),
  name_grouping = "example",
  name_variant = NULL,
  db_configs = sc::config$db_configs,
  field_types =  c(
    "granularity_time" = "TEXT",
    "granularity_geo" = "TEXT",
    "country_iso3" = "TEXT",
    "location_code" = "TEXT",
    "border" = "INTEGER",
    "age" = "TEXT",
    "sex" = "TEXT",

    "date" = "DATE",

    "isoyear" = "INTEGER",
    "isoweek" = "INTEGER",
    "isoyearweek" = "TEXT",
    "season" = "TEXT",
    "seasonweek" = "DOUBLE",

    "calyear" = "INTEGER",
    "calmonth" = "INTEGER",
    "calyearmonth" = "TEXT",

    "value_n" = "INTEGER",
    "value_pr100" = "DOUBLE",
    "x_n" = "INTEGER"
  ),
  keys = c(
    "granularity_time",
    "location_code",
    "date",
    "age",
    "sex"
  ),
  censors = list(
    restr = list(
      "value_n" = sc::censor_function_factory_nothing("value_n"),

      "value_pr100" = sc::censor_function_factory_values_0_4(column_name_to_be_censored = "value_pr100", column_name_value = "value_n"),

      "x_n" = sc::censor_list_function_factory(list(
        sc::censor_function_factory_nothing(column_name_to_be_censored = "x_n", granularity_time = "day", granularity_geo = c("nation")),
        sc::censor_function_factory_everything(column_name_to_be_censored = "x_n", granularity_time = "day", granularity_geo_not = c("nation")),
        sc::censor_function_factory_nothing(column_name_to_be_censored = "x_n", granularity_time = "isoweek", granularity_geo = c("nation")),
        sc::censor_function_factory_nothing(column_name_to_be_censored = "x_n", granularity_time = "isoweek", granularity_geo_not = c("nation"))
      ))
    ),
    anon = list(
      value_n = sc::censor_function_factory_values_0_4("value_n"),

      "value_pr100" = sc::censor_function_factory_values_0_4(column_name_to_be_censored = "value_pr100", column_name_value = "value_n"),

      "x_n" = sc::censor_list_function_factory(list(
        sc::censor_function_factory_nothing(column_name_to_be_censored = "x_n", granularity_time = "day", granularity_geo = c("nation")),
        sc::censor_function_factory_everything(column_name_to_be_censored = "x_n", granularity_time = "day", granularity_geo_not = c("nation")),
        sc::censor_function_factory_nothing(column_name_to_be_censored = "x_n", granularity_time = "isoweek", granularity_geo = c("nation")),
        sc::censor_function_factory_nothing(column_name_to_be_censored = "x_n", granularity_time = "isoweek", granularity_geo_not = c("nation"))
      ))
    )
  ),
  validator_field_types = sc::validator_field_types_sykdomspulsen,
  validator_field_contents = sc::validator_field_contents_sykdomspulsen,
  info = "This db table is used for..."
)

sc::drop_table("restr_example")
sc::drop_table("anon_example")

d <- data.table(
  granularity_time = c("day", "day", "isoweek", "isoweek"),
  granularity_geo = c("nation","county"),
  country_iso3 = "nor",
  location_code = c("norge","county03"),
  border = 2020,
  age = "total",
  sex = "total",

  date = c(as.Date("1990-01-07"),as.Date("1990-01-08")),

  isoyear = 1990,
  isoweek = 1,
  isoyearweek = "1990-01",
  season = "1990/1991",
  seasonweek = 24,

  calyear = NA,
  calmonth = NA,
  calyearmonth = NA,

  value_n = c(3,6,3,6)
)
d[, value_pr100 := 100]
d[, x_n := value_n]

# display the raw data
d[]

sc::fill_in_missing_v8(d)

# we have three options to get the data into the db table
# remember that "keys" defines the uniquely identifying rows of data that are allowed in the db table!
# - upsert means "update if data exists, otherwise append"
# - insert means "append" (data cannot already exist)

sc::config$schemas$redirect_example$upsert_data(d)

sc::config$schemas$redirect_example$tbl() %>%
  sc::mandatory_db_filter(
    granularity_time = NULL,
    granularity_time_not = NULL,
    granularity_geo = NULL,
    granularity_geo_not = NULL,
    country_iso3 = NULL,
    location_code = NULL,
    age = "total",
    age_not = NULL,
    sex = "total",
    sex_not = NULL
  ) %>%
  dplyr::select(
    granularity_time,
    location_code,
    date,
    value_n,
    value_n_censored
  ) %>%
  dplyr::collect() %>%
  as.data.table() %>%
  print()

sc::config$schemas$restr_example$tbl() %>%
  sc::mandatory_db_filter(
    granularity_time = NULL,
    granularity_time_not = NULL,
    granularity_geo = NULL,
    granularity_geo_not = NULL,
    country_iso3 = NULL,
    location_code = NULL,
    age = "total",
    age_not = NULL,
    sex = "total",
    sex_not = NULL
  ) %>%
  dplyr::select(
    granularity_time,
    location_code,
    date,
    value_n,
    value_n_censored,
    value_pr100,
    value_pr100_censored,
    x_n,
    x_n_censored
  ) %>%
  dplyr::collect() %>%
  as.data.table() %>%
  print()

sc::config$schemas$anon_example$tbl() %>%
  sc::mandatory_db_filter(
    granularity_time = NULL,
    granularity_time_not = NULL,
    granularity_geo = NULL,
    granularity_geo_not = NULL,
    country_iso3 = NULL,
    location_code = NULL,
    age = "total",
    age_not = NULL,
    sex = "total",
    sex_not = NULL
  ) %>%
  dplyr::select(
    granularity_time,
    location_code,
    date,
    value_n,
    value_n_censored,
    value_pr100,
    value_pr100_censored,
    x_n,
    x_n_censored
  ) %>%
  dplyr::collect() %>%
  as.data.table() %>%
  print()
