con <- file("/tmp/computer", "r")
COMPUTER_NAME <- readLines(con, n = 1)
close(con)
Sys.setenv(COMPUTER = COMPUTER_NAME)

## # Cleaning up previous runs data
## for (baseFolder in c("/data_clean", "/results", "/data_app")) {
##   files <- list.files(file.path(baseFolder, "sykdomspuls"))
##   if (length(files) > 0) {
##     for (f in files) unlink(file.path(baseFolder, "sykdomspuls", f))
##   }
## }

unlink(file.path("/junit", "sykdomspuls.xml"))
Sys.sleep(1)

a <- testthat:::JunitReporter$new()
a$start_reporter()
a$out <- file(file.path("/junit", "sykdomspuls.xml"), "w+")
a$start_context("sykdomspuls")

# Run process

devtools::install_local(".", force=T)
devtools::load_all()

conn <- get_db_connection(db_config=config$db_config)
DBI::dbExecute(conn, "delete from rundate")

output <- processx::run("Rscript", "/sykdomspulsen/inst/src/orchestrate.R", error_on_status = F, echo = T)
cat("\n\nstdout\n\n")
cat(output$stdout)
cat("\n\nstderr\n\n")
cat(output$stderr)

if (output$status == 0) {
  cat("\n**PASS 1**\n")
  a$add_result("sykdomspuls", "RunAll", testthat::expectation("success", "Pass"))
} else {
  cat("\n**FAIL 1**\n")
  a$add_result("sykdomspuls", "RunAll", testthat::expectation("error", "Fail"))
}

## Run API
process <- processx::process$new("Rscript", "/sykdomspulsen/inst/src/run_api.R")
if (process$is_alive()) {
  cat("\n**PASS 2**\n")
  a$add_result("sykdomspuls", "API_0min", testthat::expectation("success", "Pass"))
} else {
  cat("\n**FAIL 2**\n")
  a$add_result("sykdomspuls", "API_0min", testthat::expectation("error", "Fail"))
}
Sys.sleep(60 * 10)
if (process$is_alive()) {
  cat("\n**PASS 3**\n")
  a$add_result("sykdomspuls", "API_10min", testthat::expectation("success", "Pass"))
} else {
  cat("\n**FAIL 3**\n")
  a$add_result("sykdomspuls", "API_10min", testthat::expectation("error", "Fail"))
}

req <- httr::GET("http://localhost:8000/test?x=0")
json <- httr::content(req, as = "text", encoding = "UTF-8")
res <- jsonlite::fromJSON(json)

print(res)

if (res == "0") {
  cat("\n**PASS 4**\n")
  a$add_result("sykdomspuls", "API_received_0", testthat::expectation("success", "Pass"))
} else {
  cat("\n**FAIL 4**\n")
  a$add_result("sykdomspuls", "API_received_0", testthat::expectation("error", "Fail"))
}

x <- process$kill()

a$end_context("sykdomspuls")
a$end_reporter()
close(a$out)
