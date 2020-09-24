options(error=function()traceback(2))

devtools::load_all()

config$tasks$run_all(log=TRUE)
