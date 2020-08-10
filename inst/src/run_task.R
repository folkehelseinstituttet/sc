options(error=function()traceback(2))

##Intialise
devtools::load_all()

args <- commandArgs(trailingOnly = TRUE)


print(names(config$tasks$list_task))
config$tasks$task_run(args[1], log=FALSE)
