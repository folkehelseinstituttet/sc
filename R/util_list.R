get_list <- function(l, key, default=NULL){
  if(key %in% names(l)){
    return(l[[key]])
  }else{
    return(default)
  }
}
