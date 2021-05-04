#' mandatory_db_filter
#' @param .data Data
#' @param granularity_time Granularity time to filter on (include)
#' @param granularity_time_not Granularity time to filter on (exclude)
#' @param granularity_geo Granularity geo to filter on (include)
#' @param granularity_geo_not Granularity geo to filter on (exclude)
#' @param country_iso3 country_iso3 to filter on (include)
#' @param location_code location_code to filter on (include)
#' @param age Age to filter on (include)
#' @param age_not Age to filter on (exclude)
#' @param sex Sex to filter on (include)
#' @param sex_not Sex to filter on (exclude)
#' @export
mandatory_db_filter <- function(
  .data,
  granularity_time = NULL,
  granularity_time_not = NULL,
  granularity_geo = NULL,
  granularity_geo_not = NULL,
  country_iso3 = NULL,
  location_code = NULL,
  age = NULL,
  age_not = NULL,
  sex = NULL,
  sex_not = NULL
){
  retval <- .data

  if(!is.null(granularity_time)) retval <- retval %>% dplyr::filter(granularity_time %in% !!granularity_time)
  if(!is.null(granularity_time_not)) retval <- retval %>% dplyr::filter(!granularity_time %in% !!granularity_time_not)

  if(!is.null(granularity_geo)) retval <- retval %>% dplyr::filter(granularity_geo %in% !!granularity_geo)
  if(!is.null(granularity_geo_not)) retval <- retval %>% dplyr::filter(!granularity_geo %in% !!granularity_geo_not)

  if(!is.null(country_iso3)) retval <- retval %>% dplyr::filter(!country_iso3 %in% !!country_iso3)

  if(!is.null(location_code)) retval <- retval %>% dplyr::filter(!location_code %in% !!location_code)

  if(!is.null(age)) retval <- retval %>% dplyr::filter(age %in% !!age)
  if(!is.null(age_not)) retval <- retval %>% dplyr::filter(!age %in% !!age_not)

  if(!is.null(sex)) retval <- retval %>% dplyr::filter(sex %in% !!sex)
  if(!is.null(sex_not)) retval <- retval %>% dplyr::filter(!sex %in% !!sex_not)

  return(retval)
}
