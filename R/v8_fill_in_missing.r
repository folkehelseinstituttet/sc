fix_variables <- function(d){
  if("date" %in% names(d)){
    if(inherits(d$date, "IDate")){
      d[, date:= lubridate::as_date(date)]
    }
  }
}


x_season_int <- function(yrwk, start_week = 30) {
  retval <- as.numeric(stringr::str_split(yrwk, "-")[[1]])
  yr <- retval[1]
  wk <- retval[2]

  if (wk >= start_week) {
    start <- glue::glue("{yr}/{yr+1}")
  } else {
    start <- glue::glue("{yr-1}/{yr}")
  }
  return(start)
}

x_season <- Vectorize(x_season_int, vectorize.args = c("yrwk"))

#' fill_in_missing_v8
#' @param d dataset
#' @export
fill_in_missing_v8 <- function(d){
  fix_variables(d)

  stopifnot("granularity_time" %in% names(d))
  stopifnot("age" %in% names(d))
  stopifnot("sex" %in% names(d))

  if(!"granularity_geo" %in% names(d)){
    d[, granularity_geo:=dplyr::case_when(
      stringr::str_detect(location_code,"^municip[0-9]") ~ "municip",
      stringr::str_detect(location_code,"^county[0-9]") ~ "county",
      stringr::str_detect(location_code,"^norge") ~ "nation",
      stringr::str_detect(location_code,"^wardoslo[0-9]") ~ "wardoslo",
      stringr::str_detect(location_code,"^extrawardoslo[0-9]") ~ "extrawardoslo",
      stringr::str_detect(location_code,"^missingwardoslo[0-9]") ~ "missingwardoslo",
      stringr::str_detect(location_code,"^wardbergen[0-9]") ~ "wardbergen",
      stringr::str_detect(location_code,"^missingwardbergen[0-9]") ~ "missingwardbergen",
      stringr::str_detect(location_code,"^wardtrondheim[0-9]") ~ "wardtrondheim",
      stringr::str_detect(location_code,"^missingwardtrondheim[0-9]") ~ "missingwardtrondheim",
      stringr::str_detect(location_code,"^wardstavanger[0-9]") ~ "wardstavanger",
      stringr::str_detect(location_code,"^missingwardstavanger[0-9]") ~ "missingwardstavanger",
      stringr::str_detect(location_code,"^ward[0-9]") ~ "ward",
      stringr::str_detect(location_code,"^baregion[0-9]") ~ "baregion",
      stringr::str_detect(location_code,"^missingcounty[0-9]") ~ "missingcounty",
      stringr::str_detect(location_code,"^missingmunicip[0-9]") ~ "missingmunicip",
      stringr::str_detect(location_code,"^notmainlandcounty[0-9]") ~ "notmainlandcounty",
      stringr::str_detect(location_code,"^notmainlandmunicip[0-9]") ~ "notmainlandmunicip",

      location_code=="IS" ~ "nation",

      location_code=="SE" ~ "nation",
      stringr::str_detect(location_code,"SE") ~ "county",

      location_code=="FI" ~ "nation",
      stringr::str_detect(location_code,"fi_hospitaldistrict") ~ "hospitaldistrict",

      location_code=="DK" ~ "nation",
      stringr::str_detect(location_code,"DK") ~ "region"
    )]
  }
  if(!"border" %in% names(d)){
    d[, border := config$border]
  }
  if(!"age" %in% names(d)){
    d[, age := "total"]
  }
  if(!"sex" %in% names(d)){
    d[,sex := "total"]
  }
  # only providing season
  if(!"date" %in% names(d) & !"isoyearweek" %in% names(d) & "season" %in% names(d)){
    x <- copy(fhidata::days)
    x[,season := x_season(yrwk)]
    setorder(x,-yrwk)
    x[,row:=1:.N,by=.(season)]
    x <- x[row==1]
    d[
      x,
      on="season",
      date:=sun
    ]
  }
  # only providing year
  if(!"date" %in% names(d) & !"isoyearweek" %in% names(d) & "isoyear" %in% names(d)){
    x <- copy(fhidata::days)
    setorder(x,-yrwk)
    x[,row:=1:.N,by=.(year)]
    x <- x[row==1]
    x[, year := as.numeric(year)]
    d[
      x,
      on="isoyearweek==year",
      date:=sun
    ]
  }

  # only providing yrwk
  if(!"date" %in% names(d) & "isoyearweek" %in% names(d)){
    days <- fhidata::days
    d[
      days,
      on="isoyearweek==yrwk",
      date:=sun
    ]

    if("granularity_time" %in% names(d)){
      d[is.na(date) & granularity_time=="total", date:=as.Date("1900-01-01")]
    }
  }

  if(!"date" %in% names(d) & !"isoyearweek" %in% names(d)){
    d[,date:=as.Date("1900-01-01")]
  }
  if(!"isoyearweek" %in% names(d)){
    dates <- unique(d[, "date"])
    dates[,isoyearweek := fhiplot::isoyearweek_c(date)]
    d[dates,on="date",isoyearweek := isoyearweek]
  }
  if(!"season" %in% names(d)){
    dates <- unique(d[, "isoyearweek"])
    dates[,season := x_season(isoyearweek)]
    d[dates,on="isoyearweek",season := season]
  }
  if(!"isoyear" %in% names(d)){
    dates <- unique(d[, "date"])
    dates[,isoyear := fhiplot::isoyear_n(date)]
    d[dates, on="date",isoyear := isoyear]
  }
  if(!"isoweek" %in% names(d)){
    dates <- unique(d[, "date"])
    dates[,isoweek := fhiplot::isoweek_n(date)]
    d[dates,on="date", isoweek := isoweek]
  }
  if(!"seasonweek" %in% names(d)){
    dates <- unique(d[, "isoweek"])
    dates[,x := fhiplot::week_to_seasonweek_n(isoweek)]
    d[dates, on="isoweek", seasonweek := x]
  }

  if(!"calyear" %in% names(d)){
    dates <- unique(d[, "date"])
    dates[, granularity_time := "day"]
    dates[,x := lubridate::year(date)]
    d[dates, on=c("granularity_time","date"), calyear := x]
  }
  if(!"calmonth" %in% names(d)){
    dates <- unique(d[, "date"])
    dates[, granularity_time := "day"]
    dates[,x := lubridate::month(date)]
    d[dates, on=c("granularity_time","date"), calmonth := x]
  }
  if(!"calyearmonth" %in% names(d)){
    dates <- unique(d[, "date"])
    dates[, granularity_time := "day"]
    dates[,x := paste0(lubridate::year(date), formatC(lubridate::month(date), width=2, flag="0"))]
    d[dates, on=c("granularity_time","date"), calyearmonth := x]
  }
  if(!"country_iso3" %in% names(d)){
    d[, country_iso3 := "nor"]
  }
}
