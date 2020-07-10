#inweek data

require(dplyr)
require(tidyverse)
require(readr)
require(lubridate)
require(RPostgreSQL)
require(RJDBC)
require(googlesheets)
require(RMySQL)

if(!exists("currentHFWeek")){
  
  source("ep_pullRecentODLPDL.R")
  
} 
# update inweek data

source("EP_ODL_FCDB_InWeek_Script.R")
