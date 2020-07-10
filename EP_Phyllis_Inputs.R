# fcbd data, bob data, inweek data + start new week

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

# fcdb data update
source("EP_ODL_FCDB_Script.R")

# update swap ratio data
source("ep_swapRatioUpdate.R")

# bob data update
source("EP_BOB_DWH_Script.R")
