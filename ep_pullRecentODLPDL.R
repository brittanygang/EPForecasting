## script for processing odl data
# loading libraries
require(dplyr)
require(tidyverse)
require(readr)
require(lubridate)
require(RPostgreSQL)
require(RJDBC)
require(googlesheets)
require(RMySQL)
# getting credentials locally

if(grepl('window', tolower(osVersion))){
  source("C://dbcredentials.R")
} else {
  source("~/dbcredentials.R")
}
#for bodega, postgres - connect
drv <- dbDriver("PostgreSQL")
postgresCon <- dbConnect(drv, dbname = "US_Datamart",
                         host = 'datamart-db-us000-live-bi.ccj54oujhzp1.eu-west-1.rds.amazonaws.com', port = 5432,
                         user = pguser, password = pgpass)

drv2 <- RJDBC::JDBC("com.cloudera.impala.jdbc4.Driver", classPath = "G:/My Drive/Forecasting/HelloFresh/forecasting_models/snapshot/data/ImpalaJDBC4.jar") 
impalaConnection <- RJDBC::dbConnect(drv2, "jdbc:impala://cloudera-impala-proxy.live.bi.hellofresh.io:21050/;AuthMech=3;ssl=1", dwhuser, dwhpass)

#getting most recent odl
#get user input current week

getInput <- function() {
  cat("\n", "Enter current HF Week (ex. 2019-W47):", "\n")
  userInput <<- scan(what = 'character',n=1)
  cat("\n", "You entered", userInput, "\n")
}

userInput <- 
  if(interactive()){
    userInput <<- readline(prompt = "Enter current HF Week (ex. 2019-W47):")
    print(userInput)
  } else {
    getInput()
  }
currentHFWeekToQueryFor <- gsub("'|\"","",gsub("--|  ", "-",
                                gsub(" ","-",
                                     toupper(userInput))))

recentODL <- dbGetQuery(postgresCon, paste0("SELECT * from bodega.everyplate_odl where hellofresh_week = '",currentHFWeekToQueryFor,"'
                        and delivery_region != 'NO SERVICE'"))

# recentODL <- dbGetQuery(postgresCon, "SELECT * from bodega.odl where hellofresh_week = (
#             
#     SELECT
#         MAX (hellofresh_week)
#    FROM
#       bodega.odl)
#                         and delivery_region != 'NO SERVICE'")

# save hellofresh week. weekdays of odl here
currentHFWeek <- as.character(unique(recentODL$hellofresh_week))


#recentODL$addon_swap <- gsub("-1", "", recentODL$addon_swap)

weekdaysOfODL <- unique(recentODL$weekday)



#pulling PDL for same week of ODL, for weekdays not in ODL
recentPDL <- dbGetQuery(postgresCon, paste0("SELECT * from bodega.everyplate_pdl where hellofresh_week = '",
                                            currentHFWeekToQueryFor,"' and weekday not in ('",paste0(weekdaysOfODL, collapse = "', '"),"')
                                            and   delivery_region != 'NO SERVICE'"))

#recentPDL$addon_swap <- gsub("-1", "", recentPDL$addon_swap)


# authorizing googlesheets
token <- gs_auth()
saveRDS(token, file = "googlesheets_token.rds")
suppressMessages(gs_auth(token = "googlesheets_token.rds", verbose = FALSE))

#getting valid product skus
validSKUS <- paste0("select distinct *
                    from dimensions.product_dimension where
                    is_mealbox = TRUE and country = 'ER' and is_seasonal = FALSE and is_active = TRUE")
validSKUS <- dbGetQuery(impalaConnection, validSKUS)



if(nrow(data.frame(recentODL[which(recentODL$sku %in% validSKUS$product_sku), ])) > 0){
  recentODL <- recentODL[which(recentODL$sku %in% validSKUS$product_sku), ]
}

if(nrow(data.frame(recentPDL[which(recentPDL$sku %in% validSKUS$product_sku), ])) > 0){
  recentPDL <- recentPDL[which(recentPDL$sku %in% validSKUS$product_sku), ]
}
