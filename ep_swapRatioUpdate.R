#loading libraries
library(googlesheets)
library(dplyr)
library(RPostgreSQL)
library(RJDBC)
library(ggplot2)

if(!exists("currentHFWeek")){
  
  source("ep_pullRecentODLPDL.R")
  
} 

drv2 <- RJDBC::JDBC("com.cloudera.impala.jdbc4.Driver", classPath = "G:/My Drive/Forecasting/HelloFresh/forecasting_models/snapshot/data/ImpalaJDBC4.jar") 
impalaConnection <- RJDBC::dbConnect(drv2, "jdbc:impala://cloudera-impala-proxy.live.bi.hellofresh.io:21050/;AuthMech=3;ssl=1", dwhuser, dwhpass)

query <- paste0('with raw as (
                select dd.hellofresh_week, bmc.fk_customer, bmc.product_sku, split_part(bmc.product_sku,"-",3) as recipes, split_part(bmc.product_sku,"-",4) as serving_size, bmc.user_choice, bmc.default_meal_combination,
                case when user_choice is null then "default" else "choice" end as d_or_c
                from fact_tables.box_meal_combination bmc
                inner join dimensions.date_dimension dd on dd.sk_date = bmc.fk_delivery_date
                where bmc.country = "ER" and split_part(bmc.product_sku,"-",3) != "0" and split_part(bmc.product_sku,"-",4) != "0"
),
                raw2 as (
                select raw.hellofresh_week, raw.fk_customer, raw.product_sku, raw.recipes, raw.serving_size, case when d_or_c = "choice" then 1 else 0 end as choice, case when d_or_c = "default" then 1 else 0 end as default
                from raw
                )
                select raw2.hellofresh_week, raw2.recipes, raw2.serving_size,"classic" as Preference, sum(raw2.choice) as choice, sum(default) as default, sum(raw2.choice) / count(*) as choice_ratio, sum(default) / count(*) as default_ratio
                from raw2
                group by 1,2,3')

swapRatioDF <- RJDBC::dbGetQuery(impalaConnection, query)

swapRatioDF <- swapRatioDF[which(swapRatioDF$hellofresh_week < currentHFWeek), ]

swapRatioDF <- swapRatioDF[which(swapRatioDF$hellofresh_week >= '2018-W01'), ]
#swapRatioDF <- swapRatioDF[which(swapRatioDF$current_product_sku %in% validSKUS$product_sku), ]

#swapRatioDF <- swapRatioDF %>% rowwise() %>% do({
#  X <- data.frame(., stringsAsFactors = F)
#  X$plan <- gsub("[^0-9.-]|-", "", X$current_product_sku)
#  X$kits <- substr(X$plan, nchar(X$plan) - 2,nchar(X$plan) - 2) 
#  X$servings <- substr(X$plan, nchar(X$plan) - 1,nchar(X$plan) - 1) 
#  X$plan <- NULL
#  X
#})



#getting na / no preference to be the same
#swapRatioDF[which(swapRatioDF$subscription_preference_name == "" |
#              is.na(swapRatioDF$subscription_preference_name)), "subscription_preference_name"] <- "no preference"


#changing 6 and 5 and 4kits/serving numbers 
#swapRatioDF[which(swapRatioDF$kits == 6 &
#                    swapRatioDF$servings == 4), c("default_orders", "choice_orders")] <-
  
#  swapRatioDF[which(swapRatioDF$kits == 6 &
#                     swapRatioDF$servings == 4), c("default_orders", "choice_orders")] * 2

#swapRatioDF[which(swapRatioDF$kits == 6 &
#                    swapRatioDF$servings == 4), c("kits")] <- "3"


#swapRatioDF[which(swapRatioDF$kits == 6 &
#                   swapRatioDF$servings == 2), c("default_orders", "choice_orders")] <-
  
# swapRatioDF[which(swapRatioDF$kits == 6 &
#                     swapRatioDF$servings == 2), c("default_orders", "choice_orders")]  * 2

#swapRatioDF[which(swapRatioDF$kits == 6 &
#                   swapRatioDF$servings == 2), c("kits")] <- "3"


#swapRatioDF[which(swapRatioDF$kits == 4 &
#                   swapRatioDF$servings == 4), c("default_orders", "choice_orders")] <-
# swapRatioDF[which(swapRatioDF$kits == 4 &
#                     swapRatioDF$servings == 4), c("default_orders", "choice_orders")] * 2

#swapRatioDF[which(swapRatioDF$kits == 4 &
#                    swapRatioDF$servings == 4),  c("kits")] <- "2"


#fiveBoxes <- swapRatioDF[which(swapRatioDF$kits == 5), ]

#splitting 5 boxers into 2 and 3 boxes
#fiveBoxesTransformed <- fiveBoxes %>% rowwise() %>% do({
#  X <- data.frame(., stringsAsFactors = F)
#  twoBox <- X
# threeBox <- X
# twoBox$kits <- "2"
# threeBox$kits <- "3"
  
# Y <- bind_rows(twoBox, threeBox)
# Y
#})

#if(nrow(fiveBoxes) > 0){
# swapRatioDF <- swapRatioDF[-which(swapRatioDF$kits == 5), ]
# swapRatioDF <- bind_rows(swapRatioDF, fiveBoxesTransformed)
#}

#calculating swap ratio per week
#swapRatioDF <- swapRatioDF %>% group_by(hellofresh_week,kits,servings, subscription_preference_name) %>% 
# do({
#   X <- data.frame(., stringsAsFactors = F)
#   X$swapRatio <- sum(X$choice_orders)/(sum(X$default_orders) + sum(X$choice_orders))
#   X
# })

#swapRatioDF <- swapRatioDF[c("hellofresh_week","kits", "servings", "subscription_preference_name", "swapRatio")] %>% distinct()


swapRatioDF <- swapRatioDF[order(swapRatioDF$hellofresh_week, decreasing = T), ]

# formatting
#swapRatioDF$swapRatio <- round(swapRatioDF$swapRatio, 2)
names(swapRatioDF) <- c("HF Week", "Kits/Box", "Serving Size","Preference","Choice", "Default", "Choice Ratio", "Default Ratio")
#swapRatioDF$Default <- 1 - swapRatioDF$Choice

#updating googlesheet
write_csv(swapRatioDF, path = "EPswapRatioData.csv")

gs_upload("EPswapRatioData.csv",sheet_title = "ep_forecasting_scripts_output_swapratio", overwrite = T, verbose = T)


RJDBC::dbDisconnect(impalaConnection)
