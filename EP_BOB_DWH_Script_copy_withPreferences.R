# bob script - gets run weekly, every tuesday, and appended on the bottom#

require(dplyr)
require(tidyverse)
require(readr)
require(lubridate)
require(RPostgreSQL)
require(RJDBC)
require(googlesheets)
require(RMySQL)

# sourcing credentials, making sure current HF week is in environment
if(!exists("currentHFWeek")){
  
  source("ep_pullRecentODLPDL.R")
  
} 

#for bodega, postgres
drv <- dbDriver("MySQL")
mysqlConnection <- dbConnect(drv,
                             host = 'intfood-db-er001.live.bi.hellofresh.io',
                             user= bobuser, password = bobpass, dbname="bob_live_er")

#get 1 - 4 out weeks from today

weeksAheadList <<- list()
j <- 1
# 1 - 5 weeks out bob pull
for(i in 1:5){
  
  weekInputTemp <<- gsub("w", "", tolower(currentHFWeek))
  weekYear <<- gsub( "-.*$", "", weekInputTemp)
  weekNumber <<- gsub( ".*-", "", weekInputTemp)
  
  
  
  if(as.numeric(weekNumber) + i <= 52){
    
    weeksAhead <<- paste0('week_', as.numeric(weekNumber) + i)
    assign(weeksAhead,  paste0(weekYear,"-W", as.numeric(weekNumber) + i))
    weeksAheadList <<- append(weeksAheadList, get(weeksAhead))
  }
  else {
    weekYear <<- as.numeric(weekYear) + 1
    weekNumber <<-  j
    j <<-  j + 1
    weeksAhead <<- paste0('week_', as.numeric(weekNumber))
    assign(weeksAhead,  paste0(weekYear,"-W0", as.numeric(weekNumber)))
    
    weeksAheadList <<- append(weeksAheadList, get(weeksAhead))
  }
}

weeksToQueryFor <- as.character(unlist(weeksAheadList))

#future choice query. getting orders for future weeks from bob.

query <- paste0( "select t1.subscription_id, t1.plan, substring_index(substring_index(t1.plan, '-', -3),  '-', 1) as kits, substring_index(substring_index(t1.plan, '-', -2),  '-', 1) as serving_size,
 t1.scm_week_choice_for, t1.recipe,  t1.meals_preset
from (
SELECT
    subscription_recipe.fk_subscription AS subscription_id,
    CASE WHEN subscription_recipe.week < 10 THEN concat(subscription_recipe.`year`,'-W0',subscription_recipe.`week`) ELSE concat(subscription_recipe.`year`,'-W',subscription_recipe.`week`) END AS scm_week_choice_for,
    concat('-',REPLACE(subscription_recipe.recipes, ' ', '-' ),'-') AS recipe,
    CASE WHEN one_off_sku IS NULL THEN subscription_forecast.sku ELSE one_off_sku END AS plan,
    subscription.meals_preset
FROM
    subscription_recipe
LEFT JOIN subscription_forecast ON
    subscription_recipe.fk_subscription = subscription_forecast.fk_subscription
LEFT JOIN
    subscription ON subscription_recipe.fk_subscription = id_subscription
LEFT JOIN (select t1.fk_subscription, t1.week_id, t1.one_off_sku, t1.week_number, t1.year from
    (SELECT fk_subscription, week_id, sku AS one_off_sku, CAST(right(week_id,2) AS UNSIGNED) AS week_number, CAST(left(week_id,4) AS UNSIGNED) AS year
     FROM subscription_change_schedule) t1
     inner JOIN
     (SELECT fk_subscription,
MAX(CAST(created_at AS CHAR)) as ui
FROM subscription_change_schedule group by 1) t2
on t2.fk_subscription = t1.fk_subscription
) as one_off
    ON one_off.week_number = subscription_recipe.week AND subscription_recipe.fk_subscription = one_off.fk_subscription AND subscription_recipe.year = one_off.year
where     
   CASE WHEN subscription_recipe.week < 10 THEN concat(subscription_recipe.`year`,'-W',subscription_recipe.`week`) ELSE concat(subscription_recipe.`year`,'-W',subscription_recipe.`week`) END in  (",
                 paste0("'",weeksToQueryFor,"'", sep = "", collapse = ","), ") 
   ) as t1")

bobDF <- dbGetQuery(mysqlConnection, query)

#recipe count is redundant with kits
bobDF$recipe_count <- NULL
names(bobDF) <- c("subscription_id", "plan", "kits", "serving_size", "scm_week_choice_for", "recipe",
                  "meals_preset")
bobDF$meals_preset <- tolower(bobDF$meals_preset)

#keeping only valid skus
if(nrow(bobDF[which(bobDF$plan %in% validSKUS$product_sku), ]) > 0){
bobDF <- bobDF[which(bobDF$plan %in% validSKUS$product_sku), ]
  
}

bobDF_clean <- bobDF

#appending modified on and current week
bobDF_clean$modifiedOn <- Sys.Date()
bobDF_clean$`Pull Week` <- as.character(currentHFWeek)

bobDF_clean[which(bobDF_clean$meals_preset == "" | is.na(bobDF_clean$meals_preset)),
            "meals_preset"] <- "nopreference"



#changing 6 and 5 and 4kits/serving numbers 
#bobDF_clean[which(bobDF_clean$kits == 6 &
                   # bobDF_clean$serving_size == 4), c("kits")] <- "3"

#bobDF_clean[which(bobDF_clean$kits == 6 &
#                  bobDF_clean$serving_size == 2), c("kits")] <- "3"

#bobDF_clean[which(bobDF_clean$kits == 4 &
                 # bobDF_clean$serving_size == 4),  c("kits")] <- "2"

#fiveBoxes <- bobDF_clean[which(bobDF_clean$kits == 5), ]

#splitting 5 boxers into 2 and 3 boxes
#fiveBoxesTransformed <- fiveBoxes %>% rowwise() %>% do({
# X <- data.frame(., stringsAsFactors = F)
# twoBox <- X
# threeBox <- X
# twoBox$kits <- "2"
# threeBox$kits <- "3"
 
#  twoBox$recipe <- sub('^(.*?-.*?-.*?)-.*', '\\1', twoBox$recipe)
# threeBox$recipe <- sub('.*-(.*?-.*-.*)', '-', threeBox$recipe)
  
# Y <- bind_rows(twoBox, threeBox)
# Y
#})

#if(nrow(fiveBoxes) > 0){
# bobDF_clean <- bobDF_clean[-which(bobDF_clean$kits == 5), ]
# bobDF_clean <- bind_rows(bobDF_clean, fiveBoxesTransformed)
# if(nrow(bobDF_clean[-which(nchar(bobDF_clean$recipe) == 1), ]) > 0){
#   bobDF_clean <- bobDF_clean[-which(nchar(bobDF_clean$recipe) == 1), ] 
# }
#}

bobDF_clean$modifiedOn <- Sys.Date()
bobDF_clean$`Pull Week` <- as.character(currentHFWeek)
bobDF_clean$Pull.Week  <- NULL
# aggregating to see counts of recipe slots
choiceDF <- bobDF_clean %>% group_by(modifiedOn, `Pull Week`, scm_week_choice_for, kits, serving_size, meals_preset) %>% 
  do({
    X <- data.frame(., stringsAsFactors = F)
    ME <- data.frame(table(as.numeric(unlist(strsplit(X$recipe, "-")))+80))
    names(ME) <- c("Recipe Slot", "Count")
    RE <- bobDF_clean[which((bobDF_clean$kits ==
                             unique(X[['kits']])[1] &
                             bobDF_clean$serving_size == 
                             unique(X[['serving_size']])[1] &
                             bobDF_clean$`Pull Week` ==
                             unique(X[['Pull.Week']])[1] &
                             bobDF_clean$scm_week_choice_for ==
                             unique(X[['scm_week_choice_for']])[1])), ]
   RE <-  data.frame(table(as.numeric(unlist(strsplit(RE$recipe, "-")))))
   ME$mealSplitPercent <- round(ME$Count/sum(RE$Freq), digits = 6)
   ME
 })

names(choiceDF) <- c("Pull Date", "Pull Week", "HF Week", "Kits/Box", "Serving Size", "Preference",
                     "Recipe Slot", "Count", "Meal Split")
#reordering columns
choiceDF <- choiceDF[c(seq(1,ncol(choiceDF)-2), ncol(choiceDF), ncol(choiceDF) - 1)]

options(scipen = 999)
choiceDF <- choiceDF %>% dplyr::filter(`Serving Size` > 0)
choiceDF$Preference <- gsub("No Preference", 'nopreference', choiceDF$Preference)

#saving local file, uploading to sheets, its faster this way, rather than using the excruciatingly slow gs_edit_cells.
write_csv(choiceDF, path = "ep_bobChoice.csv")

# refreshing the googlesheet to make it easier to write to, for some reason
gs_edit_cells(ss = gs_title("ep_forecasting_scripts_output_bobChoice"),
              ws = "ep_forecasting_scripts_output_bobChoice",
              input = data.frame(NA), anchor = "A1", trim = TRUE, col_names = TRUE)

gs_upload("ep_bobChoice.csv",sheet_title = "ep_forecasting_scripts_output_bobChoice", overwrite = T, verbose = T)
#updating googlesheet

#################

# pulling default recipe mapping

#getting dates converted to hellofresh weeks from dwh
drv2 <- RJDBC::JDBC("com.cloudera.impala.jdbc4.Driver", classPath = "C:/Users/Kathryn Porter/Documents/ClouderaImpala_JDBC_2.6.12.1013/ClouderaImpala_JDBC_2.6.12.1013/ClouderaImpalaJDBC4-2.6.12.1013/ImpalaJDBC4.jar") 
impalaConnection <- RJDBC::dbConnect(drv2, "jdbc:impala://cloudera-impala-proxy.live.bi.hellofresh.io:21050/;AuthMech=3;ssl=1",  dwhuser, dwhpass)

  #for bodega, postgres
drv <- dbDriver("MySQL")
mysqlConnection <- dbConnect(drv,
                           host = 'intfood-db-er001.live.bi.hellofresh.io',
                            user= bobuser, password = bobpass, dbname="bob_live_er")

 #now use jennys query to pull count of preference and serving size by week
query <- paste0("with past_4_weeks as (
select pc.fk_delivery_date, 
dd.hellofresh_week as hellofresh_delivery_week, pd.product_sku,
split_part(pd.product_sku, '-', 3) as kits, split_part(pd.product_sku, '-', 4) as serving_size
, pc.fk_subscription 
, pc.subscription_preference_name
from
fact_tables.box_meal_combination pc
inner join (select distinct product_sku, sk_product from dimensions.product_dimension where split_part(product_sku,'-',3) != '0' and split_part(product_sku,'-',4) != '0') pd
on pc.fk_product = pd.sk_product
inner join dimensions.date_dimension dd on pc.fk_delivery_date = dd.sk_date
where country = 'ER' and dd.date_string_backwards between LEFT(CAST(adddate(NOW(),-28) AS STRING),10) and left(cast(now() as string),10)
)
, past_4_weeks_num as (
select hellofresh_delivery_week, split_part(product_sku, '-', 3) as kits, split_part(product_sku, '-', 4) as serving_size, count(past_4_weeks.fk_subscription) as total_count_per_box_type
from past_4_weeks
group by 1,2,3
)
, past_4_weeks_denom as (
select hellofresh_delivery_week, count(*) as total_count
from past_4_weeks
group by 1)
select past_4_weeks_num.hellofresh_delivery_week, past_4_weeks_num.kits, past_4_weeks_num.serving_size,past_4_weeks_num.subscription_preference_name, past_4_weeks_num.total_count_per_box_type, round((past_4_weeks_num.total_count_per_box_type * 100 / past_4_weeks_denom.total_count), 2) as box_share, dense_rank() over (order by past_4_weeks_num.hellofresh_delivery_week desc) as week_num
from past_4_weeks_denom inner join past_4_weeks_num on past_4_weeks_denom.hellofresh_delivery_week = past_4_weeks_num.hellofresh_delivery_week")


defaultDF <- dbGetQuery(impalaConnection, query)

defaultDF$subscription_preference_name <- gsub(" |nopref^|no pref|no preference", "nopreference", defaultDF$subscription_preference_name)
defaultDF$subscription_preference_name <- trimws(defaultDF$subscription_preference_name)

#defaultDF$subscription_preference_name <- gsub("nopork", "porkfree", defaultDF$subscription_preference_name)
#defaultDF$subscription_preference_name <- gsub("caloriefocus|caloriesmart|light|lowcalorie", "fit", defaultDF$subscription_preference_name)
#defaultDF$subscription_preference_name <- gsub("quickest|fussfree|quickestrecipes", "quick", defaultDF$subscription_preference_name)
#defaultDF$subscription_preference_name <- gsub("nobeef", "beeffree", defaultDF$subscription_preference_name)
#defaultDF$subscription_preference_name <- gsub("shellfishfree", "seafoodfree", defaultDF$subscription_preference_name)
defaultDF[which(defaultDF$subscription_preference_name %in% c("", " ", NA)), 'subscription_preference_name'] <- 'nopreference'
defaultDF$subscription_preference_name <- tolower(defaultDF$subscription_preference_name)

# any defualts 5x2 and no preference
defaultDF[which(defaultDF$kits == 5), c("subscription_preference_name")] <- 'nopreference'
defaultDF[which(defaultDF$kits == 6), c("subscription_preference_name")] <- 'nopreference'

#fixing kits/box
#fiveBoxes <- defaultDF[which(defaultDF$kits == 5), ]

#splitting 5 boxers into 2 and 3 boxes
#fiveBoxesTransformed <- fiveBoxes %>% rowwise() %>% do({
# X <- data.frame(., stringsAsFactors = F)
# twoBox <- X
# threeBox <- X
# twoBox$kits<- "2"
# threeBox$kits <- "3"
  
# Y <- bind_rows(twoBox, threeBox)
# Y
#})

#names(fiveBoxesTransformed) <- names(defaultDF)

#if(nrow(fiveBoxes) > 0){
#  defaultDF <- defaultDF[-which(defaultDF$kits == 5), ]
#  defaultDF <- bind_rows(defaultDF, fiveBoxesTransformed)
  
#}

#counting 6 kits/box as 2 3 kits/box, and 4 by 4 as 2 2 by 4s
#defaultDF[which(defaultDF$kits == 6), c("subscr_count")] <- 
#  defaultDF[which(defaultDF$kits == 6), c("subscr_count")] * 2

#defaultDF[which(defaultDF$kits == 6), c("kits")] <- 3 


#defaultDF[which(defaultDF$kits== 4 & defaultDF$serving_size == 4), c("subscr_count")] <- 
# defaultDF[which(defaultDF$kits == 4 & defaultDF$serving_size== 4), c("subscr_count")] * 2

#defaultDF[which(defaultDF$kits== 4 & defaultDF$serving_size== 4), c("kits")] <- "2"

defaultDF[which(defaultDF$subscription_preference_name == "" | is.na(defaultDF$subscription_preference_name)), "subscription_preference_name"] <- "nopreference"

names(defaultDF) <- c("HF Week", "Kits/Box", "Serving Size", "Preference","Count", "Share", "Week Num")
defaultDF <- defaultDF %>% group_by(`HF Week`, `Kits/Box`, `Serving Size`, Preference,`Week Num`) %>% summarise(Count = sum(Count))


options(scipen = 999)
defaultDFPreferenceShare <- defaultDF %>%  group_by(`HF Week`, Preference) %>% do({
  
 X <- data.frame(., stringsAsFactors = F)
  
  Y <- defaultDF[which(defaultDF$`HF Week` == unique(X$`HF.Week`)), ]
  
  mealSplitPercentage  <- data.frame('mealSplitPercentage' =  round((sum(X$Count) / sum(Y$Count)), 6),
                                     'Count' = sum(X$Count))
  
 mealSplitPercentage
})


weeksAheadList <- c()
j <- 1
for(i in 1:5){
  
 weekInputTemp <<- gsub("w", "", tolower(max(defaultDF$`HF Week`)))
 weekYear <<- gsub( "-.*$", "", weekInputTemp)
 weekNumber <<- gsub( ".*-", "", weekInputTemp)
  
  
  
 if(as.numeric(weekNumber) + i <= 52){
    
   weeksAhead <<- paste0('week_', as.numeric(weekNumber) + i)
   assign(weeksAhead,  paste0(weekYear,"-0", as.numeric(weekNumber) + i))
   weeksAheadList <<- append(weeksAheadList, get(weeksAhead))
 }
 else {
   weekYear <<- as.numeric(weekYear) + 1
   weekNumber <<-  j
   j <<-  j + 1
   weeksAhead <<- paste0('week_', as.numeric(weekNumber))
   assign(weeksAhead,  paste0(weekYear,"-0", as.numeric(weekNumber)))
   weeksAheadList <<- append(weeksAheadList, get(weeksAhead))
 }
}

#getting week1 -4 mapping
week1 <- paste(unique(defaultDF[which(defaultDF$`Week Num` == 1), "HF Week"]))
week2 <- paste(unique(defaultDF[which(defaultDF$`Week Num` == 2), "HF Week"]))
week3 <- paste(unique(defaultDF[which(defaultDF$`Week Num` == 3), "HF Week"]))
week4 <- paste(unique(defaultDF[which(defaultDF$`Week Num` == 4), "HF Week"]))

#mapping kits per box / serving size / preference of future weeks 'meal splits' to weighted average of past weeks
futurePreferenceShare <- defaultDFPreferenceShare %>% group_by(Preference) %>% do({
 X <- data.frame(., stringsAsFactors = F)
  
  # getting the meal split percentages per cohort in previous weeks
 mealSplitPercentage1 <- sum(X[which(X$HF.Week == week1), 'mealSplitPercentage' ])
 mealSplitPercentage2 <- sum(X[which(X$HF.Week == week2), 'mealSplitPercentage' ])
 mealSplitPercentage3 <- sum(X[which(X$HF.Week == week3), 'mealSplitPercentage' ])
 mealSplitPercentage4 <- sum(X[which(X$HF.Week == week4), 'mealSplitPercentage' ])
  
  
countPercentage1 <- sum(X[which(X$HF.Week == week1), 'Count' ])
 countPercentage2 <- sum(X[which(X$HF.Week == week2), 'Count' ])
 countPercentage3 <- sum(X[which(X$HF.Week == week3), 'Count' ])
 countPercentage4 <- sum(X[which(X$HF.Week == week4), 'Count' ])
  
 mealSplitVector <- c(mealSplitPercentage1, mealSplitPercentage2, mealSplitPercentage3, mealSplitPercentage4)
  
  
 countVector <- c(countPercentage1, countPercentage2, countPercentage3, countPercentage4)
  
 #  if week doesnt exist, map it as NA
 mealSplitVector[which(mealSplitVector == 0)] <- NA
  
 countVector[which(countVector == 0)] <- NA
  
  #weighting the meal split percentage by category by the given values from Lauren
 mealSplitPercentage <- weighted.mean(mealSplitVector, 
               w = c(.5,.3,.15,.05), na.rm = T)
 countWeighted <- weighted.mean(countVector, 
                                     w = c(.5,.3,.15,.05), na.rm = T)
  
 data.frame(cbind(mealSplitPercentage, countWeighted))
})

 futurePreferenceShareMatrix <- expand.grid(unlist(weeksAheadList),  unique(futurePreferenceShare$Preference))
 names(futurePreferenceShareMatrix)  <- c("HF Week", "Preference")

futurePreferenceShare <- futurePreferenceShareMatrix %>% left_join(futurePreferenceShare, by = c("Kits/Box","Serving Size"))
futurePreferenceShare$`HF Week` <- paste0(substr(futurePreferenceShare$`HF Week`, 1,5), "W",substr(futurePreferenceShare$`HF Week`, 6,nchar(as.character(futurePreferenceShare$`HF Week`))))



totalPreferenceDF <- bind_rows(defaultDFPreferenceShare, futurePreferenceShare)


# getting preference order by week / preference
futureDFWithKits <- expand.grid(unlist(weeksAheadList), unique(defaultDF$`Kits/Box`), unique(defaultDF$`Serving Size`))

names(futureDFWithKits) <- names(defaultDF)[1:3]
futureDFWithKits$`HF Week` <- paste0(substr(futureDFWithKits$`HF Week`, 1,5),
                                    "W",substr(futureDFWithKits$`HF Week`,
                                              6,nchar(as.character(futureDFWithKits$`HF Week`))))


totalOutputDF <- futureDFWithKits
totalOutputDF[] <- lapply(totalOutputDF, as.character)
totalOutputDF <- totalOutputDF[-which(totalOutputDF$`Kits/Box` == 4 &
                    totalOutputDF$`Serving Size` == 4), ] 
totalOutputDF <- totalOutputDF[-which(totalOutputDF$`Kits/Box` == 5 &
                                       totalOutputDF$`Serving Size` == 4), ] 


## mapping recipes from googlesheet to the output df now

recipeMapping <- gs_read(gs_title("Default per Preference Log for Forecast"), ws = 'Preference Defaults')

recipeMapping$Type <- as.character(tolower(gsub('[[:punct:] ]+| ','',ifelse(tolower(recipeMapping$Type)  %in% c('signature'), 'variety',
                                 ifelse(tolower(recipeMapping$Type) == 'family-friendly', 'family',

                                       ifelse(tolower(recipeMapping$Type)   == 'fit', 'caloriesmart',

                                                ifelse(tolower(recipeMapping$Type) == 'null|classic', 'nopreference',
                                                      tolower(recipeMapping$Type) )))))))
totalOutputDF$Preference <- as.character(totalOutputDF$Preference)
#using googlesheet to map out recipes per product
recipesPerProduct <- totalOutputDF %>% group_by(`HF Week`, `Kits/Box`,`Serving Size`, Preference) %>% do({
  
 X <- data.frame(., stringsAsFactors = F)
 relevantWeek <- unique(X$`HF.Week`)
 relevantPreference <- tolower(gsub('[[:punct:] ]+| ','',ifelse(unique(X$Preference) %in% c('signature', 'chefschoice'), 'variety',
                                                                ifelse(unique(X$Preference) == 'family-friendly', 'family',
                                                                       ifelse(unique(X$Preference) == 'fit', 'caloriesmart',

                                                                              ifelse(unique(X$Preference) %in% c('classic','nopreference'), 'null',
                                                                       unique(X$Preference)))))))
 amountOfRecipes <- as.numeric(X$`Kits.Box`)
 amountOfRecipes <- ifelse(is.na(amountOfRecipes), 2, amountOfRecipes)

 relevantRecipeMapping <- recipeMapping[which(recipeMapping$Week == relevantWeek &
                                                tolower(gsub(" ", "", recipeMapping$Type)) == relevantPreference), ]

 recipes <- data.frame(paste0(relevantRecipeMapping[c(4:(3+amountOfRecipes))], collapse =  ','))
 names(recipes) <- c("recipes")
 recipes$Count <- sum(X$Count, na.rm = T)
# recipes

#})

#unconcatenating recipe slots
dfWithRecipes <- recipesPerProduct %>% group_by(`HF Week`,`Kits/Box`,`Serving Size`, Preference) %>% do({
  
 X <- data.frame(.,stringsAsFactors = F)
 RE <- data.frame('Recipe Slot' = gsub(" ", "", unlist(strsplit(X$recipes, ','))))

 mealSplitForWeekAndPref <- totalPreferenceDF[which(totalPreferenceDF$Preference == unique(X$Preference) &
                                                      totalPreferenceDF$`HF Week` == unique(X$`HF.Week`)), ]
 RE$mealSplitPercentage <- mealSplitForWeekAndPref$mealSplitPercentage / nrow(RE)
  

 RE$Count <- ifelse(is.na(mealSplitForWeekAndPref$countWeighted / nrow(RE)), 
                   mealSplitForWeekAndPref$Count / nrow(RE), 
                    mealSplitForWeekAndPref$countWeighted / nrow(RE))
  
 RE
})






#appending modified on and current hf week
dfWithRecipes$`Pull Date` <- Sys.Date()
dfWithRecipes$`Pull Week` <- currentHFWeek

# reordering columns 
dfWithRecipes <- dfWithRecipes[c(ncol(dfWithRecipes) - 1, ncol(dfWithRecipes) , seq(1, ncol(dfWithRecipes) - 2))]

dfWithRecipes <- dfWithRecipes[order(dfWithRecipes$`HF Week`, decreasing = F), ]

#weeks out nomenclature
#pulling current sheet and looking at how long it is

write_csv(dfWithRecipes, path = "bobDefault.csv")

# refreshing the googlesheet to make it easier to write to, for some reason
gs_edit_cells(ss = gs_title("ep_forecasting_scripts_output_bobDefault"),
            ws = "ep_forecasting_scripts_output_bobDefault",
           input = data.frame(NA), anchor = "A1", trim = TRUE, col_names = TRUE)

gs_upload("bobDefault.csv",sheet_title = "ep_forecasting_scripts_output_bobDefault", overwrite = T, verbose = T)

#refresh importranges
GSUpload <- gs_title("HFUS In-Week Forecasting")
try(gs_ws_delete(ss = GSUpload,ws = "UpdatePlease", verbose = TRUE), silent = T)

GSUpload <- gs_title("HFUS In-Week Forecasting")
gs_ws_new(ss = GSUpload, row_extent=1, col_extent=1, ws_title="UpdatePlease")
