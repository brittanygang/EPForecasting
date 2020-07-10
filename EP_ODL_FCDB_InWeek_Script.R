## this inweek script gets run every day at around 8:30 am


source("ep_pullRecentODLPDL.R")

#4/11 CZ COMMENT: Because there is no ODL on Saturday, 
#df <- recentPDL

df <- recentODL %>% bind_rows(recentPDL[names(recentPDL) %in% names(recentODL)])

#getting rid of -1s in addon_swap column
#df$addon_swap <- gsub("-1", "", df$addon_swap)

df$product_type <- ifelse(grepl("class", df$finale_box_type, ignore.case = T, perl = T), 'classic',
                          ifelse(grepl("din", df$finale_box_type, ignore.case = T, perl = T), 'dinner', NA))

# in-week script - output is by product type, weekday, delivery region
thirdOutput <- df %>% group_by(hellofresh_week, 
                                    weekday,
                                    substr(delivery_region, 0, 2), 
                                    product_type,
                                    substr(boxtype, 1, 1),
                                    substr(boxtype, 3, 3)
                                    
) %>%
  do({
    X <- data.frame(., stringsAsFactors = F)
    ME <- data.frame(table(as.numeric(unlist(strsplit(X$meal_swap, " ")))), stringsAsFactors = F)
    ZE <- data.frame(table(as.numeric(unlist(strsplit(X[which(nchar(X$meal_swap) <= 2), 'meal_swap'], " ")))), stringsAsFactors = F)
    if(nrow(ZE) < 1){
      ZE <- data.frame('Var1' = "1" , "Count" = 0, stringsAsFactors = F)
    }
    names(ME) <- c("Recipe Slot", "Total Count")
    names(ZE) <- c("Recipe Slot", "Count of Boxes with 1 Recipe")
    ME <- ME %>% left_join(ZE, by = 'Recipe Slot')
    data.frame(ME, stringsAsFactors = F)
  })

names(thirdOutput) <- c("HelloFresh Week", "Weekday", "DC","Product Type",
                        "Total Kits", "Serving Size", "Recipe Slot", "Total Count", "Count Of Boxes With 1 Recipe")

thirdOutput[which(is.na(thirdOutput$`Count Of Boxes With 1 Recipe`)), "Count Of Boxes With 1 Recipe"] <- 0
#saving run date
thirdOutput$modifiedOn <- Sys.Date()
thirdOutput$source <- ifelse(thirdOutput$Weekday %in% weekdaysOfODL, 'ODL', "PDL")
#saving local file, uploading to sheets, its faster this way, rather than using the excruciatingly slow gs_edit_cells.
write_csv(thirdOutput, path = "EP_thirdOutput.csv")


# refreshing the googlesheet to make it easier to write to, for some reason
gs_edit_cells(ss = gs_title("ep_forecasting_scripts_output_inweek"),
              ws = "ep_forecasting_scripts_output_inweek",
               input = data.frame(NA), anchor = "A1", trim = TRUE, col_names = TRUE)

gs_upload("EP_thirdOutput.csv",sheet_title = "ep_forecasting_scripts_output_inweek", overwrite = T, verbose = T)

# counts_input
thirdOutput_counts <- df %>% group_by(hellofresh_week, 
weekday,
substr(delivery_region, 0, 2)

) %>%
  do({
    X <- data.frame(., stringsAsFactors = F)
    ME <- length(unlist(strsplit(X$meal_swap, " ")))
    ZE <- nrow(X)
    returnDF <- data.frame(cbind(ZE, ME), stringsAsFactors = F)
    names(returnDF) <- c("Box Count", "Meal Count")
    data.frame(returnDF, stringsAsFactors = F)
  })

thirdOutput_counts$Source <- ifelse(thirdOutput_counts$weekday %in% weekdaysOfODL, "ODL", "PDL")
names(thirdOutput_counts) <- c("HF Week", "Delivery Day", "DC", "Box Count", "Meal Count", "Source")

gs_edit_cells(ss = gs_title("EP DWIGHT"),
              ws = "counts_import",
              input = thirdOutput_counts, anchor = "A1",trim = T,  col_names = T)



# now with add ons

#thirdOutput_addon <- df %>% group_by(hellofresh_week, 
#                                     weekday,
#                                    substr(delivery_region, 0, 2), 
#                                    substr(boxtype, 1, 1),
#                                    substr(boxtype, 3, 3)
                                    
#) %>%
#  do({
#   X <- data.frame(., stringsAsFactors = F)
#   ME <- data.frame(table(unlist(strsplit(X$addon_swap, " "))))
#   ME$amount <- gsub("[^0-9]", "", ME$Var1)
#   ME$Var1<- gsub("\\d", "", ME$Var1)
#   # multiplying add on amount by frequency of choice
#   ME$Freq <- ME$Freq * as.numeric(ME$amount)
#   ME <- ME %>% group_by(Var1) %>% summarise(sum(Freq))
#   names(ME) <- c("Add On", "Total Count")
#   ME
# })


#names(thirdOutput_addon) <- c("HelloFresh Week", "Weekday", "DC",
#                        "Total Kits", "Serving Size", "Add On", "Total Count")

#thirdOutput_addon$modifiedOn <- Sys.Date()
#adding source of data
#thirdOutput_addon$source <- ifelse(thirdOutput_addon$Weekday %in% weekdaysOfODL, 'ODL', "PDL")
#if(nrow(thirdOutput_addon[-which(is.na(thirdOutput_addon$`Total Count`)), ]) > 0){
  
#  thirdOutput_addon <- thirdOutput_addon[-which(is.na(thirdOutput_addon$`Total Count`)), ]
  
#}
#write_csv(thirdOutput_addon, path = "thirdOutput_addon.csv")


#gs_edit_cells(ss = gs_title("forecasting_scripts_output_inweek_addon"),
#              ws = "forecasting_scripts_output_inweek_addon",
#             input = data.frame(NA), anchor = "A1", trim = TRUE, col_names = TRUE)
#gs_upload("thirdOutput_addon.csv",sheet_title = "forecasting_scripts_output_inweek_addon", overwrite = T)


# get importranges to update
#GSUpload <- gs_title("HFUS In-Week Forecasting")
#try(gs_ws_delete(ss = GSUpload,ws = "UpdatePlease", verbose = TRUE), silent = T)

#GSUpload <- gs_title("HFUS In-Week Forecasting")
#try(gs_ws_new(ss = GSUpload, row_extent=1, col_extent=1, ws_title="UpdatePlease"), silent = T)

# start new week
if(weekdays(Sys.Date()) %in% c('Tuesday', 'Monday','Saturday')){
  
  newWeek <- askYesNo("Do you want to start a new week?", default = FALSE, 
                      prompts = getOption("askYesNo", gettext(c("Yes", "No", 'Cancel'))))
  
} else {
  newWeek <- F
}

if(newWeek){
  
  drv <- dbDriver("PostgreSQL")
  postgresCon <- dbConnect(drv, dbname = "US_Datamart",
                           host = 'datamart-db-us000-live-bi.ccj54oujhzp1.eu-west-1.rds.amazonaws.com', port = 5432,
                           user = pguser, password = pgpass)
  getInput <- function() {
    cat("\n", "Enter new HF Week (ex. 2019-W48):", "\n")
    userInput <<- scan(what = 'character',n=1)
    cat("\n", "You entered", userInput, "\n")
  }
  if(interactive()){
    userInput <<- readline(prompt = "Enter current HF Week (ex. 2019-W47):")
  } else {
    getInput()
  }
  
  
  currentHFWeekToQueryFor <- gsub("'|\"","",gsub("--|  ", "-",
                                                 gsub(" ","-",
                                                      toupper(userInput))))
  
  
  #getting most recent odl
  newPDL <- dbGetQuery(postgresCon, paste0("SELECT * from bodega.everyplate_pdl where hellofresh_week = '",currentHFWeekToQueryFor,"'
                        and delivery_region != 'NO SERVICE'"))
  
  newPDL$product_type <- ifelse(grepl("class", newPDL$finale_box_type, ignore.case = T, perl = T), 'classic',
                            ifelse(grepl("din", newPDL$finale_box_type, ignore.case = T, perl = T), 'dinner'))
  
#  newPDL$addon_swap <- gsub("-1", "", newPDL$addon_swap)
  
  if(nrow(data.frame(newPDL[which(newPDL$sku %in% validSKUS$product_sku), ])) > 0){
    newPDL <- newPDL[which(newPDL$sku %in% validSKUS$product_sku), ]
  }
  
  
  # in-week script - output is by product type, weekday, delivery region
  thirdOutputNewPDL <- newPDL %>% group_by(hellofresh_week, 
                                 weekday,
                                 substr(delivery_region, 0, 2), 
                                 product_type,
                                 substr(boxtype, 1, 1),
                                 substr(boxtype, 3, 3)
                                 
  ) %>%
    do({
      X <- data.frame(., stringsAsFactors = F)
      ME <- data.frame(table(as.numeric(unlist(strsplit(X$meal_swap, " ")))), stringsAsFactors = F)
      ZE <- data.frame(table(as.numeric(unlist(strsplit(X[which(nchar(X$meal_swap) <= 2), 'meal_swap'], " ")))), stringsAsFactors = F)
      if(nrow(ZE) < 1){
        ZE <- data.frame('Var1' = "1" , "Count" = 0, stringsAsFactors = F)
      }
      names(ME) <- c("Recipe Slot", "Total Count")
      names(ZE) <- c("Recipe Slot", "Count of Boxes with 1 Recipe")
      ME <- ME %>% left_join(ZE, by = 'Recipe Slot')
      data.frame(ME, stringsAsFactors = F)
    })
  
  names(thirdOutputNewPDL) <-  c("HelloFresh Week", "Weekday", "DC", "Product Type",
                                 "Total Kits", "Serving Size", "Recipe Slot", "Total Count", "Count Of Boxes With 1 Recipe")
  thirdOutputNewPDL[which(is.na(thirdOutputNewPDL$`Count Of Boxes With 1 Recipe`)), "Count Of Boxes With 1 Recipe"] <- 0

  #saving run date, appending to googlesheet
  thirdOutputNewPDL$modifiedOn <- Sys.Date()
  thirdOutputNewPDL$source <- "PDL"
  
  write_csv(thirdOutputNewPDL, path = "EP_thirdOutputNewPDL.csv")
  gs_upload("EP_thirdOutputNewPDL.csv",sheet_title = "ep_forecasting_scripts_output_inweek", overwrite = T)
  
  gs_edit_cells(ss = gs_title("ep_forecasting_scripts_output_inweek"),
                ws = "ep_forecasting_scripts_output_inweek",
                input = data.frame(NA), anchor = "A1", trim = TRUE, col_names = TRUE)
  
  
  gs_upload("EP_thirdOutputNewPDL.csv",sheet_title = "ep_forecasting_scripts_output_inweek", overwrite = T)
  
  # counts_input
  thirdOutput_counts <- newPDL %>% group_by(hellofresh_week, 
                                        weekday,
                                        substr(delivery_region, 0, 2)
                                        
  ) %>%
    do({
      X <- data.frame(., stringsAsFactors = F)
      ME <- length(unlist(strsplit(X$meal_swap, " ")))
      ZE <- nrow(X)
      returnDF <- data.frame(cbind(ZE, ME), stringsAsFactors = F)
      names(returnDF) <- c("Box Count", "Meal Count")
      data.frame(returnDF, stringsAsFactors = F)
    })
  
  thirdOutput_counts$Source <- "PDL"
  names(thirdOutput_counts) <- c("HF Week", "Delivery Day", "DC", "Box Count", "Meal Count", "Source")
  
  
  gs_edit_cells(ss = gs_title("EP DWIGHT"),
                ws = "counts_import",
                input = thirdOutput_counts, anchor = "A1",trim = TRUE,   col_names = T)
  
  # wow_counts_input
  wow_data <- dbGetQuery(postgresCon, "SELECT * from bodega.everyplate_odl where hellofresh_week in (
            
    SELECT
        distinct hellofresh_week
   FROM
      bodega.everyplate_odl
      order by 1 desc limit 3)")
  
  wow_data$product_type <- ifelse(grepl("class", wow_data$finale_box_type, ignore.case = T, perl = T), 'classic',
                                ifelse(grepl("din", wow_data$finale_box_type, ignore.case = T, perl = T), 'dinner'))
  
#  wow_data$addon_swap <- gsub("-1", "", wow_data$addon_swap)
  
  thirdOutput_counts_new <- wow_data %>% group_by(hellofresh_week, 
                                        weekday,
                                        substr(delivery_region, 0, 2)
                                        
  ) %>%
    do({
      X <- data.frame(., stringsAsFactors = F)
      ME <- length(unlist(strsplit(X$meal_swap, " ")))
      ZE <- nrow(X)
      returnDF <- data.frame(cbind(ZE, ME), stringsAsFactors = F)
      names(returnDF) <- c("Box Count", "Meal Count")
      data.frame(returnDF, stringsAsFactors = F)
    })
  
  thirdOutput_counts_new$Source <- "ODL"
  names(thirdOutput_counts_new) <- c("HelloFresh Week", "Delivery Day", "DC", "Box Count", "Meal Count", "Source")
  
  gs_edit_cells(ss = gs_title("EP DWIGHT"),
                ws = "wow_counts_import",
                input = thirdOutput_counts_new, anchor = "A1",trim = T,  col_names = T)
  
  
  # now with add ons
  
#  thirdOutputNewPDL_addon <- newPDL %>% group_by(hellofresh_week, 
#                                       weekday,
#                                       substr(delivery_region, 0, 2), 
#                                       substr(boxtype, 1, 1),
#                                       substr(boxtype, 3, 3)
#                                       
#  ) %>%
#    do({
#      X <- data.frame(., stringsAsFactors = F)
#      ME <- data.frame(table(unlist(strsplit(X$addon_swap, " "))))
#      ME$amount <- gsub("[^0-9]", "", ME$Var1)
#      ME$Var1<- gsub("\\d", "", ME$Var1)
#      # multiplying add on amount by frequency of choice
#      ME$Freq <- ME$Freq * as.numeric(ME$amount)
#      ME <- ME %>% group_by(Var1) %>% summarise(sum(Freq))
#      names(ME) <- c("Add On", "Total Count")
#      ME
#    })
  
#  names(thirdOutputNewPDL_addon) <- c("HelloFresh Week", "Weekday", "DC",
#                                "Total Kits", "Serving Size", "Add On", "Total Count")
  
#  thirdOutputNewPDL_addon$modifiedOn <- Sys.Date()
  #adding source of data
#  thirdOutputNewPDL_addon$source <- "PDL"
  
  
#  write_csv(thirdOutputNewPDL_addon, path = "thirdOutputNewPDL_addon.csv")
  
#  gs_edit_cells(ss = gs_title("forecasting_scripts_output_inweek_addon"),
#                ws = "forecasting_scripts_output_inweek_addon",
#                input = data.frame(NA), anchor = "A1", trim = TRUE, col_names = TRUE)
  
#  gs_upload("thirdOutputNewPDL_addon.csv",sheet_title = "forecasting_scripts_output_inweek_addon", overwrite = T)
  
}

# get importranges to update
#GSUpload <- gs_title("HFUS In-Week Forecasting")
#try(gs_ws_delete(ss = GSUpload,ws = "UpdatePlease", verbose = TRUE), silent = T)
#GSUpload <- gs_title("HFUS In-Week Forecasting")
#try(gs_ws_new(ss = GSUpload, row_extent=1, col_extent=1, ws_title="UpdatePlease"), silent = T)
