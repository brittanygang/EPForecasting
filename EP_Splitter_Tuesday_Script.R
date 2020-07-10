# splitter script - gets run weekly, every tuesday, and appended on the bottom

require(dplyr)
require(tidyverse)
require(readr)
require(lubridate)
require(RPostgreSQL)
require(RJDBC)
require(googlesheets)
require(grid)
require(gridExtra)
require(gdata)
require(gmailr)

if(grepl('window', tolower(osVersion))){
  source("C://dbcredentials.R")
} else {
  source("~/dbcredentials.R")
}
# # get all the dataframes
GSUpload <- gs_title("EP PHYLLIS")

Topline <- gs_read(GSUpload, ws = 'topline')

DCSplit <- gs_read(GSUpload, ws = "dc_splits")

#sleeping to get past rate limit for googlesheets
Sys.sleep(2)

productSplit <- gs_read(GSUpload, ws = "product_splits")

Sys.sleep(5)
mealSplit <- gs_read(GSUpload, ws = 'meal_splits')

Sys.sleep(5)

# first output - multiply out topline number by dc split, product split

firstSplitterIntermediary <- Topline %>% full_join(DCSplit, by = 'HF Week') %>% 
  full_join(productSplit, by = c("HF Week", "DC"))

# getting box forecast per row
output1 <- firstSplitterIntermediary %>% rowwise() %>% do({
  X <- data.frame(., stringsAsFactors = F)
  Topline_number <- as.numeric(X$Topline)
  DC_percent <- as.numeric(gsub("%", "", X$Finalized...x)) / 100
  Product_percent <- as.numeric(gsub("%", "", X$Finalized...y)) / 100
  Box_forecast <- Topline_number * DC_percent * Product_percent
  
  returnDF <-data.frame(cbind(unique(X$HF.Week), unique(X$DC), unique(X$Weeks.Out), unique(X$Kits.Box), unique(X$Serving.Size), Box_forecast))
  names(returnDF) <- c("HF Week", "DC", "Weeks Out", "Kits/Box", "Serving Size", "Box Forecast")
  returnDF
})

#adding product combo, and ordering by columns
output1$`Product Combo` <- paste0(output1$`Kits/Box`, "x", output1$`Serving Size`)
output1 <- output1 %>% select(c("HF Week", "DC","Weeks Out","Product Combo", "Kits/Box", "Serving Size", "Box Forecast"))

if(nrow(output1[-which(is.na(output1$`Box Forecast`)), ]) > 0){
  output1 <- output1[-which(is.na(output1$`Box Forecast`)), ]
}

# second output of splitter is meal forecast
secondSplitterIntermediary <- Topline %>% full_join(DCSplit, by = 'HF Week') %>% full_join(productSplit, by = c("HF Week", "DC")) %>% full_join(mealSplit, by = c("HF Week", "DC", "Kits/Box", "Serving Size"))

output2 <- tbl_df(data.frame(secondSplitterIntermediary)) %>% 
  mutate(topline = as.numeric(Topline),
         dc_pct = as.numeric(gsub("%", "", Finalized...x)) / 100,
         product_pct=as.numeric(gsub("%", "", Finalized...y)) / 100,
         meal_pct = as.numeric(gsub("%", "", Finalized..)) / 100,
         kits = as.numeric(Kits.Box),
         meal_forecast = round(topline * dc_pct * product_pct * meal_pct * kits)) %>% 
  select(HF.Week, DC , Weeks.Out, Kits.Box, Serving.Size, Recipe.Slot, meal_forecast) %>% 
  filter(!is.na(HF.Week))

names(output2) <- c("HF Week", "DC", "Weeks Out", "Kits/Box", "Serving Size","Recipe Slot", "Meal Forecast")
# saving this output for csv
csvOutput <- output2

#aggregating, weighted sum of meal count weighted by kits/box
csvOutput <- csvOutput %>% group_by(`HF Week`, DC, `Weeks Out`,`Kits/Box`, `Serving Size`, `Recipe Slot`) %>% summarise("Meal Forecast" = sum(as.numeric(`Meal Forecast`))) %>% 
  ungroup()
  #mutate(DC = ifelse(DC == "NJ", "EN",
  #                   ifelse(DC == "CA", "EC", "ET")))
output2 <- output2 %>% group_by(`HF Week`, DC, `Weeks Out`, `Serving Size`, `Recipe Slot`) %>% summarise("Meal Forecast" = sum( as.numeric(`Meal Forecast`)))
#output2$DC <- ifelse(output2$DC == "NJ", "EN",
#                     ifelse(output2$DC == "CA", "EC", "ET"))

if(nrow(output2[-which(is.na(output2$`Meal Forecast`)), ]) > 0){
  output2 <- output2[-which(is.na(output2$`Meal Forecast`)), ]                                                                                              
}

#ordering resultant dataframe
output2 <- output2[order(output2$`HF Week`, output2$DC, output2$`Weeks Out`, output2$`Serving Size`, as.numeric(gsub("[[:alpha:]]","", output2$`Recipe Slot`))), ]
output2[] <- lapply(output2[], as.character)

# csv output for oa_tools
if(nrow(csvOutput[-which(is.na(csvOutput$`Meal Forecast`)), ]) > 0){
  csvOutput <- csvOutput[-which(is.na(csvOutput$`Meal Forecast`)), ]                                                                                              
}

csvOutput[] <- lapply(csvOutput[], as.character)

csvOutput$Country <- 'ER'
csvOutput$DC <- ifelse(csvOutput$DC == "NJ", "EN",
                                          ifelse(csvOutput$DC == "CA", "EC", "ET"))
csvOutput$meal_number <- paste0(ifelse(csvOutput$DC == "EN", 1,
                                       ifelse(csvOutput$DC == "EC", 2,
                                              3)), ifelse(nchar(csvOutput$`Recipe Slot`) < 2,
                                                          paste0("0", csvOutput$`Recipe Slot`), csvOutput$`Recipe Slot`))
csvOutput$box_name <- paste0("Dinner Box ", csvOutput$`Serving Size`, " person")

csvOutput$product_family <- "dinner-box"
csvOutput$`Recipe Slot` <- NULL
csvOutput$`Weeks Out` <- NULL
names(csvOutput) <- c("week", "dc", "servings", "size", "meals_to_deliver", "country", "meal_number", 
                      "box_name", "product_family")
csvOutput <- csvOutput[c("week", "country", "dc",  "box_name", "product_family", 
                         "servings", "size", "meal_number", "meals_to_deliver")]

weekToRemove <- min(csvOutput$week[which(!is.na(csvOutput$week))])
csvOutput <- csvOutput[-which(csvOutput$week == weekToRemove), ]

if(nrow(csvOutput[which(!is.na(csvOutput$week)), ]) > 0){
  csvOutput <- csvOutput[which(!is.na(csvOutput$week)), ]
}

# inserting garlic quantity as recipe 80
 csvOutput <- csvtest <- tbl_df(rbind(csvOutput,
                           csvOutput %>% distinct(week,country,dc,box_name,product_family,servings,size) %>% 
                             mutate(meal_number = ifelse(dc == 'EC', '280',
                                                         ifelse(dc == 'ET', '380', '180'))) %>% 
                             left_join(output1 %>% ungroup() %>% mutate(DC = ifelse(DC == 'CA', 'EC',
                                                                                    ifelse(DC == 'TX', 'ET', 'EN'))) %>% 
                                         select(`HF Week`,DC,`Kits/Box`,`Serving Size`,`Box Forecast`), 
                                       by = c('week' = 'HF Week', 'dc' = 'DC', 'servings' = 'Kits/Box',
                                              'size' = 'Serving Size')) %>% 
                             mutate(meals_to_deliver = ifelse(servings == '3' & size == '2',1,2) *
                                      round(as.numeric(`Box Forecast`))) %>% 
                             select(-`Box Forecast`))) %>% 
   arrange(dc,servings,size,meal_number)
#ep_garlic_csv <- tbl_df(gs_read(GSUpload, ws = 'garlic_OT'))
#csvOutput <- tbl_df(rbind(csvOutput, ep_garlic_csv))


#pasting csv's for each csvoutput hf week separately.
for(i in unique(csvOutput$week)){
  csvWeek <- csvOutput[which(csvOutput$week == i), ]
  
  mainDir <- 'OT_uploads'
  subDir <- i
  
  dir.create(file.path(mainDir, subDir), showWarnings = T, recursive = T)
  setwd(file.path(mainDir, subDir))
  write_csv(csvWeek, path = paste0(i,'_OT_Upload2_',Sys.Date(),".csv"),col_names = T) 
  setwd("../..")
}

#writing data to googlesheets
GSExternal <- gs_title("EP OSCAR")

currentSheet1 <- gs_read(GSExternal, ws= 'forecast_product')
currentSheet2 <- gs_read(GSExternal, ws= 'forecast_recipes')

#ordering outputs and adding modified on date
output1$modifiedOn <- Sys.Date()

output1 <- output1[order(output1$modifiedOn, output1$`HF Week`, output1$`Weeks Out`,  output1$`Kits/Box`, output1$`Serving Size`), ]
output1 <- output1[c(ncol(output1),seq(1:(ncol(output1) - 1)))]

output2$modifiedOn <- Sys.Date()

output2 <- output2[order(output2$`HF Week`, output2$`Weeks Out`, output2$`Serving Size`, as.numeric(gsub("[[:alpha:]]","", output2$`Recipe Slot`))), ]
output2 <- output2[c(ncol(output2),seq(1:(ncol(output2) - 1)))]


output1$`Box Forecast` <- ceiling(as.numeric(output1$`Box Forecast`))
#appending to sheets

if(nrow(output1[which(output1$`Weeks Out` == '1 Weeks Out'), ]) > 0){
  output1 <- output1[-which(output1$`Weeks Out` == '1 Weeks Out'), ]
}


if(nrow(output2[which(output2$`Weeks Out` == '1 Weeks Out'), ]) > 0){
  output2 <- output2[-which(output2$`Weeks Out` == '1 Weeks Out'), ]
}

if(nrow(output2[which(!is.na(output2$`HF Week` )), ]) > 0){
  output2 <- output2[which(!is.na(output2$`HF Week` )), ]
}

#ep_garlic_output <- tbl_df(gs_read(GSUpload, ws = 'garlic_splitter'))
#output2 <-  tbl_df(rbind(output2, ep_garlic_output))

gs_edit_cells(ss = GSExternal, ws = 'forecast_product', input = output1, anchor = paste0('A', nrow(currentSheet1) + 2), col_names = F)
gs_edit_cells(ss = GSExternal, ws = 'forecast_recipes', input = output2, anchor = paste0('A', nrow(currentSheet2) + 2), col_names = F)

## generating graphs for emails


#reading in current data

options('scipen' = 999)
#getting meal variances plot
currentSheet2_complete <- gs_read(GSExternal, ws= 'forecast_recipes')
currentSheet2_complete <- currentSheet2_complete[which(currentSheet2_complete$`HF Week` %in% unique(output1$`HF Week`)), ]

eptsSheet <- gs_title("EveryPlate Forecast Tracking Sheet")

eptsData <- gs_read(eptsSheet, ws = "EveryPlate")

eptsDataThisWeek <- eptsData[which(eptsData$scm_week %in% unique(output1$`HF Week`)), ] %>% 
  mutate(forecast_volume = as.numeric(gsub(',','',forecast_volume)))

if(nrow(eptsDataThisWeek[which(!is.na(eptsDataThisWeek$forecast_volume)), ]) > 0){
  eptsDataThisWeek <- eptsDataThisWeek[which(!is.na(eptsDataThisWeek$forecast_volume)), ]
}

for(i in unique(eptsDataThisWeek$scm_week)){
  forecastType <- as.character(tail(eptsDataThisWeek[which(eptsDataThisWeek$scm_week == i), 'forecast_type'], n = 1))
  if(nrow(eptsDataThisWeek) > 0){
    
    #ordering the dataframe 
    eptsDataTemp <- eptsDataThisWeek[which(eptsDataThisWeek$scm_week == i), ]
    eptsDataTemp <- eptsDataTemp %>% complete(forecast_type  = c("6 Weeks Out", "5 Weeks Out",
                                                                 "4 Weeks Out" ,"3 Weeks Out",
                                                                 "2 Weeks Out", "Tuesday", "Wednesday", 
                                                                 "Thursday", "Friday", "Saturday", "Sunday",
                                                                 "Monday", "Final")) %>% distinct()
    
    eptsDataTemp <- eptsDataTemp[order(factor(eptsDataTemp$forecast_type, levels = c("6 Weeks Out", "5 Weeks Out",
                                                                                     "4 Weeks Out" ,"3 Weeks Out",
                                                                                     "2 Weeks Out", "Tuesday", "Wednesday", 
                                                                                     "Thursday", "Friday", "Saturday", "Sunday",
                                                                                     "Monday", "Final"))), ]
    
    
    #creating topline forecast plot and saving
    toplineForecastPlot <- ggplot(data = eptsDataTemp, aes( x = seq(1,nrow(eptsDataTemp)), y = forecast_volume, group = 1)) + geom_line(color = 'blue') +  geom_point() + 
      theme_bw() +
      geom_text(label = format(eptsDataTemp$forecast_volume, big.mark=','),
                size = 3, vjust = -1, angle = 15) +
      xlab("Horizon") +
      ylab("Forecast Volume") + 
      ggtitle(paste0(i, ": ",forecastType," -  Topline Forecast")) + scale_x_discrete(limits = eptsDataTemp$forecast_type) + 
      theme(axis.text.x = element_text(angle = 60, hjust = 1), plot.title = element_text(hjust = 0.5),
            legend.position = "bottom", text = element_text(size=10)) +
      scale_y_continuous(limits = c(min(as.numeric(eptsDataTemp$forecast_volume), na.rm = T) - 10000,
                                    max(as.numeric(eptsDataTemp$forecast_volume), na.rm = T) + 10000))
    
    toplineForecastPlot
    ggsave(paste0("plots_for_emails/",i,"toplineForecastPlot.png"), width = 8, height = 5)
    
  }
  
  if(nrow(currentSheet2_complete) > 0){
    
    
    forecastType <- as.character(tail(eptsDataThisWeek[which(eptsDataThisWeek$scm_week == i), 'forecast_type'], n = 1))
    currentSheet2 <- currentSheet2_complete[which(currentSheet2_complete$`HF Week` == i), ] %>% ungroup()
    
    currentSheet2 <- currentSheet2 %>% ungroup() %>% group_by(`HF Week`, `Weeks Out`, `Recipe Slot`) %>% 
      summarise(`Meal Forecast` = sum(as.numeric(`Meal Forecast`), na.rm = T))
    
    #getting missing entries
    currentSheet2 <- currentSheet2 %>% complete(`Weeks Out`  = c("6 Weeks Out", "5 Weeks Out",
                                                                 "4 Weeks Out" ,"3 Weeks Out",
                                                                 "2 Weeks Out", "Tuesday", "Wednesday", 
                                                                 "Thursday", "Friday", "Saturday", "Sunday",
                                                                 "Monday", "Final")) %>% 
      complete(`Recipe Slot` = unique(currentSheet2$`Recipe Slot`)) %>% distinct()
    
    currentSheet2 <- currentSheet2[order(factor(currentSheet2$`Weeks Out`, levels = c("6 Weeks Out", "5 Weeks Out",
                                                                                      "4 Weeks Out" ,"3 Weeks Out",
                                                                                      "2 Weeks Out", "Tuesday", "Wednesday", 
                                                                                      "Thursday", "Friday", "Saturday", "Sunday",
                                                                                      "Monday", "Final"))), ]
    if(nrow(currentSheet2[-which(is.na(currentSheet2$`Recipe Slot`)), ]) > 0){
      
      currentSheet2 <- currentSheet2[-which(is.na(currentSheet2$`Recipe Slot`)), ]
      
    }
    
    test <- currentSheet2 %>% ungroup() %>% group_by(`Weeks Out`, `Recipe Slot`) %>% do({
      X <- data.frame(., stringsAsFactors = F)
      RE <- (sum(X$`Meal.Forecast`, na.rm = T) - sum(currentSheet2[which(currentSheet2$`Weeks Out` == '6 Weeks Out' &
                                                                           currentSheet2$`Recipe Slot` == unique(X$`Recipe.Slot`)), "Meal Forecast"], na.rm = T))/
        sum(currentSheet2[which(currentSheet2$`Weeks Out` == '6 Weeks Out' &
                                  currentSheet2$`Recipe Slot` == unique(X$`Recipe.Slot`)), "Meal Forecast"], na.rm = T) * 100
      data.frame(RE)
      
    })
    
    test[which(test$RE == -100), "RE"] <- NA
    test$`Weeks Out` <- reorder.factor(test$`Weeks Out`, new.order= c("6 Weeks Out", "5 Weeks Out",
                                                                      "4 Weeks Out" ,"3 Weeks Out",
                                                                      "2 Weeks Out", "Tuesday", "Wednesday", 
                                                                      "Thursday", "Friday", "Saturday", "Sunday",
                                                                      "Monday", "Final"))
    test <- test %>% arrange(`Weeks Out`)
    
    mealVariancePlot <- ggplot(data = test %>% filter(!is.na(RE),!is.infinite(RE)), aes(x = `Weeks Out`, y = `RE`)) + geom_line(aes(color = as.factor(`Recipe Slot`), group = as.factor(`Recipe Slot`))) +
      theme_bw() +
      xlab("Horizon") +
      ylab("Percent Change") +
      ggtitle(paste0(i, ": ",forecastType," - Meal Variances")) + scale_x_discrete(limits = test$`Weeks Out`) +
      scale_color_discrete(limits = c(unique(test$`Recipe Slot`[which(!is.na(test$RE)&(!is.infinite(test$RE)))]))) +
      scale_y_continuous(limits = c(min(test$RE[which(!is.na(test$RE)&(!is.infinite(test$RE)))], na.rm = T) - 50,
                                    max(test$RE[which(!is.na(test$RE)&(!is.infinite(test$RE)))], na.rm = T) + 50), labels = function(x) paste0(x, "%")) +
      theme(axis.text.x = element_text(angle = 60, hjust = 1), plot.title = element_text(hjust = 0.5),
            legend.position = "right",legend.box = 'vertical', text = element_text(size=10)) + labs(color = "Recipe Slot")
    mealVariancePlot
    
    ggsave(paste0("plots_for_emails/",i, "mealVariancesPlot.png"), width = 6, height = 5)
  }
  
  # setting up mail
  files = c(paste0("plots_for_emails/",i,"toplineForecastPlot.png"),
            paste0("plots_for_emails/",i, "mealVariancesPlot.png"))
  
  
  emailSubject <-paste0(i, ": ",forecastType," EveryPlate Forecast")
  
  finalToplineNumber <- tail(na.omit(eptsDataTemp$forecast_volume), n = 1, )
  
  emailBody <-paste0("<html><body>
                     Hi Everyone,
                     <br>
                     <br>
                     The forecast for EveryPlate <b>",i,  "</b> is projected to be <b>"
                     ,format(finalToplineNumber, big.mark = ',')," </b>
                     . <a href='https://docs.google.com/spreadsheets/d/1rjpDpSCr4kfuzNKG6INfk0aDWkzp6C_kKw_zfvVbr0c/edit#gid=1147808264'>EP Oscar</a> has been updated.
                     <br>
                     <br>
                     Email forecastingteam@hellofresh.com if you have any questions about the tools or the forecast.
                     <br>
                     <br>
                     <img src='cid:",files[1],"' height='400' width='500'>
                     
                     <img src='cid:",files[2],"'height='400' width='500'>
                     
                     <br>
                     <br>
                     <em> Please note: our team does not manage the forecast email distribution lists. Please send all inquiries/additions to US Help Desk. </em>
                     <br>
                     <br>
                     Best, <br> ",USER,"
                     
                     </body></html>")
  
  
  
  #gmail recipients and senders
  sender <- GMAIL_USER
  recipients <- c('purchasing@hellofresh.com', "forecastingteam@hellofresh.com", 'us-ops-planning@hellofresh.com', 'logistics@hellofresh.com','everyplateforecast@hellofresh.com')
  
  
  
  use_secret_file("gmail_secret.json")
  gm_auth_configure(path = "gmail_secret.json")
  
  test_email <- gm_mime(
    To =  recipients,
    From = sender,
    Subject = emailSubject)
  
  test_email <- test_email %>% gm_html_body(emailBody)
  
  #inline functionality
  
  
  #attach plots - inline functionality
  for(j in 1:length(files)){
    test_email <- gm_attach_file(test_email, files[j],  id = files[j])
    
    test_email$parts[[2 + j]]$header$`X-Attachment-Id` <- files[j]
    test_email$parts[[2 + j]]$header$disposition <- 'inline'
  }
  #make draft
  gm_create_draft(test_email)
}

