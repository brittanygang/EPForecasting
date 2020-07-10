# splitter script - can run everyday, for inweek

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
require(RMySQL)

if(grepl('window', tolower(osVersion))){
  source("C://dbcredentials.R")
} else {
  source("~/dbcredentials.R")
}
# get all the dataframes
GSRead <- gs_title("EP DWIGHT")

ODL_df <- gs_read(GSRead, ws = "ODL/PDL_imports")

ODL_df[] <- lapply(ODL_df, as.character)

if(nrow(ODL_df[-which(is.na(ODL_df$`HelloFresh Week`)), ]) > 0){
  ODL_df <- ODL_df[-which(is.na(ODL_df$`HelloFresh Week`)), ]
}

Sys.sleep(6)

Topline_DC <- gs_read(GSRead, ws = 'day_dc_toplines')

if(nrow(Topline_DC[-which(is.na(Topline_DC$`HF Week`)), ]) > 0){
  Topline_DC <- Topline_DC[-which(is.na(Topline_DC$`HF Week`)), ]
}
Topline_DC[] <- lapply(Topline_DC, as.character)


dayOfRun <- names(gs_read(GSRead, ws = 'Topline Adjustment'))[[3]]

#adding sleep so we dont timeout our requests
Sys.sleep(2)

DCSplit <- gs_read(GSRead, ws = "dc_splits_inweek")
DCSplit[] <- lapply(DCSplit, as.character)

Sys.sleep(2)

productSplit <- gs_read(GSRead, ws = "product_splits_inweek")
productSplit[] <- lapply(productSplit, as.character)

Sys.sleep(6)

mealSplit <- gs_read(GSRead, ws = 'meal_splits_inweek')
mealSplit[] <- lapply(mealSplit, as.character)

Sys.sleep(6)


#addonSplit <- gs_read(GSRead, ws = 'addons_splits_inweek')
#addonSplit[] <- lapply(addonSplit, as.character)

#Sys.sleep(6)

#addons_df <- gs_read(GSRead, ws = 'addons_import')
#addons_df[] <- lapply(addons_df, as.character)

# first output - multiply out topline number by dc split, product split


firstSplitterIntermediary <- Topline_DC %>% full_join(productSplit, by = c("HF Week", "DC"))

# getting box forecast per row
output1 <- firstSplitterIntermediary %>% rowwise() %>% do({
  X <- data.frame(., stringsAsFactors = F)
  Topline_number <- as.numeric(X$Topline)
  Product_percent <- as.numeric(gsub("%", "", X$Finalized..)) / 100
  Box_forecast <- Topline_number * Product_percent
  
  returnDF <-data.frame(cbind(unique(X$HF.Week), unique(X$DC),dayOfRun, unique(X$Kits.Box), unique(X$Serving.Size), Box_forecast))
  names(returnDF) <- c("HF Week", "DC","Weeks Out", "Kits/Box", "Serving Size", "Box Forecast")
  returnDF
})

# removing any na box forecast rows
if(nrow(output1[-which(is.na(output1$`Box Forecast`)), ]) > 0){
  output1 <- output1[-which(is.na(output1$`Box Forecast`)), ]
}

# adding product combo to df
output1$`Product Combo` <- paste0(output1$`Kits/Box`, "x", output1$`Serving Size`)
output1 <- output1 %>% select(c("HF Week", "DC","Weeks Out","Product Combo", "Kits/Box", "Serving Size", "Box Forecast"))

# second output of splitter is  meal countsby recipe slot
secondSplitterIntermediary <- Topline_DC %>% full_join(productSplit, by = c("HF Week", "DC")) %>% full_join(mealSplit, by = c("HF Week", "DC", "Kits/Box", "Serving Size"))

output2 <- secondSplitterIntermediary %>% rowwise() %>% do({
  X <- data.frame(., stringsAsFactors = F)
  Topline_number <- as.numeric(X$Topline)
  Product_percent <- as.numeric(gsub("%", "", X$Finalized...x)) / 100
  Meal_percent <- as.numeric(gsub("%", "", X$Finalized...y)) / 100
  Kits <- as.numeric(X$Kits.Box)
  Meal_forecast <- round(Topline_number * Product_percent * Meal_percent * Kits)
  
  returnDF <-data.frame(cbind(unique(X$HF.Week), unique(X$DC), dayOfRun, unique(X$Kits.Box), unique(X$Serving.Size), as.numeric(unique(X$Recipe.Slot)), Meal_forecast))
  names(returnDF) <- c("HF Week", "DC", "Weeks Out", "Kits/Box", "Serving Size","Recipe Slot", "Meal Forecast")
  returnDF
})


# appending addon info to second output of splitter
#secondSplitterIntermediary_addons <- Topline_DC %>% full_join(addonSplit, by = c("HF Week", "DC"))

#output2_addons <- secondSplitterIntermediary_addons %>% rowwise() %>% do({
#  X <- data.frame(., stringsAsFactors = F)
# Topline_number <- as.numeric(X$Topline)
# Addon_percent <- as.numeric(gsub("%", "", X$Finalized..)) / 100
# Meal_forecast <- round(Topline_number * Addon_percent)
  
# returnDF <- data.frame(cbind(unique(X$HF.Week), unique(X$DC), unique(X$`Delivery.Day`), "2", "2", unique(X$Letter.Slot), unique(X$Number.Slot), Meal_forecast))
# names(returnDF) <- c("HF Week", "DC", "Weeks Out", "Kits/Box", "Serving Size","Letter Slot","Number Slot", "Meal Forecast")
# returnDF
#})

# saving this output for csv
csvOutput <- output2

#aggregating, weighted sum of meal count weighted by kits/box
csvOutput <- csvOutput %>% group_by(`HF Week`, DC, `Weeks Out`,`Kits/Box`, `Serving Size`, `Recipe Slot`) %>% summarise("Meal Forecast" = sum(as.numeric(`Meal Forecast`)))

output2 <- output2 %>% group_by(`HF Week`, DC, `Weeks Out`, `Serving Size`, `Recipe Slot`) %>% summarise("Meal Forecast" = sum(as.numeric(`Meal Forecast`)))

if(nrow(output2[-which(is.na(output2$`Meal Forecast`)), ]) > 0){
  output2 <- output2[-which(is.na(output2$`Meal Forecast`)), ]                                                                                              
}

#ordering resulting dataframe, formatting
output2 <- output2[order(output2$`HF Week`, output2$DC, output2$`Weeks Out`, output2$`Serving Size`, as.numeric(output2$`Recipe Slot`)), ]

#output2_addons_append <- output2_addons
#output2_addons_append$`Number Slot` <- NULL
#output2_addons_append$`Kits/Box` <- NULL
#output2_addons_append$`Weeks Out` <- dayOfRun
#output2_addons_append <- output2_addons_append %>% group_by(`HF Week`, DC, `Weeks Out`, `Serving Size`, `Letter Slot`) %>% summarise("Meal Forecast" = sum(as.numeric(`Meal Forecast`)))


#names(output2_addons_append) <- names(output2)
#output2_addons_append[] <- lapply(output2_addons_append[], as.character)
output2[] <- lapply(output2[], as.character)

#appending addons output2 to regular output2
#output2 <- bind_rows(output2, output2_addons_append)

# checking length of current sheets to see which cell should i start writing at
GSExternal <- gs_title("EP OSCAR")
currentSheet1 <- gs_read(GSExternal, ws= 'forecast_product')
currentSheet2 <- gs_read(GSExternal, ws= 'forecast_recipes')

if(nrow(output1[which(!is.na(output1$`Box Forecast`)), ]) == 0){
  output1 <- output1[F, ]
}


if(nrow(output2[which(!is.na(output2$`Meal Forecast`)), ]) == 0){
  output2 <- output2[F, ]
}


#formatting ODL_df

ODL_df$`Product Type` <- NULL
ODL_df <- ODL_df[1:8]
names(ODL_df) <- c("HF Week", "Delivery Day", "DC", "Kits/Box", "Serving Size", "Recipe Slot", "Meal Forecast", "1Box")
ODL_df$`Box Forecast` <- ((as.numeric(ODL_df$`Meal Forecast`) - as.numeric(ODL_df$`1Box`))/as.numeric(ODL_df$`Kits/Box`)) + as.numeric(ODL_df$`1Box`)
ODL_df$`1Box` <- NULL
ODL_df[] <- lapply(ODL_df[], as.character )

#addons_df <- addons_df[1:7]
#names(addons_df) <- c("HF Week", "Delivery Day", "DC", "Kits/Box", "Serving Size", "Recipe Slot", "Meal Forecast")
# dont add box forecast to forecast_product output, so process odl_output1 before merging odl_df and addon_df 

# output1 and output2 - adding in odl data
odl_output1 <- ODL_df %>% group_by(`HF Week`, DC, `Delivery Day`,paste0(`Kits/Box`, "x", `Serving Size`), `Kits/Box`, `Serving Size`) %>% summarise("Box Forecast" = sum(as.numeric(`Box Forecast`)))

# now merge addons_df and odl_df
#addons_df$`Box Forecast` <- "0"
#addons_df[] <- lapply(addons_df[], as.character )
#addons_df$`Kits/Box` <- '2'
#addons_df$`Serving Size` <- '2'

#ODL_df <- bind_rows(ODL_df, addons_df)


names(odl_output1) <- names(output1)

odl_output1[] <- lapply(odl_output1, as.character)

output1 <- output1 %>% group_by(`HF Week`, DC, `Weeks Out`,`Product Combo`, `Kits/Box`, `Serving Size`) %>% summarise("Box Forecast" = sum(as.numeric(`Box Forecast`)))

output1[] <- lapply(output1, as.character)

output1 <- output1 %>% rowwise() %>% do({
  X <- data.frame(., stringsAsFactors = F)
  additional <- sum(as.numeric(unlist(odl_output1[which(odl_output1$DC == X$DC &
                                                          odl_output1$`HF Week` == X$HF.Week &
                                                          odl_output1$`Product Combo` == X$Product.Combo), "Box Forecast"])),na.rm = T)
  X$`Box.Forecast` <- as.numeric(X$Box.Forecast) + additional
  X
})


if(nrow(output1[which(!is.na(output1$HF.Week)), ]) == 0){
  output1 <- data.frame(odl_output1, stringsAsFactors = F)
}

names(output1) <- names(odl_output1)

output1[] <- lapply(output1, as.character)

output1 <- bind_rows(output1, odl_output1[which(!(odl_output1$DC %in% output1$DC &
                                                    odl_output1$`HF Week` %in% output1$`HF Week` &
                                                    odl_output1$`Product Combo` %in% output1$`Product Combo`)), ])


odl_output2 <- ODL_df %>% group_by(`HF Week`, DC, `Delivery Day`,`Serving Size`, `Recipe Slot`) %>% summarise("Meal Forecast" = sum(as.numeric(`Meal Forecast`)))
names(odl_output2) <- names(output2)

odl_output2[] <- lapply(odl_output2, as.character)
output2[] <- lapply(output2, as.character)

output2 <- output2 %>% rowwise() %>% do({
  X <- data.frame(., stringsAsFactors = F)
  additional <- sum(as.numeric(unlist(odl_output2[which(odl_output2$DC == X$DC &
                                                          odl_output2$`HF Week` == X$HF.Week &
                                                          odl_output2$`Serving Size` == X$Serving.Size &
                                                          odl_output2$`Recipe Slot` == X$Recipe.Slot), "Meal Forecast"])),na.rm = T)
  X$`Meal.Forecast` <- as.numeric(X$Meal.Forecast) + additional
  X
})

if(nrow(output2[which(!is.na(output2$HF.Week)), ]) == 0){
  output2 <- data.frame(odl_output2, stringsAsFactors = F)
  
}

names(output2) <- names(odl_output2)

output2[] <- lapply(output2, as.character)

output2 <- bind_rows(output2, odl_output2[which(!(odl_output2$DC %in% output2$DC &
                                                    odl_output2$`HF Week` %in% output2$`HF Week` &
                                                    odl_output2$`Serving Size` %in% output2$`Serving Size` &
                                                    odl_output2$`Recipe Slot` %in% output2$`Recipe Slot`)), ])


if(ncol(output2) == 0){
  output2 <- odl_output2
}
names(output2) <- names(odl_output2)

#ordering columns

output1$modifiedOn <- Sys.Date()

output1 <- output1[order(output1$`HF Week`, output1$`Weeks Out`,  output1$`Kits/Box`, output1$`Serving Size`), ]
output1 <- output1[c(ncol(output1),seq(1:(ncol(output1) - 1)))]

output2$modifiedOn <- Sys.Date()

output2 <- output2[order(output2$`HF Week`, output2$`Weeks Out`,   output2$`Serving Size`, as.numeric(gsub("[[:alpha:]]",24, output2$`Recipe Slot`))), ]
output2 <- output2[c(ncol(output2),seq(1:(ncol(output2) - 1)))]

#round up
output1$`Box Forecast` <- ceiling(as.numeric(output1$`Box Forecast`))

if(any(is.na(output1$`HF Week`))){
  output1 <- output1[-which(is.na(output1$`HF Week`)), ]
}

if(any(is.na(output2$`HF Week`))){
  output2 <- output2[-which(is.na(output2$`HF Week`)), ]
}

output1 <- output1 %>% group_by(modifiedOn, `HF Week`, DC, `Product Combo`, `Kits/Box`, `Serving Size`) %>% summarise(
  `Box Forecast` = sum(`Box Forecast`)
)

output1$`Weeks Out` <- dayOfRun
output1 <- output1 %>% select(modifiedOn, `HF Week`,DC, `Weeks Out`,`Product Combo`, `Kits/Box`, `Serving Size`, `Box Forecast`)


output2 <- output2 %>% group_by(modifiedOn, `HF Week`, DC, `Serving Size`, `Recipe Slot`) %>% summarise(
  `Meal Forecast` = sum(as.numeric(`Meal Forecast`))
)
output2$`Weeks Out` <- dayOfRun
output2 <- output2 %>% select(modifiedOn, `HF Week`,DC, `Weeks Out`,  `Serving Size`, `Recipe Slot`, `Meal Forecast`)

#ep_garlic_output <- tbl_df(gs_read(GSRead, ws = 'garlic_splitter'))
#output2 <-  tbl_df(rbind(output2, ep_garlic_output))

#appending to sheets
gs_edit_cells(ss = GSExternal, ws = 'forecast_product', input = output1, anchor = paste0('A', nrow(currentSheet1) + 2), col_names = F)
gs_edit_cells(ss = GSExternal, ws = 'forecast_recipes', input = output2, anchor = paste0('A', nrow(currentSheet2) + 2), col_names = F)

#forecast_inweek output - besides actual odl data, for any other inweek not in the odl we will use the splits to forecast.

restOfInweek <- secondSplitterIntermediary %>% rowwise() %>% do({
  X <- data.frame(., stringsAsFactors = F)
  Topline_number <- as.numeric(X$Topline)
  Product_percent <- as.numeric(gsub("%", "", X$Finalized...x)) / 100
  Meal_percent <- as.numeric(gsub("%", "", X$Finalized...y)) / 100
  Meal_forecast <- round(Topline_number * Product_percent * Meal_percent)
  
  returnDF <-data.frame(cbind(unique(X$HF.Week), unique(X$DC), unique(X$Delivery.Day), unique(X$Kits.Box), unique(X$Serving.Size), as.numeric(unique(X$Recipe.Slot)), Meal_forecast))
  names(returnDF) <- c("HF Week", "DC", "Weeks Out", "Kits/Box", "Serving Size","Recipe Slot", "Meal Forecast")
  returnDF
})

restOfInweek <- restOfInweek %>% group_by(`HF Week`, DC, `Weeks Out`,`Kits/Box`, `Serving Size`, `Recipe Slot`) %>% summarise("Meal Forecast" = sum(as.numeric(`Kits/Box`) * as.numeric(`Meal Forecast`)))

# formatting so its easily able to join with the odl df
names(restOfInweek) <- c("HF Week", "DC","Delivery Day",  "Kits/Box", "Serving Size", "Recipe Slot", "Meal Forecast")
restOfInweek$`Box Forecast` <- restOfInweek$`Meal Forecast`/as.numeric(restOfInweek$`Kits/Box`)
restOfInweek[] <- lapply(restOfInweek[], as.character )

if(nrow(restOfInweek[which(!is.na(restOfInweek$`Box Forecast`)), ]) == 0){
  restOfInweek <- restOfInweek[F, ]
}

#binding odl data to the rest of inweek splits
# do we get rid of rows that are already in the odl df?
forecastInweekDF <- bind_rows(ODL_df, restOfInweek)

# adding output2_adons to this
#forecastInweekDF_addons <- output2_addons
#names(forecastInweekDF_addons) <- gsub("Weeks Out", "Delivery Day", names(forecastInweekDF_addons))
#names(forecastInweekDF_addons) <- gsub("Letter Slot", "Recipe Slot", names(forecastInweekDF_addons))
#forecastInweekDF_addons$`Number Slot` <- NULL
#forecastInweekDF_addons$`Box Forecast` <- "0"

#forecastInweekDF <- bind_rows(forecastInweekDF, forecastInweekDF_addons)

#ordering it and adding modified on column
forecastInweekDF <- forecastInweekDF[order(forecastInweekDF$`HF Week`,
                                           forecastInweekDF$`Delivery Day`,
                                           forecastInweekDF$`DC`,
                                           forecastInweekDF$`Kits/Box`,
                                           forecastInweekDF$`Serving Size`,
                                           as.numeric(gsub("[[:alpha:]]","", forecastInweekDF$`Recipe Slot`))), ]

forecastInweekDF$modifiedOn <- Sys.Date()

if(nrow(forecastInweekDF[which(!is.na(forecastInweekDF$`HF Week`)), ]) > 0){
  forecastInweekDF <- forecastInweekDF[which(!is.na(forecastInweekDF$`HF Week`)), ]
}

if(nrow(forecastInweekDF[which(!is.na(forecastInweekDF$`Delivery Day`)), ]) > 0){
  forecastInweekDF <- forecastInweekDF[which(!is.na(forecastInweekDF$`Delivery Day`)), ]
}

#writing as csv to bulk upload
write_csv(forecastInweekDF, path = "ep_forecastInweek_splitterUpload.csv")

gs_upload("ep_forecastInweek_splitterUpload.csv",sheet_title = "ep_forecasting_scripts_output_splitter", overwrite = T)


# get importranges to update
try(gs_ws_delete(ss = GSExternal,ws = "UpdatePlease", verbose = TRUE),silent = T)
try(gs_ws_new(ss = GSExternal, row_extent=1, col_extent=1, ws_title="UpdatePlease"), silent = T)

# csv output for oa_tools
csvOutputAsk <- askYesNo("Do you want to write a csv?", default = FALSE, 
                         prompts = getOption("askYesNo", gettext(c("Yes", "No", 'Cancel'))))

if(csvOutputAsk){
  
  if(nrow(csvOutput[-which(is.na(csvOutput$`Meal Forecast`)), ]) > 0){
    csvOutput <- csvOutput[-which(is.na(csvOutput$`Meal Forecast`)), ]                                                                                              
  }
  
  #adding addons to csv output
  #output2_addons_append_csv <- output2_addons
  #output2_addons_append_csv$`Weeks Out` <- dayOfRun
  #output2_addons_append_csv <- output2_addons_append_csv %>% group_by(`HF Week`, DC, `Weeks Out`,`Kits/Box`, `Serving Size`, `Number Slot`) %>% summarise("Meal Forecast" = sum(as.numeric(`Meal Forecast`)))
  
  #output2_addons_append_csv[] <- lapply(output2_addons_append_csv[], as.character)
  #names(output2_addons_append_csv) <- names(csvOutput)
  
  csvOutput[] <- lapply(csvOutput[], as.character)
  
  #csvOutput <- bind_rows(csvOutput, output2_addons_append_csv)
  
  
  odl_output2_csv <- ODL_df %>% group_by(`HF Week`, DC, `Delivery Day`,`Kits/Box`, `Serving Size`, `Recipe Slot`) %>% summarise("Meal Forecast" = sum(as.numeric(`Meal Forecast`)))
  
  odl_output2_csv[] <- lapply(odl_output2_csv, as.character)

  #convertLettersToNumbers <- data.frame(table(output2_addons[c('Letter Slot', 'Number Slot')]), stringsAsFactors = F)
  #odl_output2_csv <- odl_output2_csv %>% rowwise() %>% do({
  # X <- data.frame(., stringsAsFactors = F)
  # if(!(X$Recipe.Slot %in% seq(1,20, by = 1))){
  #  X[['Recipe.Slot']] <- unique(convertLettersToNumbers[which(convertLettersToNumbers$Letter.Slot == X[['Recipe.Slot']] &
  #                                                        convertLettersToNumbers$Freq > 0), 'Number.Slot'])[1] 
  #  }
  #  X
  #}
  csvOutput[] <- lapply(csvOutput, as.character)
  savedColNames <- names(csvOutput)
  names(odl_output2_csv) <-  savedColNames
  csvOutput <- csvOutput %>% rowwise() %>% do({
    X <- data.frame(., stringsAsFactors = F)
    additional <- sum(as.numeric(unlist(odl_output2_csv[which(odl_output2_csv$DC == X$DC &
                                                                odl_output2_csv$`HF Week` == X$HF.Week &
                                                                odl_output2_csv$`Kits/Box` == X$`Kits.Box` &
                                                                odl_output2_csv$`Serving Size` == X$Serving.Size &
                                                                odl_output2_csv$`Recipe Slot` == X$Recipe.Slot), "Meal Forecast"])),na.rm = T)
    X$`Meal.Forecast` <- as.numeric(X$Meal.Forecast) + additional
    X
  })
  
  if(nrow(csvOutput[which(!is.na(csvOutput$Meal.Forecast)), ]) == 0){
    names(odl_output2_csv) <- names(csvOutput)
    csvOutput <- odl_output2_csv
  }
  
  names(csvOutput) <- savedColNames
  names(odl_output2_csv) <- savedColNames
  odl_output2_csv[] <- lapply(odl_output2_csv, as.character)
  csvOutput[] <- lapply(csvOutput, as.character)
  odl_output2_csv <- tbl_df(odl_output2_csv[which(!(odl_output2_csv$DC %in% csvOutput$DC &
                                                      odl_output2_csv$`HF Week` %in% csvOutput$`HF Week` &
                                                      odl_output2_csv$`Serving Size` %in% csvOutput$`Serving Size` &
                                                      odl_output2_csv$`Kits/Box` %in% csvOutput$`Kits/Box` &
                                                      odl_output2_csv$`Recipe Slot` %in% csvOutput$`Recipe Slot`)),])
  csvOutput <- bind_rows(csvOutput, odl_output2_csv)
  
  
  
  # formatting and adding columns to preserve format of old ot file
  csvOutput$Country <- 'ER'
  csvOutput$DC <- paste0(ifelse(csvOutput$DC == "NJ", "EN",
                                         ifelse(csvOutput$DC == "CA", "EC",
                                                "ET")))
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
  #csvOutput$dc <- ifelse(csvOutput$dc == 'CA', 'SF', csvOutput$dc)
  
  csvOutput <- csvOutput[c("week", "country", "dc",  "box_name", "product_family", 
                           "servings", "size", "meal_number", "meals_to_deliver")]
  
  if(nrow(csvOutput[which(is.na(csvOutput$meals_to_deliver)), ]) > 0){
    csvOutput <- csvOutput[-which(is.na(csvOutput$meals_to_deliver)), ]
  }
  
  if(nrow(csvOutput[which(!is.na(csvOutput$week)), ]) > 0){
    csvOutput <- csvOutput[which(!is.na(csvOutput$week)), ]
  }
  
  # inserting garlic quantity as recipe 80
   csvOutput <- tbl_df(rbind(csvOutput,
                             csvOutput %>% distinct(week,country,dc,box_name,product_family,servings,size) %>% 
                               mutate(meal_number = ifelse(dc == 'EC', '280',
                                                           ifelse(dc == 'ET', '380', '180'))) %>% 
                               left_join(output1 %>% ungroup() %>% mutate(DC = ifelse(DC == 'CA', 'EC',
                                                                                      ifelse(DC == 'TX', 'ET', 'EN'))) %>% 
                                           select(`HF Week`,DC,`Kits/Box`,`Serving Size`,`Box Forecast`), 
                                         by = c('week' = 'HF Week', 'dc' = 'DC', 'servings' = 'Kits/Box',
                                                'size' = 'Serving Size')) %>% 
                               mutate(meals_to_deliver = ifelse(servings == '3' & size == '2',1,2) *
                                        `Box Forecast`) %>% 
                               select(-`Box Forecast`))) %>% 
     arrange(dc,servings,size,meal_number)
  #ep_garlic_csv <- tbl_df(gs_read(GSRead, ws = 'garlic_OT'))
  #csvOutput <- tbl_df(rbind(csvOutput, ep_garlic_csv))
  
  # writing files for each week in this csv output
  mainDir <- 'OT_uploads'
  subDir <- unique(csvOutput$week)
  
  dir.create(file.path(mainDir, subDir), showWarnings = T, recursive = T)
  setwd(file.path(mainDir, subDir))
  
  write_csv(csvOutput, path = paste0(unique(csvOutput$week),'_OT_Upload3_',Sys.Date(),".csv"),col_names = T) 
  
  setwd("../..")
}

# updating topline adjustment cell in the forecast tracking sheet
updatingTheCell <- gs_read(GSRead, ws = 'Topline Adjustment')
weekToAdjust <- names(updatingTheCell)[2]
weekdayToAdjust <-  names(updatingTheCell)[3]
toplineNumberToAdjust <- as.numeric(gsub(",","", as.character(updatingTheCell[13,17])))

GSAdjust <- gs_title("EveryPlate Forecast Tracking Sheet")
EPTS <- gs_read(GSAdjust, ws = "EveryPlate")
EPTS[which(EPTS$`scm_week` == weekToAdjust &
             EPTS$`forecast_type` == weekdayToAdjust), "forecast_volume"] <- toplineNumberToAdjust

#get cell and adjust that number directly.
gs_edit_cells(GSAdjust, ws = "EveryPlate",input = toplineNumberToAdjust,
              anchor = paste0("F", which(EPTS$`scm_week` == weekToAdjust &
                                           EPTS$`forecast_type` == weekdayToAdjust) + 1), col_names = F)


## generating graphs for emails



options(scipen = 999)
# reading the topline numbers
gsToRead <- gs_title("EP DWIGHT")
toplineAdjustment <- gs_read(gsToRead, ws = 'Topline Adjustment')

finalToplineNumber <- as.numeric(gsub(",","", as.character(toplineAdjustment[13,17])))
difference <- as.numeric(gsub(",","", as.character(toplineAdjustment[3,4])))

changeType <- ifelse(difference > 0, 'increased', 'decreased')
#getting epdata this week
epSheet <- gs_title("EveryPlate Forecast Tracking Sheet")

epData <- gs_read(epSheet, ws = "EveryPlate")
epData <- epData %>% mutate(forecast_volume = as.numeric(gsub(',','',forecast_volume)))

epDataThisWeek <- epData[which(epData$`scm_week` == names(toplineAdjustment)[2]), ]

if(nrow(epDataThisWeek[which(!is.na(epDataThisWeek$`forecast_volume`)), ]) > 0){
  epDataThisWeek <- epDataThisWeek[which(!is.na(epDataThisWeek$`forecast_volume`)), ]
}

if(nrow(epDataThisWeek) > 0){
  #ordering the dataframe 
  epDataThisWeek <- epDataThisWeek %>% complete(`forecast_type`  = c("6 Weeks Out", "5 Weeks Out",
                                                                         "4 Weeks Out" ,"3 Weeks Out",
                                                                         "2 Weeks Out", "Tuesday", "Wednesday", 
                                                                         "Friday", "Saturday", "Sunday",
                                                                          "Monday", "Final")) %>% distinct()
  
  epDataThisWeek <- epDataThisWeek[sort(order(c("6 Weeks Out", "5 Weeks Out",
                                                    "4 Weeks Out" ,"3 Weeks Out",
                                                    "2 Weeks Out", "Tuesday", "Wednesday", 
                                                    "Friday", "Saturday", "Sunday",
                                                    "Monday", "Final"))), ]
  
  epDataTemp <- epDataThisWeek[order(factor(epDataThisWeek$`forecast_type`, levels = c("6 Weeks Out", "5 Weeks Out",
                                                                                             "4 Weeks Out" ,"3 Weeks Out",
                                                                                             "2 Weeks Out", "Tuesday", "Wednesday", 
                                                                                             "Friday", "Saturday", "Sunday",
                                                                                             "Monday", "Final"))), ]
  #creating topline forecast plot and saving
  toplineForecastPlot <- ggplot(data = epDataTemp, aes( x = seq(1,nrow(epDataTemp)), y = `forecast_volume`, group = 1)) + geom_line(color = 'blue') +  geom_point() + 
    theme_bw() +
    geom_text(label = format(epDataTemp$forecast_volume, big.mark=','),
              size = 3, vjust = -1, angle = 15) +
    xlab("Horizon") +
    ylab("Forecast Volume") + 
    ggtitle(paste0(unique(epDataTemp$`scm_week`), " - ",dayOfRun, " Topline Forecast")) + scale_x_discrete(limits = epDataTemp$`forecast_type`) + 
    theme(axis.text.x = element_text(angle = 60, hjust = 1), plot.title = element_text(hjust = 0.5),
          legend.position = "bottom", text = element_text(size=10)) +
    scale_y_continuous(limits = c(min(as.numeric(epDataTemp$`forecast_volume`), na.rm = T) - 100000,
                                  max(as.numeric(epDataTemp$`forecast_volume`), na.rm = T) + 50000))
  
  toplineForecastPlot
  ggsave(paste0("plots_for_emails/",names(toplineAdjustment)[2],dayOfRun,"_inweekToplineForecastPlot.png"), width = 8, height = 5)
  
}

#getting meal variances plot
currentSheet2 <- gs_read(GSExternal, ws= 'forecast_recipes')
currentSheet2 <- currentSheet2[which(currentSheet2$`HF Week` ==  names(toplineAdjustment)[2]), ]

if(nrow(currentSheet2) > 0){
  
  currentSheet2 <- currentSheet2 %>% group_by(`HF Week`, `Weeks Out`, `Recipe Slot`) %>% summarise(
    `Meal Forecast` = sum(as.numeric(`Meal Forecast`), na.rm = T)
  )
  
  currentSheet2 <- currentSheet2 %>% complete(`Weeks Out`  = c("6 Weeks Out", "5 Weeks Out",
                                                               "4 Weeks Out" ,"3 Weeks Out",
                                                               "2 Weeks Out", "Tuesday", "Wednesday", 
                                                               "Friday", "Saturday", "Sunday",
                                                               "Monday", "Final")) %>% 
    complete(`Recipe Slot` = as.numeric(unique(currentSheet2$`Recipe Slot`))) %>% distinct()
  currentSheet2 <- currentSheet2[order(factor(currentSheet2$`Weeks Out`, levels = c("6 Weeks Out", "5 Weeks Out",
                                                                                    "4 Weeks Out" ,"3 Weeks Out",
                                                                                    "2 Weeks Out", "Tuesday", "Wednesday", 
                                                                                    "Friday", "Saturday", "Sunday",
                                                                                    "Monday", "Final"))), ] 
  if(nrow(currentSheet2[-which(is.na(currentSheet2$`Recipe Slot`)), ]) > 0){
    
    currentSheet2 <- currentSheet2[-which(is.na(currentSheet2$`Recipe Slot`)), ]
    
  }
  test <- currentSheet2 %>% group_by(`Weeks Out`, `Recipe Slot`) %>% do({
    X <- data.frame(., stringsAsFactors = F)
    RE <- (sum(X$`Meal.Forecast`, na.rm = T) - sum(currentSheet2[which(currentSheet2$`Weeks Out` == '6 Weeks Out' &
                                                                         currentSheet2$`Recipe Slot` == unique(X$`Recipe.Slot`)), "Meal Forecast"], na.rm = T))/
      sum(currentSheet2[which(currentSheet2$`Weeks Out` == '6 Weeks Out' &
                                currentSheet2$`Recipe Slot` == unique(X$`Recipe.Slot`)), "Meal Forecast"], na.rm = T) * 100
    data.frame(RE)
    
  })
  
  # making missing observations NA
  test[which(test$RE == -100), "RE"] <- NA
  
  test$`Weeks Out` <- reorder.factor(test$`Weeks Out`, new.order= c("6 Weeks Out", "5 Weeks Out",
                                                                    "4 Weeks Out" ,"3 Weeks Out",
                                                                    "2 Weeks Out", "Tuesday", "Wednesday", 
                                                                    "Friday", "Saturday", "Sunday",
                                                                    "Monday", "Final"))
  test <- test %>% arrange(`Weeks Out`)
  
  mealVariancePlot <- ggplot(data = test %>% filter(RE!='Inf'), aes(x = `Weeks Out`, y = `RE`)) + 
    geom_line(aes(color = as.factor(`Recipe Slot`), group = as.factor(`Recipe Slot`))) +
    theme_bw() +
    xlab("Horizon") +
    ylab("Percent Change") + 
    ggtitle(paste0(unique(epDataThisWeek$scm_week), " - ",dayOfRun, " Meal Variances")) + 
    scale_x_discrete(limits = test$`Weeks Out`) + 
    scale_color_discrete(limits = c(unique(as.numeric(as.character(test$`Recipe Slot`))))) +
    scale_y_continuous(limits = c(min(test$RE, na.rm = T) - 50,
                                  max(test$RE[which(test$RE!='Inf')], na.rm = T) + 50), labels = function(x) paste0(x, "%")) +
    theme(axis.text.x = element_text(angle = 60, hjust = 1), plot.title = element_text(hjust = 0.5),
          legend.position = "right",legend.box = 'vertical', text = element_text(size=10)) + labs(color = "Recipe Slot") 
  mealVariancePlot
  
  ggsave(paste0("plots_for_emails/",names(toplineAdjustment)[2],dayOfRun,"_inweekMealVariancesPlot.png"), width = 6, height = 5)
  
}

#grabbing overview chart
overview <- gs_read(gsToRead, ws = 'overview_chart')
ggsave(paste0("plots_for_emails/",
              names(toplineAdjustment)[2],dayOfRun,"_inweekOverviewChart.png"),plot = grid.table(overview, rows = NULL), width = 6, height = 5)
# setting up mail

files = c(paste0("plots_for_emails/",
                 names(toplineAdjustment)[2],dayOfRun,"_inweekToplineForecastPlot.png"),
          paste0("plots_for_emails/",
                 names(toplineAdjustment)[2],dayOfRun,"_inweekMealVariancesPlot.png"),
          paste0("plots_for_emails/",names(toplineAdjustment)[2],dayOfRun,"_inweekOverviewChart.png"))

emailSubject <- paste0(unique(epDataThisWeek[which(!is.na(epDataThisWeek$`scm_week`)), 'scm_week']), ": EveryPlate In-Week Forecast (",dayOfRun,")")

#graphs arent displaying in sent, but in draft they are. remove inline graphs for now.
emailBody <-paste0("<html><body>
Hi Everyone,
<br>
<br>
The forecast for <b>",names(toplineAdjustment)[2],  "</b>  has ",changeType, " by <b>", abs(as.numeric(difference)),
"</b> and is projected to be <b>",paste0(substr(as.character(finalToplineNumber),1,2), ",",
substr(as.character(finalToplineNumber),3,nchar(as.character(finalToplineNumber))))," </b>
. <a href='https://docs.google.com/spreadsheets/d/1rjpDpSCr4kfuzNKG6INfk0aDWkzp6C_kKw_zfvVbr0c/edit#gid=1147808264'>EP Oscar</a> has been updated.
<br>
<br>
Email forecastingteam@hellofresh.com with any questions regarding today's forecast update.
<br>
<br>
<img src='cid:",files[1],"' height='400' width='500'>

<img src='cid:",files[2],"'height='400' width='500'>

<img src='cid:",files[3],"'height='400' width='500'>
<br>
<br>
<em> Please note: our team does not manage the forecast email distribution lists. Please send all inquiries/additions to US Help Desk. </em>
<br>
  <br>
  Best, <br> ",USER,"

</body></html>")


#recipients specified by Lauren
sender <- GMAIL_USER
recipients <- c('everyplateforecast@hellofresh.com', "forecastingteam@hellofresh.com", 'us-ops-planning@hellofresh.com')


use_secret_file("gmail_secret.json")
gm_auth_configure(path = "gmail_secret.json")

test_email <- gm_mime(
  To =  recipients,
  From = sender,
  Subject = emailSubject)

test_email <- test_email %>% gm_html_body(emailBody)

#inline functionality
for(i in 1:length(files)){
  test_email <- test_email %>% gm_attach_file(files[i], id = files[i])
  test_email$parts[[2 + i]]$header$`X-Attachment-Id` <- files[i]
  test_email$parts[[2 + i]]$header$disposition <- 'inline'
  
}

gm_create_draft(test_email)

