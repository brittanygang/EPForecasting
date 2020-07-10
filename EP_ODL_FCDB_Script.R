## script for processing odl data

if(!exists("currentHFWeek")){
  
  source("ep_pullRecentODLPDL.R")
  
} 
# processing pull of recent odl
# grouping by week, dc, kit / serving size, and counting frequency

firstOutput <- recentODL %>% group_by(hellofresh_week, 
                                    substr(delivery_region, 0, 2), 
                                    substr(boxtype, 1, 1),
                                    substr(boxtype, 3, 3)
                                    
                                    ) %>%
  do({
    X <- data.frame(., stringsAsFactors = F)
    ME <- data.frame(table(as.numeric(unlist(strsplit(X$meal_swap, " ")))))
    names(ME) <- c("Recipe Slot", "Total Count")
    data.frame(ME, stringsAsFactors = F)
  })

# adding modified on date
firstOutput$modifiedOn <- Sys.Date()

# rearranging columns
firstOutput <- firstOutput[c(ncol(firstOutput), seq(1,ncol(firstOutput) - 1))]
names(firstOutput) <- c("Pull Date", "HF Week", "DC", "Kits/Box", "Serving Size", "Recipe Slot", "Count")

# writing to googlesheet 
googleSheetToEdit <- gs_title("EP PHYLLIS")
currentSheet <- gs_read(googleSheetToEdit, ws= 'odl_meal_counts')

if(nrow(currentSheet[-which(currentSheet$`HF Week` == unique(firstOutput$`HF Week`)), ]) > 0){
  currentSheet <- currentSheet[-which(currentSheet$`HF Week` == unique(firstOutput$`HF Week`)), ]
}


gs_edit_cells(ss = googleSheetToEdit, ws = "odl_meal_counts", input = firstOutput , anchor = paste0('A', nrow(currentSheet) + 2), col_names = F)

# same thing for addons, but add add_on slot,

# name, total_orders, percentage of total orders that have addons

#firstOutput_addon <- recentODL %>% group_by(hellofresh_week, 
#                                          substr(delivery_region, 0, 2), 
#                                          substr(boxtype, 1, 1),
#                                          substr(boxtype, 3, 3)
                                          
#) %>%
#  do({
#    X <- data.frame(., stringsAsFactors = F)
#    ME <- data.frame(table(unlist(strsplit(X$addon_swap, " "))))
#    ME$amount <- gsub("[^0-9]", "", ME$Var1)
#    ME$Var1<- gsub("\\d", "", ME$Var1)
#    # multiplying add on amount by frequency of choice
#    ME$Freq <- ME$Freq * as.numeric(ME$amount)
#    ME <- ME %>% group_by(Var1) %>% summarise(sum(Freq))
#    names(ME) <- c("Add On", "Total Count")
#    data.frame(ME, stringsAsFactors = F)
#  })

#firstOutput_addon$modifiedOn <- Sys.Date()
#rearranging columns
#firstOutput_addon <- firstOutput_addon[c(ncol(firstOutput_addon), seq(1,ncol(firstOutput_addon) - 1))]

#names(firstOutput_addon) <- c("Pull Date", "HF Week", "DC", "Kits/Box", "Serving Size", "Add On", "Count")



#if(nrow(firstOutput_addon[-which(is.na(firstOutput_addon$`Total Count`)), ]) > 0){
  
 # firstOutput_addon <- firstOutput_addon[-which(is.na(firstOutput_addon$`Total Count`)), ]
  
#}

# writing to googlesheet - appending
#googleSheetToEdit <- gs_title("EP PHYLLIS")
#currentSheet <- gs_read(googleSheetToEdit, ws= 'odl_addon_counts')

#if(nrow(currentSheet[-which(currentSheet$`HF Week` == unique(firstOutput_addon$`HF Week`)), ]) > 0){
#  currentSheet <- currentSheet[-which(currentSheet$`HF Week` == unique(firstOutput_addon$`HF Week`)), ]
#}

#gs_edit_cells(ss = googleSheetToEdit, ws = "odl_addon_counts", input = firstOutput_addon , anchor = paste0('A', nrow(currentSheet) + 2), col_names = F)



# second output dataframe - rows by delivery day

secondOutput <- recentODL %>% group_by(hellofresh_week, 
                                    substr(delivery_region, 0, 2)) %>%
  do({
    X <- data.frame(., stringsAsFactors = F)
    RE <- data.frame(table(X$weekday), stringsAsFactors = F)
    
    ##  counting rows , not meals
    RE$percentOfTotal <- format((RE$Freq/nrow(recentODL)) , digits = 6)
    RE$Freq <- format((RE$Freq/sum(RE$Freq)) , digits = 6)
    data.frame(RE, stringsAsFactors = F)
    
  })

secondOutput$modifiedOn <- Sys.Date()
secondOutput <- secondOutput[c(ncol(secondOutput), seq(1,ncol(secondOutput) - 1))]

names(secondOutput) <- c("Pull Date", "HF Week", "DC", "Delivery Day", "Percent Split within DC", "Percent Split Overall")

#writing to googlesheet - getting current sheet and looking at how long it is, and appending to end
googleSheetToEdit <- gs_title("EP PHYLLIS")
currentSheet <- gs_read(googleSheetToEdit, ws= 'odl_delivery_days')

if(nrow(currentSheet[-which(currentSheet$`HF Week` == unique(secondOutput$`HF Week`)), ]) > 0){
  currentSheet <- currentSheet[-which(currentSheet$`HF Week` == unique(secondOutput$`HF Week`)), ]
}

gs_edit_cells(ss = googleSheetToEdit, ws = "odl_delivery_days", input = secondOutput, anchor = paste0('A', nrow(currentSheet) + 2), col_names = F)

