# splitter runner script

versionAsk <- askYesNo("Choose 'Yes' For Inweek Version, 'No' For Tuesday", default = FALSE)
if(versionAsk){
  source("EP_Splitter_Inweek.R")
} else {
  source("EP_Splitter_Tuesday_Script.R")
}
