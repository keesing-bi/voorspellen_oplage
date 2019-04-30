##########################################################################################################################
############################################# Circulation estimates KNL ##################################################
####################################### By Elfride, Madeline, Adam and Jeffrey ###########################################
##########################################################################################################################

########################################### cleaning our environment #####################################################

rm(list=ls())
gc()

########################################### if needed install packaged ###################################################

# install.packages("dplyr")
# install.packages("repr")
# install.packages("ggplot2")
# install.packages("odbc")
# install.packages("DBI")
# install.packages("lubridate")


############################################### loading in libraries #####################################################

library(dplyr)
library(repr)
library(ggplot2)
library(odbc)
library(DBI)
library(lubridate)

############################################## loading in data tables ####################################################

source("Circulation_load_datatables.R")

fDistribution_aggr <- LoadDateTables(table = 'fDistribution') 

dProduct <- LoadDateTables(table = 'dProduct')

############################################# filtering the tables ######################################################

Merge_dProd_fDist <- 
  merge.data.frame(dProduct, fDistribution_aggr,  by.x = "DW_Id", by.y = "DW_Product_id", all = TRUE)

t_filter <- filter(Merge_dProd_fDist, year(Merge_dProd_fDist$`Order Date`) == year(Sys.Date()) & month(Merge_dProd_fDist$`Order Date`) == month(Sys.Date()))

# write.csv(t_filter, file = 'test planning.csv') 
##  planning klopt
