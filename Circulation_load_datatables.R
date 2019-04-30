##########################################################################################################################
############################################# Circulation estimates KNL ##################################################
####################################### By Elfride, Madeline, Adam and Jeffrey ###########################################
##########################################################################################################################
##########################################################################################################################
##########################################################################################################################
################## this function is meant to load in the data tables for the circulation estimate script #################
##########################################################################################################################
##########################################################################################################################
##########################################################################################################################

########################################### if needed install packaged ###################################################

# install.packages("dplyr")
# install.packages("odbc")
# install.packages("DBI")

############################################# loading in libraries #######################################################

LoadDateTables <- function(table) {
  
  library(dplyr)
  library(odbc)
  library(DBI)
  
  print(paste("Loading the data table ", table ))
  
  con <- dbConnect(odbc::odbc(), .connection_string = "Driver={SQL Server};server=jetenterprise;database=Nav13_DWH;trusted_connection= true;")
  
  
  if (table == 'fDistribution'){
    Load_table <- dbGetQuery(con, "
                          select
                             AA.[DW_Product_id]
                             ,AA.[Distributor]
                             ,AA.[Distributor Product Code]
                             ,AA.[Distributor Product Name]
                             ,AA.[Sourcefile]
                             ,sum(AA.[CMA Value]) as [CMA Value]
                             ,sum(AA.[Distributed]) as [Distributed]
                             ,sum(AA.[Distributors Costs]) as [Distributors Costs]
                             ,sum(AA.[Other Direct Cost]) as [Other Direct Cost]
                             ,sum(AA.[Editorial Costs]) as [Editorial Costs]
                             ,sum(AA.[Gross Sales Value]) as [Gross Sales Value]
                             ,sum(AA.[Print Costs]) as [Print Costs]
                             ,sum(AA.[Return]) as [Return]
                             ,sum(AA.[Sales Volume]) as [Sales Volume]
                             ,sum(AA.[Zero Sales]) as [Zero Sales]
                             from (
                             SELECT 
                             fact.[DW_Id]
                             ,fact.[DW_Account]
                             ,fact.DW_Sales_id
                             --      ,[EAN]
                             ,case when [DW_Product_id] is null then [DW_Product_id_unknown] else [DW_Product_id] end as [DW_Product_id] 
                             ,[DW_Pos_id]
                             ,fact.[Distributor]
                             ,[Distributor POS Code]
                             ,fact.[Distributor Product Code]
                             ,[Distributor Product Name]
                             ,[CMA Value]
                             
                             ,cast([Distributed] as int) as [Distributed]
                             ,[Dist_Cost] * cast([Gross Sales Value] as decimal(38,2)) as [Distributors Costs]
                             ,[Other_Cost] * cast([Gross Sales Value] as decimal(38,2)) as [Other Direct Cost]
                             ,[Editorial Costs]
                             ,cast([Gross Sales Value] as decimal(38,2)) as [Gross Sales Value]
                             --	  ,cast(case when dim.[VAT group] like 'H%' then cast([Sales Volume] as int) * (dim.[Cover price] /1.21)
                             --		else  cast([Sales Volume] as int) * (dim.[Cover price EUR] /1.06) end as decimal(38,2)) as [Gross Sales Value]
                             ,[Print Costs]
                             
                             ,cast([Return] as int) as [Return]
                             ,cast([Sales Volume] as int) as [Sales Volume]
                             
                             ,case when cast([Return] as int) = cast([Distributed] as int) then 1 
                             else 0 end as [Zero Sales]
                             
                             ,case when dim.[Preliminary Sales Reporting Date (PSRD)] is null or [Preliminary Sales Reporting Date (PSRD)] = '' then '01-01-1900' else dim.[Preliminary Sales Reporting Date (PSRD)] end as [Sales date (PSRD)]
                             ,[Sourcefile]
                             --	  ,Closed
                             
                             
                             
                             FROM 
                             ( 
                             SELECT * FROM  [NAV13_DWH].[dbo].[fDistributorSales]
                             )fact 
                             left join [NAV13_DWH].[dbo].[dProduct] dim 
                             on fact.DW_Product_id = Dim.DW_Id
                             left join [NAV13_DWH].[dbo].[Dist_Cost_perc] Cost 
                             on  fact.Distributor = Cost.[Distributor]
                             and year(dim.[Preliminary Sales Reporting Date (PSRD)]) = cast(Cost.[Year] as int)
                             where year(dim.[Last Sales Date (LSD)]) >= 2016 
                             and year(dim.[Preliminary Sales Reporting Date (PSRD)])  >= 2016
                             and (Dim.Closed <> 'N'   or Closed is null )
                             and dim.Country = 'NL'
                             and dim.[Available for Circulation] = 1
                             and dim.[Edition code] not like '%G1'
                             and dim.[Edition code] not like '%G2'
                             and dim.[Edition code] not like '%G3'
                             and dim.[Edition code] not like '%G4'
                             )AA
                             group by
                             AA.[DW_Product_id]
                             ,AA.[Distributor]
                             ,AA.[Distributor Product Code]
                             ,AA.[Distributor Product Name]
                             ,AA.[Sourcefile]
                             
                             
                             "
    )
  } else if (table == 'dProduct'){
    Load_table <- dbGetQuery(con, "
                                    select
                                    *,
                                    cast([Purchase Date] as date) as [Order Date]
                                    from dProduct
                                    where Country = 'NL'
                                    and [Available for Circulation] = 1
                                    and [Edition code] not like '%G1'
                                    and [Edition code] not like '%G2'
                                    and [Edition code] not like '%G3'
                                    and [Edition code] not like '%G4'
                                  "
    )
  }
  
 
  
  dbDisconnect(con)
  
  return(Load_table)
  
}


