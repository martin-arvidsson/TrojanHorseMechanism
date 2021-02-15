#
# 1 - Create data sets used for empirical and simulation analysesa
# > Annual data sets on individuals
# > Annual data sets on companies
# ------------------------------------------------------------------------------------------



# Parameters & functions
# ------------------------------------------------------------------------------------------
base_folder <- '//MICRO.INTRA/PROJEKT/P0515$/P0515_GEM/Martin/TrojanHorseMechanism/'
raw_ind_folder <- paste0(base_folder,'data/raw/individual/')
bkg_folder <- paste0(base_folder,'data/raw/bkg/')
astsni_translator_file <- paste0(base_folder,'data/misc/astsni_02G_for_all.csv')
uni_path <- paste0(base_folder,'data/raw/uni/dbo.Hreg_Reg.csv')
landkod_westrest_file <- paste0(base_folder,"data/misc/LandKodWestRest.txt")
sun2000_translator_file <- paste0(base_folder,'data/misc/sun2000_edlevel_translator.csv')
export_formatted_ind_folder <- paste0(base_folder,'data/formatted/individual/')
export_formatted_firm_folder <- paste0(base_folder,'data/formatted/firm/')
years <- 2000:2017
source(file = paste0(base_folder,'1_prepare_data/1_misc_data_prep_functions.R'))
library(data.table)


# (1) Import raw data
# --------------------------------------------
# -  Individuals
raw_ind_files <- list.files(raw_ind_folder)
individ_list <- lapply(raw_ind_files,function(x)fread(paste0(raw_ind_folder,x)))
# - Bkg
bkg_files <- list.files(bkg_folder)
bkg.data <- fread(paste0(bkg_folder,bkg_files[1]))
cat("... Raw data imported!", "\n")



# (2) Format AstSni codes
# --------------------------------------------
individ_list <- format_astsni_codes(individ_list=individ_list,
                                    astsni_translator_file=astsni_translator_file)



# (3) Exclude Comvux-students
# --------------------------------------------
  
# Only keep rows where Stud <= stud_req
individ_list <- lapply(individ_list, 
                       FUN = function(dt) dt[Stud <= 0,])
  
# Remove 'Stud' variable (no longer relevant)
individ_list <- lapply(individ_list, 
                       FUN = function(dt) dt[, Stud := NULL])



# (4) Exclude individuals registered at University
# --------------------------------------------

# Import Uni register
hreg <- fread(input = uni_path)

# Set-common-keys for hreg & each dt in individ_list
individ_list <- lapply(individ_list, 
                       FUN = function(dt) setkeyv(dt, c('PersonLopNr','Ar')))
setkeyv(hreg, c('PersonLopNr','Ar'))

# Merge + Exclude rows that match for each year/list
individ_list <- lapply(individ_list, 
                       FUN = function(dt) dt[!hreg,])

# Remove Uni register
remove(hreg)



# (5) Exclude senior citizens
# --------------------------------------------
individ_list <- lapply(individ_list, 
                       FUN = function(dt) dt[AldPens <= 0,])
individ_list <- lapply(individ_list, 
                       FUN = function(dt) dt[, AldPens := NULL])



# (5) Merge 'individ_list' & 'bkg.data' and create variables:
# - Age
# - Female
# - NonWestern
# --------------------------------------------
for(i in 1:length(individ_list)){
  temp <- merge(x = individ_list[[i]], 
                y = bkg.data, 
                by = "PersonLopNr")
  # - Age
  temp[,Alder := Ar-FodelseAr]
  # - Female
  temp[,Female := ifelse(test = Kon==2, yes = 1, no = 0)]
  # - NonWestern
  rest <- fread(landkod_westrest_file)[west!="1",]$LandKod
  temp[,NonWestern := ifelse(test = LandKod %in% rest, yes = 1, no = 0)]
  # - Delete old columns
  temp[,c('FodelseAr','Kon','LandKod') := NULL]
  # - Replace
  individ_list[[i]] <- temp
  rm(temp);gc()
}

# Only include people >= 18
individ_list <- lapply(X = individ_list, FUN = function(dt) dt[Alder >= 18, ])



# (6) Format education variable (--> number of years)
# --------------------------------------------
sun2000_translator <- fread(sun2000_translator_file)
for(i in 1:length(individ_list)){
  individ_list[[i]] <- merge(individ_list[[i]],
                             y=sun2000_translator,
                             by='Sun2000niva')
  individ_list[[i]][,Sun2000niva:=NULL]
  setnames(individ_list[[i]],old = 'Ed_level', new = 'Sun2000niva')
}

# Exclude individuals with '999' (= unknown education)
individ_list <- lapply(individ_list, function(x) x[Sun2000niva != 999, ])

# Keep only complete cases
individ_list <- lapply(individ_list, function(x) x[complete.cases(x),])

cat("... Individ lists formatted!", "\n")



# (7) Change names of some columns (e.g. remove superfluous KU1-part)
# --------------------------------------------
for(i in 1:length(individ_list)){
  setnames(x = individ_list[[i]],
           old = c('KU1CfarLopNr','KU1PeOrgLopNr','KU1Ink','KU1AstKommun','KU1SektorKod'),
           new = c('WorkplaceId','PeOrgLopNr','Ink','AstKommun','SektorKod'))
  
}



# (8) Unify workplace information (AstSnis, Sectors, Kommun)
# --------------------------------------------
individ_list <- unify_wp_data(individ_list = individ_list)



# (9) Create "firm_list" from "individ_list"
# --------------------------------------------
firm_list <- list()
for(i in 1:length(individ_list)){
  # - Averages
  avgs <- individ_list[[i]][,lapply(.SD,mean),
                            .SDcols=c('Female','Alder','NonWestern','Sun2000niva','Ink'),
                            by=c('WorkplaceId','Ar')]
  setnames(avgs,
           old = c('Female','Alder','NonWestern','Sun2000niva','Ink'),
           new = c('Female.Percentage','AvgAge','NonWestern.Percentage','AvgEducation','AvgWage'))
  # - Get PrivateOrg from SektorKod
  privpublic <- unique(individ_list[[i]][,.(WorkplaceId,Ar,SektorKod)])
  privpublic <- privpublic[,PrivateOrg := ifelse(test = SektorKod %in% c(21,22), yes = 1, no = 0)]
  privpublic[,SektorKod := NULL]
  
  # - Select other categorical variables
  firm_categs <- unique(individ_list[[i]][,.(WorkplaceId,Ar,PeOrgLopNr,AstSni,AstKommun)])
  
  # - Count number of employees
  n_employees <- individ_list[[i]][,.(AntalSys=.N),by=c('WorkplaceId','Ar')]
  
  # Merge
  firm_list[[i]] <- merge(x=avgs,y=privpublic,by=c('WorkplaceId','Ar')) 
  firm_list[[i]] <- merge(x=firm_list[[i]],y=n_employees,by=c('WorkplaceId','Ar'))
  firm_list[[i]] <- merge(x=firm_list[[i]],y=firm_categs,by=c('WorkplaceId','Ar'))
}

cat("... Firm lists formatted!", "\n")



# (10) Exclude firms that continually were very small
# --------------------------------------------
firm_minSysReq_anytimepoint <- 10

# Identify companies that had at least AntalSys emloyees at some/any time-point
firm_list_all <- rbindlist(firm_list, use.names = T, fill = T)
firm_list_all <- firm_list_all[,.(WorkplaceId,AntalSys)]
setkeyv(firm_list_all, c('WorkplaceId','AntalSys'))
firm_list_all_max <- firm_list_all[,tail(.SD,1),by='WorkplaceId']
firm_list_all_filtered <- firm_list_all_max[AntalSys >= firm_minSysReq_anytimepoint]
# - filter accordingly
firm_list <- lapply(firm_list, FUN = function(x) x[WorkplaceId %in% firm_list_all_filtered$WorkplaceId,])
individ_list <- lapply(individ_list, FUN = function(x) x[WorkplaceId %in% firm_list_all_filtered$WorkplaceId,])


 
# (11) Remove outliers
# --------------------------------------------
cat("... Removing Outliers!", "\n")
outlier_list <- list(PeOrgLopNr=c(487499),WorkplaceId=318378)
firm_list <- lapply(firm_list, 
                    FUN = function(x) x[!(WorkplaceId %in% outlier_list$WorkplaceId) & !(PeOrgLopNr %in% outlier_list$PeOrgLopNr),])
individ_list <- lapply(individ_list, 
                      FUN = function(x) x[!(WorkplaceId %in% outlier_list$WorkplaceId) & !(PeOrgLopNr %in% outlier_list$PeOrgLopNr),])



# (12) Export Ind + Ftg lists to defined path
# --------------------------------------------

# Individual
cat("... Exporting formatted individual data", "\n")
for(i in 1:length(individ_list)){
  current_year <- unique(individ_list[[i]]$Ar)
  fwrite(x = individ_list[[i]], file = paste0(export_formatted_ind_folder,'Formatted_ind_',current_year,'.csv'))
}


# Firm
cat("... Exporting formatted firm data", "\n")
for(i in 1:length(firm_list)){
  current_year <- unique(firm_list[[i]]$Ar)
  fwrite(x = firm_list[[i]], file = paste0(export_formatted_firm_folder,'Formatted_firm_',current_year,'.csv'))
}

cat("... Finished!", "\n")
