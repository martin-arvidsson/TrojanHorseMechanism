# ------------------------------------------------------------------------------------------
#
# 2.2 - Trojan composition follow-up
#
# ------------------------------------------------------------------------------------------



# (0) Load packages
# ------------------------------------------------------------------------------------------
library(data.table)



# (1) Import raw data
# ------------------------------------------------------------------------------------------

# Base folder
base_folder <- "//MICRO.INTRA/PROJEKT/P0515$/P0515_GEM/Martin/TrojanHorseMechanism/"

# List files
ind_folder <- paste0(base_folder,"data/formatted/individual/")
firm_folder <- paste0(base_folder,"data/formatted/firm/")
ind_files <- list.files(ind_folder)
firm_files <- list.files(firm_folder)

# Import
ind_dts <- lapply(ind_files,function(x)fread(input = paste0(ind_folder,x)))
firm_dts <- lapply(firm_files,function(x)fread(input = paste0(firm_folder,x)))
# ------------------------------------------------------------------------------------------



# (2) Construct movers_dt each year
# ------------------------------------------------------------------------------------------
movers_dt <- list()
for(t in 2:length(ind_dts)){

  # - from --> to
  from <- ind_dts[[t-1]][,.(PersonLopNr,Female,Ar_t0=Ar,Workplace_t0=WorkplaceId)]
  to <- ind_dts[[t]][,.(PersonLopNr,Female,Ar_t1=Ar,Workplace_t1=WorkplaceId)]
  movers_dt[[t-1]] <- merge(x=from,
                            y=to,
                            by=c('PersonLopNr','Female'))
  # add information about from-place and to-place [at time t_0]
  fp_info_t0 <- firm_dts[[t-1]][,.(WorkplaceId,FP_t0=Female.Percentage)]
  # - adding origin FP (t-1)
  movers_dt[[t-1]] <- merge(x=movers_dt[[t-1]],
                            y=fp_info_t0[,.(Workplace_t0=WorkplaceId, FP_origin=FP_t0)],
                            by.x='Workplace_t0',by.y='Workplace_t0')
  # - adding target FP (t-1)
  movers_dt[[t-1]] <- merge(x=movers_dt[[t-1]],
                            y=fp_info_t0[,.(Workplace_t1=WorkplaceId, FP_target=FP_t0)],
                            by.x='Workplace_t1',by.y='Workplace_t1')
  
  # - order columns
  setcolorder(movers_dt[[t-1]],c('PersonLopNr','Female','Ar_t0','Ar_t1','Workplace_t0','Workplace_t1','FP_origin','FP_target'))
  gc()
}
# ------------------------------------------------------------------------------------------



# (3) Go year-by-year and:
#     3.1)  Identify eligible treatments at t-1
#     3.2)  Categorize treatments according to
#           the Female % of origin firm: [0-0.1,...,0.9-1.0]
#     3.3)  Identify followers along treatment paths,
#           and classify by gender
#     3.4)  Compute composition of followers (SGP/OGP)
# ------------------------------------------------------------------------------------------
followers_composition <- list()
treatments_destination_origin_diff <- list()
for(t in 2:length(movers_dt)){

  # Extract ties at time t
  current_treatments <- movers_dt[[t]][Workplace_t0!=Workplace_t1][,.(.N),by=c('Workplace_t0','Workplace_t1','Female','Ar_t0','Ar_t1','FP_origin','FP_target')]
  
  # Filter out cases with "double treatment" (both male and female moves)
  double <- current_treatments[,.(.N),by=c('Workplace_t0','Workplace_t1')][N==2]
  setkeyv(current_treatments,c('Workplace_t0','Workplace_t1'))
  setkeyv(double,c('Workplace_t0','Workplace_t1'))
  current_treatments <- current_treatments[!double]
  
  # Filter out cases where tie existed t-1
  temp_tmin1 <- unique(movers_dt[[t-1]][,c('Workplace_t0','Workplace_t1'),with=F])
  setkeyv(temp_tmin1,c('Workplace_t0','Workplace_t1'))
  setkeyv(current_treatments,c('Workplace_t0','Workplace_t1'))
  current_treatments <- current_treatments[!temp_tmin1,]
  
  # Rename some column-names (for clarification)
  setnames(current_treatments,'Female','Female_tie')
  setnames(current_treatments,'N','treatment_dosage')
  
  # Categorize origin-firms according to FP_origin
  current_treatments[FP_origin<=0.1,FP_origin2 := '0.0 - 0.1']
  current_treatments[FP_origin>0.1 & FP_origin <= 0.2,FP_origin2 := '0.1 - 0.2']
  current_treatments[FP_origin>0.2 & FP_origin <= 0.3,FP_origin2 := '0.2 - 0.3']
  current_treatments[FP_origin>0.3 & FP_origin <= 0.4,FP_origin2 := '0.3 - 0.4']
  current_treatments[FP_origin>0.4 & FP_origin <= 0.5,FP_origin2 := '0.4 - 0.5']
  current_treatments[FP_origin>0.5 & FP_origin <= 0.6,FP_origin2 := '0.5 - 0.6']
  current_treatments[FP_origin>0.6 & FP_origin <= 0.7,FP_origin2 := '0.6 - 0.7']
  current_treatments[FP_origin>0.7 & FP_origin <= 0.8,FP_origin2 := '0.7 - 0.8']
  current_treatments[FP_origin>0.8 & FP_origin <= 0.9,FP_origin2 := '0.8 - 0.9']
  current_treatments[FP_origin>0.9,FP_origin2 := '0.9 - 1.0']
  
  # Consider outcomes (followers)
  current_followers <- movers_dt[[t+1]][Workplace_t0!=Workplace_t1][,.(.N),by=c('Workplace_t0','Workplace_t1','Ar_t0','Ar_t1','Female')]
  setnames(current_followers,c('Ar_t0','Ar_t1'),c('following_Ar_t0','following_Ar_t1'))
  current_followers <- dcast.data.table(current_followers, 
                                        Workplace_t0 + Workplace_t1 + following_Ar_t0 + following_Ar_t1 ~ Female, value.var = 'N')
  current_followers[is.na(current_followers)] <- 0
  setnames(current_followers,c('0','1'),c('Male_Follower','Female_Follower'))
  
  # Merge
  treatment_composition_followup <- merge(x=current_treatments, 
                                          y=current_followers,
                                          by=c('Workplace_t0','Workplace_t1'))
  
  # Count Male and Female followers by FP_origin2
  treatment_composition_followup2 <- treatment_composition_followup[,.(sum_Male_Followers=sum(Male_Follower),
                                                                       sum_Female_Followers=sum(Female_Follower)),
                                                                    by=c('FP_origin2','Female_tie')]
  # Compute proportions
  treatment_composition_followup2[Female_tie==1,OGP_followers := sum_Male_Followers/(sum_Male_Followers+sum_Female_Followers)]
  treatment_composition_followup2[Female_tie==0,OGP_followers := sum_Female_Followers/(sum_Female_Followers+sum_Male_Followers)]
  
  # Add SGP-origin info
  # 0.0 - 0.1
  treatment_composition_followup2[Female_tie==0 & FP_origin2=='0.9 - 1.0', SGP_origin2 := '0.0 - 0.1']
  treatment_composition_followup2[Female_tie==1 & FP_origin2=='0.0 - 0.1', SGP_origin2 := '0.0 - 0.1']
  # 0.1 - 0.2
  treatment_composition_followup2[Female_tie==0 & FP_origin2=='0.8 - 0.9', SGP_origin2 := '0.1 - 0.2']
  treatment_composition_followup2[Female_tie==1 & FP_origin2=='0.1 - 0.2', SGP_origin2 := '0.1 - 0.2']
  # 0.2 - 0.3
  treatment_composition_followup2[Female_tie==0 & FP_origin2=='0.7 - 0.8', SGP_origin2 := '0.2 - 0.3']
  treatment_composition_followup2[Female_tie==1 & FP_origin2=='0.2 - 0.3', SGP_origin2 := '0.2 - 0.3']
  # 0.3 - 0.4
  treatment_composition_followup2[Female_tie==0 & FP_origin2=='0.6 - 0.7', SGP_origin2 := '0.3 - 0.4']
  treatment_composition_followup2[Female_tie==1 & FP_origin2=='0.3 - 0.4', SGP_origin2 := '0.3 - 0.4']
  # 0.4 - 0.6
  treatment_composition_followup2[Female_tie==0 & FP_origin2=='0.5 - 0.6', SGP_origin2 := '0.4 - 0.6']
  treatment_composition_followup2[Female_tie==1 & FP_origin2=='0.4 - 0.5', SGP_origin2 := '0.4 - 0.6']
  treatment_composition_followup2[Female_tie==0 & FP_origin2=='0.4 - 0.5', SGP_origin2 := '0.4 - 0.6']
  treatment_composition_followup2[Female_tie==1 & FP_origin2=='0.5 - 0.6', SGP_origin2 := '0.4 - 0.6']
  # 0.6 - 0.7
  treatment_composition_followup2[Female_tie==0 & FP_origin2=='0.3 - 0.4', SGP_origin2 := '0.6 - 0.7']
  treatment_composition_followup2[Female_tie==1 & FP_origin2=='0.6 - 0.7', SGP_origin2 := '0.6 - 0.7']
  # 0.7 - 0.8
  treatment_composition_followup2[Female_tie==0 & FP_origin2=='0.2 - 0.3', SGP_origin2 := '0.7 - 0.8']
  treatment_composition_followup2[Female_tie==1 & FP_origin2=='0.7 - 0.8', SGP_origin2 := '0.7 - 0.8']
  # 0.8 - 0.9
  treatment_composition_followup2[Female_tie==0 & FP_origin2=='0.1 - 0.2', SGP_origin2 := '0.8 - 0.9']
  treatment_composition_followup2[Female_tie==1 & FP_origin2=='0.8 - 0.9', SGP_origin2 := '0.8 - 0.9']
  # 0.9 - 1.0
  treatment_composition_followup2[Female_tie==0 & FP_origin2=='0.0 - 0.1', SGP_origin2 := '0.9 - 1.0']
  treatment_composition_followup2[Female_tie==1 & FP_origin2=='0.9 - 1.0', SGP_origin2 := '0.9 - 1.0']
  
  # Specify SG/OG followers
  treatment_composition_followup2[Female_tie==1, sum_OG_Followers := sum_Male_Followers]
  treatment_composition_followup2[Female_tie==1, sum_SG_Followers := sum_Female_Followers]
  treatment_composition_followup2[Female_tie==0, sum_OG_Followers := sum_Female_Followers]
  treatment_composition_followup2[Female_tie==0, sum_SG_Followers := sum_Male_Followers]
  
  # Compute OGP-followers by SGP_origin2 pooled across genders
  treatment_composition_followup3 <- copy(treatment_composition_followup2)
  treatment_composition_followup3 <- treatment_composition_followup3[,.(sum_OG_Followers=sum(sum_OG_Followers),
                                                                        sum_SG_Followers=sum(sum_SG_Followers)),
                                                                     by=SGP_origin2]
  treatment_composition_followup3[,OGP_followers := sum_OG_Followers/(sum_OG_Followers+sum_SG_Followers)]
  treatment_composition_followup3[,SGP_followers := sum_SG_Followers/(sum_SG_Followers+sum_OG_Followers)]
  
  # Add year indicator
  treatment_year <- paste0(unique(treatment_composition_followup$Ar_t0),'-',unique(treatment_composition_followup$Ar_t1))
  followup_year <- paste0(unique(treatment_composition_followup$following_Ar_t0),'-',unique(treatment_composition_followup$following_Ar_t1))
  treatment_composition_followup3[,treatment_year := treatment_year]
  treatment_composition_followup3[,followup_year := followup_year]
  
  # Consider the difference between origin - target in 
  # composition (from the point of view of the first-mover)
  # -----------------------------------------------
  # // First, classify (as above for outcomes) SGP-origin
  current_treatments[Female_tie==0 & FP_origin2=='0.9 - 1.0', SGP_origin2 := '0.0 - 0.1']
  current_treatments[Female_tie==1 & FP_origin2=='0.0 - 0.1', SGP_origin2 := '0.0 - 0.1']
  # 0.1 - 0.2
  current_treatments[Female_tie==0 & FP_origin2=='0.8 - 0.9', SGP_origin2 := '0.1 - 0.2']
  current_treatments[Female_tie==1 & FP_origin2=='0.1 - 0.2', SGP_origin2 := '0.1 - 0.2']
  # 0.2 - 0.3
  current_treatments[Female_tie==0 & FP_origin2=='0.7 - 0.8', SGP_origin2 := '0.2 - 0.3']
  current_treatments[Female_tie==1 & FP_origin2=='0.2 - 0.3', SGP_origin2 := '0.2 - 0.3']
  # 0.3 - 0.4
  current_treatments[Female_tie==0 & FP_origin2=='0.6 - 0.7', SGP_origin2 := '0.3 - 0.4']
  current_treatments[Female_tie==1 & FP_origin2=='0.3 - 0.4', SGP_origin2 := '0.3 - 0.4']
  # 0.4 - 0.6
  current_treatments[Female_tie==0 & FP_origin2=='0.5 - 0.6', SGP_origin2 := '0.4 - 0.6']
  current_treatments[Female_tie==1 & FP_origin2=='0.4 - 0.5', SGP_origin2 := '0.4 - 0.6']
  current_treatments[Female_tie==0 & FP_origin2=='0.4 - 0.5', SGP_origin2 := '0.4 - 0.6']
  current_treatments[Female_tie==1 & FP_origin2=='0.5 - 0.6', SGP_origin2 := '0.4 - 0.6']
  # 0.6 - 0.7
  current_treatments[Female_tie==0 & FP_origin2=='0.3 - 0.4', SGP_origin2 := '0.6 - 0.7']
  current_treatments[Female_tie==1 & FP_origin2=='0.6 - 0.7', SGP_origin2 := '0.6 - 0.7']
  # 0.7 - 0.8
  current_treatments[Female_tie==0 & FP_origin2=='0.2 - 0.3', SGP_origin2 := '0.7 - 0.8']
  current_treatments[Female_tie==1 & FP_origin2=='0.7 - 0.8', SGP_origin2 := '0.7 - 0.8']
  # 0.8 - 0.9
  current_treatments[Female_tie==0 & FP_origin2=='0.1 - 0.2', SGP_origin2 := '0.8 - 0.9']
  current_treatments[Female_tie==1 & FP_origin2=='0.8 - 0.9', SGP_origin2 := '0.8 - 0.9']
  # 0.9 - 1.0
  current_treatments[Female_tie==0 & FP_origin2=='0.0 - 0.1', SGP_origin2 := '0.9 - 1.0']
  current_treatments[Female_tie==1 & FP_origin2=='0.9 - 1.0', SGP_origin2 := '0.9 - 1.0']
  # // Also classify SGP_origin & SGP_target 
  current_treatments[Female_tie==1,SGP_origin := FP_origin]
  current_treatments[Female_tie==0,SGP_origin := 1-FP_origin]
  current_treatments[Female_tie==1,SGP_target := FP_target]
  current_treatments[Female_tie==0,SGP_target := 1-FP_target]
  # // Then, compute difference between destination - origin by SGP_origin2 category
  destination_origin_diff <- current_treatments[,.(Workplace_t0,
                                                   Workplace_t1,
                                                   SGP_diff_target_origin=SGP_target-SGP_origin,
                                                   SGP_origin2)]
  destination_origin_diff[,treatment_year := treatment_year]
  
  # Store
  followers_composition[[t-1]] <- treatment_composition_followup3
  treatments_destination_origin_diff[[t-1]] <- destination_origin_diff
  rm(treatment_composition_followup3);rm(destination_origin_diff)
  print(t-1)
  gc()
}
# ------------------------------------------------------------------------------------------



# Followers (Fig. 2B)
# ------------------------------------------------------------------------------------------
# Rbindlist
followers_composition <- rbindlist(followers_composition,use.names = T, fill = T)
# Compute across years
followers_composition2 <- followers_composition[,.(sum_OG_Followers=sum(sum_OG_Followers),
                                                   sum_SG_Followers=sum(sum_SG_Followers)),
                                                by=SGP_origin2]
followers_composition2[,OGP_followers := sum_OG_Followers/(sum_OG_Followers+sum_SG_Followers)]
followers_composition2[,SGP_followers := sum_SG_Followers/(sum_SG_Followers+sum_OG_Followers)]
# Export composition-followup-results
saveRDS(object = followers_composition2,
        file = '//micro.intra/projekt/P0515$/P0515_Gem/Martin/TrojanHorseMechanism/output/empirical_analysis/trojan_followers_composition.rds')
# ------------------------------------------------------------------------------------------



# Initial movers (Fig. 2C)
# ------------------------------------------------------------------------------------------
# Rbindlist
treatments_destination_origin_diff <- rbindlist(treatments_destination_origin_diff,use.names = T, fill = T)
# Compute across years
treatments_destination_origin_diff2 <- treatments_destination_origin_diff[,.(mean_SGP_diff_target_origin=mean(SGP_diff_target_origin)),
                                                                          by=SGP_origin2]
# Export
saveRDS(object = treatments_destination_origin_diff,
        file = '//micro.intra/projekt/P0515$/P0515_Gem/Martin/TrojanHorseMechanism/output/empirical_analysis/treatments_target_origin_diff.rds')
saveRDS(object = treatments_destination_origin_diff2,
        file = '//micro.intra/projekt/P0515$/P0515_Gem/Martin/TrojanHorseMechanism/output/empirical_analysis/treatments_target_origin_diff_summary.rds')
# ------------------------------------------------------------------------------------------
