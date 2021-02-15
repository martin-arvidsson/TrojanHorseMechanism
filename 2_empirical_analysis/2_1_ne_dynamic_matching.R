# ------------------------------------------------------------------------------------------
#
# 2.1 - Node-embedding based dynamic matching
#
# ------------------------------------------------------------------------------------------



# (0) Needed packages
# ------------------------------------------------------------------------------------------
library(data.table)
library(word2vec)
library(rjson)



# (0) Base settings
# ------------------------------------------------------------------------------------------
base_folder <- "//MICRO.INTRA/PROJEKT/P0515$/P0515_GEM/Martin/TrojanHorseMechanism/"
tie_type <- 'Female'



# (1) Import raw data
# ------------------------------------------------------------------------------------------

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

  # from --> to
  from <- ind_dts[[t-1]][,.(PersonLopNr,Female,Ar_t0=Ar,Workplace_t0=WorkplaceId)]
  to <- ind_dts[[t]][,.(PersonLopNr,Female,Ar_t1=Ar,Workplace_t1=WorkplaceId)]
  movers_dt[[t-1]] <- merge(x=from,y=to,by=c('PersonLopNr','Female'))
  
  # add information about origin and target [at time t_0]
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
# -- extra (movers) --
keep_net_stats_cols <- c('PersonLopNr','Ar_t0','Workplace_t0','Workplace_t1','Female')
movers_dt <- rbindlist(movers_dt,use.names = T, fill = T)
movers_dt <- movers_dt[,c(keep_net_stats_cols),with=F]
setnames(movers_dt,"Ar_t0","Ar")
network_ties_stats <- movers_dt

# -- extra (firm_dts) --
firm_stats <- rbindlist(firm_dts,use.names = T, fill = T)
# ------------------------------------------------------------------------------------------



# (3) Split "network_ties" by year and...
#       (i) Create list of only moves (=nties)
#       (ii) Create matchDataList (as in simulation)
# ------------------------------------------------------------------------------------------
network_ties_stats$Ar <- as.numeric(network_ties_stats$Ar)
matchDataList <- split(network_ties_stats, network_ties_stats$Ar) 
nties <- lapply(matchDataList, function(x) x[Workplace_t0 != Workplace_t1, ])



# (4) Split "firm_stats" by year
# ------------------------------------------------------------------------------------------
firm_list <- copy(firm_stats)
firm_list$Ar <- as.numeric(firm_list$Ar)
firm_list <- firm_list[order(Ar)]
firm_list <- split(firm_list, firm_list$Ar)



# (5) Add grp-indentifier for the three considered categorical variables
# - Municipality
# - Industry
# - Public/Private
# ------------------------------------------------------------------------------------------
firm_list <- lapply(firm_list,function(x)x[,coarsened_group_1 := .GRP, by=c('AstKommun','AstSni','PrivateOrg')])



# (6) Compute avg in/out-degree for each firm each year
# ------------------------------------------------------------------------------------------
degree_stats <- rbindlist(nties,use.names = T, fill = T)
degree_stats <- degree_stats[Workplace_t0!=Workplace_t1]
outdegree_dt <- degree_stats[,.(outdegree=.N),by=c('Workplace_t0','Ar')]
indegree_dt <- degree_stats[,.(indegree=.N),by=c('Workplace_t1','Ar')]
degree_stats <- merge(x=indegree_dt,
                      y=outdegree_dt,
                      by.x=c('Workplace_t1','Ar'),
                      by.y=c('Workplace_t0','Ar'),
                      all.x = T,
                      all.y = T)
setnames(degree_stats,'Workplace_t1','WorkplaceId')
degree_stats[is.na(indegree),indegree := 0]
degree_stats[is.na(outdegree),outdegree := 0]

for(i in 1:length(firm_list)){
  firm_list[[i]] <- merge(x=firm_list[[i]],
                         y=degree_stats,
                         by=c('WorkplaceId','Ar'),
                         all.X=T,all.y=F)
}
# Re-bind into combined ''firm_stats''
firm_stats <- rbindlist(firm_list, use.names = T, fill = T)
# ------------------------------------------------------------------------------------------



# (7) Node Embeddings [Step 1]: Extract networks to use for node-embeddings
# ------------------------------------------------------------------------------------------
export_nets_for_ne <- TRUE
if(export_nets_for_ne==TRUE){
  ne_lag_type <- 'tmin2_tmin1'
  if(ne_lag_type=='tmin2_tmin1'){
    start_t <- 3
  }else{
    start_t <- 2
  }
  ne_nets <- list()
  tick <- 1
  for(t in start_t:(length(nties)-1)){
    current_treatment_year <- unique(nties[[t]]$Ar)
    # V1: Extract time t & t-1 ties
    # ===============================
    if(ne_lag_type=='tmin1_t'){
      current_ne_net <- rbindlist(nties[((t-1):(t))],use.names = T, fill = T)
      # V2: Extract time t-1 & t-2 ties
      # ===============================
    }else if(ne_lag_type=='tmin2_tmin1'){
      current_ne_net <- rbindlist(nties[((t-2):(t-1))],use.names = T, fill = T)
      # V3: Extract time t-1
      # ===============================
    }else if(ne_lag_type=='tmin1'){
      current_ne_net <- nties[[t-1]]
    }
    current_ne_net <- current_ne_net[,.(.N),by=c('Workplace_t0','Workplace_t1',tie_type,'Ar')]
    current_ne_net <- current_ne_net[,.(.N),by=c('Workplace_t0','Workplace_t1')]
    current_ne_net <- unique(current_ne_net[,.(Workplace_t0,Workplace_t1,treatment_year=current_treatment_year)])
    ne_nets[[tick]] <- current_ne_net
    tick <- tick + 1
  }
  ne_nets <- rbindlist(ne_nets,use.names = T, fill = T)
  # Export
  fwrite(x = ne_nets, file = paste0(base_folder,'data/input_nets_node_embeddings/node_embedding_nets_tmin2_tmin1.csv'))
}
# ------------------------------------------------------------------------------------------



# ///////////////////////////////////////////////////////////////////////////////////////////////////////////////
# /// (8) Node Embeddings [Step 2]: Run "generate_rws.py" to drop random walkers on the networks from step #7 ///
# ///////////////////////////////////////////////////////////////////////////////////////////////////////////////



# (9) Node Embeddings [Step 3]: Import rws & fit node embeddings 
# ------------------------------------------------------------------------------------------
# - RW settings
p <- 1
q <- 1
# - NE settings
d <- 20
w <- 10
# - rw files
rw_folder <- '\\\\micro.intra/projekt/P0515$/P0515_Gem/Martin/TrojanHorseMechanism/data/node_embeddings/rws/p1q1/'
setwd(rw_folder)
rw_files <- list.files(rw_folder)
rw_files <- rw_files[stringi::stri_detect(str = rw_files, regex = '.json')]
# - id files
id_folder <- '\\\\micro.intra/projekt/P0515$/P0515_Gem/Martin/TrojanHorseMechanism/data/node_embeddings/rws/p1q1/id_translation_csvs/'
id_files <- list.files(id_folder)
id_dts <- lapply(id_files,function(x)fread(paste0(id_folder,x),select = c(2:4)))
id_dts <- rbindlist(id_dts,use.names = T, fill = T)
# - export folder
output_ne_path <- paste0('\\\\micro.intra/projekt/P0515$/P0515_Gem/Martin/TrojanHorseMechanism/data/node_embeddings/output/ne_d',d,'_w',w,'_p',p,'_q',q,'.rds')

# Loop over files
node_embedding_dts <- list()
for(i in 1:length(rw_files)){
  
  ## Test
  #i <- 1
  
  # (0) Identify treatment year
  current_treatment_year <- strsplit(x = rw_files[i],split = '_')[[1]][2]
  current_treatment_year <- gsub(x = current_treatment_year, pattern = '.json',replacement = '')
  current_treatment_year <- as.integer(current_treatment_year)
  # (1) Set ids
  current_id_dt <- id_dts[treatment_year==current_treatment_year]
  # (2) Import RW-vectors from Python
  tst <- rjson::fromJSON(file = paste0(rw_folder,rw_files[i]))
  tst <- sapply(tst,function(x)paste(x, collapse = ' '))
  # (3) Fit word2vec model
  model <- word2vec(x = tst, type = 'cbow', dim = d, window = w, iter = 10)
  embeddings <- as.matrix(model)
  embedding_row_ids <- rownames(embeddings)
  embeddings_dt <- data.table(embeddings)
  embeddings_dt[,sparse_mat_id := embedding_row_ids]
  embeddings_dt[,sparse_mat_id := as.numeric(sparse_mat_id)]
  embeddings_dt[is.na(sparse_mat_id),sparse_mat_id := -1]
  setkeyv(embeddings_dt,'sparse_mat_id')
  # (4) Merge WorkplaceId
  embeddings_dt2 <- merge(x=embeddings_dt,
                          y=current_id_dt,
                          by.x='sparse_mat_id',
                          by.y='sparse_mat_id')
  # (5) Store
  node_embedding_dts[[i]] <- embeddings_dt2
  rm(embeddings_dt2);rm(embeddings_dt);rm(embeddings);gc()
  # (6) Print
  print(i)
}
# Rbind
node_embedding_dts <- rbindlist(node_embedding_dts,use.names = T, fill = T)
# Export
saveRDS(object = node_embedding_dts, file = output_ne_path)
# ------------------------------------------------------------------------------------------



# (10) Node Embeddings [Step 4]: Import node-embeddings & Fit kmeans --> embedding clusters
# ------------------------------------------------------------------------------------------
output_ne_clusters_path <- "\\\\micro.intra/projekt/P0515$/P0515_Gem/Martin/TrojanHorseMechanism/data/node_embeddings/output/ne_clusters_d20_p1_q1_k30.rds"
# - Import
node_embedding_dts <- readRDS(output_ne_path)
# - Evaluate number of clusters
d <- 25 #20
cluster_embeddings <- T
if(cluster_embeddings==TRUE){
  ks <- seq(1,100,10)
  embedding_dims <- paste0('V',1:d)
  tr_years <- unique(node_embedding_dts$treatment_year)
  wss <- list()
  for(i in 1:length(tr_years)){
    #i <- 1
    wss[[i]] <- sapply(ks,
                       function(k) (kmeans(x = node_embedding_dts[treatment_year==tr_years[i]][,c(embedding_dims),with=F],k,nstart=50,iter.max = 100)$tot.withinss))
    plot(ks,wss[[i]])
    print(i)
  }
}
# - For d=20, use 30 clusters!
k <- 30
d <- 20
embedding_dims <- paste0('V',1:d)
tr_years <- unique(node_embedding_dts$treatment_year)
node_clusters <- list()
for(i in 1:length(tr_years)){
  #i <- 1
  output <- kmeans(x = node_embedding_dts[treatment_year==tr_years[i]][,c(embedding_dims),with=F],
                   centers = k,
                   nstart=50,
                   iter.max = 250)
  node_clusters[[i]] <- data.table(WorkplaceId=node_embedding_dts[treatment_year==tr_years[i]]$node_id,
                                   cluster_id=output$cluster,
                                   Ar=node_embedding_dts[treatment_year==tr_years[i]]$treatment_year)
  print(i)
}
# Rbind
node_clusters <- rbindlist(node_clusters, use.names = T, fill = T)
# Save clusters
saveRDS(object = node_clusters, file = output_ne_clusters_path)
# ------------------------------------------------------------------------------------------



# (11) Node Embeddings [Step 5]: Import embedding clusters & merge with firm_stats dt
# ------------------------------------------------------------------------------------------
# Settings
d <- 20
k <- 30
date_id <- '210207'
ne <- NULL

# Match on clusters based on the node embeddings?
use_ne_clusters <- TRUE
if(use_ne_clusters==TRUE){
  ne_clusters <- readRDS(file = output_ne_clusters_path)
  ne <- readRDS(file = output_ne_path)
  ne[,sparse_mat_id := NULL]
  firm_stats <- merge(x=firm_stats,
                      y=ne,
                      by.x=c('WorkplaceId','Ar'),
                      by.y=c('node_id','treatment_year'))
}else{
  ne_clusters <- NULL
}
# ------------------------------------------------------------------------------------------



# (12) Dynamic matching
# ------------------------------------------------------------------------------------------

# Baseline # of breaks
nbins <- 3

# Tie-type?
tie_type <- 'Female'

# Loop
treatment_dts <- list()
for(t in 3:(length(nties)-1)){
  
  # Extract time t ties
  current_treatments <- nties[[t]][,.(.N),by=c('Workplace_t0','Workplace_t1',tie_type,'Ar')]
  
  # Filter out cases with "double treatment" (both male and female moves)
  double <- current_treatments[,.(.N),by=c('Workplace_t0','Workplace_t1')][N==2]
  setkeyv(current_treatments,c('Workplace_t0','Workplace_t1'))
  setkeyv(double,c('Workplace_t0','Workplace_t1'))
  current_treatments <- current_treatments[!double]
  
  # Filter out cases where tie existed t-1
  temp_tmin1 <- unique(nties[[t-1]][,c('Workplace_t0','Workplace_t1'),with=F])
  setkeyv(temp_tmin1,c('Workplace_t0','Workplace_t1'))
  setkeyv(current_treatments,c('Workplace_t0','Workplace_t1'))
  current_treatments <- current_treatments[!temp_tmin1,]
  
  # Add-on basic (origin) firm info for current treatments
  current_firm_stats <- firm_list[[t]][,.(WorkplaceId,
                                          coarsened_group_1=coarsened_group_1,
                                          FP=Female.Percentage,
                                          AA=AvgAge,
                                          NWP=NonWestern.Percentage,
                                          ED=Avg.Ed.Level,
                                          AW_log=log(AvgWage),
                                          log_indeg=log(1+indegree),
                                          log_outdeg=log(1+outdegree),
                                          log_wp_size=log(AntalSys))]
  
  # We match on embedding clusters, not the original embedding columns
  emb_cols <- NULL
  
  sdcols <- setdiff(colnames(current_firm_stats),c('WorkplaceId','coarsened_group_1'))
  current_firm_stats_coarsened <- copy(current_firm_stats)
  
  # - spec cols (that are not balanced) -
  spec_cols1 <- c('FP')
  if(!is.null(spec_cols1)){
    current_firm_stats_coarsened_spec1 <- copy(current_firm_stats_coarsened)
    current_firm_stats_coarsened_spec1 <- current_firm_stats_coarsened_spec1[,c(spec_cols1,'WorkplaceId'),with=F][,(spec_cols1) := lapply(.SD,function(x)as.numeric(cut(x,breaks=hist(x,breaks=(nbins+1))$breaks))),.SDcols=spec_cols1]
  }
  spec_cols2 <- c('ED')
  if(!is.null(spec_cols2)){
    current_firm_stats_coarsened_spec2 <- copy(current_firm_stats_coarsened)
    current_firm_stats_coarsened_spec2 <- current_firm_stats_coarsened_spec2[,c(spec_cols2,'WorkplaceId'),with=F][,(spec_cols2) := lapply(.SD,function(x)as.numeric(cut(x,breaks=hist(x,breaks=(nbins+2))$breaks))),.SDcols=spec_cols2]
  }
  spec_cols3 <- c('log_outdeg')
  if(!is.null(spec_cols3)){
    current_firm_stats_coarsened_spec3 <- copy(current_firm_stats_coarsened)
    current_firm_stats_coarsened_spec3 <- current_firm_stats_coarsened_spec3[,c(spec_cols3,'WorkplaceId'),with=F][,(spec_cols3) := lapply(.SD,function(x)as.numeric(cut(x,breaks=hist(x,breaks=(nbins+3))$breaks))),.SDcols=spec_cols3]
  }
  spec_cols4 <- emb_cols
  if(!is.null(spec_cols4)){
    current_firm_stats_coarsened_spec4 <- copy(current_firm_stats_coarsened)
    current_firm_stats_coarsened_spec4 <- current_firm_stats_coarsened_spec4[,c(spec_cols4,'WorkplaceId'),with=F][,(spec_cols4) := lapply(.SD,function(x)as.numeric(cut(x,breaks=hist(x,breaks=(nbins-1))$breaks))),.SDcols=spec_cols4]
  }else{
    current_firm_stats_coarsened_spec4 <- NULL
  }
  
  # - regular -
  current_firm_stats_coarsened <- current_firm_stats_coarsened[,(sdcols) := lapply(.SD,function(x)as.numeric(cut(x,breaks=hist(x,breaks=nbins)$breaks))),
                                                             .SDcols=sdcols]
  
  if(!is.null(spec_cols1)){
    current_firm_stats_coarsened <- current_firm_stats_coarsened[,-c(spec_cols1,spec_cols2,spec_cols3,spec_cols4),with=F]
    current_firm_stats_coarsened <- merge(x=current_firm_stats_coarsened,
                                         y=current_firm_stats_coarsened_spec1,
                                         by=c('WorkplaceId'))
    current_firm_stats_coarsened <- merge(x=current_firm_stats_coarsened,
                                         y=current_firm_stats_coarsened_spec2,
                                         by=c('WorkplaceId'))
    current_firm_stats_coarsened <- merge(x=current_firm_stats_coarsened,
                                         y=current_firm_stats_coarsened_spec3,
                                         by=c('WorkplaceId'))
    if(!is.null(current_firm_stats_coarsened_spec4)){
      current_firm_stats_coarsened <- merge(x=current_firm_stats_coarsened,
                                           y=current_firm_stats_coarsened_spec4,
                                           by=c('WorkplaceId'))
    }
  }
  
  # If we want to use node-embedding clusters
  if(use_ne_clusters==TRUE){
    current_year <- unique(firm_list[[t]]$Ar)
    current_firm_stats_coarsened <- merge(x=current_firm_stats_coarsened,
                                         y=ne_clusters[Ar==current_year,],
                                         by.x=c('WorkplaceId'),
                                         by.y=c('WorkplaceId'))
    sdcols <- c(sdcols,'cluster_id')
  }
  
  # --
  if('indeg' %in% colnames(current_firm_stats_coarsened)){
    current_firm_stats_coarsened[is.na(indeg),indeg:=0]
    current_firm_stats_coarsened[is.na(outdeg),outdeg:=0]
  }else{
    current_firm_stats_coarsened[is.na(log_indeg),log_indeg:=log(1)]
    current_firm_stats_coarsened[is.na(log_outdeg),log_outdeg:=log(1)]
  }
  current_firm_stats_coarsened[is.na(FP),FP:=0]
  current_firm_stats_coarsened[is.na(NWP),NWP:=0]
  current_firm_stats_coarsened[,coarsened_group_2 := .GRP, by=c('coarsened_group_1',sdcols)]
  current_firm_stats_coarsened2 <- current_firm_stats_coarsened[,.(WorkplaceId,coarsened_group_2)]
  
  treatments_info_origin_firm <- merge(x=unique(current_treatments[,.(origin=Workplace_t0,target=Workplace_t1,Ar)]),
                                       y=current_firm_stats_coarsened2,
                                       by.x='origin',
                                       by.y='WorkplaceId')
  
  current_treatment_dt <- copy(treatments_info_origin_firm)
  current_treatment_dt[,match_grp := .GRP,by=c('origin','target')]
  current_treatment_dt_treated <- copy(current_treatment_dt)
  current_treatment_dt_treated[,treated := 1]
  
  # Created dt for 'control'
  current_treatment_dt_control <- merge(x=current_treatment_dt,
                                        y=current_firm_stats_coarsened2,
                                        by='coarsened_group_2',
                                        all.x=T,all.y=F,
                                        allow.cartesian = T)
  current_treatment_dt_control[,origin := WorkplaceId]
  current_treatment_dt_control[,WorkplaceId := NULL]
  current_treatment_dt_control[,treated := 0]
  setcolorder(current_treatment_dt_control,colnames(current_treatment_dt_treated))
  
  # Filter out control-cases where ties exist in t
  setkeyv(current_treatment_dt,c('origin','target'))
  current_treatment_dt_control <- current_treatment_dt_control[!current_treatment_dt]
  
  # Filter out control-cases where ties exist in t-1
  setkeyv(current_treatment_dt_control, c('origin','target'))
  setkeyv(temp_tmin1,c('Workplace_t0','Workplace_t1'))
  current_treatment_dt_control <- current_treatment_dt_control[!temp_tmin1]
  
  # Rbind 'treated' and 'control'
  current_treatment_dt2 <- rbindlist(list(current_treatment_dt_treated,
                                          current_treatment_dt_control),
                                     use.names = T, fill = T)
  # Remove self-controls
  current_treatment_dt2[,N:=.N,by=c('origin','match_grp')]
  setkeyv(current_treatment_dt2,c('origin','match_grp'))
  current_treatment_dt2 <- current_treatment_dt2[!(N==2 & treated==0)]
  current_treatment_dt2[,N:=NULL]
  
  # Remove cases without match
  current_treatment_dt2[,n_matches := .N, by=match_grp]
  current_treatment_dt2 <- current_treatment_dt2[n_matches>1]
  current_treatment_dt2[,n_matches := NULL]
  current_treatment_dt2[,match_grp := .GRP,by='match_grp']
  setkeyv(current_treatment_dt2,'match_grp')
  
  # Filter out controls where a move exists the year prior to treatment
  current_treatment_dt2_controls <- current_treatment_dt2[treated==0]
  setkeyv(current_treatment_dt2_controls,c('origin','target'))
  setnames(temp_tmin1,c('Workplace_t0','Workplace_t1'),c('origin','target'))
  setkeyv(temp_tmin1,c('origin','target'))
  current_treatment_dt2_controls <- current_treatment_dt2_controls[!temp_tmin1]
  current_treatment_dt2 <- rbindlist(list(current_treatment_dt2[treated==1],
                                          current_treatment_dt2_controls),use.names = T, fill = T)
  
  # (Again) remove cases without match
  current_treatment_dt2[,n_matches := .N,by=match_grp]
  current_treatment_dt2 <- current_treatment_dt2[n_matches>1]
  current_treatment_dt2[,n_matches := NULL]
  current_treatment_dt2[,match_grp := .GRP,by='match_grp']
  setkeyv(current_treatment_dt2,'match_grp')
  
  # Remove cases where either treated or control pair is within same PeOrgLopNr, but the other isn't
  wp_peorg_id_dt <- unique(rbindlist(list(firm_list[[t-1]][,.(WorkplaceId,PeOrg=PeOrgLopNr)],
                                          firm_list[[t]][,.(WorkplaceId,PeOrg=PeOrgLopNr)],
                                          firm_list[[t+1]][,.(WorkplaceId,PeOrg=PeOrgLopNr)])))
  current_treatment_dt2 <- merge(x=current_treatment_dt2,
                                 y=wp_peorg_id_dt[,.(WorkplaceId,origin_PeOrg=PeOrg)],
                                 by.x=c('origin'),
                                 by.y=c('WorkplaceId'),
                                 all.x=T,
                                 allow.cartesian = T)
  current_treatment_dt2 <- merge(x=current_treatment_dt2,
                                 y=wp_peorg_id_dt[,.(WorkplaceId,target_PeOrg=PeOrg)],
                                 by.x=c('target'),
                                 by.y=c('WorkplaceId'),
                                 all.x=T,
                                 allow.cartesian = T)
  current_treatment_dt2 <- current_treatment_dt2[complete.cases(current_treatment_dt2)]
  current_treatment_dt2[,N := .N,by=match_grp]
  current_treatment_dt2[,sum_treated := sum(treated),by=match_grp]
  current_treatment_dt2 <- current_treatment_dt2[N>=2 & sum_treated>0]
  current_treatment_dt2[,same_peorg := ifelse(origin_PeOrg==target_PeOrg,1,0)]
  treated_same_peorg <- unique(current_treatment_dt2[treated==1,.(treated_same_peorg=sum(same_peorg)),by=match_grp])
  current_treatment_dt2 <- merge(x=current_treatment_dt2,
                                 y=treated_same_peorg,
                                 by='match_grp')
  current_treatment_dt2[treated_same_peorg>0,treated_same_peorg := 1]
  current_treatment_dt2 <- current_treatment_dt2[!(same_peorg != treated_same_peorg & sum_treated==1)]
  current_treatment_dt2 <- current_treatment_dt2[same_peorg==0]
  current_treatment_dt2[,N := .N, by=match_grp]
  current_treatment_dt2[,sum_treated := sum(treated), by=match_grp]
  current_treatment_dt2 <- current_treatment_dt2[(N>=2 & sum_treated>=1)]
  current_treatment_dt2 <- unique(current_treatment_dt2[,.(match_grp,origin,target,Ar,coarsened_group_2,treated)])
  
  # Add info on treatment type (male/female)
  match_grp_kind <- current_treatment_dt2[treated==1,.(origin,target,match_grp)]
  if(tie_type=='Female'){
    match_grp_kind <- merge(x=match_grp_kind,
                            y=current_treatments[,.(origin=Workplace_t0,
                                                    target=Workplace_t1,
                                                    treatment_kind=ifelse(Female==1,yes = 'Female', no = 'Male'),
                                                    treatment_dosage=N)],
                            by=c('origin','target'))
  }else{
    match_grp_kind <- merge(x=match_grp_kind,
                            y=current_treatments[,.(origin=Workplace_t0,
                                                    target=Workplace_t1,
                                                    treatment_kind=ifelse(NonWestern==1,yes = 'NW',no = 'W'),
                                                    treatment_dosage=N)],
                            by=c('origin','target'))
  }
  current_treatment_dt2 <- merge(x=current_treatment_dt2,
                                 y=match_grp_kind[,.(match_grp,treatment_kind,treatment_dosage)],
                                 by=c('match_grp'),
                                 all.x=T,all.y=F)
  current_treatment_dt2[treated==0,treatment_dosage:=0]
  
  # Add outcomes
  current_outcomes <- nties[[t+1]][,.(.N),by=c('Workplace_t0','Workplace_t1',tie_type,'Ar')]
  current_outcomes <- dcast.data.table(current_outcomes, Workplace_t0 + Workplace_t1 + Ar ~ get(tie_type), value.var = 'N')
  
  if(tie_type=='Female'){
    setnames(current_outcomes,c('0','1'),c('y_Male','y_Female'))
    current_outcomes[is.na(y_Male),y_Male:=0]
    current_outcomes[is.na(y_Female),y_Female:=0]
  }else if(tie_type=='NonWestern'){
    setnames(current_outcomes,c('0','1'),c('y_W','y_NW'))
    current_outcomes[is.na(y_W),y_W:=0]
    current_outcomes[is.na(y_NW),y_NW:=0]
  }
  
  setnames(current_outcomes,c('Workplace_t0','Workplace_t1'),c('origin','target'))
  current_treatment_dt3 <- merge(x=current_treatment_dt2,
                                 y=current_outcomes[,-c('Ar'),with=F],
                                 by=c('origin','target'),
                                 all.x=T,all.y=F)
  
  # Add outcome
  if(tie_type=='Female'){
    current_treatment_dt3[is.na(y_Male),y_Male:=0]
    current_treatment_dt3[is.na(y_Female),y_Female:=0]
  }else if(tie_type=='NonWestern'){
    current_treatment_dt3[is.na(y_NW),y_NW:=0]
    current_treatment_dt3[is.na(y_W),y_W:=0]
  }
  
  # Add composition information about ORIGIN
  current_treatment_dt3 <- merge(x=current_treatment_dt3,
                                 y=current_firm_stats_coarsened[,.(WorkplaceId,origin_FP=FP,origin_NWP=NWP)],
                                 by.x='origin',
                                 by.y='WorkplaceId',
                                 all.x=T,all.y=F)
  
  # Add composition information about TARGET
  current_treatment_dt3 <- merge(x=current_treatment_dt3,
                                 y=current_firm_stats_coarsened[,.(WorkplaceId,target_FP=FP,target_NWP=NWP)],
                                 by.x='target',
                                 by.y='WorkplaceId',
                                 all.x=T,all.y=F)
  
  # Store
  treatment_dts[[t]] <- current_treatment_dt3
  print(t)
  gc()
}

# Rbindlist
treatment_dts <- rbindlist(treatment_dts, use.names = T, fill = T)
treatment_dts <- treatment_dts[complete.cases(treatment_dts)]

# How many treatments?
nrow(treatment_dts[treated==1])
# ------------------------------------------------------------------------------------------



# (13) Add more detailed information (to later compute balance)
# ------------------------------------------------------------------------------------------
treatment_dts <- merge(x=treatment_dts,
                       y=firm_stats[,.(Ar,
                                        WorkplaceId,
                                        origin_FP_perc=Female.Percentage,
                                        origin_NWP_perc=NonWestern.Percentage,
                                        origin_PeOrgLopNr=PeOrgLopNr,
                                        origin_Kommun = AstKommun,
                                        origin_AvgAge = AvgAge,
                                        origin_AvgWage = AvgWage,
                                        origin_AstSni = AstSni,
                                        origin_PrivateOrg = PrivateOrg,
                                        origin_AntalSys = AntalSys,
                                        origin_edu_level = Avg.Ed.Level,
                                        origin_indeg = indegree,
                                        origin_outdeg = outdegree,
                                        origin_V1=V1,
                                        origin_V2=V2,
                                        origin_V3=V3,
                                        origin_V4=V4,
                                        origin_V5=V5,
                                        origin_V6=V6,
                                        origin_V7=V7,
                                        origin_V8=V8,
                                        origin_V9=V9,
                                        origin_V10=V10,
                                        origin_V11=V11,
                                        origin_V12=V12,
                                        origin_V13=V13,
                                        origin_V14=V14,
                                        origin_V15=V15,
                                        origin_V16=V16,
                                        origin_V17=V17,
                                        origin_V18=V18,
                                        origin_V19=V19,
                                        origin_V20=V20
                         )],
                         by.x=c('origin','Ar'),
                         by.y=c('WorkplaceId','Ar'))
treatment_dts <- merge(x=treatment_dts,
                       y=firm_stats[,.(Ar,
                                      WorkplaceId,
                                      target_FP_perc=Female.Percentage,
                                      target_NWP_perc=NonWestern.Percentage,
                                      target_PeOrgLopNr=PeOrgLopNr,
                                      target_Kommun = AstKommun,
                                      target_AvgAge = AvgAge,
                                      target_AvgWage = AvgWage,
                                      target_AstSni = AstSni,
                                      target_PrivateOrg = PrivateOrg,
                                      target_AntalSys = AntalSys,
                                      target_edu_level = Avg.Ed.Level,
                                      target_indeg = indegree,
                                      target_outdeg = outdegree,
                                      target_V1=V1,
                                      target_V2=V2,
                                      target_V3=V3,
                                      target_V4=V4,
                                      target_V5=V5,
                                      target_V6=V6,
                                      target_V7=V7,
                                      target_V8=V8,
                                      target_V9=V9,
                                      target_V10=V10,
                                      target_V11=V11,
                                      target_V12=V12,
                                      target_V13=V13,
                                      target_V14=V14,
                                      target_V15=V15,
                                      target_V16=V16,
                                      target_V17=V17,
                                      target_V18=V18,
                                      target_V19=V19,
                                      target_V20=V20
                       )],
                       by.x=c('target','Ar'),
                       by.y=c('WorkplaceId','Ar'))

if(tie_type=='Female'){
  # Seg indicatior
  treatment_dts[,init_seg_move := ifelse(origin_FP_perc<target_FP_perc, 1, 0)]
  # Compute prob of following
  treatment_dts[,y_Male_binary := ifelse(y_Male>0,1,0)]
  treatment_dts[,y_Female_binary := ifelse(y_Female>0,1,0)]
}else if(tie_type=='NonWestern'){
  # Seg indicatior
  treatment_dts[,init_seg_move := ifelse(origin_NWP_perc<target_NWP_perc, 1, 0)]
  # Compute prob of following
  treatment_dts[,y_W_binary := ifelse(y_W>0,1,0)]
  treatment_dts[,y_NW_binary := ifelse(y_NW>0,1,0)]
}
# ------------------------------------------------------------------------------------------



# (14) Add weights, outcome, and export
# ------------------------------------------------------------------------------------------

# Add row-weight
treatment_dts[treated==0,n_controls := .N, by = .(match_grp,Ar,treatment_kind)]
treatment_dts[treated==1,n_controls := 1]
treatment_dts[,w := 1 / n_controls]

# Binary outcome
treatment_dts[y_Male>1,y_Male := 1]
treatment_dts[y_Female>1,y_Female := 1]

# Export
saveRDS(object = treatment_dts, file = paste0('\\\\micro.intra/projekt/P0515$/P0515_Gem/Martin/ABM/output/cem_matched_dt_',date_id,'_d',d,'_k',k,'.rds'))
# ------------------------------------------------------------------------------------------



# (15) Consider balance
# ------------------------------------------------------------------------------------------
treatment_dts[,match_group2 := paste0(match_grp,Ar,treatment_kind),by=c('match_grp','Ar','treatment_kind')]
cem_balance_dt <- treatment_dts[,.(FP_diff=abs(origin_FP_perc-target_FP_perc),
                                     NWP_diff=abs(origin_NWP_perc-target_NWP_perc),
                                     sameKommun=ifelse(origin_Kommun==target_Kommun,1,0),
                                     sameIndustry=ifelse(origin_AstSni==target_AstSni,1,0),
                                     sameSector=ifelse(origin_PrivateOrg==target_PrivateOrg,1,0),
                                     edu_diff=abs(origin_edu_level-target_edu_level),
                                     age_diff=abs(origin_AvgAge-target_AvgAge),
                                     log_wage_diff=abs(log(origin_AvgWage)-log(target_AvgWage)),
                                     log_origin_outdeg=log(1+origin_outdeg), 
                                     log_origin_indeg=log(1+origin_indeg),
                                     log_origin_AntalSys=log(origin_AntalSys), 
                                     # -- node embeddings --
                                     V1_diff = abs(origin_V1-target_V1),
                                     V2_diff = abs(origin_V2-target_V2),
                                     V3_diff = abs(origin_V3-target_V3),
                                     V4_diff = abs(origin_V4-target_V4),
                                     V5_diff = abs(origin_V5-target_V5),
                                     V6_diff = abs(origin_V6-target_V6),
                                     V7_diff = abs(origin_V7-target_V7),
                                     V8_diff = abs(origin_V8-target_V8),
                                     V9_diff = abs(origin_V9-target_V9),
                                     V10_diff = abs(origin_V10-target_V10),
                                     V11_diff = abs(origin_V11-target_V11),
                                     V12_diff = abs(origin_V12-target_V12),
                                     V13_diff = abs(origin_V13-target_V13),
                                     V14_diff = abs(origin_V14-target_V14),
                                     V15_diff = abs(origin_V15-target_V15),
                                     V16_diff = abs(origin_V16-target_V16),
                                     V17_diff = abs(origin_V17-target_V17),
                                     V18_diff = abs(origin_V18-target_V18),
                                     V19_diff = abs(origin_V19-target_V19),
                                     V20_diff = abs(origin_V20-target_V20),
                                     treated,
                                     match_group2,
                                     w)]

num_cols <-  cem_balance_dt[,sapply(.SD,class)]
num_cols <- names(num_cols)[num_cols=='numeric']
num_cols <- num_cols[!num_cols%in%c('treated','w')]
means <- cem_balance_dt[,lapply(.SD,weighted.mean,w),by=treated, .SDcols = num_cols]
means <- melt.data.table(data = means, id.vars = c('treated'))
means_wide <- dcast.data.table(means,variable~treated,value.var = 'value')
setnames(means_wide,c(2,3),c('mean_tr0','mean_tr1'))
setnames(means,'value','wmean')
cem_balanc_dt_long <- melt.data.table(cem_balance_dt, id.vars=c('treated','match_group2','w'))
cem_balanc_dt_long <- merge(x=cem_balanc_dt_long,y=means,by=c('treated','variable'))
cem_balanc_dt_long[,wmean2 := weighted.mean(x = value, w = w),by=.(variable,treated)]
cem_balanc_dt_long[,sqdiff := (value-wmean)^2]
cem_balanc_dt_long[,wsqdiff := sqdiff * w]
cem_balanc_dt_long[,N := .N, by = .(variable,treated)]
cem_balanc_dt_long2 <- unique(cem_balanc_dt_long[,.(num=sum(wsqdiff),
                                                    sum_w=sum(w),
                                                    N=N),by=.(variable,treated)])
cem_balanc_dt_long2[,sd_w := sqrt( num / (((N-1)*sum_w)/N)),by=.(variable,treated)]
sd_info <- dcast.data.table(cem_balanc_dt_long2, variable ~ treated, value.var = "sd_w")
setnames(sd_info,c(2,3),c('sd_tr0','sd_tr1'))
balance_dt <- merge(x=means_wide,
                    y=sd_info,
                    by='variable')
balance_dt[,mean_diff := abs(mean_tr1-mean_tr0)]
balance_dt[,denom := sqrt((sd_tr1^2 + sd_tr0^2)/2)]
balance_dt[,d := mean_diff / denom]
balance_dt[,matching_type := 'cem']
balance_dt

# Export balance stats
saveRDS(object = balance_dt, file = paste0('\\\\micro.intra/projekt/P0515$/P0515_Gem/Martin/ABM/output/CEM_balance_dt_2000_2017_nosamepeorg_with_ne_d',d,'_k',k,'_',date_id,'.rds'))
