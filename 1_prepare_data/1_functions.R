#
#
# 1 - Misc functions for data preparation
#
# ------------------------------------------------------------------------------------------


# Format AstSni codes
# ------------------------------------------------------------------------------------------
format_astsni_codes <- function(individ_list,astsni_translator_file){
  
  g02_for_all <- fread(astsni_translator_file)
  for(i in 1:length(individ_list)){
    
    # Set current
    temp <- individ_list[[i]]
    
    # -2001
    if(unique(temp$Ar)<=2001){
      sni_translate <- unique(g02_for_all[,.(SNI92,Sni2002G)],by=c('SNI92'))
      temp <- merge(x=temp,
                    y=sni_translate,
                    by.x='KU1AstSNI92',
                    by.y='SNI92',
                    all.x=T,all.y=F)
      temp[is.na(Sni2002G),Sni2002G:=0]
      temp[,KU1AstSNI92 := NULL]
      setnames(temp,'Sni2002G','AstSni')
      individ_list[[i]] <- temp
      
      # 2002-2006
    }else if(unique(temp$Ar)>2001 & unique(temp$Ar)<2007){
      sni_translate <- unique(g02_for_all[,.(SNI2002,Sni2002G)],by=c('SNI2002'))
      temp <- merge(x=temp,
                    y=sni_translate,
                    by.x='KU1AstSNI2002',
                    by.y='SNI2002',
                    all.x=T,all.y=F)
      temp[is.na(Sni2002G),Sni2002G:=0]
      temp[,KU1AstSNI2002 := NULL]
      setnames(temp,'Sni2002G','AstSni')
      individ_list[[i]] <- temp
      
      # 2007-2017
    }else if(unique(temp$Ar)>=2007){ 
      sni_translate <- unique(g02_for_all[,.(SNI2007,Sni2002G)],by=c('SNI2007'))
      temp <- merge(x=temp,
                    y=sni_translate,
                    by.x='KU1AstSNI2007',
                    by.y='SNI2007',
                    all.x=T,all.y=F)
      temp[is.na(Sni2002G),Sni2002G:=0]
      temp[,KU1AstSNI2007 := NULL]
      setnames(temp,'Sni2002G','AstSni')
      individ_list[[i]] <- temp
    }
  }
  cat("... AstSni codes formatted!", "\n")
  return(individ_list)
}



# Unify workplace information across years
# ------------------------------------------------------------------------------------------
unify_wp_data <- function(individ_list){
  # Rbind all years
  individ_all <- rbindlist(individ_list,use.names = T, fill = T)
  # - astsni
  astsni_count <- individ_all[,.(.N),by=c('WorkplaceId','AstSni')]
  setkeyv(astsni_count,c('WorkplaceId','N'))
  astsni_topvote <- astsni_count[,tail(.SD,1),by='WorkplaceId']
  # - sector
  sector_count <- individ_all[,.(.N),by=c('WorkplaceId','SektorKod')]
  setkeyv(sector_count,c('WorkplaceId','N'))
  sector_topvote <- sector_count[,tail(.SD,1),by='WorkplaceId']
  # - kommun
  kommun_count <- individ_all[,.(.N),by=c('WorkplaceId','AstKommun')]
  setkeyv(kommun_count,c('WorkplaceId','N'))
  kommun_topvote <- kommun_count[,tail(.SD,1),by='WorkplaceId']
  # - peorg
  peorg_count <- individ_all[,.(.N),by=c('WorkplaceId','PeOrgLopNr')]
  setkeyv(peorg_count,c('WorkplaceId','N'))
  peorg_topvote <- peorg_count[,tail(.SD,1),by='WorkplaceId']
  # - combined info
  wp_info <- merge(x=unique(astsni_topvote[,.(WorkplaceId,AstSni)]),
                   y=unique(sector_topvote[,.(WorkplaceId,SektorKod)]),
                   by='WorkplaceId')
  wp_info <- merge(x=wp_info,
                   y=unique(kommun_topvote[,.(WorkplaceId,AstKommun)]),
                   by='WorkplaceId')
  wp_info <- merge(x=wp_info,
                   y=unique(peorg_topvote[,.(WorkplaceId,PeOrgLopNr)]),
                   by='WorkplaceId')
  # - replace
  individ_list <- lapply(individ_list,function(x)x[,c('AstSni','SektorKod','AstKommun','PeOrgLopNr') := NULL])
  individ_list <- lapply(individ_list,function(x)merge(x=x,
                                                       y=wp_info,
                                                       by='WorkplaceId',
                                                       all.x=T,all.y=F))
  # return
  return(individ_list)
}
# ------------------------------------------------------------------------------------------