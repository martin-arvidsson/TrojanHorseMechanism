#
#
# 1 - Download raw MONA data
#
# ------------------------------------------------------------------------------------------


# 1.1) Load download function
# ------------------------------------------------------------------------------------------
DlFromMona <- function(var.names, 
                       table.names,
                       table.years = NULL,
                       table.interval = NULL,
                       folder.path, 
                       conditions = NULL,
                       req.complete.rows = TRUE)
{
  
  # (1) Load packages
  # ------------------------------
  suppressMessages(require(RODBC))
  suppressMessages(require(RJSONIO))
  suppressMessages(require(rjson))
  suppressMessages(require(foreign))
  suppressMessages(require(data.table))
  
  # (2) Close all connections
  # ------------------------------
  odbcCloseAll()
  
  # (3) Set up connection to Database
  # ------------------------------
  myconn <- odbcDriverConnect(connection = paste('driver={SQL Server}',
                                                 'server=mq02\\b',
                                                 'database=P0515_IFFS_Segregeringens_dynamik',
                                                 'trusted_connection=true', sep=';'))
  
  
  # (4) Create table names?
  #  ---> Based on...
  #         - years
  #         - year-interval
  # ------------------------------
  if(!is.null(table.years) | !is.null(table.interval)){
    table.names <- getTableNames(name_of_table = table.names, 
                                 years = table.years, 
                                 interval = table.interval)
  }
  
  # (5) Loop through table.names and download + export
  # ------------------------------
  for(tb in table.names){
    
    # Test
    #tb <- table.names[1]
    
    # -- SQL retrieve data --
    # ** WITH condition **
    if(is.null(conditions)){
      dat <- sqlQuery(myconn, paste('select ', paste(var.names, collapse = ','), ' from ', tb, sep = ''))
    }
    # ** NO condition **
    if(!is.null(conditions)){
      dat <- sqlQuery(myconn, paste('select ', paste(var.names, collapse = ','), ' from ', tb, ' where ', conditions, sep = ''))
    }
    
    # Data.table
    dat <- as.data.table(dat)
    
    # -- Only keep complete rows? --
    if(req.complete.rows==TRUE){dat <- dat[complete.cases(dat),]}
    
    # -- Save data to .csv file --
    fwrite(x = dat, 
           file = paste(folder.path, tb,'.csv', sep=''))
    
    # -- Rm dat to clear memoryspace --
    remove(dat)
  }
  
  # Close all connections
  odbcCloseAll()
}


# 1.2) Download Background data
var.names = c('PersonLopNr', 'LandKod', 'Kon' ,'FodelseAr')
table.names <- 'dbo.Bakgrundsdata'
folder.path <- '//MICRO.INTRA/PROJEKT/P0515$/P0515_GEM/Martin/TrojanHorseMechanism/data/raw/bkg/'
DlFromMona(var.names = var.names, 
           table.names = table.names, 
           folder.path = folder.path)

# 1.3) Download individual-level data
years = c(2000:2001)
years = c(2002:2006)
years = c(2007:2017)
if(max(years)<2002){ast_var <- "KU1AstSNI92"}
if(min(years)>=2002 & max(years)<2007){ast_var <- "KU1AstSNI2002"}
if(min(years)>=2007 & max(years)<2018){ast_var <- "KU1AstSNI2007"}
var.names <- c("PersonLopNr","Ar","Sun2000niva","KU1CfarLopNr","KU1PeOrgLopNr","KU1Ink","KU1AstKommun","KU1SektorKod",ast_var,"Stud","AldPens")
rm_years_ku = TRUE
conditions = 'Ku1AstKommun < 300 and Ku1AstKommun > 0'
req.complete.rows = TRUE
table.names = paste('dbo.LISA',years,'_Individ', sep='')
folder.path <- '//MICRO.INTRA/PROJEKT/P0515$/P0515_GEM/Martin/TrojanHorseMechanism/data/raw/individual/'
DlFromMona(var.names = var.names,
           table.names = table.names,
           folder.path = folder.path,
           req.complete.rows = req.complete.rows,
           conditions = conditions)

# 1.4) Download uni-data
var.names <- c("PersonLopNr","ar","termin")
table.names <- c("dbo.Hreg_Reg_HT93_VT07","dbo.Hreg_Reg_HT07_VT12","dbo.Hreg_Reg_HT13_VT17")
folder.path <- "//micro.intra/projekt/P0515$/P0515_Gem/Martin/TrojanHorseMechanism/data/raw/uni/"
var.names <- c("PersonLopNr","ar","termin")
req.complete.rows = TRUE
conditions <- NULL
DlFromMona(var.names = var.names,
           table.names = table.names,
           folder.path = folder.path,
           #rm_years_ku = TRUE,
           req.complete.rows = req.complete.rows)
# - combine
uni_dts <- list()
setwd(folder.path)
fls <- list.files(folder.path)
dt <- lapply(fls,fread)
dt <- rbindlist(dt,use.names = T, fill = T)
dt <- dt[,.(.N),by=c('PersonLopNr','ar')][ar>=1999,.(PersonLopNr,Ar=ar)]
fwrite(x = dt, file = "//micro.intra/projekt/P0515$/P0515_Gem/Martin/TrojanHorseMechanism/data/raw/uni/dbo.Hreg_Reg.csv")
