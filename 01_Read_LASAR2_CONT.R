##  This script retrieves continuous data from LASER 2 Continuous
# Setup Steps
# This creates a data source name driver for R to use when connecting to the database

# 1. Open Windows Explorer and navigate to: C:\Windows\SysWOW64\
# 2. Find and open an exe named "odbcad32.exe"
# 3. Click the Tab "System DSN"
# 4. Click > ADD
# 5. Select the driver "SQL Server"
# 6. Name: LASAR_CONT2
#    Description: Leave Blank
#    Server: DEQSQL1\GIS
#   Click NEXT
# 7. Choose "With Windows NT authentication..."
#   Click NEXT
# 8. Change the default database to: LASAR_CONT2
#   Click NEXT
# 9. Don't change anything here > Click FINISH
# 10. Click "Test Data Source" to verify it works - Click OK
# 11. Click OK on ODBC screen

# Make sure to use the 32 bit version of R if you are using a 64 bit computer

# If you use R studio set the defualt to 32 bit 
# 1. In R studio go to Tools > Options
# 2. For the R version, Click CHANGE
# 3. Select "Choose a specfic version of R"
# 4. Select [32-bit] C:\Program Files\R\R-...
# 5. Click OK, Click OK.
# 6. Close R Studio and restart again for changes to take affect.

#----------------------------------------------------------------------------
## Load required R packages
# install.packages("RODBC") #Delete first hashtag and run if RODBC is not installed
library(RODBC)

## Designate outfile path and name
thedate <-format(Sys.Date(), "%Y_%m_%d")
outpath <-"D:/"
outfile <- paste0("LASAR2_Query_",thedate,".csv") 

#----------------------------------------------------------------------------
### Query information 
# Note that tables in LASAR2_CONT do not contain parameter or Sample matrix names 
# You have to look up by the numeric keys

# HUC query
# The script will identify all the sub HUC division within the given HUC 
# and include those in the query. All the HUC levels must be included because 
# sometimes stations aren't identified as being a member for all of them.
myHUCs <- c(1710020503)

# Parameter query
# XLU_LASAR_PARAMETERS_KEY /  INDEX_COLUMN / UNIT_NAME
# 975   / Field Temperature / C 
# 2215  / Field Temperature / F 
# 2347  / Temperature       / C
# 10595 / Field Temperature / mg/L
# 11760 / Temperature       / F
myParam <- c(975, 2215, 10595, 2347, 11760)

# Sample matrix query
# ('Surface water','Bay/Estuary/Ocean', 'Canal', 'Reservoir', 'Lake', 'TEMPORARY')
mySampleMatrix <- c(1, 2, 20, 21, 23, 26)

# Date/Time
myStartdate <- "1900-01-01 00:00:00"
myEnddate <- "2012-12-31 00:00:00"

# QA/QC
myQAQC <- c('A','A+','B')

#-------------------------------------------------------------------------------
## Make a connection to LASAR2 to get the tables needed to identify the stations 
## and for merging

channel <- odbcConnect("LASAR2")

# Grab the needed tables
t.tablenames <- sqlTables(channel, errors = FALSE)
t.XLU_AREA <- sqlFetch(channel, "XLU_AREA")
t.STATION_AREA <- sqlFetch(channel,"STATION_AREA")
t.STATION <- sqlFetch(channel, "STATION")
t.XLU_STATUS <- sqlFetch(channel, "XLU_STATUS")
t.XLU_LASAR_PARAMETERS <- sqlFetch(channel, "XLU_LASAR_PARAMETERS")
t.SAMPLE_MATRIX <- sqlFetch(channel, "SAMPLE_MATRIX")

close(channel)

#-------------------------------------------------------------------------------
## Build a list of the stations in the HUC

# Just get the needed columns
t.STATION <- t.STATION[ , c("STATION_KEY", "LOCATION_DESCRIPTION", "DECIMAL_LAT","DECIMAL_LONG")]
t.XLU_STATUS <- t.XLU_STATUS[,c("XLU_STATUS_KEY","STATUS")]
t.XLU_LASAR_PARAMETERS <- t.XLU_LASAR_PARAMETERS[,c("XLU_LASAR_PARAMETERS_KEY", "PARAMETER_NM", "INDEX_COLUMN", "UNIT", "UNIT_NAME", "CAS_NUMBER")]

# Get all the Area keys for the HUC and each of its sub divisions
myAreas <- t.XLU_AREA[grep(paste(as.integer(myHUCs),collapse="|"), t.XLU_AREA$AREA_ABBREVIATION),]
myAreaKeys <- myAreas$AREA_KEY

# Get a list of all the stations in my area
myAreaStations <- t.STATION_AREA[t.STATION_AREA$XLU_AREA %in% myAreaKeys,]
myStations <- as.integer(myAreaStations$STATION) # Should be an integer

#-------------------------------------------------------------------------------
## Make a connection to LASAR2 Cont and query based on 
## Station, Date range, parameter, and sample matrix

channel <- odbcConnect("LASAR_CONT2")

# format for the SQL query
myStations <- paste0("'",paste(myStations,collapse="','"),"'")
myParam <- paste0("'",paste(myParam,collapse="','"),"'")
mySampleMatrix <- paste0("'",paste(mySampleMatrix,collapse="','"),"'")

# Create SQL Query
myQuery <- paste0("SELECT sa.[STATION], sa.[SAMPLING_EVENT],
                 pr.[DATE_TIME], pr.[RESULT], 
                 pr.[QA_QC_STATUS], pb.[SAMPLE_MATRIX], pb.[XLU_LASAR_PARAMETERS] 
                 FROM [LASAR_CONT2].[dbo].[CONT_PARAMETER_RESULT] pr 
                 JOIN [LASAR_CONT2].[dbo].[CONT_PROBE] pb on pr.CONT_PROBE = pb.CONT_PROBE_KEY 
                 JOIN [LASAR_CONT2].[dbo].[CONT_SAMPLE] sa on pb.CONT_SAMPLE = sa.CONT_SAMPLE_KEY 
                 WHERE 
                 sa.STATION IN (",myStations,") AND 
                 pb.XLU_LASAR_PARAMETERS IN (",myParam,") AND 
                 pr.DATE_TIME >= ('",myStartdate,"') AND 
                 pr.DATE_TIME <= ('",myEnddate,"') AND 
                 pb.SAMPLE_MATRIX IN (",mySampleMatrix,")")

# remove the line returns (\n)
myQuery <- strwrap(myQuery, width=10000000, simplify=TRUE)

# Retreive data
mydata <- sqlQuery(channel, myQuery, stringsAsFactors = FALSE, na.strings = "NA")

close(channel)
#-------------------------------------------------------------------------------
## Make another connection to LASAR2 to get the sampling organization based on the 
## sampling event numbers from the query above

mySamplingEvents <- unique(mydata$SAMPLING_EVENT)
mySamplingEvents <- paste0("'",paste(mySamplingEvents,collapse="','"),"'")

channel <- odbcConnect("LASAR2")

myQuery <- paste0("SELECT s2.[SAMPLING_EVENT], og.[NAME], su.[SUBPROJECT_NAME]
                  FROM [LASAR2].[dbo].[SAMPLE] s2
                  JOIN [LASAR2].[dbo].[ORGANIZATION] og on s2.SAMPLING_ORGANIZATION = og.ORGANIZATION_KEY
                  JOIN [LASAR2].[dbo].[SAMPLING_SUBPROJECT] su on s2.SAMPLING_SUBPROJECT = su.SAMPLING_SUBPROJECT_KEY
                  WHERE s2.SAMPLING_EVENT IN (",mySamplingEvents,")")

# remove the line returns (\n)
myQuery <- strwrap(myQuery, width=100000, simplify=TRUE)
t.SAMPLING <- sqlQuery(channel, myQuery, stringsAsFactors = FALSE, na.strings = "NA")

close(channel)

#-------------------------------------------------------------------------------
## Organize the data, filter based on QA/QC status, and output to csv

# remove duplicated rows and rename
t.SAMPLING <- t.SAMPLING[!duplicated(t.SAMPLING), ]
names(t.SAMPLING)[names(t.SAMPLING) == 'NAME'] <- 'SAMPLING_ORGANIZATION'

# Merge with other tables
mydata <- merge(mydata,t.SAMPLE_MATRIX, by.x="SAMPLE_MATRIX", by.y="SAMPLE_MATRIX_KEY")
mydata <- merge(mydata, t.XLU_STATUS, by.x ="QA_QC_STATUS", by.y="XLU_STATUS_KEY")
mydata <- merge(mydata, t.STATION,by.x="STATION", by.y="STATION_KEY")
mydata <- merge(mydata, t.XLU_LASAR_PARAMETERS, by.x="XLU_LASAR_PARAMETERS", by.y="XLU_LASAR_PARAMETERS_KEY")
mydata <- merge(mydata, t.SAMPLING, by="SAMPLING_EVENT")
mydata <- mydata[mydata$STATUS %in% myQAQC,]

# Build a dataframe of all the final stations
myStations <- as.integer(unique(mydata$STATION))
myStations.df <-t.STATION[t.STATION$STATION %in% myStations,]

# reorganize the columns
mydata <- mydata[,c(3,13,14,15,6,7,19,18,12,4,8,9,20,16,2,17,1,21,22)]

# Sort the data
mydata <- mydata[with(mydata, order(STATION, DATE_TIME)), ]

# Write ouput to file
write.csv(mydata, paste0(outpath,outfile),row.names = FALSE)
