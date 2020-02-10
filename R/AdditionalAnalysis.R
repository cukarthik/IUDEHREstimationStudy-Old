additionalAnalysisFolder <- "additional_analysis"

#' Run AdditionalAnalysis package
#'
#' @details
#' Run the AdditionalAnalysis package, which implements additional analysis for the IUD Study.
#'
#' @param connectionDetails    An object of type \code{connectionDetails} as created using the
#'                             \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#'                             DatabaseConnector package.
#' @param cdmDatabaseSchema    Schema name where your patient-level data in OMOP CDM format resides.
#'                             Note that for SQL Server, this should include both the database and
#'                             schema name, for example 'cdm_data.dbo'.
#' @param cohortDatabaseSchema Schema name where intermediate data can be stored. You will need to have
#'                             write priviliges in this schema. Note that for SQL Server, this should
#'                             include both the database and schema name, for example 'cdm_data.dbo'.
#' @param cohortTable          The name of the table that will be created in the work database schema.
#'                             This table will hold the exposure and outcome cohorts used in this
#'                             study.
#' @param oracleTempSchema     Should be used in Oracle to specify a schema where the user has write
#'                             priviliges for storing temporary tables.
#' @param outputFolder         Name of local folder where the results were generated; make sure to use forward slashes
#'                             (/). Do not use a folder on a network drive since this greatly impacts
#'                             performance.
#'
#' @export
runCohortCharacterization <- function(connectionDetails,
                                      cdmDatabaseSchema,
                                      cohortDatabaseSchema,
                                      cohortTable,
                                      oracleTempSchema,
                                      cohortId,
                                      outputFolder,
                                      cohortsToCreate,
                                      cohortCounts, 
                                      minCellCount) {
  
  index <- grep(cohortId, cohortCounts$cohortDefinitionId)
  if (length(index)==0) {
    
    ParallelLogger::logInfo(paste("Skipping Cohort Characterization for", cohortsToCreate$name[index], " becasue of no count."))  
#    stop(paste0("ERROR: Trying to characterize a cohort that was not created! CohortID --> ", cohortsToCreate$cohortId[i], " Cohort Name --> ", cohortsToCreate$name[i]))
  
  } else if (cohortCounts$personCount[index] < minCellCount) {
     
      ParallelLogger::logInfo(paste("Skipping Cohort Characterization for", cohortsToCreate$name[index], " low cell count."))  
  
  } else {
    
    covariateSettings <- FeatureExtraction::createDefaultCovariateSettings()
    covariateSettings$DemographicsAge <- TRUE # Need to Age (Median, IQR)
    covariateSettings$DemographicsPostObservationTime <- TRUE # Need to calculate Person-Year Observation post index date (Median, IQR)
    
    covariateData2 <- FeatureExtraction::getDbCovariateData(connectionDetails = connectionDetails,
                                                            cdmDatabaseSchema = cdmDatabaseSchema,
                                                            cohortDatabaseSchema = cohortDatabaseSchema,
                                                            cohortTable = cohortTable,
                                                            cohortId = cohortId,
                                                            covariateSettings = covariateSettings,
                                                            aggregated = TRUE)
    summary(covariateData2)
    result <- FeatureExtraction::createTable1(covariateData2, specifications = getCustomizeTable1Specs(), output = "one column"  )
    #  FeatureExtraction::saveCovariateData(covariateData2, file.path(outputFolder,paste0(cohortId,"_covariates")))
    print(result, row.names = FALSE, right = FALSE)
    analysisFolder <- file.path(outputFolder, additionalAnalysisFolder)
    if (!file.exists(analysisFolder)) {
      dir.create(analysisFolder, recursive = TRUE)
    }
    write.csv(result, file.path(outputFolder, additionalAnalysisFolder, paste0(cohortId,"_table1.csv")), row.names = FALSE)
    
  }
}

# Moves all table1, cumulative incidence, filtered cohortCounts, and graphs from diagnostic folder to the export folder
copyAdditionalFilesToExportFolder <- function(outputFolder, 
                                              cohortCounts,
                                              minCellCount) {
  #copy table1, cumlative incidence, and cohort counts per year files
  filesToCopy <- list.files(path=file.path(outputFolder, additionalAnalysisFolder), full.names = TRUE, pattern="_table1|cumlativeIncidence|per_year|Kaplan")

  # copy the files to export folder
  exportFolder <- file.path(outputFolder, "export")
  if (!file.exists(exportFolder)) {
    dir.create(exportFolder, recursive = TRUE)
  }
  file.copy(filesToCopy, file.path(outputFolder, "export"))
  
  #filter the cohort counts for counts greater than minCellCount
  for (row in 1:nrow(cohortCounts)) {
    pc <- cohortCounts[row, "personCount"]

    if(pc < minCellCount) {
      print(paste("Cohort count is less than ", minCellCount ," --> ", pc))
      cohortCounts[row, "personCount"] <- paste0("<", minCellCount)
      cohortCounts[row, "cohortCount"] <- paste0("<", minCellCount)
    }
  }  
  analysisFolder <- file.path(outputFolder, additionalAnalysisFolder)
  if (!file.exists(analysisFolder)) {
    #dir.create(analysisFolder, recursive = TRUE)
    ParallelLogger::logInfo("Cannot copy files b/c additional analysese were not done...")
  }
  write.csv(cohortCounts, file.path(exportFolder, "filtered_cohort_counts.csv"), row.names = FALSE)
  
  #copy the graphs from the diagnostic folder
  filesToCopy <- list.files(path=file.path(outputFolder, "diagnostics"), full.names = TRUE, pattern=".png")
  file.copy(filesToCopy, file.path(outputFolder, "export"))
  
}

createKMGraphs <- function(outputFolder, cohortsToCreate) {
  x <- list.files(path=paste0(outputFolder,"/cmOutput"), pattern="StratPop", full.names = TRUE)
  cohortsToCreate$stringsAsFactors=FALSE
  for (i in 1:length(x)) {
    studyPop <- readRDS(x[i])
    r <- extractParametersFromName(x[i])
    CohortMethod::plotKaplanMeier(studyPop,
                                  targetLabel= gettext(cohortsToCreate$atlasName[ which(cohortsToCreate$cohortId == r$target)]), 
                                  comparatorLabel = gettext(cohortsToCreate$atlasName[ which(cohortsToCreate$cohortId == r$comparator)]), 
                                  title = r$title,
                                  fileName = file.path(outputFolder,additionalAnalysisFolder,paste0("Kaplan Meier Plot ",r$title,".png")))
  }
}

extractParametersFromName <- function(fileName) {
  result <- list("target" = "", "outcome" = "", "comparator" = "", title="No Title")
  f <- unlist(strsplit(fileName, "_"))
  cnt <- 0 #count to determine title
  for (i in 1:length(f)) {
    if (startsWith(f[i],"c1")) {
      result$comparator <- gsub(substr(f[i],2,nchar(f[i])), pattern=".rds$", replacement="")
    } else if (startsWith(f[i],"t1")) {
      result$target <- gsub(substr(f[i],2,nchar(f[i])), pattern=".rds$", replacement="")
    } else if (startsWith(f[i],"o1")) {
      result$outcome <- gsub(substr(f[i],2,nchar(f[i])), pattern=".rds$", replacement="")
    } else if (f[i]=='s1') {
      cnt <- cnt+1
    } else if (f[i]=='s2') {
      cnt <- cnt+2
    } else if (f[i]=='s3') {
      cnt <- cnt+4 # crude analysis
    }
  }
  
  if (cnt == 2) {
    result$title <- "Subgroup Analysis"
  } else if (cnt == 3) {
    result$title <- "Matched Analysis"
  } else if (cnt == 4) {
    result$title <- "Stratification Analysis"
  } else if (cnt == 5) {
    result$title <- "Crude Analysis"
  }
  
  return(result)
}
getCustomizeTable1Specs <- function() {
  s <- FeatureExtraction::getDefaultTable1Specifications()
  appendedTable1Spec <- rbind(s, c("Age", 2,"")) # Add Age as a continuous variable to table1
  appendedTable1Spec <- rbind(appendedTable1Spec, c("PriorObservationTime", 8,"")) # Add Observation prior index date
  appendedTable1Spec <- rbind(appendedTable1Spec, c("PostObservationTime", 9,"")) # Add Observation post index date
  return(appendedTable1Spec)
}

  

calculateCumulativeIncidence <- function(connectionDetails,
                                         cohortDatabaseSchema,
                                         cdmDatabaseSchema,
                                         cohortTable,
                                         oracleTempSchema,
                                         targetCohortId,
                                         outcomeCohortId,
                                         outputFolder) {

  conn <- DatabaseConnector::connect(connectionDetails)
  sql <- SqlRender::loadRenderTranslateSql("CumulativeIncidence.sql",
                                           "IUDEHRStudy",
                                           dbms = connectionDetails$dbms,
                                           target_database_schema = cohortDatabaseSchema,
                                           cdm_database_schema = cdmDatabaseSchema,
                                           study_cohort_table = cohortTable,
                                           outcome_cohort = outcomeCohortId,
                                           target_cohort = targetCohortId,
                                           oracleTempSchema = oracleTempSchema)
  cumlativeIncidence <- DatabaseConnector::querySql(conn, sql)
  analysisFolder <- file.path(outputFolder, additionalAnalysisFolder)
  if (!file.exists(analysisFolder)) {
    dir.create(analysisFolder, recursive = TRUE)
  }
  output <- file.path(outputFolder, additionalAnalysisFolder, paste0(targetCohortId, "_", outcomeCohortId,"_cumlativeIncidence.csv"))
  write.table(cumlativeIncidence, file=output, sep = ",", row.names=FALSE, col.names = TRUE, append=FALSE)
}

#Retrieves and writes yearly inclusion counts for all cohorts
calculatePerYearCohortInclusion <- function(connectionDetails,
                                            cohortDatabaseSchema,
                                            cohortTable,
                                            oracleTempSchema,
                                            outputFolder,
                                            minCellCount) {
  
  sql <- SqlRender::loadRenderTranslateSql("GetCountsPerYear.sql",
                                           "IUDEHRStudy",
                                           dbms = connectionDetails$dbms,
                                           target_database_schema = cohortDatabaseSchema,
                                           study_cohort_table = cohortTable,
                                           oracleTempSchema = oracleTempSchema)
  conn <- DatabaseConnector::connect(connectionDetails)
  counts <- DatabaseConnector::querySql(conn, sql)
  filtered_counts <- counts[counts["PERSON_COUNT"]>minCellCount,]

  analysisFolder <- file.path(outputFolder, additionalAnalysisFolder)
  if (!file.exists(analysisFolder)) {
    dir.create(analysisFolder, recursive = TRUE)
  }
  output <- file.path(outputFolder, additionalAnalysisFolder, "cohort_counts_per_year.csv")
  write.table(filtered_counts, file=output, sep = ",", row.names=FALSE, col.names = TRUE)
  
}
