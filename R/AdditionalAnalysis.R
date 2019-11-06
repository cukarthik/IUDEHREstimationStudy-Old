#' Run CohortMethod package
#'
#' @details
#' Run the CohortMethod package, which implements the comparative cohort design.
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
                                      cohortCounts, 
                                      minCellCount) {
  
  index <- grep(cohortId, cohortCounts$cohortDefinitionId)
  if (length(index)==0) {
    
    ParallelLogger::logInfo(paste("Skipping Cohort Characterization for", cohortsToCreate$name[i], " becasue of no count."))  
#    stop(paste0("ERROR: Trying to characterize a cohort that was not created! CohortID --> ", cohortsToCreate$cohortId[i], " Cohort Name --> ", cohortsToCreate$name[i]))
  
  } else if (cohortCounts$personCount[index] < minCellCount) {
     
      ParallelLogger::logInfo(paste("Skipping Cohort Characterization for", cohortsToCreate$name[i], " low cell count."))  
  
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
    write.csv(result, file.path(outputFolder, "additional_analysis", paste0(cohortId,"_table1.csv")), row.names = FALSE)
    
  }
}

# Moves all table1, cumulative incidence, and filtered cohortCounts to the export folder
copyAdditionalFilesToExportFolder() <- function() {
  
}

getCustomizeTable1Specs <- function() {
  s <- FeatureExtraction::getDefaultTable1Specifications()
  appendedTable1Spec <- rbind(s, c("Age", 2,"")) # Add Age as a continuous variable to table1
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
                                           "IUDCLW",
                                           dbms = connectionDetails$dbms,
                                           target_database_schema = cohortDatabaseSchema,
                                           cdm_database_schema = cdmDatabaseSchema,
                                           study_cohort_table = cohortTable,
                                           outcome_cohort = outcomeCohortId,
                                           target_cohort = targetCohortId,
                                           oracleTempSchema = oracleTempSchema)
  cumlativeIncidence <- DatabaseConnector::querySql(conn, sql)
  output <- file.path(outputFolder, "additional_analysis", paste0(targetCohortId, "_", outcomeCohortId,"_cumlativeIncidence.csv"))
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
                                           "IUDCLW",
                                           dbms = connectionDetails$dbms,
                                           target_database_schema = cohortDatabaseSchema,
                                           study_cohort_table = cohortTable,
                                           oracleTempSchema = oracleTempSchema)
  conn <- DatabaseConnector::connect(connectionDetails)
  counts <- DatabaseConnector::querySql(conn, sql)
  filtered_counts <- counts[counts["PERSON_COUNT"]>minCellCount,]

  output <- file.path(outputFolder, "additional_analysis", "cohort_counts_per_year.csv")
  write.table(filtered_counts, file=output, sep = ",", row.names=FALSE, col.names = TRUE)
  
}
