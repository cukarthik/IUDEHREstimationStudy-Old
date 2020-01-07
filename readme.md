Relative Risk of Cervical Neoplasms Associated with Copper and Levonorgestrel Secreting Intrauterine Devices: <br> Real World Evidence from the OHDSI Network
==============================

<img src="https://img.shields.io/badge/Study%20Status-Design%20Finalized-brightgreen.svg" alt="Study Status: Design Finalized"> 

- Analytics use case(s): **`Characterization` and `Population-Level Estimation`**
- Study type: **`Clinical Application`**
- Tags: **iud**
- Study lead: **Matthew Spotnitz**
- Study lead forums tag: **[Study Forum Post](https://forums.ohdsi.org/t/iud-study-updates/8851)**
- Study start date: **09/23/2019**
- Study end date: **-**
- Protocol: **[Protocol](https://github.com/cukarthik/IUDEHREstimationStudy/blob/master/documents/IUD%20Cervical%20Neoplasms%20Estimation%20Protocol.docx)**
- Publications: **[Prior single site study (linking pending)]()**
- Results explorer: **-**


Requirements
============

- A database in [Common Data Model version 5](https://github.com/OHDSI/CommonDataModel) in one of these platforms: SQL Server, Oracle, PostgreSQL, IBM Netezza, Apache Impala, Amazon RedShift, Google BigQuery, or Microsoft APS.
- R version 3.5.0 or newer
- On Windows: [RTools](http://cran.r-project.org/bin/windows/Rtools/)
- [Java](http://java.com)
- 25 GB of free disk space

See [these instructions](https://ohdsi.github.io/MethodsLibrary/rSetup.html) on how to set up the R environment on Windows.

How to run
==========
1. In `R`, use the following code to install the dependencies:

	```r
	install.packages("devtools")
	library(devtools)
	install_github("ohdsi/SqlRender", ref = "v1.6.0")
	install_github("ohdsi/DatabaseConnector", ref = "v2.3.0")
	install_github("ohdsi/OhdsiSharing", ref = "v0.1.3")
	install_github("ohdsi/FeatureExtraction", ref = "v2.2.3")
	install_github("ohdsi/CohortMethod", ref = "v3.0.2")
	install_github("ohdsi/EmpiricalCalibration", ref = "v2.0.0")
	install_github("ohdsi/MethodEvaluation", ref = "v1.0.2")
	```

	If you experience problems on Windows where rJava can't find Java, one solution may be to add `"--no-multiarch"` to each `install_github` call, for example these are two ways to ignore the i386 architecture:
	
	```r
	install_github("ohdsi/SqlRender", args = "--no-multiarch")
	install_github("ohdsi/SqlRender", INSTALL_opts=c("--no-multiarch"))
	```
	
	OR for all installs, one can try:
	
	```r
	options(devtools.install.args = "--no-multiarch")
	```
	
	Alternatively, ensure that you have installed both 32-bit and 64-bit JDK versions, as mentioned in the [video tutorial](https://youtu.be/K9_0s2Rchbo).
	
2. In `R`, use the following `devtools` command to install the IUDCLW package:

	```r
	install() # Note: it is ok to delete inst/doc
	```
	
3. Once installed, you can execute the study by modifying and using the code below. For your convenience, this code is also provided under `extras/CodeToRun.R`:

	```r
	library(IUDCLW)
	
	# Optional: specify where the temporary files (used by the ff package) will be created:
	options(fftempdir = "c:/FFtemp")
	
	# Maximum number of cores to be used:
	maxCores <- parallel::detectCores()
	
	# Minimum cell count when exporting data:
	minCellCount <- 10
	
	# The folder where the study intermediate and result files will be written:
	outputFolder <- "c:/IUDCLW"
	
	# Details for connecting to the server:
	# See ?DatabaseConnector::createConnectionDetails for help
	connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "postgresql",
									server = "some.server.com/ohdsi",
									user = "joe",
									password = "secret")
	
	# The name of the database schema where the CDM data can be found:
	cdmDatabaseSchema <- "cdm_synpuf"
	
	# The name of the database schema and table where the study-specific cohorts will be instantiated:
	cohortDatabaseSchema <- "scratch.dbo"
	cohortTable <- "my_study_cohorts"
	
	# Some meta-information that will be used by the export function:
	databaseId <- "Synpuf"
	databaseName <- "Medicare Claims Synthetic Public Use Files (SynPUFs)"
	databaseDescription <- "Medicare Claims Synthetic Public Use Files (SynPUFs) were created to allow interested parties to gain familiarity using Medicare claims data while protecting beneficiary privacy. These files are intended to promote development of software and applications that utilize files in this format, train researchers on the use and complexities of Centers for Medicare and Medicaid Services (CMS) claims, and support safe data mining innovations. The SynPUFs were created by combining randomized information from multiple unique beneficiaries and changing variable values. This randomization and combining of beneficiary information ensures privacy of health information."
	
	# For Oracle: define a schema that can be used to emulate temp tables:
	oracleTempSchema <- NULL
	
	execute(connectionDetails = connectionDetails,
            cdmDatabaseSchema = cdmDatabaseSchema,
            cohortDatabaseSchema = cohortDatabaseSchema,
            cohortTable = cohortTable,
            oracleTempSchema = oracleTempSchema,
            outputFolder = outputFolder,
            databaseId = databaseId,
            databaseName = databaseName,
            databaseDescription = databaseDescription,
            createCohorts = TRUE,
            synthesizePositiveControls = TRUE,
            runAnalyses = TRUE,
            runDiagnostics = TRUE,
            packageResults = TRUE,
            maxCores = maxCores)
	```

4. Please email both Matt Spotnitz (mes2165 at cumc dot columbia dot edu) and Karthik Natarajan (kn2174 at cumc dot columbia dot edu) an account to upload results. Then upload the file ```export/Results<DatabaseId>.zip``` in the output folder to the study coordinator. 
		
5. To view the results, use the Shiny app:

	```r
	prepareForEvidenceExplorer("Result<databaseId>.zip", "/shinyData")
	launchEvidenceExplorer("/shinyData", blind = TRUE)
	```
  
  Note that you can save plots from within the Shiny app. It is possible to view results from more than one database by applying `prepareForEvidenceExplorer` to the Results file from each database, and using the same data folder. Set `blind = FALSE` if you wish to be unblinded to the final results.

Development
===========
IUDEHRStudy was developed in ATLAS and R Studio. The package was modified to include additional analyses from the initial Atlas package. All additional analyses and code are located in the _AdditionalAnalysis.R_ file. The following are the additional analyses and modifications:
1. Calculates counts to additional cohorts for sensitivity analysis
2. Calculates the cumulative incidence of the cohorts
3. Calculates the yearly distribution of all cohorts
4. Creates KM graphs for the cohorts of interest
5. Copies all diagnostic graphs in the diagnostic folder to the export folder 
6. All cohort counts and distributions are filtered based on minimum cell count

License
=======
The IUDEHRStudy package is licensed under Apache License 2.0
