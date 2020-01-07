{DEFAULT @target_database_schema = "results"}
{DEFAULT @study_cohort_table = "cohort"}
{DEFAULT @cdm_database_schema = "cdm"}

SELECT cohort1.subject_id,
	DATEDIFF(day, cohort1.cohort_start_date, observation_period_end_date) AS time_to_obs_end,
	CASE 
		WHEN cohort2.subject_id IS NOT NULL THEN DATEDIFF(day, cohort1.cohort_start_date, cohort2.cohort_start_date)
		ELSE -1
	END AS time_to_outcome,
	CASE 
		WHEN cohort2.subject_id IS NOT NULL THEN CAST(1 AS INT)
		ELSE CAST(0 AS INT)
	END AS has_outcome
FROM @target_database_schema.@study_cohort_table cohort1
INNER JOIN @cdm_database_schema.observation_period
	ON cohort1.subject_id = observation_period.person_id
	AND cohort1.cohort_start_date >= observation_period_start_date
	AND cohort1.cohort_start_date <= observation_period_end_date
LEFT JOIN (
		SELECT *
		FROM @target_database_schema.@study_cohort_table 
		WHERE cohort_definition_id = @outcome_cohort
	) cohort2
	ON cohort1.subject_id = cohort2.subject_id
	AND cohort1.cohort_start_date <= cohort2.cohort_start_date
WHERE cohort1.cohort_definition_id = @target_cohort;
