CREATE TABLE #Codesets (
  ancestor_concept_id int NOT NULL,
  concept_id int NOT NULL
)
;

INSERT INTO #Codesets (ancestor_concept_id, concept_id)
 SELECT ancestor_concept_id, descendant_concept_id
 FROM @cdm_database_schema.CONCEPT_ANCESTOR
 WHERE ancestor_concept_id IN (@outcome_ids)
;

INSERT INTO @target_database_schema.@target_cohort_table (
	subject_id,
	cohort_definition_id,
	cohort_start_date,
	cohort_end_date
)
SELECT 
	s.subject_id,
	s.cohort_definition_id,
	s.cohort_start_date,
	s.cohort_start_date cohort_end_date
FROM (
    SELECT d.person_id subject_id,
        c.ancestor_concept_id cohort_definition_id,
        d.device_exposure_start_date cohort_start_date
FROM @cdm_database_schema.device_exposure d
INNER JOIN #Codesets c ON c.concept_id = d.device_concept_id
) s
;

TRUNCATE TABLE #Codesets;
DROP TABLE #Codesets;
