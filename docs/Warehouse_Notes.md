# Warehouse schema

### Fact tables
- `fact_icustays` (fact-less)
	- source: `icu/icustays`
	- note: 
		- Originally connects to `diagnoses_icd` by `subject_id` and `hadm_id`. Replaced with `diagnoses_icd_key` 

- `fact_microbiologyevents`
	- source `hosp/microbiologyevents`
	- Note
		- Ignore `storetime` and `storedate`
		- (?) `spec_itemid`, `org_itemid` -> `hosp/d_labitems`
		- (?) what is `quantity`

- `fact_chartevents`
	- source `icu/chartevents`

- `fact_edstays` (fact-less)
	- source `ed/edstay`

### Dimension tables
- `dim_date`: unique grouped dates
- `dim_time`: unique grouped timestamps

- `dim_diagnoses_icd`:
	- source: `hosp/diagnoses_icd` + `hosp/d_icd_diagnoses`
	- note:
		- Remove `subject_id` and `hadm_id`, replace with `diagnoses_icd_key`

- `dim_d_items`
	- source: `icu/d_items`

- `dim_triage`
	- source: `ed/triage` 

# How to obtained tables

- Table 0: `icu_stays` with year

```postgresql
SELECT * 
FROM fact_icustays AS fis
LEFT JOIN dim_date AS dd ON fis.indate = dd.datekey;
```


- Table 1: `hosp/diagnoses_icd` and `icu/icustays` with `sepsis_flag`

```postgresql
SELECT 
	-- ... other columns as needed
	long_title LIKE 'sepsis' AS sepsis_flag -- condition still unclear
FROM fact_icustays AS fis
LEFT JOIN dim_diagnoses_icd AS ddi ON fis.diagnoses_icd_key = ddi.diagnoses_icd_key;
```


- Table 2: `hosp/microbiologyevents` + `icu/icustays`, not null `org_name` 

```postgresql
SELECT *
FROM fact_microbiologyevents AS fm
LEFT JOIN fact_icustays AS fis 
ON fm.subject_id = fis.subject_id AND fm.hadm_id = fis.hadm_id
WHERE fm IS NOT NULL; 
```
	

- Table 3: `icu/chartevents` with hour, group by `stay_id`, hour in day

```postgresql
SELECT
    fce.stay_id AS stay_id,
    dd.full_date AS chart_date,
    dt.hour AS hour_of_day,
    AVG(fce.valuenum) AS avg_value,
    MAX(fce.valuenum) AS max_value,
    MIN(fce.valuenum) AS min_value,
    COUNT(*) AS n_obs
FROM fact_chartevents fce
JOIN dim_time dt
    ON fce.charttime = dt.time_key
JOIN dim_date dd
    ON fce.chartdate = dd.date_key
WHERE fce.itemid = 220045  -- Heart Rate
GROUP BY
    fce.stay_id,
    dd.full_date,
    dt.hour
ORDER BY
    fce.stay_id,
    dd.full_date,
    dt.hour;
```


- Table 4: `ed/edstays` + `triage` with `stay_duration`

```postgresql
SELECT *
FROM fact_edstays fes
LEFT JOIN dim_triage USING(stay_id) 
```


