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
- `dim_date`: unique grouped dates, generated automatically when creating the warehouse.
- `dim_time`: unique grouped timestamps, generated automatically when creating the warehouse.

- `dim_diagnoses_icd`:
	- source: `hosp/diagnoses_icd` + `hosp/d_icd_diagnoses`
	- note:
		- Remove `subject_id` and `hadm_id`, replace with `diagnoses_icd_key`

- `dim_d_items`
	- source: `icu/d_items`

- `dim_triage`
	- source: `ed/triage` 

- `dim_admission` 
	- source: `hosp/dim_admission`