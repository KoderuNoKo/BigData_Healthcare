# General Information

- Dataset: MIMIC-IV, v3.1
- Modules used:
	- HOSP: Hospital wide electronic health record
	- ICU: Highly granular information collected from the clinical information system used within the ICU.
	- ED: Data for emergency department patients.
- Source: Data obtained from [PhysioNet](https://physionet.org/content/mimiciv/3.1/)
- Dataset Documentations: Available at https://mimic.mit.edu/

# Important Terms

- **Electronic Medical Record (EMR):** Records patient data within one specific medical practice or organization.

- **Electronic Health Record (EHR):** A comprehensive and patient-centered record that extends beyond a single practice to encompass information from multiple healthcare providers and organizations.

- **Healthcare Common Procedure Coding System (HCPCS)** 
	- **HCPCS Level I**: Comprised of **Current Procedural Terminology (CPT®)**, a numeric coding system maintained by the American Medical Association (AMA).
	- **HCPCS Level II**: A standardized coding system that is used primarily to identify products, supplies, and services not included in the CPT® codes, such as ambulance services or durable medical equipment, prosthetics, orthotics, and supplies (DMEPOS) when used outside a physician's office.

- **ICD-10** the tenth revision of the International Classification of Diseases, a system developed by the WHO for coding and classifying diagnoses, symptoms, and procedures. 
	- ICD-10-CM (Diagnosis Codes): To code diseases, signs, symptoms, and abnormal clinical findings for all healthcare settings.
	- ICD-10-PCS (Procedure Codes): To classify procedures performed in inpatient settings.

- **Electronic medication administration record (eMAR)** is the digital version of the traditional paper medication administration records used in healthcare facilities. It serves as a legal record of all the drugs that healthcare professionals have administered to a patient and is a part of the patient’s **EHR**.


# Notes

### Project Notes

- Focus on timeseries data for streaming and simulating velocity of data.
	
### Data Notes

- Date shifting
All dates in the database have been shifted to protect patient confidentiality. Dates will be internally consistent for the same patient, but randomly distributed in the future. Dates of birth which occur in the present time are not true dates of birth. Furthermore, dates of birth which occur before the year 1900 occur if the patient is older than 89. In these cases, the patient’s age at their first admission has been fixed to 300. => *Birthdate is as good as worthless*




# Modules

## Hosp 

- **Focus**: general hospital data, including ED, inpatient, and some outpatient events.
    
- **Tables include**:
    - **Demographics**: patients, admissions, transfers.
    - **Labs**: labevents, d_labitems.
    - **Microbiology**: microbiologyevents, d_micro.
    - **Medications**: prescriptions, pharmacy, medication administration (emar, emar_detail), provider orders.
    - **Billing / coding**: diagnoses_icd, d_icd_diagnoses, procedures_icd, d_icd_procedures, hcpcsevents, d_hcpcs, drgcodes.
    - **Clinical measurements**: omr (online medical record).
    - **Services and providers**: provider table with deidentified IDs.
        
- **Notable quirks / consideration**:
    - Contains outpatient/ED labs (so not confined to inpatient stays).
    - Deaths sourced from both hospital + state records, reconciled.
    - The eMAR system was implemented during 2011-2013. As a result, eMAR data is not available for all patients.

> ***Mostly general persistent data***

## ICU

- **Focus**: high-granularity bedside monitoring.
    
- **Star schema**:
    - `icustays` (core table: ICU episodes).    
    - `d_items` (dictionary of items measured).
    - `*_events` (time-stamped clinical events).

- **Event tables include**:
    - `chartevents`: charted vitals and notes.
    - `inputevents`, `ingredientevents`: IV & fluid inputs.
    - `outputevents`: urine output, drains.
    - `procedureevents`: procedures in ICU.
    - `datetimeevents`: e.g., start/end times for interventions.
        
- **Caregivers**: linked via caregiver_id.
    
- **Cohort handling**:
    - Consecutive transfers merged into a single ICU stay.
    - Non-consecutive ICU readmissions kept separate (researcher decides how to merge).

> ***Includes timeseries data, good for data stream simulation***

## ED

- **Focus:**  data for emergency department patients collected while they are in the ED. Information includes reason for admission, triage assessment, vital signs, and medicine reconciliaton. The `subject_id` and `hadm_id` identifiers *allow MIMIC-IV-ED to be linked to other MIMIC-IV modules*.
    
- **Tables:**
    
    - **edstays:** Metadata for each ED stay (arrival/departure, demographics, identifiers).
        
    - **diagnosis:** ED-specific diagnoses assigned during visit.
        
    - **triage:** Triage assessment (acuity level, chief complaint).
        
    - **vitalsign:** Vital sign measurements (heart rate, BP, resp, temp, etc.).
        
    - **medrecon:** Medication reconciliation records.
        
    - **pyxis:** Records of medication dispensing from automated cabinets.
        
- **Uses:** Supports studies on ED workflow, patient acuity, early warning, triage performance, admission/discharge prediction, and medication safety.