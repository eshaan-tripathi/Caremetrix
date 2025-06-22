##TABLE DETAILS
#PROVIDERS
| Column Name      | Data Type | Description                                                                 |
|------------------|-----------|-----------------------------------------------------------------------------|
| `id`             | UUID      | Unique identifier for the provider.                                         |
| `organization`   | UUID      | Foreign key to the organization (e.g., hospital or clinic group).           |
| `name`           | String    | Full name of the provider (e.g., “Gabriel934 Reilly981”).                  |
| `gender`         | String    | Gender of the provider. Values: `M` (Male), `F` (Female), `O` (Other).      |
| `speciality`     | String    | Medical speciality (e.g., "GENERAL PRACTICE", "CARDIOLOGY").                |
| `address`        | String    | Street address of the provider’s practice.                                 |
| `city`           | String    | City where the provider practices.                                          |
| `state`          | String    | State abbreviation (e.g., "MA" for Massachusetts).                         |
| `zip`            | String    | Zip/postal code of the provider’s address.                                 |
| `lat`            | Float     | Latitude coordinate of the provider’s practice location.                   |
| `lon`            | Float     | Longitude coordinate of the provider’s practice location.                  |
| `encounters`     | Integer   | Number of encounters the provider has had with patients.                   |
| `procedures`     | Integer   | Number of procedures the provider has performed.                           |
| `source_file`    | String    | S3 path or file path of the source file where the record was ingested from.|
| `ingestion_time` | Timestamp | Timestamp when this record was ingested into the system.                   |
| `effective_date` | Timestamp | Date from which the record is considered valid (for SCD2 tracking).        |
| `end_date`       | Timestamp | Date the record stopped being valid. Defaults to `2300-01-01` if current.  |
| `is_current`     | Boolean   | Flag indicating if the record is the most recent (`true`) or historical.   |

#ALEERGIES
| Column Name       | Data Type | Description                                                                 |
|-------------------|-----------|-----------------------------------------------------------------------------|
| `start`           | Timestamp | Date the allergy was first recorded.                                        |
| `stop`            | Timestamp | Date the allergy was stopped (if applicable).                              |
| `patient`         | UUID      | Unique identifier for the patient.                                          |
| `encounter`       | UUID      | Encounter ID during which the allergy was noted.                            |
| `code`            | String    | Standard code for the allergen (e.g., SNOMED CT, RxNorm).                   |
| `system`          | String    | Coding system used for the allergen.                                        |
| `description`     | String    | Human-readable description of the allergen.                                 |
| `type`            | String    | Type of allergy (e.g., allergy, intolerance).                               |
| `category`        | String    | Category of the allergen (e.g., medication, food, environment).             |
| `reaction1`       | String    | First reaction code (if applicable).                                        |
| `description1`    | String    | Description of the first reaction.                                          |
| `severity1`       | String    | Severity of the first reaction.                                             |
| `reaction2`       | String    | Second reaction code (if applicable).                                       |
| `description2`    | String    | Description of the second reaction.                                         |
| `severity2`       | String    | Severity of the second reaction.                                            |
| `source_file`     | String    | Source file path (e.g., S3 URI).                                            |
| `ingestion_time`  | Timestamp | Time of ingestion into the system.                                          |
| `effective_date`  | Timestamp | Date the record became effective (SCD2).                                    |
| `end_date`        | Timestamp | End date of validity (default `2300-01-01`).                                |
| `is_current`      | Boolean   | Flag indicating if the record is current (`true`) or outdated (`false`).    |

#CAREPLANS
| Column Name       | Data Type | Description                                                                 |
|-------------------|-----------|-----------------------------------------------------------------------------|
| `id`              | UUID      | Unique care plan ID.                                                        |
| `start`           | Timestamp | Start date of the care plan.                                                |
| `stop`            | Timestamp | End date of the care plan.                                                  |
| `patient`         | UUID      | Patient ID.                                                                 |
| `encounter`       | UUID      | Encounter ID when the care plan was initiated.                              |
| `code`            | String    | Care plan code (e.g., SNOMED CT).                                           |
| `description`     | String    | Description of the care plan.                                               |
| `reasoncode`      | String    | Optional reason code for the care plan.                                     |
| `reasondescription`| String   | Description of the reason for the care plan.                                |
| `source_file`     | String    | Source file path.                                                           |
| `ingestion_time`  | Timestamp | Time of ingestion into the system.                                          |
| `effective_date`  | Timestamp | Date the record became effective.                                           |
| `end_date`        | Timestamp | End date of the record (default `2300-01-01`).                              |
| `is_current`      | Boolean   | Flag for whether this record is the current version.                        |

#CLAIMS
| Column Name              | Data Type | Description                                                             |
|--------------------------|-----------|-------------------------------------------------------------------------|
| `id`                     | UUID      | Unique claim ID.                                                        |
| `patientid`              | UUID      | ID of the patient for this claim.                                       |
| `providerid`             | UUID      | ID of the provider who performed the service.                           |
| `primarypatientinsuranceid` | UUID   | ID of the primary insurance.                                            |
| `secondarypatientinsuranceid` | UUID | ID of the secondary insurance.                                          |
| `departmentid`           | UUID      | Department ID where the service occurred.                               |
| `patientdepartmentid`    | UUID      | Patient department/unit ID.                                             |
| `diagnosis1`–`diagnosis8`| String    | Diagnosis codes (e.g., SNOMED CT or ICD-10).                            |
| `referringproviderid`    | UUID      | Referring provider ID.                                                  |
| `appointmentid`          | UUID      | Related appointment ID.                                                 |
| `currentillnessdate`     | Timestamp | Onset date of the current illness.                                      |
| `servicedate`            | Timestamp | Service date of the claim.                                              |
| `supervisingproviderid`  | UUID      | Supervising provider ID.                                                |
| `status1`, `status2`, `statusp` | String | Claim statuses for primary, secondary, patient.                        |
| `outstanding1`, `outstanding2`, `outstandingp` | Float | Outstanding amounts.                              |
| `lastbilleddate1`, `lastbilleddate2`, `lastbilleddatep` | Timestamp | Dates when billed.          |
| `healthcareclaimtypeid1`, `healthcareclaimtypeid2` | Integer | Claim type identifiers.                |
| `source_file`            | String    | Source file path.                                                       |
| `ingestion_time`         | Timestamp | Ingestion timestamp.                                                    |
| `effective_date`         | Timestamp | Effective date (SCD2).                                                  |
| `end_date`               | Timestamp | End date of validity.                                                   |
| `is_current`             | Boolean   | Is the record currently valid.                                          |

#CLAIMS_TRANSACTIONS
| Column Name           | Data Type | Description                                                             |
|-----------------------|-----------|-------------------------------------------------------------------------|
| `id`                  | UUID      | Transaction line item ID.                                               |
| `claimid`             | UUID      | Foreign key to `claims` table.                                          |
| `chargeid`            | UUID      | ID of the specific charge related to the transaction.                   |
| `patientid`           | UUID      | Patient ID.                                                             |
| `type`                | String    | Transaction type: `CHARGE`, `PAYMENT`, `TRANSFEROUT`, etc.              |
| `amount`              | Float     | Amount associated with the transaction.                                 |
| `method`              | String    | Payment method (if applicable).                                         |
| `fromdate`, `todate`  | Timestamp | Date range of the billed service.                                       |
| `placeofservice`      | UUID      | Location where the service occurred.                                    |
| `procedurecode`       | String    | Code for the procedure performed.                                       |
| `modifier1`, `modifier2` | String | Procedure code modifiers.                                               |
| `diagnosisref1`–`diagnosisref4` | String | Linked diagnosis references.                             |
| `units`               | Integer   | Number of units billed.                                                 |
| `departmentid`        | UUID      | Department responsible for the transaction.                             |
| `unitamount`          | Float     | Unit cost of the service/procedure.                                     |
| `transferoutid`       | UUID      | Reference to the related transfer ID (if applicable).                   |
| `transfertype`        | String    | Reason/type of transfer.                                                |
| `payments`            | Float     | Amount paid.                                                            |
| `adjustments`         | Float     | Adjustments/write-offs.                                                 |
| `transfers`           | Float     | Amount transferred to another payer or party.                           |
| `outstanding`         | Float     | Amount still due.                                                       |
| `appointmentid`       | UUID      | Appointment ID linked to the transaction.                               |
| `linenote`            | String    | Notes related to the line item.                                         |
| `patientinsuranceid`  | UUID      | Patient insurance responsible for payment.                              |
| `feescheduleid`       | UUID      | Fee schedule used for pricing.                                          |
| `providerid`          | UUID      | Performing provider.                                                    |
| `supervisingproviderid` | UUID    | Supervising provider (if applicable).                                   |
| `notes_part1`, `notes_part2` | String | Additional transaction notes.                                    |
| `source_file`         | String    | File where data was sourced from.                                       |
| `ingestion_time`      | Timestamp | Ingestion timestamp.                                                    |
| `effective_date`      | Timestamp | Start of record validity.                                               |
| `end_date`            | Timestamp | End of record validity (default `2300-01-01`).                          |
| `is_current`          | Boolean   | Whether the record is the latest/current.                               |

#CONDITIONS
| Column Name        | Data Type | Description                                                                 |
|--------------------|-----------|-----------------------------------------------------------------------------|
| `start`            | Timestamp | Start date of the condition or when it was first diagnosed or noted.       |
| `stop`             | Timestamp | End date of the condition (if resolved or no longer active).               |
| `patient`          | UUID      | Unique identifier for the patient with the condition.                      |
| `encounter`        | UUID      | Identifier for the encounter when the condition was documented.            |
| `system`           | String    | Coding system used for the condition (e.g., SNOMED CT URL).                |
| `code`             | String    | Standardized medical code for the condition (e.g., SNOMED CT concept ID).  |
| `description_part1`| String    | Human-readable name or label for the condition.                            |
| `description_part2`| String    | (Optional) Additional descriptive text if available.                       |
| `source_file`      | String    | Path to the original file (e.g., S3 URI) where this data was sourced.      |
| `ingestion_time`   | Timestamp | Timestamp when this record was ingested into the data lake/system.         |
| `effective_date`   | Timestamp | The date this record became effective in the system (used in SCD2).        |
| `end_date`         | Timestamp | The date this record became outdated or superseded (`2300-01-01` if active).|
| `is_current`       | Boolean   | Indicates if the record is the most recent (`true`) or historical (`false`).|

#DEVICES
| Column Name       | Data Type | Description                                                                 |
|-------------------|-----------|-----------------------------------------------------------------------------|
| `start`           | Timestamp | Date and time when the device was used, implanted, or became active.        |
| `stop`            | Timestamp | Date and time when the device was no longer used or was removed.            |
| `patient`         | UUID      | Unique identifier of the patient using or receiving the device.             |
| `encounter`       | UUID      | Encounter ID during which the device was administered or recorded.          |
| `code`            | String    | Standardized code representing the type of device (e.g., SNOMED CT).        |
| `description`     | String    | Human-readable description of the device (e.g., "Dental x-ray system").     |
| `udi`             | String    | Unique Device Identifier (UDI) string — includes manufacturer, version, etc.|
| `source_file`     | String    | Path to the source file where this data originated (e.g., S3 location).     |
| `ingestion_time`  | Timestamp | Timestamp when the record was ingested into the system or data lake.        |
| `effective_date`  | Timestamp | Date from which this version of the record is considered valid (SCD2).      |
| `end_date`        | Timestamp | Date this record stopped being valid (`2300-01-01` means still active).     |
| `is_current`      | Boolean   | Indicates if this is the current version (`true`) or historical (`false`).  |

#ENCOUNTERS
| Column Name          | Data Type | Description                                                                 |
|----------------------|-----------|-----------------------------------------------------------------------------|
| `id`                 | UUID      | Unique identifier for the encounter.                                        |
| `start`              | Timestamp | Start date and time of the encounter.                                       |
| `stop`               | Timestamp | End date and time of the encounter.                                         |
| `patient`            | UUID      | Unique identifier of the patient involved in the encounter.                 |
| `organization`       | UUID      | Organization (e.g., hospital or clinic) where the encounter occurred.       |
| `provider`           | UUID      | Provider responsible for the encounter.                                     |
| `payer`              | UUID      | Insurance payer or entity covering the encounter cost.                      |
| `encounterclass`     | String    | Class of encounter (e.g., `inpatient`, `ambulatory`, `wellness`).          |
| `code`               | String    | Standard code for the encounter (e.g., SNOMED CT or CPT).                  |
| `base_encounter_cost`| Float     | Base cost of the encounter before claims or adjustments.                   |
| `total_claim_cost`   | Float     | Total amount claimed for the encounter.                                    |
| `payer_coverage`     | Float     | Amount covered by the insurance payer.                                     |
| `reasoncode`         | String    | Code representing the reason for the encounter (optional).                 |
| `reasondescription`  | String    | Description of the reason for the encounter (optional).                    |
| `description_part1`  | String    | Human-readable description of the encounter purpose.                       |
| `description_part2`  | String    | Additional description (optional or null).                                 |
| `source_file`        | String    | Path to the file from which the data was sourced (e.g., S3 location).       |
| `ingestion_time`     | Timestamp | Timestamp when the record was ingested into the system.                    |
| `effective_date`     | Timestamp | Date when this version of the record became effective (SCD2).              |
| `end_date`           | Timestamp | End date of this version’s validity (default `2300-01-01` if current).     |
| `is_current`         | Boolean   | `true` if this is the latest version of the record, `false` otherwise.     |

#IMAGING_STUDIES
| Column Name           | Data Type | Description                                                                 |
|------------------------|-----------|-----------------------------------------------------------------------------|
| `id`                   | UUID      | Unique identifier for the imaging study record.                             |
| `date`                 | Timestamp | Date and time the imaging study was performed.                              |
| `patient`              | UUID      | Identifier for the patient who underwent the imaging.                       |
| `encounter`            | UUID      | Identifier for the clinical encounter during which the image was taken.     |
| `series_uid`           | String    | Unique DICOM Series UID – identifies the series of images.                  |
| `bodysite_code`        | String    | Coded representation of the anatomical body site imaged.                    |
| `bodysite_description` | String    | Human-readable name of the body part imaged (e.g., Thoracic structure).     |
| `modality_code`        | String    | Short code for the imaging modality (e.g., CT, MRI, US).                    |
| `modality_description` | String    | Full description of the modality (e.g., Computed Tomography).               |
| `instance_uid`         | String    | Unique DICOM UID for the specific image or image instance.                  |
| `sop_code`             | String    | DICOM SOP (Service-Object Pair) Class UID representing the image type.      |
| `sop_description`      | String    | Description of the SOP class (e.g., CT Image Storage).                      |
| `procedure_code`       | String    | Code representing the procedure that required this imaging study.           |
| `source_file`          | String    | Path to the source file where this record came from (e.g., S3 location).    |
| `ingestion_time`       | Timestamp | Time when the record was ingested into the system.                          |
| `effective_date`       | Timestamp | Start of this record’s validity (for Slowly Changing Dimensions – SCD2).    |
| `end_date`             | Timestamp | End of the record’s validity (`2300-01-01` if current).                     |
| `is_current`           | Boolean   | `true` if the record is currently valid; `false` if historical.             |

#IMMUNIZATIONS
| Column Name         | Data Type | Description                                                                 |
|----------------------|-----------|-----------------------------------------------------------------------------|
| `date`               | Timestamp | Date and time the immunization was administered.                            |
| `patient`            | UUID      | Unique identifier for the patient who received the immunization.            |
| `encounter`          | UUID      | ID of the encounter during which the immunization occurred.                 |
| `code`               | String    | Code representing the vaccine/immunization administered.                    |
| `base_cost`          | Float     | Base cost of the immunization.                                              |
| `description_part1`  | String    | Name or label of the immunization (e.g., "Hep B adolescent").               |
| `description_part2`  | String    | Additional description, e.g., target population ("pediatric", etc.).        |
| `source_file`        | String    | Path to the original file source (e.g., an S3 URI).                          |
| `ingestion_time`     | Timestamp | When the record was ingested into the system.                               |
| `effective_date`     | Timestamp | Start date for the record’s validity (used in SCD2 versioning).             |
| `end_date`           | Timestamp | End date of the record’s validity (`2300-01-01` if still current).          |
| `is_current`         | Boolean   | `true` if this is the current version; `false` if it's historical.          |

#MEDICATIONS
| Column Name         | Data Type | Description                                                                 |
|----------------------|-----------|-----------------------------------------------------------------------------|
| `start`              | Timestamp | Date and time when the medication was first prescribed/administered.        |
| `stop`               | Timestamp | Date and time when the medication was stopped or completed.                 |
| `patient`            | UUID      | Unique identifier for the patient receiving the medication.                 |
| `payer`              | UUID      | Insurance payer or organization responsible for medication cost.            |
| `encounter`          | UUID      | Clinical encounter during which the medication was prescribed.              |
| `code`               | String    | Code representing the medication (e.g., RxNorm code).                       |
| `description`        | String    | Human-readable name of the medication.                                      |
| `base_cost`          | Float     | Cost of a single dispense or dose (before coverage).                        |
| `payer_coverage`     | Float     | Amount covered by insurance or payer.                                       |
| `dispenses`          | Integer   | Number of times the medication was dispensed.                               |
| `totalcost`          | Float     | Total cost of all dispenses (typically base_cost × dispenses).              |
| `reasoncode`         | String    | Code representing the reason for the medication (e.g., diagnosis code).     |
| `reasondescription`  | String    | Description of the medical reason (e.g., disease name).                     |
| `source_file`        | String    | Source file path for the record (e.g., S3 path of ingestion file).          |
| `ingestion_time`     | Timestamp | Timestamp when the record was loaded into the system.                       |
| `effective_date`     | Timestamp | Start of this record’s validity in the data warehouse (for SCD2 tracking).  |
| `end_date`           | Timestamp | End of validity (`2300-01-01` if the record is current).                    |
| `is_current`         | Boolean   | `true` if the record is the latest/current version, `false` if historical.  |

#OBSERVTIONS
| Column Name         | Data Type | Description                                                                 |
|---------------------|-----------|-----------------------------------------------------------------------------|
| `observation_sk`    | BigInt    | Surrogate key or internal unique identifier for the observation.            |
| `date`              | Timestamp | Date and time when the observation was recorded.                            |
| `patient`           | UUID      | Unique ID of the patient the observation refers to.                         |
| `encounter`         | UUID      | ID of the clinical encounter during which the observation was made.         |
| `category`          | String    | Category of the observation (e.g., `vital-signs`, `laboratory`).            |
| `code`              | String    | LOINC or SNOMED code identifying the type of observation.                   |
| `units`             | String    | Unit of measurement (e.g., `mm[Hg]`, `{nominal}`); may be null for text.    |
| `type`              | String    | Type of value: `numeric`, `text`, or another descriptor.                     |
| `description_part1` | String    | Primary description of the observation (e.g., `Diastolic Blood Pressure`).  |
| `description_part2` | String    | Additional descriptive information (e.g., SNOMED term); optional.           |
| `value_part1`       | String    | Main recorded value of the observation (e.g., `75.0`, `Negative`).          |
| `value_part2`       | String    | Additional observation value (e.g., interpretation, units); optional.       |
| `source_file`       | String    | Source file path (e.g., S3 location from which data was ingested).          |
| `ingestion_time`    | Timestamp | Timestamp when the record was ingested into the data system.                |
| `effective_date`    | Timestamp | Start of the record’s validity period (for SCD2 tracking).                  |
| `end_date`          | Timestamp | End of the record’s validity_



