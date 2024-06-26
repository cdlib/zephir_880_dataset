# Zephir 880 Dataset

This repository contains scripts to generate an 880 dataset for analysis. HathiTrust is analyzing all volume records that include an 880-245 and require data be extracted from Zephir for this analysis. There are two scripts that generate this dataset.

## Requirements
- Python 3.12+
- Poetry
- Database credentials in a `.env` file
  
## 880 Volume Dataset
- Script: `generate_880_volume_dataset.py` 
- Outupt: `880_volume_dataset.csv`

Generates a dataset of volumes that contain an 880-245. While it can be used to count all volumes with 880-245 in the supplied record, it's primary purpose is to build the final dataset. The dataset includes the following columns:
- `cid`
- `namespace`,
- `contribsys_id`
- `htid`
- `language`
- `var_usfeddoc`
- `var_score`
- `vufind_sort`
  
## 880 Record Dataset
script: `generate_880_record_dataset.py`
output: `880_record_dataset.tsv`

Generates a dataset of records with 880-245 fields. The script uses the output from the volume dataset, and makes the following changes. 
- Removes duplicates of the same record, which is common when one contributor has multiple volumes with the same bibliographic record. It achieves this by keeping only one entry per contributor system ID (ILS number).
- Calculates the expected bibliographic data `selection order` Zephir will use when building a combined cluster record.
- Adds the `title` field and associated `880` field. If there is an error in the 880 field linking, the script will use null values for the `880`.
  
The dataset includes the following columns:
- `cid`
- `namespace`
- `contribsys_id`
- `htid`
- `language`
- `selection order`
- `title`
- `880`
