# Package

The `mdlpipeline` package contains an end-to-end ETL pipeline in the `sync.enrollments.SyncEnrollments` class for syncronizing enrollments in Moodle (LMS) to match PowerCampus (SIS) for configured courses.

## Utils Subpackages

**mdltools**

The `mdltools` subpackage contains client request abstractions dedicated to interacting with the Moodle Web Services API.

**sqltools**

The `sqltools` subpackage contains a database handler abstaction and queries for retrieving enrollment data from the PowerCampus database.

## scripts

The `scripts/` directory contains runner code for `mdlpipeline`. The `schedule_sync` module contains a daemon to syncronize enrollments every fifteen minutes.

## Installing

**git**

`$ git clone git@github.com:UWS-IS/campus-enrollment-sync.git`

**pip**

**Pipfile**

## Configuring

### Data requirements

The `data/` folder contains JSON mapping files encoding the one-to-many relationships of Moodle courses to PowerCampus sections. For example the following entry is for Moodle with shortname `"COUN6552COUN855202Winter2021"` and courseid of `4766`, which retrieves enrollments from only section `"02"` of the following courses `["COUN6552", "COUN8552"]`.

```json
{
  "COUN6552COUN855202Winter2021":
  {
      "courses": [
          "COUN6552",
          "COUN8552"
      ],
      "section": "02",
      "id": 4766
  }
```

An entry for a course not tied to any particular section looks as follows:

```json
"BSC5102Winter2021": {
      "courses": [
        "BSC5102"
      ],
      "id": 4748
    },
```

This mapping file needs to be reconstructed for each term.

### Enviornment Variables

Replace empty strings with values to load in local run enviornment (e.g. store in `.bashrc`).

```shell
# Needed for Moodle API access
export MDL_TOKEN "" # Moodle Web Services API token

# Needed for database access
export MS_USER  "" # Micorsoft UWSNET user
export MS_PW # Microsfot USENET password

# Needed for Push to Moodle Conduit
export SFTP_HOST "" # OpenLMS SFTP host
export SFTP_PW  "" # OpenLMS SFTP password
```

## Running

```python
>>> from mdlpipeline.sync.enrollments import SyncEnrollments
>>> # Store mapping file in `MAPPING_JSON`
>>> with open('data/wc_pc_mapping_wi21.json', 'r') as f:
        MAPPING_JSON = json.load(f)
>>> # Data pull to find add/drops for webCampus's Conduit system
>>> se = asyncio.run(SyncEnrollments.pull(MAPPING_JSON))
>>> # Log Conduit file in log/ directory
>>> se.log_conduit()
>>> # Push file via SFTP
>>> se.push_conduit()
```
