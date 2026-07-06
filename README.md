# PM2.5 Pollution and Mortality

A Hadoop/MapReduce pipeline that joins county-level PM2.5 air quality data with U.S. mortality rates to quantify the relationship between air pollution and death rates — controlling for race and age as confounders.

> NYU Real-Time & Big Data Analytics project by [Kristo Papadhimitri](mailto:kp1404@nyu.edu) and Ihwa Hussain. Run on Google Dataproc. Full write-up: [docs/report.pdf](docs/report.pdf).

## Table of Contents

- [Overview](#overview)
- [Data Sources](#data-sources)
- [Pipeline](#pipeline)
- [Repository Structure](#repository-structure)
- [Getting Started](#getting-started)
- [Results](#results)
- [Limitations & Obstacles](#limitations--obstacles)
- [References](#references)

## Overview

Air pollution's health effects are well documented, but most analysis stops at the national or state level. This project takes it down to the county: joining daily PM2.5 concentration readings with mortality rates by county, race, and age group to see whether — and how much — local air quality predicts local death rates.

Race and age are included deliberately, not incidentally. Both are known confounders: older populations and infants have elevated mortality independent of pollution (skewing results in counties like college towns vs. retirement communities), and documented disparities in environmental and medical racism mean race correlates with both exposure and outcome. Splitting on both isolates the pollution effect more cleanly than a naive location-only join would.

## Data Sources

| Dataset | Source | Size | Granularity |
|---|---|---|---|
| Mortality Rates by County, Race, and Ethnicity, 2000–2019 | [IHME](https://ghdx.healthdata.org/record/ihme-data/united-states-mortality-rates-county-race-ethnicity-2000-2019) | 1.08 GB | County / race / age group / year |
| Daily County-Level PM2.5 Concentrations, 2001–2019 | [CDC National Environmental Public Health Tracking Network](https://www.cdc.gov/ephtracking) | 282 MB | County / day |

Despite the stated 2001–2019 range, the PM2.5 file only actually contained usable data from 2016–2019 — worth knowing going in if you're deciding what year range to pull.

## Pipeline

Five MapReduce jobs, run in order, take raw CSVs down to a single joined dataset small enough to query in Hive.

**1. Clean and key the PM2.5 data** (`Formatted2019Data` / `Formatted2019Mapper`)
Filters the raw daily file down to 2019, drops rows with a blank `PM25_pop_pred`, and solves the fact that county FIPS codes alone aren't unique nationally — state FIPS is zero-padded to 2 digits, county FIPS to 3, and concatenated into a single composite key (`07` + `021` → `07021`). Mapper-only job.

**2. Aggregate daily → yearly PM2.5** (`PM25Driver` / `PM25Mapper` / `PM25Reducer`)
Takes the keyed daily readings and reduces them to one population-weighted yearly average per county (weighting by the daily count folded in at the mapper stage). Output: ~3,000 records, one per county — small enough to become the "small" side of a replicated join later.

**3. Clean the mortality data** (`IHME` / `IHMEMapper`, keyed by `DeathCategoryWritable`)
Filters the raw IHME dataset down to death-rate records (not life expectancy) at county granularity, across all races and age groups, both sexes combined, for all available years. `DeathCategoryWritable` bundles state FIPS, county FIPS, race, age group, and year into a single writable key. Mapper-only job.

**4. Profile top 10 mortality counties** (`IHMEProfile` / `IHMEProfileMapper` / `IHMEProfileReducer`, using `FIPSDeathsWritable`)
Both mapper and reducer maintain a size-capped `TreeMap` sorted by death rate, evicting the lowest entry once it exceeds 10 — a standard top-N pattern that lets each mapper pre-filter before a single reducer merges everything into a final top 10.

**5. Join mortality and PM2.5 data** (`PMJoin` / `PMJoinMapper`, using `DeathPMPair`)
Since the PM2.5 data is now much smaller than the mortality data, this is a **replicated join**: `PMJoinMapper` loads the PM2.5 data into a `HashMap` (FIPS → PM2.5 level) from the distributed cache in `setup()`, then joins it against each mortality record as the mortality data streams through the mapper. No reducer needed — the join happens entirely map-side. Output: one row per mortality record with its matching PM2.5 level attached (`DeathPMPair` holds both values).

From there, the joined CSV is small enough to load into Hive for the actual analysis — correlation, regression, and the binned breakdowns in the results below.

## Repository Structure

```
.
├── Formatted2019Data.java       # Step 1 driver: clean + key raw PM2.5 CSV
├── Formatted2019Mapper.java     # Step 1 mapper
├── PM25Driver.java              # Step 2 driver: daily → yearly PM2.5 average
├── PM25Mapper.java              # Step 2 mapper
├── PM25Reducer.java             # Step 2 reducer
├── IHME.java                    # Step 3 driver: clean raw mortality data
├── IHMEMapper.java              # Step 3 mapper
├── DeathCategoryWritable.java   # Custom key: state/county/race/age/year
├── IHMEProfile.java             # Step 4 driver: top-10 mortality profiling
├── IHMEProfileMapper.java       # Step 4 mapper
├── IHMEProfileReducer.java      # Step 4 reducer
├── FIPSDeathsWritable.java      # Custom writable: state/county/death rate
├── PMJoin.java                  # Step 5 driver: replicated join
├── PMJoinMapper.java            # Step 5 mapper
├── DeathPMPair.java             # Custom writable: death rate + PM2.5 pair
├── RBDA_Shell_Commands.txt      # Exact hadoop jar invocations, in order
├── docs/
│   └── report.pdf               # Full project write-up (methodology, all figures, references)
├── assets/
│   └── pm25_distribution.jpg    # Histogram of PM2.5 levels across counties
└── README.md
```

<!-- TODO: add a build file (pom.xml or build.gradle) — currently no dependency/build config is checked in -->

## Getting Started

These jobs were run on Google Dataproc; any Hadoop cluster with HDFS should work.

1. Upload the raw datasets to HDFS.
2. Compile each driver/mapper/reducer set into its own jar (jar names below match what's used in `RBDA_Shell_Commands.txt`; adjust to taste).
3. Run in order:

```bash
# Clean mortality data
hadoop jar IHME.jar IHME IHME_DATA/* CLEANED_MORTALITY_DATA

# Profile top 10 mortality counties
hadoop jar IHMEProfile.jar IHMEProfile CLEANED_MORTALITY_DATA/* TOP_10_MORTALITY_DATA

# Clean and key PM2.5 data
hadoop jar Format2019.jar Formatted2019Data 2019/PM2.5-Data.csv Format2019/output

# Aggregate to yearly PM2.5 averages
hadoop jar PM25Profile.jar PM25Driver Format2019/output/part-m-00000

# Replicated join
hadoop jar PMJoin.jar PMJoin CLEANED_MORTALITY_DATA/* JOINED_PM_MORTALITY_DATA
```

⚠️ **Before running the join:** `PMJoin.java` currently hardcodes the distributed cache file path to `/user/fh828_nyu_edu/PM25_DATA.txt` — a specific NYU HPC user directory from development. Update this to wherever your PM2.5 output actually lives before running it yourself.

## Results

![PM2.5 distribution across counties](https://github.com/AirKristo/pm25-pollution-mortality/figures/pm25_distribution.jpg)

Most counties fell in the 6–9 µg/m³ range, with a sharp drop-off above 10 — matching the top-10 tables below, where even the most polluted counties top out around 10.7.

**Most polluted counties** (highest average PM2.5, µg/m³) clustered in Texas, California, Michigan, and Georgia — Tulare County, CA topped the list at 10.72.

**Least polluted counties** were overwhelmingly in Wyoming — 9 of the 10 lowest-PM2.5 counties nationally, led by Teton County, WY at 2.77.

**Infant mortality** was highest in large, remote counties in South Dakota, Mississippi, and Alaska (which also tend to lack pollution data), and lowest in New Jersey and Massachusetts counties, which had slightly below-average PM2.5.

**Regression:**
```
Mortality Rate = 0.000357404 × PM2.5 + 0.0062368
R² = 0.131025
p < 0.0001
```
The relationship is highly statistically significant, but PM2.5 alone explains only ~13% of the variance in mortality rates — air quality matters, but it's one factor among several (socioeconomic conditions, healthcare access, lifestyle) that would need to be modeled to explain the rest.

**By race:** AIAN (American Indian and Alaska Native) populations showed the highest mortality rates across pollution bins, consistent with documented disparities beyond air quality alone — pointing to systemic and environmental factors compounding on top of pollution exposure.

## Limitations & Obstacles

- The PM2.5 file's stated 2001–2019 range didn't hold up — usable data only went back to 2016.
- Large, remote counties frequently lacked pollution monitoring data entirely, which likely biases the "worst" and "best" county rankings toward areas that *do* have good data coverage.
- Some counties were double-counted due to census FIPS re-designations (county renames/mergers over the data's time span).
- IHME's data organization changed mid-project, which meant losing the original codebook partway through and having to re-derive field meanings from the data itself.
- Some race/age combinations were missing entirely in low-population counties.

## References

1. Morello-Frosch, R. "Environmental Justice and Regional Inequality in Southern California: Implications for Future Research." *Environmental Health Perspectives*, 2011.
2. Xing, Y-F. et al. "The impact of PM2.5 on the human respiratory system." *Journal of Thoracic Disease* 8.1 (2016): E69–E74.
3. Porta, M. *A Dictionary of Epidemiology*, 6th ed. Oxford University Press, 2014.
4. Institute for Health Metrics and Evaluation. *United States Mortality Rates and Life Expectancy by County, Race, and Ethnicity 2000-2019*. IHME, 2022.
5. National Environmental Public Health Tracking Network. *Daily County-Level PM2.5 Concentrations, 2001-2019*. CDC, 2023.
