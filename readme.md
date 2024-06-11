![Sentinel Logo](https://dev.sentinelsystem.org/projects/AP/repos/sentinel-analytic-packages/raw/resources/logo.png?at=refs%2Fheads%2Fmaster)

# Welcome to Sentinel's SynPUFs Software Toolkit

Sentinel has made available the [<b>CMS 2008-2010 Data Entrepreneurs’ Synthetic Public Use Files (SynPUFs)</b>](https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/index.html) in the Sentinel Common Data Model (SCDM) format. This transformation of data allows for the running of Sentinel's Routine Querying System tools, including the Cohort Identification and Descriptive Analysis (CIDA) tool, on the SynPUFs data. The CMS SynPUFs are available in the form of 20 mutually exclusive datasets, which together make up a 5% sample of the entire CMS database from 2008-2010. Each of the 20 datasets contains about 110,000 members. The intended use of these data in SCDM format is to generate familiarity with the CIDA tool and its capabilities and to allow for methodological expansion.

The SynPUFs datasets in the SCDM format are available in the form of SAS datasets, and the CIDA tools are available as SAS programs. In order to access the SCDM-formatted datasets and the CIDA tools, users must have:

- SAS version 9.4 or higher
- at least 2.65GB of disk space per dataset

# Using SCDM-Formatted SynPUFs

The 20 SCDM-formatted datasets can be found on the [Sentinel website](https://www.sentinelinitiative.org/methods-data-tools/software-packages-toolkits/medicare-claims-synthetic-public-use-files-sentinel-0)

Alternatively, Linux users may download all 20 subsamples by using the following command line script:

```bash
for num in {1..20}; do
    curl -o "scdm_v8_subsamples_$num.zip" "https://www.sentinelinitiative.org/sites/default/files/surveillance-tools/software-toolkit/scdm_v8_subsamples_$num.zip"
    curl -o "scdm_v8_diagnosis_$num.zip" "https://www.sentinelinitiative.org/sites/default/files/surveillance-tools/software-toolkit/scdm_v8_diagnosis_$num.zip"
    curl -o "scdm_v8_procedure_$num.zip" "https://www.sentinelinitiative.org/sites/default/files/surveillance-tools/software-toolkit/scdm_v8_procedure_$num.zip"
done
```

These datasets have undergone Sentinel's rigorous Data Quality Review and Characterization testing. For more information on Sentinel's quality assurance process and to view Sentinel's current Data Quality Review and Characterization programs, navigate to the [<b>Data Quality Review and Characterization Programs page</b>](https://dev.sentinelsystem.org/projects/QA/repos/qa_package/browse).

Users can run a CIDA package on one SCDM-formatted SynPUFs dataset, or users can combine multiple datasets using a SAS program developed by the Sentinel Operations Center (SOC). The instructions to aggregate datasets and run the CIDA package can be found [<b>here</b>](https://dev.sentinelsystem.org/projects/SYNPUF/repos/synpuf_demo_package/browse).

SOC has provided a demonstration package on CIDA version 5.3.1 with all the parameters filled in which can be found [<b>here</b>](https://dev.sentinelsystem.org/projects/SYNPUF/repos/synpuf_demo_package/browse).

- This package is based off a Sentinel level 2 request that used Propensity Score Matching to assess the risk of angioedema among new angiotensin-converting enzyme (ACE) inhibitor users compared to beta blocker users.
- The results of this demonstration package run on two combined SynPUFs datasets can be found on the [<b>Sentinel website</b>](https://www.sentinelinitiative.org/sentinel/surveillance-tools/software-toolkits/Medicare-SynPUFs-in-SCDM).

If you would like to parameterize your own CIDA package, navigate to the most recent version of [<b>CIDA</b>](https://dev.sentinelsystem.org/projects/AD/repos/qrp/browse) and the [<b>Routine Querying System Tools documentation</b>](https://dev.sentinelsystem.org/projects/SENTINEL/repos/sentinel-routine-querying-tool-documentation/browse).

## SynPUF Tools

The translational_tools directory contains the following programs:

- prepare_scdm.sas: Aggregates two or more SynPUF subsamples into a single SCDM-formatted dataset.
- translate_synpufs_to_scdm.sas: Translates the CMS SynPUF datasets into the SCDM format.

The synpuf_export directory contains a SAS program allowing you to export SynPUF subsamples to .csv, .txt. or .json format.

A technical specification and data dictionary for descriptive statistics are available in the docs folder. Descriptive statistics are available in .csv format in the descriptive_statistics directory.

## Quick Links

- [SynPUFs SAS Datasets](https://www.sentinelinitiative.org/methods-data-tools/software-packages-toolkits/medicare-claims-synthetic-public-use-files-sentinel-0)
- [SynPUFs Demonstrational Sentinel CIDA Package](https://dev.sentinelsystem.org/projects/SYNPUF/repos/synpuf_demo_package/browse)
- [SynPUFs Example Sentinel Modular Program Report](https://www.sentinelinitiative.org/sentinel/surveillance-tools/software-toolkits/Medicare-SynPUFs-in-SCDM)
- [SynPUFs Translational Code](https://dev.sentinelsystem.org/projects/SYNPUF/repos/synpuf_overview/browse/translational_code)

______________________
<a name="f1"></a> <sup>1</sup> https://www.sentinelinitiative.org/sentinel/data/distributed-database-common-data-model