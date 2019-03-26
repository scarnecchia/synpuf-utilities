![alt text](files/resources/logo.png)

# Welcome to the Sentinel's SynPUFs Repositories 

Sentinel has made available the [<b>CMS 2008-2010 Data Entrepreneurs’ Synthetic Public Use Files (SynPUFs)</b>](https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/index.html) in the Sentinel Common Data Model (SCDM) format. This transformation of data allows for the running of the Cohort Identification and Descriptive Analysis (CIDA) tool on the SynPUFs data. The CMS SynPUFs are available in the form of 20 mutually exclusive datasets, which together make up a 5% sample of the entire CMS database from 2008-2010. Each of the 20 datasets contains about 110,000 members. The intended use of these data in SCDM format is to generate familiarity with the CIDA tool and its capabilities, and to allow for methodological expansion.  

The SynPUFs datasets in the SCDM format are available in the form of SAS datasets, and the CIDA tools are available as SAS programs. In order to access the SCDM-formatted datasets and the CIDA tools, users must have:
*  SAS version 9.3 or higher
*  at least 2.65GB of disk space per dataset 

### Using SCDM-Formatted SynPUFs  

The 20 SCDM-formatted datasets can be found [<b>here</b>](https://dev.sentinelsystem.org/projects/SYNPUF/repos/synpuf_sas_datasets/browse).  
*  These datasets have undergone Sentinel's rigorous Quality Assurance (QA) testing. To view Sentinel's current QA program, navigate to the [<b>QA Package Repository</b>](https://dev.sentinelsystem.org/projects/QA/repos/qa_package/browse).  

Users can run a CIDA package on one SCDM-formatted SynPUFs dataset, or users can combine multiple datasets using a SAS program developed by the Sentinel Operations Center (SOC). The instructions to aggregate datasets and run the CIDA package can be found [<b>here</b>](https://dev.sentinelsystem.org/projects/SYNPUF/repos/synpuf_demo_package/browse). 

SOC has provided a demonstration package on CIDA version 5.3.1 with all the parameters filled in which can be found [<b>here</b>](https://dev.sentinelsystem.org/projects/SYNPUF/repos/synpuf_demo_package/browse). 
*  This package is based off a Sentinel level 2 request that used Propensity Score Matching to assess the risk of angioedema among new angiotensin-converting enzyme (ACE) inhibitors users compared to beta blocker users.  

SOC has also run the demonstration CIDA package on two combined SynPUFs datasets, and the results can be found on the [<b>Sentinel website</b>](https://www.sentinelinitiative.org/sentinel/surveillance-tools/software-toolkits/Medicare-SynPUFs-in-SCDM).

If you would like to parameterize your own CIDA package, navigate to the most recent version of  [</b>CIDA</b>](https://dev.sentinelsystem.org/projects/AD/repos/qrp/browse) and its [<b>accompanying documentation</b>](https://dev.sentinelsystem.org/projects/SENTINEL/repos/sentinel-routine-querying-tool-documentation/browse).

### Quick Links
*  [SynPUFs SAS Datasets](https://dev.sentinelsystem.org/projects/SYNPUF/repos/synpuf_sas_datasets/browse)
*  [SynPUFs Demonstrational Sentinel CIDA Package](https://dev.sentinelsystem.org/projects/SYNPUF/repos/synpuf_demo_package/browse)
*  [Sentinel Website SynPUFs page](https://www.sentinelinitiative.org/sentinel/surveillance-tools/software-toolkits/Medicare-SynPUFs-in-SCDM)

