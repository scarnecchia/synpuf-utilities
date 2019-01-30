

##	PURPOSE
This documentation provides resources to demonstrate the ability to execute the Sentinel Cohort Identification and Descriptive Analysis (CIDA) tool on standardized Medicare Claims Synthetic Public Use Files (SynPUFs). These resources include a demonstration Sentinel Statistical Analysis System (SAS) package, a standardized dataset made compatible to execute the tool, and detailed instructions on how to use it.  

<br>
##	BACKGROUND  
###	WHAT ARE THE SENTINEL COMMON DATA MODEL AND THE SENTINEL DISTRIBUTED DATABASE?
Sentinel uses a distributed data approach in which Data Partners maintain physical and operational control over electronic data in their existing environments. The distributed approach is achieved by using a standardized data structure referred to as the Sentinel Common Data Model (SCDM)<sup><a name=a1>[1](#f1)</a></sup>.  Data Partners transform their data locally according to the Common Data Model, which enables them to execute standardized computer programs that run identically at each Data Partner site. Data Partners are able to review the results of the queries before sending them back to the Sentinel Operations Center (SOC). Queries are distributed and results are returned through a secure portal in order to preserve privacy. The combined collection of datasets across all Data Partners is known as the Sentinel Distributed Database (SDD).  

###	WHAT ARE MEDICARE CLAIMS SYNTHETIC PUBLIC USE FILES?
Medicare Claims Synthetic Public Use Files (SynPUFs)<sup><a name=a2>[2](#f2)</a></sup> were created to allow interested parties to gain familiarity using Medicare claims data while protecting beneficiary privacy. These are synthetic claims datasets created by combining randomized information from various beneficiaries. Each record from the SynPUFs dataset contains extracted claims information from at least three unique beneficiaries. These records were further altered by changing variable values to provide additional deidentification from the beneficiaries.  

###	WHAT ARE SCDM-FORMATTED SYNPUFS?
The SOC has transformed SynPUFs into the SCDM<sup><a name=a1>[1](#f1)</a></sup> format so that users can utilize Sentinel tools with the SynPUFs dataset. The SCDM is a standardized data structure which enables the execution of the standardized Sentinel Statistical Analysis System (SAS) Cohort Identification and Descriptive Analysis (CIDA) packages. The SCDM-formatted SynPUFs contain the same information as SynPUFs, but in a standardized format that is compatible with Sentinel analytic tools.  

The SCDM-formatted SynPUFs are available on the Sentinel website for public use. There are 20 subsamples of the SynPUFs dataset available for users which can be used individually or combined together to create a larger dataset. Each subsample consists of seven data tables containing information related to: health plan enrollment, member demographics, health care utilization (e.g., outpatient pharmacy dispensings and medical encounters, diagnoses, and procedures), and death.  

SynPUFs Technical Specification, SynPUFs User Documentation, and SynPUFs CIDA Package are available for viewing and downloading via webpage Medicare Claims Synthetic Public Use Files in Sentinel Common Data Model Format: User Documentation and Demonstration Routine Querying Package.  

The data tables are available for download via .zip files and can be found on the SynPUFs dataset website page. Note that all tables in a subsample must be downloaded in order to execute the Sentinel analytic tool on the SCDM-formatted SynPUFs. For detailed instructions, please see  Instruction Steps.

###	WHAT IS THE COHORT IDENTIFICATION AND DESCRIPTIVE ANALYSIS TOOL?
Sentinel routine querying tools are SAS programs designed to run against the SCDM. They allow rapid implementation of standard queries across the SDD. As part of the Routine Querying System, Cohort Identification and Descriptive Analysis (CIDA)<sup><a name=a3>[3](#f3)</a></sup> is a tool made up of SAS macros that allows the user to select the cohort(s) of interest. CIDA may be used to calculate background rates of health outcomes of interest (HOIs) (e.g., prevalence of acute myocardial infarction), or rates of medical product use (e.g., new warfarin use), or it may be used for more complex queries that identify the occurrence of HOIs during exposure to a medical product of interest (e.g., number of incident diagnoses of angioedema during new treatment with angiotensin-converting enzyme inhibitors (ACE inhibitors)).  

The CIDA program, by default, will output summary-level counts (e.g., number of new users, number of HOIs) stratified by various parameters (e.g., age group, sex, year, year-month). CIDA will also output metrics on eligible members and eligible member-days associated with each result stratum, allowing for the calculation of proportions and rates, and an attrition table to determine the number of eligible members removed from consideration after application of various cohort selection criteria. For definitions for eligible members and eligible member-days, please refer to the glossary in Appendix D.  
  
The CIDA tool may be used alone or in conjunction with additional tools that perform more complex adjustment for confounders. For example, certain analyses may require comparing individuals who are exposed to the treatment of interest (e.g., ACE inhibitors) with individuals who are on an active comparator treatment (e.g., beta blockers). The demonstration Sentinel CIDA package includes an execution of module for the Propensity Score Analysis (PSA) tool. The CIDA tool can generate output containing information on exposures, outcomes, and covariates that are inputs to the PSA tool. The PSA tool uses the information output by the CIDA tool to estimate a propensity score based on user-defined covariates in user-defined cohorts, and/or via a high-dimensional propensity score approach. The PSA tool then uses the exposed cohort and the comparator cohort, for matching and/or stratification, based on propensity score and calculates hazard ratios, incidence rate differences and 95% confidence intervals.  

A Sentinel CIDA package is a standardized system of structured files, folders, and SAS programs that are formatted for the CIDA tool to read; therefore, these items must be set up in a specific manner in order to successfully execute the program against the SCDM-formatted SynPUFs. For detailed instructions, please see section Instructional Steps.  

For more information about the CIDA program, please visit Sentinel Routine Query Tools.  

###	DISCLAIMER FOR USING SYNPUFS  
The SynPUFs is a synthetic claims dataset created by combining randomized information from beneficiaries. Each record from the SynPUFs dataset contains extracted claims information from at least three unique beneficiaries. These records were further altered by changing variable values to provide additional beneficiary deidentification. Due to the synthetic nature of the dataset, results generated by SynPUFs may not be used to make any meaningful scientific conclusion.<sup><a name=a4>[4](#f4)</a></sup>   

The Sentinel Operations Center has converted SynPUFs into the SCDM format to demonstrate the functionality of the Sentinel routine query tool CIDA; results generated by the SCDM-formatted SynPUFs may not be used to make any meaningful scientific conclusion.


<br><br>
Continue reading about using the SCDM-formatted SynPUFs.

<br>
<br>

______________________
<a name="f1"></a> <sup>1</sup> https://www.sentinelinitiative.org/sentinel/data/distributed-database-common-data-model  

<a name="f2"></a> <sup>2</sup> https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/SynPUF_DUG.pdf  

<a name="f3"></a> <sup>3</sup> https://www.sentinelinitiative.org/sentinel/surveillance-tools/routine-querying-tools/routine-querying-system  

<a name="f4"></a> <sup>4</sup> https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/SynPUF_DUG.pdf