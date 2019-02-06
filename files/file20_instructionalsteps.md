## Instructional Steps  
The following steps are for using the pre-parameterized CIDA package. 

1.	Review the SynPUFs [Technical Specification](https://dev.sentinelsystem.org/projects/SYNPUF/repos/synpuf_user_documentation/browse/Sentinel_Tech_Spec_SynPUF_to_SCDM_v1.0.pdf) before proceeding.
2.	Please make sure your computer meets all the minimal system requirements before proceeding.
3.	Create a folder on your local drive (making sure it has enough disk space). As an example, we created the folder CIDA on B drive.  
![CIDA on B drive](files/resources/image01.png)  
4.  Navigate to the [<b>SynPUF CIDA package</b>](https://dev.sentinelsystem.org/projects/SYNPUF/repos/synpuf_cida_package/browse)
5.  Select the three dots next to the master branch and click Download  
![Download repository](files/resources/image02.png)  
6.  Save the downloaded demo_mpl2r_wp001_nsdp_v01.zip file to the folder you just created (for example, B:\CIDA).  
![B:\CIDA\](files/resources/image03.png)  
7.  Navigate to the [<b>SynPUF SAS datasets</b>](https://dev.sentinelsystem.org/projects/SYNPUF/repos/synpuf_sas_datasets/browse).
8.  Download all three zip files for each sample you choose to include by right clicking on each and clicking "Save link as..."  
![All three zip files](files/resources/image04.png)  
9.  Save each zip file to the same location as the package (for example, B:\CIDA).
10.  Unzip the downloaded files to the same folder (for example, B:\CIDA).  
![Extracting files](files/resources/image05.png)  
11.  Make sure the pathway is correct and click "Extract"  
![Select a destination and extract files](files/resources/image06.png)  
12.  Repeat the above steps for downloading additional subsamples. Steps 13-18 include instructions on executing against 3 subsamples of SynPUFs. You can execute on any number of subsamples of your choice, including 1 subsample only.
13.  Making sure all folders were successfully unzipped, you can now delete the zip files.  
![Delete zip](files/resources/image07.png)  
14.	 Make sure that your final folder contains the following folder/files:
*	 demo_mpl2r_wp001_nsdp_v01
* 	death_1.sas7bdat – death\_n.sas7bdat
* 	demographic\_1.sas7bdat – demographic\_n.sas7bdat
*	 diagnosis\_1.sas7bdat – diagnosis\_n.sas7bdat
* 	dispensing\_1.sas7bdat – dispensing\_n.sas7bdat
* 	encounter\_1.sas7bdat – encounter\_n.sas7bdat
* 	enrollment\_1.sas7bdat – enrollment\_n.sas7bdat
* 	procedure\_1.sas7bdat – procedure\_n.sas7bdat  
15.  Open folder demo_mpl2r\_wp001\_nsdp_v01 &#8594; open SAS file prepare\_scdm.sas.  
![Prepare.scdm.sas](files/resources/image08.png)  
16.	 In SAS file prepare_scdm.sas, edit the following parameters ONLY:
	%let inlib= point to the location of your downloaded datasets (for example, B:\CIDA).
	%let outlib= point to the location where the SCDM-formatted SynPUFs will be saved (for example, B:\CIDA).
	%let first_subsample= enter the minimum subsample number to include in the SCDM-formatted SynPUFs (for example, 1).
	%let last_subsample= enter the maximum subsample number to include in the SCDM-formatted SynPUFs (for example, 3).
Note: If you are running on 1 subsample only, %let last_subsample= 1;
	%let YN_Cleanup= to indicate whether to delete the raw subsamples from user library once the SCDM synpufs are created (for example, Y).
