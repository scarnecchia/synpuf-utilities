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
 
14.	 Make sure that your final folder contains the following folder/files: <ul><li>demo_mpl2r_wp001_nsdp_v01</li><li></li><li> death_1.sas7bdat – death\_n.sas7bdat</li><li>demographic\_1.sas7bdat – demographic\_n.sas7bdat</li><li>diagnosis\_1.sas7bdat – diagnosis\_n.sas7bdat</li><li>dispensing\_1.sas7bdat – dispensing\_n.sas7bdat</li><li>encounter\_1.sas7bdat – encounter\_n.sas7bdat</li><li>enrollment\_1.sas7bdat – enrollment\_n.sas7bdat</li><li>procedure\_1.sas7bdat – procedure\_n.sas7bdat</li></ul>  

15.  Open folder demo_mpl2r\_wp001\_nsdp_v01 &#8594; open SAS file prepare\_scdm.sas.  
 
![Prepare.scdm.sas](files/resources/image08.png)  

16.	 In SAS file prepare_scdm.sas, edit the following parameters ONLY:<ul><li>%let inlib= point to the location of your downloaded datasets (for example, B:\CIDA).</li><li>%let outlib= point to the location where the SCDM-formatted SynPUFs will be saved (for example, B:\CIDA).</li><li>%let first_subsample= enter the minimum subsample number to include in the SCDM-formatted SynPUFs (for example, 1).</li><li>%let last_subsample= enter the maximum subsample number to include in the SCDM-formatted SynPUFs (for example, 3).<br>Note: If you are running on 1 subsample only, %let last_subsample= 1;</li><li>%let YN_Cleanup= to indicate whether to delete the raw subsamples from user library once the SCDM synpufs are created (for example, Y).</li></ul>  

![SAS example](files/resources/image09.png)  

17.	 Save and run the program.

![Save](files/resources/image10.png) 
  
![Run](files/resources/image11.png)
  
18.  Navigate to the log window and search for errors.  

![Find](files/resources/image12.png) 

![Find next](files/resources/image13.png)  

If you find any error, please contact the SOC at info@sentinelsystem.org.  

19.	Go back to folder demo\_mpl2r\_wp001\_nsdp_v01,<ul><li>open folder sasprograms</li><li> open SAS file demo\_mpl2r\_wp001\_nsdp\_v01.sas</li></ul>  

![demo_mpl2r_wp001_nsdp_v01.sas](files/resources/image14.png) 
 
20.  In SAS file demo_mpl2r_wp001_nsdp_v01.sas, edit the following parameters ONLY:<ul><li>%let \_mscdm= point to the location of your downloaded datasets<br>(for example, B:\CIDA).</li><li>%let \_root\_dplocal= point to the location of your downloaded datasets<br>(for example, B:\CIDA).</li><li>%let \_root\_msoc= point to the location of your downloaded datasets<br>(for example, B:\CIDA).</li><li> %let \_root\_inputfiles= point to the location of your downloaded datasets<br>(for example, B:\CIDA).</li><li>	%let \_root\_sasprograms= point to the location of your downloaded datasets<br>(for example, B:\CIDA).</li></ul>  

![Editing parameters](files/resources/image15.png)  

21.  Save and close SAS.  

![Save and close](files/resources/image16.png)  

22.	 Go back to folder demo\_mpl2r\_wp001\_nsdp_v01,<ul><li>open folder sasprograms</li><li> Right click on demo\_mpl2r\_wp001\_nsdp\_v01.sas <br>Select “Batch Submit with SAS 9.4”</li></ul>  

![Batch submit](files/resources/image17.png)  

23.	 The following dialog will appear. When the program finishes running, this dialog would vanish. (Hint: If your program ran for only seconds and this dialog closes, there is a high probability that there was an error.)  

![Dialog box that appears when program is running](files/resources/image18.png)  

24.	 After the batch submit dialog closes, text (.log) files will appear in folders sasprogram and msoc.  

![Files appearing in sasprograms folder](files/resources/image19.png)  

![Files appearing in msoc folder](files/resources/image20.png)
  
25.  Open file demo\_mpl2r\_wp001\_nsdp\_v01.log and search for error in this page.<br>Use Edit &#8594; Find &#8594; type error &#8594; click Find Next<br>Or use Ctrl + F &#8594; type error &#8594; click Find Next  

![Edit - Find](files/resources/image21.png)  

![\Find what: error, find next](files/resources/image22.png)  

The above “errors” are part of the program and are normal.   
 
26.	 If you find any error, please contact the SOC at info@sentinelsystem.org.  

27.  Open folder demo\_mpl2r\_wp001\_nsdp\_v01, the results are contained in the folder msoc  

28.	 Please keep in mind that interpreting results generated by SynPUFs may not be used to make any meaningful scientific conclusion. For details, please refer to the [Disclaimer for using SynPUFs](https://dev.sentinelsystem.org/projects/SYNPUF/repos/synpuf_user_documentation/browse).

