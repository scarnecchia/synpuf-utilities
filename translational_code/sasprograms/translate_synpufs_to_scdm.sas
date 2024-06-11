dm 'out;clear;log;clear';
%************************************************************************************************;
%* PROGRAM NAME:    translate_synpufs_to_scdm.sas                                               *;
%* VERSION:         1.1.0                                                                       *;
%* CREATED:         03/20/2018                                                                  *;
%* LAST MODIFIED:   11/12/2020                                                                  *;
%*----------------------------------------------------------------------------------------------*;
%* PURPOSE                                                                                      *;
%*  This main program is used to run a set of macros to translate the Medicare Synthetic        *;
%*  Public Use Files (SynPUFs) into the appropriate Sentinel Common Data Model (SCDM) tables.   *;
%*----------------------------------------------------------------------------------------------*;
%* CONTACT INFO                                                                                 *;
%*  Sentinel System Coordinating Center                                                         *;
%*  info@sentinelsystem.org                                                                     *;
%*----------------------------------------------------------------------------------------------*;
%* CHANGE LOG:                                                                                  *;
%*                                                                                              *;
%*  Version        Date       Initials  Comment                                                 *;
%*  -------------  ---------- --------  --------------------------------------------------------*;
%*  1.0.0          04/02/2018 DC        - first release version                                 *;
%*  1.1.0          11/12/2020 DC        - modify to support scdm 8                              *;
%************************************************************************************************;


%* -----            ----------------------------------------------------------------------------*;
%* ----- USER INPUT ----------------------------------------------------------------------------*;
%* -----            ----------------------------------------------------------------------------*;

%* Directory containing the SynPUFs subsample SAS input files.                                  *;
%* Example: %let synpuf = //mycomputer/sentinel/synpuf_datasets/                                *;
%let synpuf =  ;

%* Directory that contains the final SCDM table SAS output files.                               *;
%* Example: %let scdm = //mycomputer/sentinel/scdm/                                             *;
%let scdm =  ;

%* Directory that contains the SAS lookup files.                                                *;
%* Example: %let infolder = //mycomputer/sentinel/synpuf/inputfiles/                            *;
%let infolder =  ;

%* Directory that contains the SAS macro program files.                                         *;
%* Example: %let sasmacr = //mycomputer/sentinel/synpuf/inputfiles/macros/                      *;
%let sasmacr =  ;

%* Number of first SynPUFs subsample to process. Valid values include 1 through 20.             *;
%* Example: %let first_subsample = 11                                                           *;
%let first_subsample =  ;

%* Number of last SynPUFS subsample to process. Valid values include 1 through 20. To process   *;
%* only one subsample, enter the same value as for first_subsample.                             *;
%* Example: %let last_subsample = 13                                                            *;
%let last_subsample =  ;

%* Enter Y to run the program in QC mode (i.e. temporary datasets will not be deleted).         *;
%* Enter N to run the program in production mode (i.e. temporary datasets will be automatically *;
%* deleted from the work library).                                                              *;
%* Example: YN_QCmode = N ;
%let YN_QCmode = N ;

%* ############################                                   ############################# *;
%* ############################ DO NOT ALTER CODE BELOW THIS LINE ############################# *;
%* ############################                                   ############################# *;


%* -----                    --------------------------------------------------------------------*;
%* ----- ASSIGN LIBRARIES   --------------------------------------------------------------------*;
%* -----                    --------------------------------------------------------------------*;

libname synpuf      "&synpuf" access=readonly;
libname scdm        "&scdm";
libname infolder    "&infolder";


%* -----                ------------------------------------------------------------------------*;
%* ----- ASSIGN MACROS  ------------------------------------------------------------------------*;
%* -----                ------------------------------------------------------------------------*;

%* Utility macros                                                                               *;
%include "&sasmacr./assign_idvar.sas";
%include "&sasmacr./assign_max_varlength.sas";
%include "&sasmacr./assign_sort_order.sas";
%include "&sasmacr./create_episodes_spans_2.sas";
%include "&sasmacr./codetype_algorithm.sas";
%include "&sasmacr./run_everything.sas";

%* Main macros                                                                                  *;
%include "&sasmacr./step_a_process_bene_file.sas";
%include "&sasmacr./step_b_process_pde_file.sas";
%include "&sasmacr./step_c_process_op_file.sas";
%include "&sasmacr./step_d_process_car_file.sas";
%include "&sasmacr./step_e_process_ip_file.sas";
%include "&sasmacr./step_f_final_processing.sas";


%* -----            ----------------------------------------------------------------------------*;
%* ----- RUN MACROS ----------------------------------------------------------------------------*;
%* -----            ----------------------------------------------------------------------------*;

%run_everything(startnum=&first_subsample., endnum=&last_subsample.);

%* -----                ------------------------------------------------------------------------*;
%* ----- END OF PROGRAM ------------------------------------------------------------------------*;
%* -----                ------------------------------------------------------------------------*;
