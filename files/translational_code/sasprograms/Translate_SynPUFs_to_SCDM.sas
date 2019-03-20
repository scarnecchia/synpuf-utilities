dm 'out;clear;log;clear';
%************************************************************************************************;
%* PROGRAM NAME:	Translate_SynPUFs_to_SCDM.sas  												*;
%* VERSION: 		1.0.0																		*;
%* CREATED: 		03/20/2018																	*;
%* LAST MODIFIED: 	04/02/2018																	*;
%*----------------------------------------------------------------------------------------------*;
%* PURPOSE																						*;
%*  This master program is used to run a set of macros to translate the Medicare Synthetic 		*;
%*	Public Use Files (SynPUFs) into the appropriate Sentinel Common Data Model (SCDM) tables.	*;
%*----------------------------------------------------------------------------------------------*;
%* CONTACT INFO 																				*;
%*  Sentinel System Coordinating Center															*;
%*  info@sentinelsystem.org																		*;
%*----------------------------------------------------------------------------------------------*;
%* CHANGE LOG: 																					*;
%*																								*;
%*  Version        Date       Initials	Comment 												*;
%*  -------------  ---------- --------	--------------------------------------------------------*;
%*  1.0.0		   04/02/2018 DC		- first release version									*;
%************************************************************************************************;


%* -----        	----------------------------------------------------------------------------*;
%* ----- USER INPUT	----------------------------------------------------------------------------*;
%* -----        	----------------------------------------------------------------------------*;

%* Directory containing the SynPUFs subsample SAS input files									*;
%let synpuf		=  ;

%* Directory that contains the final SCDM table SAS output files								*;
%let scdm		=  ;

%* Directory that contains the SAS lookup files													*;
%let infolder	=  ;

%* Directory that contains the SAS macro program files											*;
%let sasmacr	=  ;


%* -----        	        --------------------------------------------------------------------*;
%* ----- ASSIGN LIBRARIES	--------------------------------------------------------------------*;
%* -----        	        --------------------------------------------------------------------*;

libname synpuf 		"&synpuf" access=readonly;
libname scdm 		"&scdm";
libname infolder 	"&infolder";


%* -----        	    ------------------------------------------------------------------------*;
%* ----- ASSIGN MACROS	------------------------------------------------------------------------*;
%* -----        	    ------------------------------------------------------------------------*;

%* Utility macros																				*;
%include "&sasmacr.create_episodes_spans_2.sas";
%include "&sasmacr.codetype_algorithm.sas";
%include "&sasmacr.run_everything.sas";

%* Main macros																					*;
%include "&sasmacr.step_a_process_bene_file.sas";
%include "&sasmacr.step_b_process_pde_file.sas";
%include "&sasmacr.step_c_process_op_file.sas";
%include "&sasmacr.step_d_process_car_file.sas";
%include "&sasmacr.step_e_process_ip_file.sas";
%include "&sasmacr.step_f_final_processing.sas";


%* -----        	----------------------------------------------------------------------------*;
%* ----- RUN MACROS	----------------------------------------------------------------------------*;
%* -----        	----------------------------------------------------------------------------*;

%run_everything(startnum=1, endnum=20, YN_DeleteTempDS=Y);

%* -----        	    ------------------------------------------------------------------------*;
%* ----- END OF PROGRAM	------------------------------------------------------------------------*;
%* -----        	    ------------------------------------------------------------------------*;
