%************************************************************************************************;
%* PROGRAM NAME:	run_everything.sas  														*;
%* VERSION: 		1.0.0																		*;
%* CREATED: 		03/20/2018																	*;
%* LAST MODIFIED: 	04/02/2018																	*;
%*----------------------------------------------------------------------------------------------*;
%* PURPOSE																						*;
%*  This macro program is used to run all of the macros to translate one or more subsamples of 	*;
%*	the Medicare Synthetic Public Use Files (SynPUFs) into the appropriate Sentinel Common Data *;
%*	Model (SCDM) tables.																		*;
%*----------------------------------------------------------------------------------------------*;
%* MACRO PARAMETERS																				*;
%*	startnum:			Enter a number 1 through 20 for the first SynPUFs subsample to process	*;
%*	endnum:				Enter a number 1 through 20 for the last SynPUFs subsample to process	*;
%*	YN_DeleteTempDS:	Enter Y to delete temporary datasets after processing is complete		*;
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


%macro run_everything( 	  startnum			= 
						, endnum			= 
						, YN_DeleteTempDS	= 
						);

	%put ## PROGRAM NOTE: THE RUN_EVERYTHING MACRO HAS STARTED ## ;

	%do s=&startnum %to &endnum;

		%* Direct SAS log to output file and save permanent copy to scdm library				*;
		filename runlog "&scdm.translate_synpufs_subsample_&s..log";
		proc printto log=runlog new;
		run;

		%put ##### TRANSLATING SUBSAMPLE &s ##### ;

		%* Step A: Processing the Beneficiary file												*;
		%PROCESS_BENE_FILE(	  INDS_bene_file_2008	= synpuf.bene_2008_&s 
							, INDS_bene_file_2009	= synpuf.bene_2009_&s 
							, INDS_bene_file_2010	= synpuf.bene_2010_&s 
							, LKDS_zip				= infolder.translate_cms_to_zip 	
							, OUTDS_temp_dem		= _temp_dem_&s 
							, OUTDS_temp_death		= _temp_death_&s 	
							, OUTDS_scdm_enr		= scdm.enrollment_&s 
							);

		%* Step B: Processing the Prescription Drug Events file									*;
		%PROCESS_PDE_FILE(    INDS_pde_file		= synpuf.pde_&s
							, OUTDS_temp_dis	= _temp_dis_&s
							);

		%* Step C: Processing the Outpatient Claims file										*;
		%PROCESS_OP_FILE(	  INDS_op_file		= synpuf.op_&s 
							, LKDS_codes		= infolder.clinical_codes 
							, LKDS_home_codes	= infolder.home_codes 
							, LKDS_is_codes		= infolder.is_codes 
							, LKDS_ed_codes		= infolder.ed_codes 
							, OUTDS_op_temp_enc	= _op_temp_enc_&s 	
							, OUTDS_op_temp_dia	= _op_temp_dia_&s 	
							, OUTDS_op_temp_pro	= _op_temp_pro_&s 	
							);

		%* Step D: Processing the Carrier Claims file											*;
		%PROCESS_CAR_FILE(	  INDS_car_file			= synpuf.car_&s 
							, LKDS_codes			= infolder.clinical_codes 
							, LKDS_home_codes		= infolder.home_codes 
							, LKDS_is_codes			= infolder.is_codes 
							, LKDS_ed_codes			= infolder.ed_codes 
							, OUTDS_car_temp_enc	= _car_temp_enc_&s 	
							, OUTDS_car_temp_dia	= _car_temp_dia_&s 	
							, OUTDS_car_temp_pro	= _car_temp_pro_&s 	
							);

		%* Step E: Processing the Inpatient Claims file											*;
		%PROCESS_IP_FILE(	  INDS_ip_file		= synpuf.ip_&s 
							, LKDS_codes		= infolder.clinical_codes 
							, INDS_op_temp_enc	= _op_temp_enc_&s 
							, INDS_car_temp_enc	= _car_temp_enc_&s 	
							, OUTDS_ip_temp_enc	= _ip_temp_enc_&s 	
							, OUTDS_ip_temp_dia	= _ip_temp_dia_&s 	
							, OUTDS_ip_temp_pro	= _ip_temp_pro_&s 	
							);

		%* Step F: Final Processing 															*;
		%FINAL_PROCESSING(	  INDS_scdm_enr		= scdm.enrollment_&s 
							, INDS_temp_dem		= _temp_dem_&s 
							, INDS_temp_death	= _temp_death_&s 
							, INDS_temp_dis		= _temp_dis_&s 
							, INDS_op_temp_enc	= _op_temp_enc_&s 
							, INDS_op_temp_dia	= _op_temp_dia_&s 
							, INDS_op_temp_pro	= _op_temp_pro_&s 
							, INDS_car_temp_enc	= _car_temp_enc_&s 
							, INDS_car_temp_dia	= _car_temp_dia_&s 
							, INDS_car_temp_pro	= _car_temp_pro_&s 
							, INDS_ip_temp_enc	= _ip_temp_enc_&s 
							, INDS_ip_temp_dia	= _ip_temp_dia_&s 
							, INDS_ip_temp_pro	= _ip_temp_pro_&s 
							, OUTDS_scdm_dem	= scdm.demographic_&s 
							, OUTDS_scdm_dis	= scdm.dispensing_&s 
							, OUTDS_scdm_enc	= scdm.encounter_&s 
							, OUTDS_scdm_dia	= scdm.diagnosis_&s 
							, OUTDS_scdm_pro	= scdm.procedure_&s 
							, OUTDS_scdm_death	= scdm.death_&s 
							);

		%* If requested, delete temporary datasets from the workspace							*;
		%if %upcase(&YN_DeleteTempDS)=Y %then %do;
			proc datasets nolist lib=work memtype=data;
				delete _:;
			quit;
		%end;

		%* Output SAS log																		*;
		proc printto log=log; 
		run;

	%end;

	%put ## PROGRAM NOTE: THE RUN_EVERYTHING MACRO HAS ENDED ## ;

%mend run_everything;
