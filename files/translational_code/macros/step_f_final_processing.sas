%************************************************************************************************;
%* PROGRAM NAME:	step_f_final_processing.sas  												*;
%* VERSION: 		1.0.0																		*;
%* CREATED: 		03/20/2018																	*;
%* LAST MODIFIED: 	04/02/2018																	*;
%*----------------------------------------------------------------------------------------------*;
%* PURPOSE:																						*;
%*  This macro program is used to complete the process of translating the Medicare Synthetic 	*;
%*	Public Use Files (SynPUFs) into the appropriate Sentinel Common Data Model (SCDM) tables.	*;
%*----------------------------------------------------------------------------------------------*;
%* MACRO PARAMETERS																				*;
%*	INDS_scdm_enr:		Enter the two-level name of the SCDM Enrollment input table				*;
%*	INDS_temp_dem:		Enter the two-level name of the Temp Demographic input table			*;
%*	INDS_temp_death:	Enter the two-level name of the Temp Death input table					*;
%*	INDS_temp_dis:		Enter the two-level name of the Temp Dispensing input table				*;
%*	INDS_op_temp_enc:	Enter the two-level name of the OP Temp Encounter input table			*;
%*	INDS_op_temp_dia:	Enter the two-level name of the OP Temp Diagnosis input table			*;
%*	INDS_op_temp_pro:	Enter the two-level name of the OP Temp Procedure input table			*;
%*	INDS_car_temp_enc:	Enter the two-level name of the CAR Temp Encounter input table			*;
%*	INDS_car_temp_dia:	Enter the two-level name of the CAR Temp Diagnosis input table			*;
%*	INDS_car_temp_pro:	Enter the two-level name of the CAR Temp Procedure input table			*;
%*	INDS_ip_temp_enc:	Enter the two-level name of the IP Temp Encounter input table			*;
%*	INDS_ip_temp_dia:	Enter the two-level name of the IP Temp Diagnosis input table			*;
%*	INDS_ip_temp_pro:	Enter the two-level name of the IP Temp Procedure input table			*;
%*	OUTDS_scdm_dem:		Enter the two-level name of the SCDM Temp Demographic output table		*;
%*	OUTDS_scdm_dis:		Enter the two-level name of the SCDM Temp Dispensing output table		*;
%*	OUTDS_scdm_enc:		Enter the two-level name of the SCDM Temp Encounter output table		*;
%*	OUTDS_scdm_dia:		Enter the two-level name of the SCDM Temp Diagnosis output table		*;
%*	OUTDS_scdm_pro:		Enter the two-level name of the SCDM Temp Procedure output table		*;
%*	OUTDS_scdm_death:	Enter the two-level name of the SCDM Temp Death output table			*;
%*----------------------------------------------------------------------------------------------*;
%* CONTACT INFO: 																				*;
%*  Sentinel System Coordinating Center															*;
%*  info@sentinelsystem.org																		*;
%*----------------------------------------------------------------------------------------------*;
%* CHANGE LOG: 																					*;
%*																								*;
%*  Version        Date       Initials	Comment 												*;
%*  -------------  ---------- --------	--------------------------------------------------------*;
%*  1.0.0		   04/02/2018 DC		- first release version									*;
%************************************************************************************************;


%macro FINAL_PROCESSING(	  INDS_scdm_enr		= 
							, INDS_temp_dem		= 
							, INDS_temp_death	= 
							, INDS_temp_dis		= 
							, INDS_op_temp_enc	= 
							, INDS_op_temp_dia	= 
							, INDS_op_temp_pro	= 
							, INDS_car_temp_enc	= 
							, INDS_car_temp_dia	= 
							, INDS_car_temp_pro	= 
							, INDS_ip_temp_enc	= 
							, INDS_ip_temp_dia	= 
							, INDS_ip_temp_pro	= 
							, OUTDS_scdm_dem	= 
							, OUTDS_scdm_dis	= 
							, OUTDS_scdm_enc	= 
							, OUTDS_scdm_dia	= 
							, OUTDS_scdm_pro	= 
							, OUTDS_scdm_death	= 
							);

	%put ## PROGRAM NOTE: THE FINAL_PROCESSING MACRO HAS STARTED ##;

	%* PRE-PROCESSING																			*;

	%* Create list of PatIDs having >0 enrollment records										*;
	proc sql noprint;
		create table _patid_list as
		select PatID, min(enr_start) as first_enr_dt
		from &INDS_scdm_enr
		group by PatID
		;
	quit;

	%* PART 1: SCDM DEMOGRAPHIC																	*;

	%* Exclude patients having 0 records in the SCDM Enrollment table							*;
	proc sql noprint;
		create table &OUTDS_scdm_dem as
		select distinct   a.*
						, case 	when a.zip ne " " then b.first_enr_dt 
								else . 
								end as ZIP_date 	length=4	format=mmddyy10.
		from  &INDS_temp_dem 	as a
			, _patid_list		as b
		where   a.PatID = b.PatID
		;
	quit;

	%* PART 2: SCDM DISPENSING																	*;

	%* Exclude patients having 0 records in the SCDM Enrollment table							*;
	proc sql noprint;
		create table &OUTDS_scdm_dis as
		select distinct a.*
		from  &INDS_temp_dis 	as a
			, _patid_list		as b
		where   a.PatID = b.PatID
		;
	quit;

	%* PART 3: SCDM ENCOUNTER																	*;

	%* Concatenate the OP, CAR, and IP Temp Encounter tables									*;
	%* Exclude records outside of 1/1/2008 - 12/31/2010											*;
	%* Exclude patients having 0 records in the SCDM Enrollment table							*;
	data _temp_scdm_enc;
		set &INDS_op_temp_enc
			&INDS_car_temp_enc
			&INDS_ip_temp_enc
			;
		where ADate between "01JAN2008"d and "31DEC2010"d;
	run;

	proc sql noprint;
		create table &OUTDS_scdm_enc as
		select distinct a.*
		from  _temp_scdm_enc 	as a
			, _patid_list		as b
		where   a.PatID = b.PatID
			;
	quit;

	proc datasets nolist lib=work;
		delete _temp_scdm_enc;
	quit;

	%* PART 4: SCDM DIAGNOSIS																	*;

	%* Concatenate the OP, CAR, and IP Temp Diagnosis tables									*;
	%* Exclude records outside of 1/1/2008 - 12/31/2010											*;
	%* Exclude patients having 0 records in the SCDM Enrollment table							*;
	data _temp_scdm_dia;
		set &INDS_op_temp_dia
			&INDS_car_temp_dia
			&INDS_ip_temp_dia
			;
		where ADate between "01JAN2008"d and "31DEC2010"d;
	run;

	proc sql noprint;
		create table &OUTDS_scdm_dia as
		select distinct a.*
		from  _temp_scdm_dia 	as a
			, _patid_list		as b
		where   a.PatID = b.PatID
			;
	quit;

	proc datasets nolist lib=work;
		delete _temp_scdm_dia;
	quit;

	%* PART 5: SCDM PROCEDURE																	*;

	%* Concatenate the OP, CAR, and IP Temp Procedure tables									*;
	%* Exclude records outside of 1/1/2008 - 12/31/2010											*;
	%* Exclude patients having 0 records in the SCDM Enrollment table							*;
	data _temp_scdm_pro;
		set &INDS_op_temp_pro
			&INDS_car_temp_pro
			&INDS_ip_temp_pro
			;
		where ADate between "01JAN2008"d and "31DEC2010"d;
	run;

	proc sql noprint;
		create table &OUTDS_scdm_pro as
		select distinct a.*
		from  _temp_scdm_pro 	as a
			, _patid_list		as b
		where   a.PatID = b.PatID
			;
	quit;

	proc datasets nolist lib=work;
		delete _temp_scdm_pro;
	quit;

	%* PART 6: SCDM DEATH																		*;

	%* Exclude patients having 0 records in the SCDM Enrollment table							*;
	proc sql noprint;
		create table &OUTDS_scdm_death as
		select distinct a.*
		from  &INDS_temp_death 	as a
			, _patid_list		as b
		where   a.PatID = b.PatID
		;
	quit;

	%* PART 7: FINISH UP																		*;

	%* Remove any remaining variable labels	from all final tables, including SCDM Enrollment	*;

	%let ds_list= 	&INDS_scdm_enr	&OUTDS_scdm_dem	&OUTDS_scdm_dis	&OUTDS_scdm_enc	
					&OUTDS_scdm_dia	&OUTDS_scdm_pro	&OUTDS_scdm_death	
					;
	%do j=1 %to %sysfunc(countw(&ds_list,,s));
		%let ds=%substr(%scan(&ds_list,&j,,s),6);

			proc datasets nolist lib=scdm;
				modify &ds;
				attrib _all_ label=" ";
			quit;
	%end;

	%put ## PROGRAM NOTE: THE FINAL_PROCESSING MACRO HAS ENDED ##;

%mend FINAL_PROCESSING;

