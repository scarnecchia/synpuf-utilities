%************************************************************************************************;
%* PROGRAM NAME:	step_d_process_car_file.sas  												*;
%* VERSION: 		1.0.0																		*;
%* CREATED: 		03/20/2018																	*;
%* LAST MODIFIED: 	04/02/2018																	*;
%*----------------------------------------------------------------------------------------------*;
%* PURPOSE:																						*;
%*  This macro program is used to process the Medicare Synthetic Public Use Files (SynPUFs) 	*;
%*	Carrier Claims file into the appropriate Sentinel Common Data Model (SCDM) tables.			*;
%*----------------------------------------------------------------------------------------------*;
%* MACRO PARAMETERS																				*;
%*	INDS_car_file:		Enter the two-level name of the SynPUFs Carrier Claims input file		*;
%*	LKDS_codes:			Enter the two-level name of the master clinical code lookup table		*;
%*	LKDS_home_codes:	Enter the two-level name of the home care code lookup table				*;
%*	LKDS_is_codes:		Enter the two-level name of the institutional stay code lookup table	*;
%*	LKDS_ed_codes:		Enter the two-level name of the emergency department code lookup table	*;
%*	OUTDS_car_temp_enc:	Enter the one/two-level name of the CAR Temp Encounter output table		*;
%*	OUTDS_car_temp_dia:	Enter the one/two-level name of the CAR Temp Diagnosis output table		*;
%*	OUTDS_car_temp_pro:	Enter the one/two-level name of the CAR Temp Procedure output table		*;
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


%macro PROCESS_CAR_FILE(  INDS_car_file			= 
						, LKDS_codes			= 
						, LKDS_home_codes		= 
						, LKDS_is_codes			= 
						, LKDS_ed_codes			= 
						, OUTDS_car_temp_enc	= 
						, OUTDS_car_temp_dia	= 
						, OUTDS_car_temp_pro	= 
						);

	%put ## PROGRAM NOTE: THE PROCESS_CAR_FILE MACRO HAS STARTED ##;

	%* PART 1: CODE TYPE ALGORITHM																*;

	%* Create CAR Temp Table 1 with one record for each unique combination of DESYNPUF_ID, 		*;
	%* CLM_ID, and diagnosis code or procedure code												*;
	data _car_temptable_1 (where=(		sourcecode not in ('0THER','XX000','00000','XXXXX','T0TAL')
									and anydigit(sourcecode) >0
									)
						   keep=desynpuf_id  
								clm_id  
								sourcecode  
								sourcecat  
								sourcetype  
								sourcevar  
								sourcevarnum  
								);
		set &INDS_car_file (keep=	desynpuf_id 
									clm_id 
									icd9_dgns_cd:
									line_icd9_dgns_cd:
									hcpcs_cd:
									);
		length sourcecode $18 sourcecat sourcetype $2 sourcevar $20 sourcevarnum 3;
		attrib _all_ label=" ";
		array dx{10} $ icd9_dgns_cd_1-icd9_dgns_cd_10;
		do dxnum=1 to 10;
			if anyalnum(dx{dxnum}) then do;
				sourcecode=translate(dx{dxnum},'0','O');
				sourcecat="DX";
				sourcetype="09";
				sourcevar="icd9_dgns_cd";
				sourcevarnum=dxnum;
				output;
			end;
		end;
		array ldx{13} $ line_icd9_dgns_cd_1-line_icd9_dgns_cd_13;
		do ldxnum=1 to 13;
			if anyalnum(ldx{ldxnum}) then do;
				sourcecode=translate(ldx{ldxnum},'0','O');
				sourcecat="DX";
				sourcetype="09";
				sourcevar="line_icd9_dgns_cd";
				sourcevarnum=ldxnum;
				output;
			end;
		end;
		array hc{45} $ hcpcs_cd_1-hcpcs_cd_45;
		do hcnum=1 to 45;
			if anyalnum(hc{hcnum}) then do;
				sourcecode=translate(hc{hcnum},'0','O');
				sourcecat="PX";
				if anyalpha(substr(sourcecode,1,1)) then sourcetype="HC";
				else if length(hc{hcnum})<5 then sourcetype="09";
				else if substr(hc{hcnum},5,1)="F" then sourcetype="C2";
				else if substr(hc{hcnum},5,1)="T" then sourcetype="C3";
				else sourcetype="C4";
				sourcevar="hcpcs_cd";
				sourcevarnum=hcnum;
				output;
			end;
		end;
	run;

	%* Run the algorithm																		*;
	%codetype_algorithm(fileabbrev=CAR);

	%* PART 2: SCDM ENCOUNTER																	*;

	%* Identify claims with ED, Home, or IS codes												*;
	proc sql noprint;
		create table _car_claimtypes as
		select    a.PatID
				, a.EncounterID
				, max(ed.ClinCodeCat ne " ") 	as ind_ed
				, max(home.ClinCodeCat ne " ") 	as ind_home
				, max(is.ClinCodeCat ne " ") 	as ind_is
		from  		_car_temppx 		as a
		left join	&LKDS_ed_codes 		as ed
			on 	a.PX 			= ed.ClinCode
			and a.PX_Codetype 	= ed.ClinCodeType
		left join	&LKDS_home_codes	as home
			on 	a.PX 			= home.ClinCode
			and a.PX_Codetype 	= home.ClinCodeType
		left join	&LKDS_is_codes		as is
			on 	a.PX 			= is.ClinCode
			and a.PX_Codetype 	= is.ClinCodeType
		group by  a.PatID
				, a.EncounterID
		;
	quit;

	%* Create the CAR Temp Enc table															*;
	proc sql noprint;
		create table &OUTDS_car_temp_enc as
		select distinct	  a.DESYNPUF_ID as PatID					length=16
						, a.CLM_ID		as EncounterID 				length=15
						, a.CLM_FROM_DT	as ADate 					length=4 	format=mmddyy10.
						, .				as DDate 					length=4 	format=mmddyy10.
						, coalescec(  a.PRF_PHYSN_NPI_1
									, a.TAX_NUM_1
									, "UNKNOWN"
									) 	as Provider					length=10
						, " " 			as Facility_Location 		length=3
						, case 	when sum(b.ind_home, b.ind_is) > 0 then "OA"
								else "AV"
								end		as EncType					length=2
						, " " 			as Facility_Code 			length=6
						, " " 			as Discharge_Disposition 	length=1
						, " " 			as Discharge_Status			length=2
						, " " 			as DRG						length=3
						, " " 			as DRG_Type					length=1
						, " " 			as Admitting_Source			length=2
		from 		&INDS_car_file 	as a
		left join	_car_claimtypes	as b
			on 	a.DESYNPUF_ID	= b.PatID
			and	a.CLM_ID		= b.EncounterID
			;
	quit;

	%* PART 3: SCDM DIAGNOSIS																	*;
	proc sql noprint;
		create table &OUTDS_car_temp_dia as
		select distinct	  a.PatID
						, a.EncounterID
						, b.ADate
						, b.Provider
						, b.EncType
						, a.DX
						, a.DX_Codetype
						, a.OrigDX
						, " " as PDX	length=1
						, "U" as PAdmit length=1
		from  _car_tempdx 			as a
			, &OUTDS_car_temp_enc 	as b
		where a.EncounterID = b.EncounterID
			;
	quit;

	%* PART 4: SCDM PROCEDURE																	*;
	proc sql noprint;
		create table &OUTDS_car_temp_pro as
		select distinct	  a.PatID
						, a.EncounterID
						, b.ADate
						, b.Provider
						, b.EncType
						, a.PX
						, a.PX_Codetype
						, a.OrigPX
		from  _car_temppx 			as a
			, &OUTDS_car_temp_enc 	as b
		where a.EncounterID = b.EncounterID
			;
	quit;

	%put ## PROGRAM NOTE: THE PROCESS_CAR_FILE MACRO HAS ENDED ##;

%mend PROCESS_CAR_FILE;

