%************************************************************************************************;
%* PROGRAM NAME:	step_a_process_bene_file.sas  												*;
%* VERSION: 		1.0.0																		*;
%* CREATED: 		03/20/2018																	*;
%* LAST MODIFIED: 	04/02/2018																	*;
%*----------------------------------------------------------------------------------------------*;
%* PURPOSE:																						*;
%*  This macro program is used to process the Medicare Synthetic Public Use Files (SynPUFs) 	*;
%*	Beneficiary file into the appropriate Sentinel Common Data Model (SCDM) tables.				*;
%*----------------------------------------------------------------------------------------------*;
%* MACRO PARAMETERS																				*;
%*	INDS_bene_file_2008:	Enter the two-level name of the SynPUFs 2008 Beneficiary input file	*;
%*	INDS_bene_file_2009:	Enter the two-level name of the SynPUFs 2009 Beneficiary input file	*;
%*	INDS_bene_file_2010:	Enter the two-level name of the SynPUFs 2010 Beneficiary input file	*;
%*	LKDS_zip:				Enter the two-level name of the ZIP-code conversion lookup table	*;
%*	OUTDS_temp_dem:			Enter the two-level name of the Temp Demographic output table		*;
%*	OUTDS_temp_death:		Enter the two-level name of the Temp Death output table				*;
%*	OUTDS_scdm_enr:			Enter the two-level name of the SCDM Enrollment output table		*;
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


%macro PROCESS_BENE_FILE(	  INDS_bene_file_2008	= 
							, INDS_bene_file_2009	= 
							, INDS_bene_file_2010	= 
							, LKDS_zip				= 
							, OUTDS_temp_dem		= 
							, OUTDS_temp_death		= 
							, OUTDS_scdm_enr		= 
							);

	%put ## PROGRAM NOTE: THE PROCESS_BENE_FILE MACRO HAS STARTED ##;

	%* PART 0: PRE-PROCESSING																	*;

	%* Combine 2008, 2009, and 2010 Beneficiary files											*;

	%let benevars =	DESYNPUF_ID BENE_BIRTH_DT BENE_SEX_IDENT_CD BENE_RACE_CD 
					SP_STATE_CODE BENE_COUNTY_CD BENE_DEATH_DT 
					BENE_HI_CVRAGE_TOT_MONS BENE_SMI_CVRAGE_TOT_MONS 
					BENE_HMO_CVRAGE_TOT_MONS PLAN_CVRG_MOS_NUM
					;

	data _temp_bene;
		length YEAR 4;
		set &INDS_bene_file_2008 (	in=		y2008 
									keep=	&benevars
									)
			&INDS_bene_file_2009 (	in=		y2009 
									keep=	&benevars
									)
			&INDS_bene_file_2010 (	in=		y2010 
									keep=	&benevars
									)
			;
		attrib _all_ label=" ";
		if 		y2008 then YEAR = 2008;
		else if y2009 then YEAR = 2009;
		else if y2010 then YEAR = 2010;
	run;


	%* PART 1: SCDM DEMOGRAPHIC																	*;

	proc sql noprint;
		create table &OUTDS_temp_dem as
		select distinct	  a.DESYNPUF_ID		as PatID				length=16
						, a.BENE_BIRTH_DT	as Birth_date			length=4 	format=mmddyy10.
						, case 	when a.BENE_SEX_IDENT_CD = "1" 	then "M"
								when a.BENE_SEX_IDENT_CD = "2" 	then "F"
																else "U"
						  end as Sex 								length=1
						, case	when a.BENE_RACE_CD = "5"	then "Y"
								when a.BENE_RACE_CD = "1"	then "N"
															else "U"
						  end as hispanic							length=1
						, case	when a.BENE_RACE_CD = "1"	then "5"
								when a.BENE_RACE_CD = "2"	then "3"
															else "0"
						  end as race								length=1
						, b.ZIP										length=5
		from 		_temp_bene	as a
		left join	&LKDS_zip	as b
		on	cats(a.SP_STATE_CODE, a.BENE_COUNTY_CD) = b.ssacd
		;
	quit;

	%* Check for duplicate records by PatID														*;
	proc sql noprint;
		create table _dem_dupes as
		select *, count(*) as counter
		from &OUTDS_temp_dem
		group by PatID
		having count(*) > 1
		;
	quit;

	proc contents noprint data=_dem_dupes out=_dem_dupes_checker (keep=nobs);
	quit;

	proc sql noprint;
		select distinct nobs into: dem_dupes_checker from _dem_dupes_checker;
	quit;

	%if &dem_dupes_checker > 0 %then %do;
		%put ERROR 1 (STEP_A): At least one PatID exists on multiple Demographic records ;
	%end;

	%* PART 2: SCDM DEATH 																		*;

	proc sql noprint;
		create table &OUTDS_temp_death as
		select distinct   DESYNPUF_ID	as PatID		length=16
						, BENE_DEATH_DT	as DeathDt		length=4	format=mmddyy10.
						, "D" 			as DtImpute		length=1
						, "L"			as Source		length=1
						, "E"			as Confidence	length=1
		from _temp_bene (where=(BENE_DEATH_DT ne .))
		;
	quit;

	%* Check for duplicate records by PatID														*;
	proc sql noprint;
		create table _death_dupes as
		select *, count(*) as counter
		from &OUTDS_temp_death
		group by PatID
		having count(*) > 1
		;
	quit;

	proc contents noprint data=_death_dupes out=_death_dupes_checker (keep=nobs);
	quit;

	proc sql noprint;
		select distinct nobs into: death_dupes_checker from _death_dupes_checker;
	quit;

	%if &death_dupes_checker > 0 %then %do;
		%put ERROR 2 (STEP_A): At least one PatID exists on multiple Death records ;
	%end;


	%* PART 3: COVERAGE TYPE ALGORITHM															*;

		%* Summarize coverage by beneficiary													*;
		%* Keep A, B, AB, and HMO for descriptive statistics									*;
		proc sql noprint;
			create table _beneficiary_coverage_summary as
			select	  DESYNPUF_ID
					, min( BENE_DEATH_DT )	format=mmddyy10. 					as _BENE_DEATH_DT
					, sum( (YEAR=2008) * BENE_HI_CVRAGE_TOT_MONS ) 				as _a_2008
					, sum( (YEAR=2009) * BENE_HI_CVRAGE_TOT_MONS ) 				as _a_2009
					, sum( (YEAR=2010) * BENE_HI_CVRAGE_TOT_MONS ) 				as _a_2010
					, sum( (YEAR=2008) * BENE_SMI_CVRAGE_TOT_MONS ) 			as _b_2008
					, sum( (YEAR=2009) * BENE_SMI_CVRAGE_TOT_MONS ) 			as _b_2009
					, sum( (YEAR=2010) * BENE_SMI_CVRAGE_TOT_MONS ) 			as _b_2010
					, min( calculated _a_2008, calculated _b_2008 ) 			as _ab_2008
					, min( calculated _a_2009, calculated _b_2009 ) 			as _ab_2009
					, min( calculated _a_2010, calculated _b_2010 ) 			as _ab_2010
					, sum( (YEAR=2008) * BENE_HMO_CVRAGE_TOT_MONS ) 			as _hmo_2008
					, sum( (YEAR=2009) * BENE_HMO_CVRAGE_TOT_MONS ) 			as _hmo_2009
					, sum( (YEAR=2010) * BENE_HMO_CVRAGE_TOT_MONS ) 			as _hmo_2010
					, min( calculated _ab_2008, (12 - calculated _hmo_2008) ) 	as _medcov_2008
					, min( calculated _ab_2009, (12 - calculated _hmo_2009) ) 	as _medcov_2009
					, min( calculated _ab_2010, (12 - calculated _hmo_2010) ) 	as _medcov_2010
					, sum( (YEAR=2008) * input(PLAN_CVRG_MOS_NUM,best.) ) 		as _drugcov_2008
					, sum( (YEAR=2009) * input(PLAN_CVRG_MOS_NUM,best.) ) 		as _drugcov_2009
					, sum( (YEAR=2010) * input(PLAN_CVRG_MOS_NUM,best.) ) 		as _drugcov_2010
			from _temp_bene
			group by DESYNPUF_ID
			;
		quit;

		%* Assign medical and drug coverage dates separately									*;
		%do i=1 %to 2;
			%if &i=1 %then %do;
				%let coverage = MedCov;
				%let not_coverage = DrugCov;
			%end;
			%else %do;
				%let coverage = DrugCov;
				%let not_coverage = MedCov;
			%end;

			data _&coverage.;
				set _beneficiary_coverage_summary (drop=_&not_coverage.: _a_: _b_: _ab: _hmo:);
				length Enr_Start Enr_End 4 &coverage. $1;
				format Enr_start Enr_End mmddyy10.;
				retain &coverage. "Y";
				%* Output records for 2008 and 2009												*;
				%* Note: Comparison between 2008 and 2010 is intentional						*;
				if _&coverage._2008 >= _&coverage._2010 then do;
					%* One record for 2008														*;
					if 		year(_BENE_DEATH_DT) = 2008
						or	sum(_&coverage._2009, _&coverage._2010) = 0
					then do;
						Enr_Start = "01JAN2008"d;
						Enr_End = min(  _BENE_DEATH_DT
									  , intnx('month', Enr_Start - 1, 0 + _&coverage._2008, 'end')
									  );
						if Enr_Start < Enr_End then output;
					end;
					else do;
						%* One record for 2008													*;
						Enr_End = "31DEC2008"d;
						Enr_Start = intnx('month', Enr_End + 1, 0 - _&coverage._2008, 'beginning');
						if Enr_Start < Enr_End then output;
						%* One record for 2009													*;
						Enr_Start = "01JAN2009"d;
						Enr_End = min(  _BENE_DEATH_DT
									  , intnx('month', Enr_Start - 1, 0 + _&coverage._2009, 'end')
									  );
						if Enr_Start < Enr_End then output;
					end;
				end;
				else if _&coverage._2008 < _&coverage._2010 then do;
					%* One record for 2009														*;
					Enr_End = "31DEC2009"d;
					Enr_Start = intnx('month', Enr_End + 1, 0 - _&coverage._2009, 'beginning');
					if Enr_Start < Enr_End then output;
					%* One record for 2008														*;
					if _&coverage._2009 < 12 then do;
						Enr_Start = "01JAN2008"d;
						Enr_End = intnx('month', Enr_Start - 1, 0 + _&coverage._2008, 'end');
						if Enr_Start < Enr_End then output;
					end;
					else do;
						Enr_End = "31DEC2008"d;
						Enr_Start = intnx('month', Enr_End + 1, 0 - _&coverage._2008, 'beginning');
						if Enr_Start < Enr_End then output;
					end;	
				end;
				%* Output records for 2010 (always start at 01JAN2010)							*;
				if _&coverage._2010 > 0 then do;
					Enr_Start = "01JAN2010"d;
					Enr_End = min(  _BENE_DEATH_DT
								  , intnx('month', Enr_Start - 1, _&coverage._2010, 'end')
								  );
					if Enr_Start < Enr_End then output;
				end;
			run;
			
			%* Create continuous spans for each coverage type									*; 
			%* This macro written by Yury Vilk, Harvard Pilgrim Health Care Institute			*;
			%create_episodes_spans_2(	  tbl_name_in			= _&coverage.
										, tbl_name_out			= _&coverage._spans
										, grace_period			= 0 
										, by_vars				= DESYNPUF_ID
										, eff_dt_var_nm			= Enr_Start
										, exp_dt_var_nm			= Enr_End
										, grace_period_var_nm	= 
										);
		%end;

		%* Combine medical and drug coverage where possible										*;

		%* The following is based on SAS paper 260-29 by Mike Rhoads, Westat, Rockville, MD		*;
		%* Accessed on 03/01/2018 at http://www2.sas.com/proceedings/sugi29/260-29.pdf			*;
		data _AllCov;
			set _MedCov_spans 	(keep=DESYNPUF_ID enr_start_span enr_end_span medcov)
				_DrugCov_spans	(keep=DESYNPUF_ID enr_start_span enr_end_span drugcov)
				;
		run;

		proc sort data=_AllCov;
			by DESYNPUF_ID enr_start_span;
		quit;

		%let FirstDateOfInterest	= "01JAN2008"d;
		%let LastDateOfInterest		= "31DEC2010"d;

		data _StatusChangeDates (keep=DESYNPUF_ID StatusChangeDate);
			set _AllCov;
			by DESYNPUF_ID;
			length StatusChangeDate 4;
			format StatusChangeDate mmddyy10.;
			if first.DESYNPUF_ID then do;
				StatusChangeDate = enr_start_span;
				output;
			end;
			StatusChangeDate = enr_start_span;
			output;
			if enr_end_span ne &LastDateOfInterest then do;
				StatusChangeDate = enr_end_span + 1;
				output;
			end;
		run;

		proc sort nodupkey data=_StatusChangeDates out=_SortedStatusChangeDates;
			by DESYNPUF_ID StatusChangeDate;
		quit;

		options mergenoby = nowarn;

		data _TimelineDates (keep=DESYNPUF_ID PeriodStart PeriodEnd);
			merge 	_SortedStatusChangeDates (	rename=(StatusChangeDate=PeriodStart)	)
					_SortedStatusChangeDates (	firstobs=2
												rename=(DESYNPUF_ID=NEXT_DESYNPUF_ID
														StatusChangeDate=NextStartDate	)
												);
			length PeriodStart PeriodEnd 4;
			format PeriodStart PeriodEnd mmddyy10.;
			if DESYNPUF_ID = NEXT_DESYNPUF_ID 	then PeriodEnd = NextStartDate - 1;
												else PeriodEnd = &LastDateOfInterest;
		run;

		options mergenoby = error;

	%* PART 4: SCDM ENROLLMENT																	*;

	proc sql noprint;
		create table &OUTDS_scdm_enr as
		select	  a.DESYNPUF_ID						as PatID		length=16
				, a.PeriodStart						as Enr_Start	length=4	format=mmddyy10.
				, a.PeriodEnd						as Enr_End		length=4	format=mmddyy10.
				, coalescec(max(b.MedCov),'N')		as MedCov		length=1
				, coalescec(max(b.DrugCov),'N')		as DrugCov		length=1
				, "N"								as Chart		length=1
		from 		_TimelineDates 	as a
		left join	_AllCov			as b
			on  a.DESYNPUF_ID = b.DESYNPUF_ID
		where	a.PeriodEnd >= b.enr_start_span
			and b.enr_end_span >= a.PeriodStart
		group by  a.DESYNPUF_ID
				, a.PeriodStart
				, a.PeriodEnd
		order by  a.DESYNPUF_ID
				, a.PeriodStart
				, a.PeriodEnd
				;
	quit;

	%put ## PROGRAM NOTE: THE PROCESS_BENE_FILE MACRO HAS ENDED ##;

%mend PROCESS_BENE_FILE;
