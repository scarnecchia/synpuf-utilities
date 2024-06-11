%************************************************************************************************;
%* PROGRAM NAME:	codetype_algorithm.sas  													*;
%* VERSION: 		1.0.0																		*;
%* CREATED: 		03/20/2018																	*;
%* LAST MODIFIED: 	04/02/2018																	*;
%*----------------------------------------------------------------------------------------------*;
%* PURPOSE																						*;
%*  This macro program is used to determine diagnosis and procedure code types from the			*;
%*	Medicare Synthetic Public Use Files (SynPUFs) for translation to the appropriate Sentinel 	*;
%*  Common Data Model (SCDM) tables.															*;
%*----------------------------------------------------------------------------------------------*;
%* MACRO PARAMETERS																				*;
%*	fileabbrev:		Enter one of the following abbreviations to process one file at a time		*;
%*						OP	= Outpatient Claims													*;
%*						CAR	= Carrier Claims													*;
%*						IP	= Inpatient Claims													*;
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


%macro codetype_algorithm(fileabbrev = );

	%put ## PROGRAM NOTE: THE CODETYPE_ALGORITHM MACRO HAS STARTED ## ;

	* Link by code only to master clinical code lookup											*;
	proc sql noprint;
		create table _&fileabbrev._temptable_2 as
		select distinct   "&fileabbrev" as sourcefile
						, a.sourcecode
						, a.sourcecat
						, a.sourcetype
						, b.clincodecat
						, b.clincodetype
						, count(distinct a.clm_id) as clm_count
		from _&fileabbrev._temptable_1 	as a
		left join &LKDS_codes			as b
			on a.sourcecode = b.clincode
		group by  a.sourcecode
				, a.sourcecat
				, a.sourcetype
			  ;
	quit;

	%* Determine match type																		*;
	proc sql;
		create table _&fileabbrev._codematch (drop=real_code_count source_code_count) as
		select    *	
			  	, count(distinct cats(clincodecat, clincodetype)) as real_code_count
				, count(distinct cats(sourcecat, sourcetype)) as source_code_count
				, case 	when calculated source_code_count=1 and calculated real_code_count=1
							then "1:1"
						when calculated source_code_count>1 and calculated real_code_count=1
							then "MANY:1"
						when calculated source_code_count=1 and calculated real_code_count>1
							then "1:MANY"
						when calculated source_code_count>1 and calculated real_code_count>1
							then "MANY:MANY"
						when calculated source_code_count=1 and calculated real_code_count=0
							then "1:0"
						when calculated source_code_count>1 and calculated real_code_count=0
							then "MANY:0"
						else "XXX"
					end as match_type 
		from _&fileabbrev._temptable_2
		group by sourcecode
		;
	quit;

	%* Assign code category and type															*;
	data _&fileabbrev._dxcodes _&fileabbrev._pxcodes _&fileabbrev._leftovers;
		set _&fileabbrev._codematch;
		length cat type $2 step 8;
		%* Step 1: 	1:0 match																	*;
		%*			Assign source cat and type=OTHER											*;
		if match_type="1:0" then do;
			cat=SourceCat;
			type='OT';
			step=1;
			if cat="DX" then output _&fileabbrev._dxcodes;
			if cat="PX" then output _&fileabbrev._pxcodes;
		end;
		%* Step 2: 	MANY:0 match																*;
		%*			Assign source cat and type=OTHER											*;
		else if match_type="MANY:0" then do;
			cat=SourceCat;
			type='OT';
			step=2;
			if cat="DX" then output _&fileabbrev._dxcodes;
			if cat="PX" then output _&fileabbrev._pxcodes;
		end;
		%* Step 3: 	1:1 match																	*;
		%* 			Assign cat and type from lookup regardless of source values					*;
		else if match_type="1:1" then do;
			cat=ClinCodeCat;
			type=ClinCodeType;
			step=3;
			if cat="DX" then output _&fileabbrev._dxcodes;
			if cat="PX" then output _&fileabbrev._pxcodes;
		end;
		%* Step 4: 	MANY:1 match																*;
		%* 			Assign cat and type from lookup regardless of source values					*;
		else if match_type="MANY:1" then do;
			cat=ClinCodeCat;
			type=ClinCodeType;
			step=4;
			if cat="DX" then output _&fileabbrev._dxcodes;
			if cat="PX" then output _&fileabbrev._pxcodes;
		end;
		%* Step 5: 	1:MANY match																*;
		%*			Use cat and type that match between source and lookup						*;
		%*			If not available, use cat that matches between source and lookup			*;
		else if match_type="1:MANY" then do;
			if sourcecat=ClinCodeCat and sourcetype=ClinCodeType then do;
				cat=SourceCat;
				type=SourceType;
				step=5.1;
				if cat="DX" then output _&fileabbrev._dxcodes;
				if cat="PX" then output _&fileabbrev._pxcodes;
			end;
			else if sourcecat=ClinCodeCat then do;
					cat=SourceCat;
					type=ClinCodeType;
					step=5.2;
					if cat="DX" then output _&fileabbrev._dxcodes;
					if cat="PX" then output _&fileabbrev._pxcodes;
			end;
			else do;
				cat=ClinCodeCat;
				type=ClinCodeType;
				step=5.3;
				output _&fileabbrev._leftovers;
			end;
		end;
		%* Step 6: 	MANY:MANY match																*;
		%*			Use cat and type that match between source and lookup						*;
		%*			If not available, use cat that matches between source and lookup			*;
		else if match_type="MANY:MANY" then do;
			if sourcecat=ClinCodeCat and sourcetype=ClinCodeType then do;
				cat=SourceCat;
				type=SourceType;
				step=6.1;
				if cat="DX" then output _&fileabbrev._dxcodes;
				if cat="PX" then output _&fileabbrev._pxcodes;
			end;
			else if sourcecat=ClinCodeCat then do;
					cat=SourceCat;
					type=ClinCodeType;
					step=6.2;
					if cat="DX" then output _&fileabbrev._dxcodes;
					if cat="PX" then output _&fileabbrev._pxcodes;
			end;
			else do;
				cat=ClinCodeCat;
				type=ClinCodeType;
				step=6.3;
				output _&fileabbrev._leftovers;
			end;
		end;
	run;

	%* Check if there are any unmatched codes in the leftovers									*;
	proc sql noprint;
		create table _&fileabbrev._unmatched_codes as
		select distinct	  a.*
		from  		_&fileabbrev._leftovers						as a
		left join 	(	select * from _&fileabbrev._dxcodes
							union
						select * from _&fileabbrev._pxcodes	)	as b
		on 		a.sourcecode = b.sourcecode
		where b.sourcecode is null
		;
	quit;
	
	%* Add leftovers, if any, to appropriate dxcodes or pxcodes file							*;
	%let leftover_cats= ;
	proc sql noprint;
		select distinct cat into: leftover_cats separated by " " from _&fileabbrev._unmatched_codes;
	quit;

	%if %sysfunc(indexw(&leftover_cats,DX)) %then %do;
		%put ## APPENDING LEFTOVERS TO DX FILE ##;
		proc append base=_&fileabbrev._dxcodes data=_&fileabbrev._unmatched_codes(where=(cat="DX"));
		quit;
	%end;

	%if %sysfunc(indexw(&leftover_cats,PX)) %then %do;
		%put ## APPENDING LEFTOVERS TO PX FILE ##;
		proc append base=_&fileabbrev._pxcodes data=_&fileabbrev._unmatched_codes(where=(cat="PX"));
		quit;
	%end;
	
	%* Create the Temp DX table																	*;
	%* Eliminate duplicate records by taking only one value for PDX								*;
	proc sql noprint;
		create table _&fileabbrev._tempdx as
		select  %if %upcase(&fileabbrev.) ne IP %then %do; 	distinct %end;	  
						  a.DESYNPUF_ID as PatID		length=16
						, a.CLM_ID		as EncounterID	length=15
						, a.sourcecode 	as DX			length=18
						, b.type 		as DX_codetype	length=2
						, a.sourcecode 	as OrigDX		length=18
						%if %upcase(&fileabbrev.)=IP %then %do;
						, min(case 	when a.sourcevarnum=1 and a.sourcecat = "DX" then "P"
									else "S"
									end) 	as PDX
						%end;
		from  _&fileabbrev._dxcodes 		as b
			, _&fileabbrev._temptable_1 	as a
		where 	a.sourcecode 	= b.sourcecode
		  	and a.sourcecat 	= b.sourcecat
		  	and a.sourcetype 	= b.sourcetype
		%if %upcase(&fileabbrev.)=IP %then %do;
		group by  a.DESYNPUF_ID 
				, a.CLM_ID		
				, a.sourcecode 	
				, b.type 		
		%end;	
		;
	quit;

	%* Create the Temp PX table																	*;
	proc sql noprint;
		create table _&fileabbrev._temppx as
		select distinct   a.DESYNPUF_ID as PatID		length=16
						, a.CLM_ID		as EncounterID	length=15
						, a.sourcecode 	as PX			length=11
						, b.type 		as PX_codetype	length=2
						, a.sourcecode 	as OrigPX		length=11
		from  _&fileabbrev._pxcodes 		as b
			, _&fileabbrev._temptable_1 	as a
		where 	a.sourcecode 	= b.sourcecode
		  	and a.sourcecat 	= b.sourcecat
		  	and a.sourcetype 	= b.sourcetype
		  ;
	quit;

	%put ## PROGRAM NOTE: THE CODETYPE_ALGORITHM MACRO HAS ENDED ## ;

%mend codetype_algorithm;
