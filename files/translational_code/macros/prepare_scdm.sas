dm 'out;clear;log;clear';
%************************************************************************************************;
%*	PROGRAM NAME: 	prepare_scdm.sas															*;
%*	VERSION:		1.0.0																		*;
%*	CREATED:		04/02/2018																	*;
%*	LAST MODIFIED:	04/02/2018																	*;
%*----------------------------------------------------------------------------------------------*;
%*	PURPOSE:		Prepare SCDM tables from one or more subsamples for use with Sentinel		*;
%*					SAS programs.																*;
%*----------------------------------------------------------------------------------------------*;
%*	BACKGROUND:																					*;
%*		Sentinel Operations Center has translated the CMS 2008-2010 SynPUFs	subsample files to	*;
%*		Sentinel Common Data Model (SCDM) format to provide the public with synthetic data		*;
%*		that can be used with Sentinel SAS programs. SynPUFs was released in 20 discrete 		*;
%*		subsamples, and each subsample was converted into a discrete set of SCDM tables. The  	*;
%*		suffix of each SAS dataset name contains the subsample number. User may download and  	*;
%*		combine any number of subsamples.														*;
%*----------------------------------------------------------------------------------------------*;
%* 	DEPENDENCIES:																				*;
%*		- User must extract subsample files from zip folder(s) prior to running this program. 	*;
%*		- User must supply parameter values described in USER INPUT section below.				*;
%*		- If combining two or more subsamples:													*;
%*			- Subsamples must be stored in same location.										*;
%*			- Subsamples must be in sequential order. For example, the program can combine  	*;
%*		  	  subsamples 3 through 6, but not 3, 4, and 6.										*;
%*		- Input Subsample filenames must not be altered for this program to work properly.		*;
%*		- Output SCDM filenames must not be altered for Sentinel SAS programs to work properly.	*;
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

%* -----        	----------------------------------------------------------------------------*;
%* ----- USER INPUT	----------------------------------------------------------------------------*;
%* -----        	----------------------------------------------------------------------------*;

	%* 1. Specify path of extracted subsamples. Do not enclose value in quotation marks.		*;
	%let inlib= C:\data\scdm_subsamples\ ;

	%* 2. Specify path for final combined SCDM tables. Do not enclose value in quotation marks. *;
	%let outlib= //data/scdm/ ;

	%* 3. Specify number of first subsample to be included. To include all subsamples, enter 1. *;
	%let first_subsample=  ;

	%* 3. Specify number of last subsample to be included. To include all subsamples, enter 20. *;
	%* 	  To include only one, enter same number as first_subsample.							*;
	%let last_subsample=  ;

	%* 4. Enter Y to delete extracted subsample datasets when finished. Enter N or leave blank 	*;
	%*	  to retain extracted subsample datasets. This will not affect downloaded zip files.	*;
	%let YN_Cleanup= N ;

%* ############################                                   ############################# *;
%* ############################ DO NOT ALTER CODE BELOW THIS LINE ############################# *;
%* ############################                                   ############################# *;

libname inlib "&inlib";
libname outlib "&outlib";


%macro prepare_scdm(startnum= , endnum= , YN_Cleanup= );

	%let tbllist=enrollment demographic dispensing encounter diagnosis procedure death;
	%let tblcount=%sysfunc(countw(&tbllist));

	%do s=&startnum %to &endnum;
		%if &s=&startnum %then %do;
			%* Create an empty base dataset for each table in the list							*;
			%do i=1 %to &tblcount;
				%let tbl=%scan(&tbllist,&i);
				%let tbl=&tbl;
				data _temp_&tbl;
					set inlib.&tbl._&s (obs=0);
				run;
				%let tbl=;
			%end;
		%end;
		%* Append each input subsample table to the corresponding base dataset					*; 
		%do j=1 %to &tblcount;
			%let tbl=%scan(&tbllist,&j);
			%let tbl=&tbl;
			proc append base=_temp_&tbl data=inlib.&tbl._&s;
			quit;
			%if YN_Cleanup= Y %then %do;
				proc datasets nolist lib=inlib;
					delete &tbl._&s;
				quit;
			%end;
			%let tbl=;
		%end;
		%* Rename each base dataset and move to the output library								*;
		%if &s=&endnum %then %do;
			%do k=1 %to &tblcount;
				%let tbl=%scan(&tbllist,&k);
				%let tbl=&tbl;
				proc datasets nolist lib=work;
					change _temp_&tbl=&tbl;
				copy out=outlib move;
					select &tbl;
				quit;
				%let tbl=;
			%end;
		%end;
	%end;

%mend prepare_scdm;

%prepare_scdm(startnum=&first_subsample, endnum=&last_subsample, YN_Cleanup=&YN_Cleanup);
