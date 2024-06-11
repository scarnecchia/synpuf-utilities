%************************************************************************************************;
%* PROGRAM NAME:	step_b_process_pde_file.sas  												*;
%* VERSION: 		1.0.0																		*;
%* CREATED: 		03/20/2018																	*;
%* LAST MODIFIED: 	04/02/2018																	*;
%*----------------------------------------------------------------------------------------------*;
%* PURPOSE:																						*;
%*  This macro program is used to process the Medicare Synthetic Public Use Files (SynPUFs) 	*;
%*	Prescription Drug Events file into the appropriate Sentinel Common Data Model (SCDM) tables.*;
%*----------------------------------------------------------------------------------------------*;
%* MACRO PARAMETERS																				*;
%*	INDS_pde_file:	Enter the two-level name of the SynPUFs Prescription Drug Events input file	*;
%*	OUTDS_temp_dis:	Enter the two-level name of the Temp Dispensing output table				*;
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


%macro PROCESS_PDE_FILE(  INDS_pde_file		= 
						, OUTDS_temp_dis	= 
						);

	%put ## PROGRAM NOTE: THE PROCESS_PDE_FILE MACRO HAS STARTED ##;

	%* PART 1: SCDM DISPENSING 																	*;

	proc sql noprint;
		create table &OUTDS_temp_dis as
		select	  DESYNPUF_ID 			as PatID	length=16 
				, SRVC_DT 			 	as RxDate	length=4 	format=mmddyy10.
				, PROD_SRVC_ID 			as NDC		length=11 
				, sum(DAYS_SUPLY_NUM)	as RxSup 	length=4 					
				, sum(QTY_DSPNSD_NUM)	as RxAmt 	length=4 
		from &INDS_pde_file (where=(anyalpha(prod_srvc_id)=0))
		group by  DESYNPUF_ID 					
				, SRVC_DT
				, PROD_SRVC_ID
				;
	quit;

	%put ## PROGRAM NOTE: THE PROCESS_PDE_FILE MACRO HAS ENDED ##;

%mend PROCESS_PDE_FILE;
