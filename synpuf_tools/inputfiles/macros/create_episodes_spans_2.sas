/*HEADER-------------------------------------------------------------------------------------------
|
| PROGRAM:  create_episodes_spans_2 (macro)
|
|
|
| PURPOSE:  This macro  takes as input a table with EFF_DT and EXP_DT (for example, elibility)and
|           and create a new table which have adjusted effective and expiration date: 
|			EFF_DT_SPAN and EXP_DT_SPAN. The adjusted episodes spans ignore gaps in coverage 
|			which are less then &grace_period. 
|           Episodes are build on the group of parameters ( &by_vars).
|
|           The main difference from %create_episodes_spans  is that the 
|			current macro allows multiple variables in by-group &id_vars used in 
|			episode definition.	 For example, a patient can be identified by combination of
|           hosp_id and internal to a hospital pat_id.
|			
| INPUT:    MACRO PARMS:
|
|           tbl_name_in:      Input table. 
|           eff_dt_var_nm:    Name of effective date variable name. Default is: MEMEFF
|			exp_dt_var_nm:    Name of effective date variable name. Default is: MEMEND
|			by_vars:		    By-group variables used in episode definition. 
|								Default is PLANREC
|           grace_period:     Numb. of days used in the adjasement of the episodes effective and
|			  				    expiration dates. Default is 45.
|
| OUTPUT:    tbl_name_out:    Output table with new variables for effective and expiration dates. 
|							  The new names have suffix _SPAN
| USAGE:     %create_episodes_spans(tbl_name_in=WORK.PAT,tbl_name_out=WORK.PAT_SPAN);
|		     For pharmacy 
|		     %create_episodes_spans(tbl_name_in=WORK.PAT,tbl_name_out=WORK.PAT_SPAN,grace_period=60,
|								 by_vars=PLANREC DRUG_COV_YN, eff_dt_var_nm=FromDt, exp_dt_var_nm=ToDt);
+-------------------------------------------------------------------------------------------------------
| HISTORY:  05/11/2007 - Yury Vilk.
|           
+------------------------------------------------------------------------------------------------HEADER*/

%MACRO create_episodes_spans_2(tbl_name_in=,
															 tbl_name_out=,
															 grace_period=45,
															 by_vars=PLANREC,
															 eff_dt_var_nm=MEMEFF,
															 exp_dt_var_nm=MEMEND,
														     grace_period_var_nm=
															 );

 

/*%GLOBAL Err_fl program_name DEBUG_FLAG;
 %get_debugging_options;

 %IF &DEBUG_FLAG.=Y %THEN 
					%DO;
					 OPTIONS NOTES;
					 OPTIONS MLOGIC MPRINT SYMBOLGEN SOURCE2;
					%END;
%ELSE %DO;
  OPTIONS NOMLOGIC NOMPRINT NOSYMBOLGEN NOSOURCE2;
  		%END;
*/

%local  lastvar 	grace_period_str;
%if &by_vars ne %then %do;
  %let lastvar = %scan(&by_vars.,-1);
%put &lastvar;
%end;

 %IF    %LENGTH(&grace_period_var_nm.)=0 
	%THEN  %LET grace_period_str=%STR(_grace_period=&grace_period.) ;
%ELSE 	 	%LET grace_period_str=%STR(_grace_period=&grace_period_var_nm.) ;

PROC SORT DATA=&tbl_name_in
		  OUT=WORK.__TMP(RENAME=(&eff_dt_var_nm.=_EFF_DT &exp_dt_var_nm.=_EXP_DT));
BY &by_vars. &eff_dt_var_nm. &exp_dt_var_nm.;
 WHERE &exp_dt_var_nm. >= &eff_dt_var_nm.;
RUN;

 DATA WORK.__TMP;
  SET WORK.__TMP;
  BY &by_vars. _EFF_DT _EXP_DT;
  RETAIN _EFF_DT_SPAN _EXP_DT_PREV;

  LENGTH _grace_period 8;

   SPAN_START_FLAG=0;
   _EXP_DT_PREV=LAG(_EXP_DT);

   &grace_period_str.;
  
  IF FIRST.&lastvar THEN
  					DO;
		SPAN_START_FLAG=1;
        _EFF_DT_SPAN=_EFF_DT;
		_EXP_DT_PREV=.;
		DAYS_BETWEEN_ORIG_SPANS=.;
					END;
  ELSE				
  					DO;
	DAYS_BETWEEN_ORIG_SPANS=_EFF_DT-_EXP_DT_PREV-1;
	IF DAYS_BETWEEN_ORIG_SPANS >_grace_period THEN DO;
											SPAN_START_FLAG=1;
											_EFF_DT_SPAN=_EFF_DT;
											   END;
					END;
FORMAT _EFF_DT_SPAN _EXP_DT_PREV DATE9.;
DROP _grace_period;
 RUN;

 
PROC SORT DATA=WORK.__TMP;
  BY &by_vars. _EFF_DT_SPAN _EXP_DT;
RUN;

 DATA &tbl_name_out;
  SET WORK.__TMP;
  BY &by_vars. _EFF_DT_SPAN _EXP_DT;

   IF LAST._EFF_DT_SPAN THEN DO;
       	             _EXP_DT_SPAN=_EXP_DT;
					 SPAN_DAYS=_EXP_DT_SPAN-_EFF_DT_SPAN+1;
					 OUTPUT;
   							END; 
FORMAT _EXP_DT_SPAN DATE9. ;
DROP _EXP_DT_PREV SPAN_START_FLAG DAYS_BETWEEN_ORIG_SPANS _EFF_DT _EXP_DT;

RENAME	_EFF_DT_SPAN=&eff_dt_var_nm._SPAN  _EXP_DT_SPAN=&exp_dt_var_nm._SPAN;

RUN;

/*

%IF &DEBUG_FLAG. NE Y %THEN 
					%DO;
OPTIONS &_DEBUG_OPTIONS.;
 PROC SQL; DROP TABLE WORK.__TMP ; QUIT;
					%END;   */     
%MEND;

