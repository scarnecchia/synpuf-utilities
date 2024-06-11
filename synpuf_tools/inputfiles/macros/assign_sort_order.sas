%************************************************************************************************;
%* PROGRAM NAME:    assign_sort_order.sas                                                       *;
%* VERSION:         1.0.0                                                                       *;
%* CREATED:         10/19/2020                                                                  *;
%* LAST MODIFIED:   10/19/2020                                                                  *;
%*----------------------------------------------------------------------------------------------*;
%* PURPOSE                                                                                      *;
%*  This macro program is used to sort a table by a specified list of variables.                *;
%*----------------------------------------------------------------------------------------------*;
%* MACRO PARAMETERS                                                                             *;
%*  INDS:          input dataset name                                                           *;
%*  LIST_SORTEDBY: space-delimited, ordered list of sort variables                              *;
%*----------------------------------------------------------------------------------------------*;
%* CONTACT INFO                                                                                 *;
%*  Sentinel System Coordinating Center                                                         *;
%*  info@sentinelsystem.org                                                                     *;
%*----------------------------------------------------------------------------------------------*;
%* CHANGE LOG:                                                                                  *;
%*                                                                                              *;
%*  Version        Date       Initials  Comment                                                 *;
%*  -------------  ---------- --------  --------------------------------------------------------*;
%*  1.0.0          10/19/2020 DC        - first release version                                 *;
%************************************************************************************************;

%macro assign_sort_order(INDS=, LIST_SORTEDBY=);

    %put ## PROGRAM NOTE: THE ASSIGN_SORT_ORDER MACRO HAS STARTED ## ;

    %* Utility macro to check if a variable exists in a dataset                                 *;
    %* This macro is copied from ms_macros.sas in the Sentinel QRP program package              *;
    %MACRO VAREXIST(DS,VAR);
    
    %PUT =====> MACRO CALLED: ms_macros v1.0 => VAREXIST;
    
    %GLOBAL VAREXIST;
    %LET VAREXIST=0;
    %LET DSID = %SYSFUNC(OPEN(&DS));
    %IF (&DSID) %THEN %DO;
        %IF %SYSFUNC(VARNUM(&DSID,&VAR)) %THEN %LET VAREXIST=1;
        %LET RC = %SYSFUNC(CLOSE(&DSID));
    %END;
    
    %put NOTE: ********END OF MACRO: ms_macros v1.0 => VAREXIST ********;
    
    %MEND VAREXIST;
    
    %* Confirm variables in LIST_SORTEDBY exist in INDS                                         *;
    %let ALLVARSEXIST=1;
    %do i=1 %to %sysfunc(countw(&LIST_SORTEDBY));
        %varexist(&INDS,%scan(&LIST_SORTEDBY,&I));
        %if &VAREXIST=0 %then %do;
            %let ALLVARSEXIST=0;
            %put ERROR: %scan(&LIST_SORTEDBY,&I) does not exist in &INDS ;
            %put ERROR: &INDS cannot be sorted ;
        %end;
    %end;
    
    %* Sort INDS by LIST_SORTEDBY if all variables exist                                        *;
    %if (&ALLVARSEXIST) %then %do;
        proc sort data=&INDS;
            by &LIST_SORTEDBY;
        quit;
    %end;

    %* Put an error message in the SAS log if one or more variables does not exist              *;
    %else %do;
        %put ERROR: One or more variables in LIST_SORTEDBY do not exist in INDS ;
        %put ERROR: INDS cannot be sorted ;
    %end; 

    %put ## PROGRAM NOTE: THE ASSIGN_SORT_ORDER MACRO HAS ENDED ## ;
    
%mend assign_sort_order;
