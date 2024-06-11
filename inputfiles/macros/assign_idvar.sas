%************************************************************************************************;
%* PROGRAM NAME:    assign_idvar.sas                                                            *;
%* VERSION:         1.0.0                                                                       *;
%* CREATED:         10/19/2020                                                                  *;
%* LAST MODIFIED:   10/21/2020                                                                  *;
%*----------------------------------------------------------------------------------------------*;
%* PURPOSE                                                                                      *;
%*  This macro program is used to assign a new numeric identifier variable to replace an old    *;
%*  identifier variable.                                                                        *;
%*----------------------------------------------------------------------------------------------*;
%* MACRO PARAMETERS                                                                             *;
%* Parameters: INDS   = input dataset                                                           *;
%*             OIDVAR = original ID variable                                                    *;
%*             NIDVAR = new ID variable                                                         *;
%*             OUTLIB = output library for crosswalk dataset                                    *;
%*----------------------------------------------------------------------------------------------*;
%* CONTACT INFO                                                                                 *;
%*  Sentinel System Coordinating Center                                                         *;
%*  info@sentinelsystem.org                                                                     *;
%*----------------------------------------------------------------------------------------------*;
%* CHANGE LOG:                                                                                  *;
%*                                                                                              *;
%*  Version        Date       Initials  Comment                                                 *;
%*  -------------  ---------- --------  --------------------------------------------------------*;
%*  1.0.0          10/21/2020 DC        - first release version                                 *;
%************************************************************************************************;

%macro assign_idvar(INDS=, OLDVAR=, LIST_OTHERVARS=, NEWVAR=, OUTLIB=);

    %put ## PROGRAM NOTE: THE ASSIGN_IDVAR MACRO HAS STARTED ## ;

    %* This utility macro checks existence of a dataset and returns the number of observations  *;
    %MACRO ISDATA(dataset=);
        %GLOBAL NOBS;
        %let NOBS=0;
        %if %sysfunc(exist(&dataset.))=1 and %LENGTH(&dataset.) ne 0 %then %do;
            data _null_;
            dsid=open("&dataset.");
            call symputx("NOBS",attrn(dsid,"NLOBS"));
            run;
        %end;   
        %PUT &NOBS.;
    %MEND ISDATA;
    
    %* Assign macro variables with maximum numeric value for each possible variable length      *;
    %* These limits apply to both Windows and Unix operating systems                            *;
    %let L3=%eval(2**13);
    %let L4=%eval(2**21);
    %let L5=%eval(2**29);
    %let L6=%eval(2**37);
    %let L7=%eval(2**45);
    %let L8=%eval(2**53);
    
    %* Delete crosswalk if it already exists                                                    *;
    %ISDATA(dataset=&OUTLIB..&NEWVAR._crosswalk);
    %if (&NOBS) %then %do;
        proc datasets lib=&OUTLIB nolist nodetails;
            delete &NEWVAR._crosswalk;
        quit;
    %end;
    
    %* Create a temp table with distinct combinations of OLDVAR + LIST_OTHERVARS from INDS      *;
    proc sort nodupkey data=&INDS (keep=&OLDVAR &LIST_OTHERVARS)
                       out=_list_&OLDVAR (rename=(&OLDVAR=orig_&OLDVAR))
                       ;
        by &OLDVAR &LIST_OTHERVARS;
    quit;

    %* Assign macro variable with number of observations in the ID list                         *;
    %ISDATA(dataset=_list_&OLDVAR);
    %let &NEWVAR._MAX=&NOBS;
    
    %put Max value of &NEWVAR: &&&NEWVAR._MAX;
    %* Determine the maximum-necessary length for the new variable                              *;
    %do i=3 %to 8;
        %if %eval(&&&NEWVAR._MAX <= &&&L&i) %then %do;
            %let VARLENGTH=&i;
            %let i=8;
        %end;
    %end;
    %put varlength of &NEWVAR: &VARLENGTH;
    
    %* Assign sequential numeric value to new identifier, using maximum-necessary length        *;
    data &OUTLIB..&NEWVAR._crosswalk;
        length &NEWVAR &VARLENGTH;
        set _list_&OLDVAR;
        &NEWVAR = _n_;
        if missing(orig_&OLDVAR) then &NEWVAR = .U;
    run;
    
    %* Clean the workspace                                                                      *;
    proc datasets lib=work nolist nodetails;
        delete _list_&OLDVAR;
    quit;

    %put ## PROGRAM NOTE: THE ASSIGN_IDVAR MACRO HAS ENDED ## ;

%mend assign_idvar;
