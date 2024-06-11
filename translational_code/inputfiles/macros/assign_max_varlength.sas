%************************************************************************************************;
%* PROGRAM NAME:    assign_max_varlength.sas                                                    *;
%* VERSION:         1.0.0                                                                       *;
%* CREATED:         10/19/2020                                                                  *;
%* LAST MODIFIED:   10/19/2020                                                                  *;
%*----------------------------------------------------------------------------------------------*;
%* PURPOSE                                                                                      *;
%*  This macro modifies a character variable to maximum-necessary length.                       *;
%*----------------------------------------------------------------------------------------------*;
%* MACRO PARAMETERS                                                                             *;
%*  INDS: input dataset                                                                         *;
%*  VAR:  variable to be modified                                                               *;
%*  OUTDS: output dataset                                                                       *;
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

%macro assign_max_varlength(INDS=, VAR=, OUTDS=);

    %put ## PROGRAM NOTE: THE ASSIGN_MAX_VARLENGTH MACRO HAS STARTED ## ;

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
    
    %* Check if VAR exists in INDS                                                              *;
    %varexist(&INDS,&VAR);
    
    %* Process only if VAR exists in INDS                                                       *;
    %if &VAREXIST > 0 %then %do;
        
        proc contents noprint data=&INDS out=_temp_contents (keep=name type varnum);
        quit;
        
        %* Assign VARORDER as list of variable names ordered by varnum                          *;
        %* Assign VARTYPE as variable type of VAR                                               *;
        proc sql noprint;
            select name into: VARORDER separated by " " 
            from _temp_contents
            order by varnum
            ;
            select type into: VARTYPE trimmed
            from _temp_contents (where=(upcase(name)=upcase("&VAR")))
            ;
        quit;
        
        %* Process only if VAR is a character variable                                          *;
        %if &VARTYPE=2 %then %do;

            %* Assign VARLENGTH with maximum-necessary value length of VAR                      *;
            proc sql noprint;
                select max(length(&VAR)) into: VARLENGTH trimmed
                from &INDS
                ;
            quit;
            
            %* Assign VARLENGTH a default value of 1 if VAR has all missing values              *;
            %if %length(&VARLENGTH) = 0 %then %do;
                %let VARLENGTH=1;
            %end;
            
            %* Assign length of VAR and preserve variable order of INDS                         *;
            %* Save modified dataset as OUTDS                                                   *;
            data &OUTDS (drop=_&VAR);
                retain &VARORDER;
                length &VAR $&VARLENGTH;
                set &INDS (rename=(&VAR=_&VAR));
                &VAR=_&VAR;
            run;

        %end;
        
        %else %if &VARTYPE=1 %then %do;
            %put NOTE: &VAR is a numeric variable in &INDS;
            %put NOTE: The assign_max_varlength macro processes character variables only;
        %end;
        
    %end;
    
    %else %do;
        %put NOTE: &VAR does not exist in &INDS;
    %end;

    %put ## PROGRAM NOTE: THE ASSIGN_MAX_VARLENGTH MACRO HAS ENDED ## ;

%mend assign_max_varlength;
