dm 'out;clear;log;clear';
%************************************************************************************************;
%*  PROGRAM NAME:   prepare_scdm.sas                                                            *;
%*  VERSION:        1.1.0                                                                       *;
%*  CREATED:        04/02/2018                                                                  *;
%*  LAST MODIFIED:  11/12/2020                                                                  *;
%*----------------------------------------------------------------------------------------------*;
%*  PURPOSE:        Prepare SCDM tables from one or more subsamples for use with Sentinel       *;
%*                  SAS programs.                                                               *;
%*----------------------------------------------------------------------------------------------*;
%*  BACKGROUND:                                                                                 *;
%*      Sentinel Operations Center has translated the CMS 2008-2010 SynPUFs subsample files to  *;
%*      Sentinel Common Data Model (SCDM) format to provide the public with synthetic data      *;
%*      that can be used with Sentinel SAS programs. SynPUFs was released in 20 discrete        *;
%*      subsamples, and each subsample was converted into a discrete set of SCDM tables. The    *;
%*      suffix of each SAS dataset name contains the subsample number. User may download and    *;
%*      combine any number of subsamples.                                                       *;
%*----------------------------------------------------------------------------------------------*;
%*  DEPENDENCIES:                                                                               *;
%*      - User must extract subsample files from zip folder(s) prior to running this program.   *;
%*      - User must supply parameter values described in USER INPUT section below.              *;
%*      - If combining two or more subsamples:                                                  *;
%*          - Subsamples must be stored in same location.                                       *;
%*          - Subsamples must be in sequential order. For example, the program can combine      *;
%*            subsamples 3 through 6, but not 3, 4, and 6.                                      *;
%*      - Input Subsample filenames must not be altered for this program to work properly.      *;
%*      - Output SCDM filenames must not be altered for Sentinel SAS programs to work properly. *;
%*----------------------------------------------------------------------------------------------*;
%* CONTACT INFO:                                                                                *;
%*  Sentinel System Coordinating Center                                                         *;
%*  info@sentinelsystem.org                                                                     *;
%*----------------------------------------------------------------------------------------------*;
%* CHANGE LOG:                                                                                  *;
%*                                                                                              *;
%*  Version        Date       Initials  Comment                                                 *;
%*  -------------  ---------- --------  --------------------------------------------------------*;
%*  1.0.0          04/02/2018 DC        - first release version                                 *;
%*  1.1.0          11/12/2020 DC        - modify to support SCDM 8                              *;
%************************************************************************************************;

%* -----            ----------------------------------------------------------------------------*;
%* ----- USER INPUT ----------------------------------------------------------------------------*;
%* -----            ----------------------------------------------------------------------------*;

%* 1. Specify path of extracted subsamples.                                                     *;
%*    Example: %let inlib = //mycomputer/sentinel/scdm_subsamples/                              *;
%let inlib =  ;

%* 2. Specify path for final combined SCDM tables.                                              *;
%*    Example: %let outlib = //mycomputer/sentinel/scdm/                                        *;
%let outlib =  ;

%* 3. Specify path for folder containing SynPUFs program macros.                                *;
%*    Example: %let sasmacr = //mycomputer/sentinel/synpuf/inputfiles/macros/                   *;
%let sasmacr =  ;

%* 4. Specify number of first subsample to be included. To include all subsamples, enter 1.     *;
%*    Example: %let first_subsample = 5                                                         *;
%let first_subsample =  ;

%* 5. Specify number of last subsample to be included. To include all subsamples, enter 20.     *;
%*    To include only one, enter same number as first_subsample.                                *;
%*    Example: %let last_subsample = 8                                                          *;
%let last_subsample =  ;

%* 6. Enter Y to delete extracted subsample datasets when finished. Enter N or leave blank      *;
%*    to retain extracted subsample datasets. This will not affect downloaded zip files.        *;
%*    Example: %let YN_Cleanup = N                                                              *;
%let YN_Cleanup =  ;

%* ############################                                   ############################# *;
%* ############################ DO NOT ALTER CODE BELOW THIS LINE ############################# *;
%* ############################                                   ############################# *;


%* -----                    --------------------------------------------------------------------*;
%* ----- ASSIGN LIBRARIES   --------------------------------------------------------------------*;
%* -----                    --------------------------------------------------------------------*;

libname inlib "&inlib";
libname outlib "&outlib";


%* -----                ------------------------------------------------------------------------*;
%* ----- ASSIGN MACROS  ------------------------------------------------------------------------*;
%* -----                ------------------------------------------------------------------------*;

%* Utility macros                                                                               *;
%include "&sasmacr./assign_idvar.sas";
%include "&sasmacr./assign_max_varlength.sas";
%include "&sasmacr./assign_sort_order.sas";

%* Main macro                                                                                   *;
%macro prepare_scdm(startnum= , endnum= , YN_Cleanup= );

    %let tbllist=enrollment demographic dispensing encounter diagnosis procedure 
                 death provider facility
                 ;
    %let tbllist=%cmpres(&tbllist);
    %let tblcount=%sysfunc(countw(&tbllist));

    %do s=&startnum %to &endnum;
        %if &s=&startnum %then %do;
            %* Create an empty base dataset for each table in the list                          *;
            %do i=1 %to &tblcount;
                %let tbl=%scan(&tbllist,&i);
                %let tbl=&tbl;
                data _temp_&tbl;
                    set inlib.&tbl._&s (obs=0);
                    length samplenum 3;
                run;
                %let tbl=;
            %end;
        %end;
        %* Append each input subsample table to the corresponding base dataset                  *; 
        %do j=1 %to &tblcount;
            %let tbl=%scan(&tbllist,&j);
            %let tbl=&tbl;
            %* Add samplenum variable before appending                                          *;
            data _temp_&tbl._&s;
                set inlib.&tbl._&s;
                length samplenum 3;
                samplenum=&s;
            run;

            %* Assign local macro variable(s)                                                   *;
            %local LIST_VARLENGTH;

            %* Use this format to add a variable type prefix to the variable length             *;
            proc format;
                value type 2 = "$"
                           1 = " "
                           ;
            quit;

            %* Extract variable type and length from base dataset                               *;
            proc contents noprint data=_temp_&tbl 
                                  out=_varlength_base (keep=name type length varnum)
                                  ;
            quit;

            %* Extract variable type and length from input dataset                              *;
            proc contents noprint data=_temp_&tbl._&s 
                                  out=_varlength_append (keep=name type length varnum)
                                  ;
            quit;

            %* Calculate max length for all variables, and assign a macro variable with list    *;
            %* of each variable name, type, and length. Preserve variable order of base.        *;
            proc sql noprint;
                create table _varlength_max as
                select  name
                      , put(max(type),type.) as pfx length=1
                      , max(length) as length
                      , min(varnum) as varnum
                from  (select  lowcase(name) as name
                             , type
                             , length
                             , varnum 
                       from _varlength_base
                       union all 
                       select  lowcase(name) as name
                             , type
                             , length 
                             , sum(varnum,900) as varnum 
                       from _varlength_append
                       )
                group by name
                ;
                select lowcase(name) || " " || cats(pfx, length) 
                       into :LIST_VARLENGTH 
                       separated by " "
                from _varlength_max
                order by varnum
                ;
            quit;

            %* Create blank template dataset to assign variable lengths and order              *;
            data _append_template;
                length &LIST_VARLENGTH;
                call missing(of _all_);
                stop;
            run;
                

            %* Add records from APPENDDS to BASEDS, and output to OUTDS                        *;
            data _temp_&tbl;
                set _append_template
                    _temp_&tbl 
                    _temp_&tbl._&s
                    ;
            run;

            %* Delete temp datasets                                                            *;
            proc datasets lib=work nolist nodetails;
                delete _varlength_base _varlength_append _varlength_max _append_template
                       _temp_&tbl._&s
                       ;
            quit;

            %if YN_Cleanup= Y %then %do;
                proc datasets nolist lib=inlib;
                    delete &tbl._&s;
                quit;
            %end;
            %let tbl=;
        %end;

    %end;

    %* Assign new ID variables to ensure uniqueness in combined scdm                        *;
        %* PatID                                                                            *;
        proc sort nodupkey data=_temp_demographic (keep=patid samplenum) 
                           out=_list_patid
                           ;
            by samplenum patid;
        quit;

        %assign_idvar(  INDS=_list_patid
                      , OLDVAR=patid
                      , LIST_OTHERVARS=samplenum
                      , NEWVAR=patid
                      , OUTLIB=work
                      );

        %* EncounterID                                                                      *;
        proc sort nodupkey data=_temp_encounter (keep=encounterid samplenum) 
                           out=_list_encounterid
                           ;
            by samplenum encounterid;
        quit;

        %assign_idvar(  INDS=_list_encounterid
                      , OLDVAR=encounterid
                      , LIST_OTHERVARS=samplenum
                      , NEWVAR=encounterid
                      , OUTLIB=work
                      );

        %* ProviderID                                                                       *;
        proc sort nodupkey data=_temp_provider (keep=providerid samplenum) 
                           out=_list_providerid
                           ;
            by samplenum providerid;
        quit;

        %assign_idvar(  INDS=_list_providerid
                      , OLDVAR=providerid
                      , LIST_OTHERVARS=samplenum
                      , NEWVAR=providerid
                      , OUTLIB=work
                      );

        %* FacilityID                                                                       *;
        proc sort nodupkey data=_temp_facility (keep=facilityid samplenum) 
                           out=_list_facilityid
                           ;
            by samplenum facilityid;
        quit;

        %assign_idvar(  INDS=_list_facilityid
                      , OLDVAR=facilityid
                      , LIST_OTHERVARS=samplenum
                      , NEWVAR=facilityid
                      , OUTLIB=work
                      );

    %* Complete each table by attaching new identifiers, assigning max varlengths where         *;
    %* required, and applying sort order                                                        *;

        %* ENROLLMENT                                                                           *;
        proc sql noprint;
            create table outlib.enrollment as
            select   b.PatID
                   , a.Enr_Start
                   , a.Enr_End
                   , a.MedCov
                   , a.DrugCov
                   , a.Chart
            from  _temp_enrollment  as a
                , patid_crosswalk   as b
            where a.PatID = b.Orig_PatID
            ;
        quit;

        %* Sort in required order                                                               *;
        %assign_sort_order(  INDS=outlib.enrollment
                           , LIST_SORTEDBY=PatID Enr_Start Enr_End MedCov DrugCov Chart
                           );

        %* DEMOGRAPHIC                                                                          *;
        proc sql noprint;
            create table outlib.demographic as
            select   b.PatID
                   , a.Birth_Date
                   , a.Sex
                   , a.Hispanic
                   , a.Race
                   , a.PostalCode
                   , a.PostalCode_date
            from  _temp_demographic as a
                , patid_crosswalk   as b
            where a.PatID = b.Orig_PatID
              and a.samplenum = b.samplenum
            ;
        quit;

        %* Sort in required order                                                               *;
        %assign_sort_order(INDS=outlib.demographic, LIST_SORTEDBY=PatID);

        %* DISPENSING                                                                           *;

        %* Get length of ProviderID variable from the Provider table                            *;
        data _null_;
            dsid=open("providerid_crosswalk");
            num=varnum(dsid,"providerid");
            call symput("PROVIDERID_LENGTH",trim(left(put(varlen(dsid,num),best.))));
            rc=close(dsid);
        run;

        proc sql noprint;
            create table outlib.dispensing as
            select   b.PatID
                   , a.ProviderID length=&PROVIDERID_LENGTH
                   , a.RxDate
                   , a.Rx
                   , a.Rx_CodeType
                   , a.RxSup
                   , a.RxAmt
            from  _temp_dispensing  as a
                , patid_crosswalk   as b
            where a.PatID = b.Orig_PatID
              and a.samplenum = b.samplenum
            ;
        quit;

        %* Assign maximum-necessary length to Rx                                                *;
        %assign_max_varlength(INDS=outlib.dispensing, VAR=Rx, OUTDS=outlib.dispensing);

        %* Sort in required order                                                               *;
        %assign_sort_order(  INDS=outlib.dispensing
                           , LIST_SORTEDBY=PatID RxDate Rx_CodeType Rx ProviderID
                           );

        %* ENCOUNTER                                                                            *;
        proc sql noprint;
            create table outlib.encounter as
             select   b.PatID
                    , c.EncounterID
                    , a.ADate
                    , a.DDate
                    , a.EncType
                    , d.FacilityID
                    , a.Discharge_Disposition
                    , a.Discharge_Status
                    , a.DRG
                    , a.DRG_Type
                    , a.Admitting_Source
            from       _temp_encounter              as a
            inner join patid_crosswalk              as b
            on  a.PatID = b.Orig_PatID
            and a.samplenum = b.samplenum
            left join encounterid_crosswalk         as c
            on  a.EncounterID = c.Orig_EncounterID
            and a.samplenum = c.samplenum
            left join facilityid_crosswalk          as d
            on  a.FacilityID = d.Orig_FacilityID
            and a.samplenum = d.samplenum
            ;
        quit;

        %* Sort in required order                                                               *;
        %assign_sort_order(INDS=outlib.encounter, LIST_SORTEDBY=PatID ADate);

        %* DIAGNOSIS                                                                            *;
        proc sql noprint;
            create table outlib.diagnosis as
            select   b.PatID
                   , c.EncounterID
                   , a.ADate
                   , d.ProviderID
                   , a.EncType
                   , a.DX
                   , a.DX_CodeType
                   , a.OrigDX
                   , a.PDX
                   , a.PAdmit
            from       _temp_diagnosis       as a
            inner join patid_crosswalk       as b
            on  a.PatID = b.Orig_PatID
            and a.samplenum = b.samplenum
            left join  encounterid_crosswalk as c
            on  a.EncounterID = c.Orig_EncounterID
            and a.samplenum = c.samplenum
            left join  providerid_crosswalk  as d
            on  a.ProviderID = d.Orig_ProviderID
            and a.samplenum = d.samplenum
            ;
        quit;

        %* Assign maximum-necessary length to Dx                                                *;
        %assign_max_varlength(INDS=outlib.diagnosis, VAR=Dx, OUTDS=outlib.diagnosis);

        %* Sort in required order                                                               *;
        %assign_sort_order(INDS=outlib.diagnosis, LIST_SORTEDBY=PatID ADate);

        %* PROCEDURE                                                                            *;
        proc sql noprint;
            create table outlib.procedure as
            select   b.PatID
                   , c.EncounterID
                   , a.ADate
                   , d.ProviderID
                   , a.EncType
                   , a.PX
                   , a.PX_CodeType
                   , a.OrigPX
            from       _temp_procedure       as a
            inner join patid_crosswalk       as b
            on  a.PatID = b.Orig_PatID
            and a.samplenum = b.samplenum
            left join  encounterid_crosswalk as c
            on  a.EncounterID = c.Orig_EncounterID
            and a.samplenum = c.samplenum
            left join  providerid_crosswalk  as d
            on  a.ProviderID = d.Orig_ProviderID
            and a.samplenum = d.samplenum
            ;
        quit;

        %* Assign maximum-necessary length to Px                                                *;
        %assign_max_varlength(INDS=outlib.procedure, VAR=Px, OUTDS=outlib.procedure);

        %* Sort in required order                                                               *;
        %assign_sort_order(INDS=outlib.procedure, LIST_SORTEDBY=PatID ADate);

        %* DEATH                                                                                *;
        proc sql noprint;
            create table outlib.death as
            select   b.PatID
                   , a.DeathDt
                   , a.Dtimpute
                   , a.Source
                   , a.Confidence
            from  _temp_death       as a
                , patid_crosswalk   as b
            where a.PatID = b.Orig_PatID
              and a.samplenum = b.samplenum
            ;
        quit;

        %* Sort in required order                                                               *;
        %assign_sort_order(INDS=outlib.death, LIST_SORTEDBY=PatID);

        %* PROVIDER                                                                             *;
        data outlib.provider;
            set providerid_crosswalk (where=(not missing(ProviderID))
                                      drop=Orig_ProviderID samplenum
                                      );
            length Specialty $2 Specialty_CodeType $1;
            Specialty="99";
            Specialty_CodeType="2";
        run;

        %* Sort in required order                                                               *;
        %assign_sort_order(INDS=outlib.provider, LIST_SORTEDBY=ProviderID);

        %* FACILITY                                                                             *;
        data outlib.facility;
            set facilityid_crosswalk (where=(not missing(FacilityID))
                                      drop=Orig_FacilityID samplenum
                                      );
            length Facility_Location $1;
            call missing(Facility_Location);
        run;

        %* Sort in required order                                                               *;
        %assign_sort_order(INDS=outlib.facility, LIST_SORTEDBY=FacilityID);

        %* Clean up the workspace                                                               *;
        proc datasets lib=work nolist nodetails memtype=data kill;
        quit;

%mend prepare_scdm;

%* Run the program                                                                              *;
%prepare_scdm(startnum=&first_subsample, endnum=&last_subsample, YN_Cleanup=&YN_Cleanup);
