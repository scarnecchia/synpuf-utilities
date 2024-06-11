%************************************************************************************************;
%* PROGRAM NAME:    step_f_final_processing.sas                                                 *;
%* VERSION:         1.1.0                                                                       *;
%* CREATED:         04/02/2018                                                                  *;
%* LAST MODIFIED:   11/12/2020                                                                  *;
%*----------------------------------------------------------------------------------------------*;
%* PURPOSE:                                                                                     *;
%*  This macro program is used to complete the process of translating the Medicare Synthetic    *;
%*  Public Use Files (SynPUFs) into the appropriate Sentinel Common Data Model (SCDM) tables.   *;
%*----------------------------------------------------------------------------------------------*;
%* MACRO PARAMETERS                                                                             *;
%*  INDS_scdm_enr:      Enter the two-level name of the SCDM Enrollment input table             *;
%*  INDS_temp_dem:      Enter the two-level name of the Temp Demographic input table            *;
%*  INDS_temp_death:    Enter the two-level name of the Temp Death input table                  *;
%*  INDS_temp_dis:      Enter the two-level name of the Temp Dispensing input table             *;
%*  INDS_op_temp_enc:   Enter the two-level name of the OP Temp Encounter input table           *;
%*  INDS_op_temp_dia:   Enter the two-level name of the OP Temp Diagnosis input table           *;
%*  INDS_op_temp_pro:   Enter the two-level name of the OP Temp Procedure input table           *;
%*  INDS_car_temp_enc:  Enter the two-level name of the CAR Temp Encounter input table          *;
%*  INDS_car_temp_dia:  Enter the two-level name of the CAR Temp Diagnosis input table          *;
%*  INDS_car_temp_pro:  Enter the two-level name of the CAR Temp Procedure input table          *;
%*  INDS_ip_temp_enc:   Enter the two-level name of the IP Temp Encounter input table           *;
%*  INDS_ip_temp_dia:   Enter the two-level name of the IP Temp Diagnosis input table           *;
%*  INDS_ip_temp_pro:   Enter the two-level name of the IP Temp Procedure input table           *;
%*  OUTDS_scdm_dem:     Enter the two-level name of the SCDM Temp Demographic output table      *;
%*  OUTDS_scdm_dis:     Enter the two-level name of the SCDM Temp Dispensing output table       *;
%*  OUTDS_scdm_enc:     Enter the two-level name of the SCDM Temp Encounter output table        *;
%*  OUTDS_scdm_dia:     Enter the two-level name of the SCDM Temp Diagnosis output table        *;
%*  OUTDS_scdm_pro:     Enter the two-level name of the SCDM Temp Procedure output table        *;
%*  OUTDS_scdm_death:   Enter the two-level name of the SCDM Temp Death output table            *;
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
%*  1.1.0          11/12/2020 DC        - modify to update output tables to SCDM 8              *;
%************************************************************************************************;


%macro FINAL_PROCESSING(      INDS_scdm_enr     = 
                            , INDS_temp_dem     = 
                            , INDS_temp_death   = 
                            , INDS_temp_dis     = 
                            , INDS_op_temp_enc  = 
                            , INDS_op_temp_dia  = 
                            , INDS_op_temp_pro  = 
                            , INDS_car_temp_enc = 
                            , INDS_car_temp_dia = 
                            , INDS_car_temp_pro = 
                            , INDS_ip_temp_enc  = 
                            , INDS_ip_temp_dia  = 
                            , INDS_ip_temp_pro  = 
                            , OUTDS_scdm_dem    = 
                            , OUTDS_scdm_dis    = 
                            , OUTDS_scdm_enc    = 
                            , OUTDS_scdm_pvd    = 
                            , OUTDS_scdm_fac    = 
                            , OUTDS_scdm_dia    = 
                            , OUTDS_scdm_pro    = 
                            , OUTDS_scdm_death  = 
                            , OUTDS_scdm_enr    = 
                            );

    %put ## PROGRAM NOTE: THE FINAL_PROCESSING MACRO HAS STARTED ##;

    %* PRE-PROCESSING                                                                           *;

    %* Create list of PatIDs having >0 enrollment records                                       *;
    proc sql noprint;
        create table _patid_list as
        select PatID, min(enr_start) as first_enr_dt
        from &INDS_scdm_enr
        group by PatID
        ;
    quit;

    %* Assign new PatID variable                                                                *;
    %assign_idvar(INDS=_patid_list, OLDVAR=patid, NEWVAR=patid, OUTLIB=work);

    %* PART 1: SCDM DEMOGRAPHIC                                                                 *;

    %* Exclude patients having 0 records in the SCDM Enrollment table                           *;
    %* Replace character PatID with numeric PatID                                               *;
    %* Rename ZIP to PostalCode                                                                 *;
    %* Assign PostalCode_date                                                                   *;
    proc sql noprint;
        create table &OUTDS_scdm_dem as
        select distinct   b.PatID
                        , a.Birth_Date
                        , a.Sex
                        , a.Hispanic
                        , a.Race
                        , a.zip as PostalCode
                        , case  when a.zip ne " " then c.first_enr_dt 
                                else . 
                                end as PostalCode_date     length=4    format=mmddyy10.
        from  &INDS_temp_dem    as a
            , patid_crosswalk   as b
            , _patid_list       as c
        where   a.PatID = b.Orig_PatID
          and   a.PatID = c.PatID
        ;
    quit;

    %* Sort in required order                                                                   *;
    %assign_sort_order(INDS=&OUTDS_scdm_dem, LIST_SORTEDBY=PatID);

    %* PART 2: SCDM ENCOUNTER                                                                   *;

    %* Concatenate the OP, CAR, and IP Temp Encounter tables                                    *;
    %* Exclude records outside of 1/1/2008 - 12/31/2010                                         *;
    data _temp_scdm_enc;
        set &INDS_op_temp_enc
            &INDS_car_temp_enc
            &INDS_ip_temp_enc
            ;
        where ADate between "01JAN2008"d and "31DEC2010"d;
    run;

    %* Assign new EncounterID variable                                                          *;
    %assign_idvar(INDS=_temp_scdm_enc, OLDVAR=encounterid, NEWVAR=encounterid, OUTLIB=work);

    %* Assign new FacilityID variable                                                          *;
    %assign_idvar(INDS=_temp_scdm_enc, OLDVAR=facility_code, NEWVAR=facilityid, OUTLIB=work);

    %* Assign new ProviderID variable                                                          *;
    %assign_idvar(INDS=_temp_scdm_enc, OLDVAR=provider, NEWVAR=providerid, OUTLIB=work);

    %* Exclude patients having 0 records in the SCDM Enrollment table                           *;
    %* Replace character PatID with numeric PatID                                               *;
    %* Replace character EncounterID with numeric EncounterID                                   *;
    %* Replace character Facility_code with numeric FacilityID                                  *;
    %* Remove Provider                                                                          *;
    proc sql noprint;
        create table &OUTDS_scdm_enc as
         select distinct  b.PatID
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
        from       _temp_scdm_enc               as a
        inner join patid_crosswalk              as b
        on a.PatID = b.Orig_PatID
        left join encounterid_crosswalk         as c
        on a.EncounterID = c.Orig_EncounterID
        left join facilityid_crosswalk          as d
        on a.Facility_code = d.Orig_Facility_code
            ;
    quit;

    %* Sort in required order                                                                   *;
    %assign_sort_order(INDS=&OUTDS_scdm_enc, LIST_SORTEDBY=PatID ADate);

    proc datasets nolist lib=work;
        delete _temp_scdm_enc;
    quit;

    %* PART 2A: SCDM PROVIDER                                                                   *;

    %* Create list of non-missing ProviderID                                                    *;
    %* Add default values for Specialty and Specialty_CodeType                                  *;
    data &OUTDS_scdm_pvd;
        set providerid_crosswalk (where=(not missing(ProviderID))
                                  drop=Orig_Provider
                                  );
        length Specialty $2 Specialty_CodeType $1;
        Specialty="99";
        Specialty_CodeType="2";
    run;

    %* Sort in required order                                                                   *;
    %assign_sort_order(INDS=&OUTDS_scdm_pvd, LIST_SORTEDBY=ProviderID);

    %* PART 2B: SCDM FACILITY                                                                   *;

    %* Create list of non-missing FacilityID                                                    *;
    %* Add Facility_Location with missing value for all records                                 *;
    data &OUTDS_scdm_fac;
        set facilityid_crosswalk (where=(not missing(FacilityID))
                                  drop=Orig_Facility_Code
                                  );
        length Facility_Location $1;
        call missing(Facility_Location);
    run;

    %* Sort in required order                                                                   *;
    %assign_sort_order(INDS=&OUTDS_scdm_fac, LIST_SORTEDBY=FacilityID);

    %* PART 3: SCDM DISPENSING                                                                  *;

    %* Get length of ProviderID variable from the Provider table                                *;
    data _null_;
        dsid=open("&OUTDS_scdm_pvd");
        num=varnum(dsid,"providerid");
        call symput("PROVIDERID_LENGTH",trim(left(put(varlen(dsid,num),best.))));
        rc=close(dsid);
    run;

    %* Exclude patients having 0 records in the SCDM Enrollment table                           *;
    %* Replace character PatID with numeric PatID                                               *;
    %* Add ProviderID                                                                           *;
    %* Rename NDC to Rx and add Rx_CodeType                                                     *;
    proc sql noprint;
        create table &OUTDS_scdm_dis as
        select distinct  b.PatID
                       , .U length=&PROVIDERID_LENGTH as ProviderID
                       , a.RxDate
                       , a.NDC as Rx
                       , "ND" length=2 as Rx_CodeType
                       , a.RxSup
                       , a.RxAmt
        from  &INDS_temp_dis    as a
            , patid_crosswalk   as b
        where   a.PatID = b.Orig_PatID
        ;
    quit;

    %* Assign maximum-necessary length to Rx                                                    *;
    %assign_max_varlength(INDS=&OUTDS_scdm_dis, VAR=Rx, OUTDS=&OUTDS_scdm_dis);

    %* Sort in required order                                                                   *;
    %assign_sort_order(INDS=&OUTDS_scdm_dis, LIST_SORTEDBY=PatID RxDate Rx_CodeType Rx ProviderID);

    %* PART 4: SCDM DIAGNOSIS                                                                   *;

    %* Concatenate the OP, CAR, and IP Temp Diagnosis tables                                    *;
    %* Exclude records outside of 1/1/2008 - 12/31/2010                                         *;
    data _temp_scdm_dia;
        set &INDS_op_temp_dia
            &INDS_car_temp_dia
            &INDS_ip_temp_dia
            ;
        where ADate between "01JAN2008"d and "31DEC2010"d;
    run;

    %* Exclude patients having 0 records in the SCDM Enrollment table                           *;
    %* Replace character PatID with numeric PatID                                               *;
    %* Replace character EncounterID with numeric EncounterID                                   *;
    %* Replace character Provider with numeric ProviderID                                       *;
    proc sql noprint;
        create table &OUTDS_scdm_dia as
        select distinct  b.PatID
                       , c.EncounterID
                       , a.ADate
                       , d.ProviderID
                       , a.EncType
                       , a.DX
                       , a.DX_CodeType
                       , a.OrigDX
                       , a.PDX
                       , a.PAdmit
        from       _temp_scdm_dia        as a
        inner join patid_crosswalk       as b
        on a.PatID = b.Orig_PatID
        left join  encounterid_crosswalk as c
        on a.EncounterID = c.Orig_EncounterID
        left join  providerid_crosswalk  as d
        on a.Provider = d.Orig_Provider
        ;
    quit;

    %* Assign maximum-necessary length to Dx                                                    *;
    %assign_max_varlength(INDS=&OUTDS_scdm_dia, VAR=Dx, OUTDS=&OUTDS_scdm_dia);

    %* Sort in required order                                                                   *;
    %assign_sort_order(INDS=&OUTDS_scdm_dia, LIST_SORTEDBY=PatID ADate);

    proc datasets nolist lib=work;
        delete _temp_scdm_dia;
    quit;

    %* PART 5: SCDM PROCEDURE                                                                   *;

    %* Concatenate the OP, CAR, and IP Temp Procedure tables                                    *;
    %* Exclude records outside of 1/1/2008 - 12/31/2010                                         *;
    data _temp_scdm_pro;
        set &INDS_op_temp_pro
            &INDS_car_temp_pro
            &INDS_ip_temp_pro
            ;
        where ADate between "01JAN2008"d and "31DEC2010"d;
    run;

    %* Exclude patients having 0 records in the SCDM Enrollment table                           *;
    %* Replace character PatID with numeric PatID                                               *;
    %* Replace character EncounterID with numeric EncounterID                                   *;
    %* Replace character Provider with numeric ProviderID                                       *;
    proc sql noprint;
        create table &OUTDS_scdm_pro as
        select distinct  b.PatID
                       , c.EncounterID
                       , a.ADate
                       , d.ProviderID
                       , a.EncType
                       , a.PX
                       , a.PX_CodeType
                       , a.OrigPX
        from       _temp_scdm_pro        as a
        inner join patid_crosswalk       as b
        on a.PatID = b.Orig_PatID
        left join  encounterid_crosswalk as c
        on a.EncounterID = c.Orig_EncounterID
        left join  providerid_crosswalk  as d
        on a.Provider = d.Orig_Provider
        ;
    quit;

    %* Assign maximum-necessary length to Px                                                    *;
    %assign_max_varlength(INDS=&OUTDS_scdm_pro, VAR=Px, OUTDS=&OUTDS_scdm_pro);

    %* Sort in required order                                                                   *;
    %assign_sort_order(INDS=&OUTDS_scdm_pro, LIST_SORTEDBY=PatID ADate);

    proc datasets nolist lib=work;
        delete _temp_scdm_pro;
    quit;

    %* PART 6: SCDM DEATH                                                                       *;

    %* Exclude patients having 0 records in the SCDM Enrollment table                           *;
    %* Replace character PatID with numeric PatID                                               *;
    proc sql noprint;
        create table &OUTDS_scdm_death as
        select distinct  b.PatID
                       , a.DeathDt
                       , a.Dtimpute
                       , a.Source
                       , a.Confidence
        from  &INDS_temp_death  as a
            , patid_crosswalk   as b
        where   a.PatID = b.Orig_PatID
        ;
    quit;

    %* Sort in required order                                                                   *;
    %assign_sort_order(INDS=&OUTDS_scdm_death, LIST_SORTEDBY=PatID);

    %* PART 7: SCDM ENROLLMENT                                                                  *;

    %* Replace character PatID with numeric PatID                                               *;
    proc sql noprint;
        create table _temp_scdm_enr as
        select distinct  b.PatID
                       , a.Enr_Start
                       , a.Enr_End
                       , a.MedCov
                       , a.DrugCov
                       , a.Chart
        from  &INDS_scdm_enr    as a
            , patid_crosswalk   as b
        where a.PatID = b.Orig_PatID
        ;
    quit;

    data &OUTDS_scdm_enr;
        set _temp_scdm_enr;
    run;

    %* Sort in required order                                                                   *;
    %assign_sort_order(  INDS=&OUTDS_scdm_enr
                       , LIST_SORTEDBY=PatID Enr_Start Enr_End MedCov DrugCov Chart
                       );

    %* PART 8: FINISH UP                                                                        *;

    %* Remove any remaining variable labels from all final tables, including SCDM Enrollment    *;

    %let ds_list=   &INDS_scdm_enr  &OUTDS_scdm_dem &OUTDS_scdm_dis &OUTDS_scdm_enc 
                    &OUTDS_scdm_dia &OUTDS_scdm_pro &OUTDS_scdm_death   
                    ;
    %do j=1 %to %sysfunc(countw(&ds_list,,s));
        %let ds=%substr(%scan(&ds_list,&j,,s),6);

            proc datasets nolist lib=scdm;
                modify &ds;
                attrib _all_ label=" ";
            quit;
    %end;

    %put ## PROGRAM NOTE: THE FINAL_PROCESSING MACRO HAS ENDED ##;

%mend FINAL_PROCESSING;

