dm 'log;clear;out;clear';
%************************************************************************************************;
%* PROGRAM NAME:    synpuf_export.sas                                                           *;
%* VERSION:         1.0.0                                                                       *;
%* CREATED:         05/02/2019                                                                  *;
%* LAST MODIFIED:   06/03/2019                                                                  *;
%*----------------------------------------------------------------------------------------------*;
%* PURPOSE                                                                                      *;
%*  This program is used to export the Sentinel Common Data Model (SCDM) tables based on the    *;
%*  Medicare Claims Synthetic Public Use Files (SynPUFs) from SAS datasets to the following     *;
%*  file formats:                                                                               *;
%*      - .csv (comma-separated values text file)                                               *;
%*      - .txt (tab-separated text file)                                                        *;
%*      - .json (JavaScript Object Notation file)                                               *;
%*----------------------------------------------------------------------------------------------*;
%* DEPENDENCIES                                                                                 *;
%*  - The input SynPUFs SCDM datasets from all twenty subsamples must be stored in a single     *;
%*    directory location.                                                                       *;
%*----------------------------------------------------------------------------------------------*;
%* CONTACT INFO                                                                                 *;
%*  Sentinel System Coordinating Center                                                         *;
%*  info@sentinelsystem.org                                                                     *;
%*----------------------------------------------------------------------------------------------*;
%* CHANGE LOG:                                                                                  *;
%*                                                                                              *;
%*  Version        Date       Initials  Comment                                                 *;
%*  -------------  ---------- --------  --------------------------------------------------------*;
%*  1.0.0          06/03/2019 DC        - initial release                                       *;
%************************************************************************************************;


%* -----            ----------------------------------------------------------------------------*;
%* ----- USER INPUT ----------------------------------------------------------------------------*;
%* -----            ----------------------------------------------------------------------------*;

%* All of the following user parameters are required                                            *;

%* To export the SynPUFs SCDM                                                                   *;

%* Enter the location of the input data directory containing the SynPUFs subsample datasets     *;
%* Use forward slashes for UNIX compatibility, and be sure to include the closing slash         *;
%* Example: %let indata_dir = //sentinel/synpufs/                                               *;
%let indata_dir =  ;

%* Enter the location of the output data directory                                              *;
%* Use forward slashes for UNIX compatibility, and be sure to include the closing slash         *;
%* Example: %let outdata_dir = //sentinel/synpufs/export/                                       *;
%let outdata_dir =  ;

%* To export any number of subsamples in sequential order, enter FN-LN, where FN = first        *;
%* subsample number and LN = last subsample number                                              *;
%* Example: %let subsamples = 5-12                                                              *;
%* To export only one subsample or a list of subsamples that are not in sequential order, enter *;
%* a space-delimited list of subsample numbers between 1 and 20                                 *;
%* Example: %let subsamples = 1 3 5 7 9                                                         *;
%let subsamples =  ;

%* Enter a space-delimited list of SCDM tables to be exported using all lowercase letters       *;
%* SynPUFs SCDM is limited to the following tables:                                             *;
%*      - demographic enrollment dispensing encounter diagnosis procedure death                 *;
%* Example: %let table_list = dispensing diagnosis procedure                                    *;
%let table_list =  ;

%* For the following, enter Y to export to the specified format, or enter N to skip             *;
%let export_csv =  ;  %* comma-delimited text file (.csv)                                      *;
%let export_txt =  ;  %* tab-delimited text file (.txt)                                        *;
%let export_json =  ; %* javascript object notation file (.json)                               *;

%* To export the SynPUFs SCDM descriptive stats                                                 *;

%* Enter Y to export the descriptive statistics file to each format, or enter N to skip         *;
%let export_descr_stats =  ;

%* If export_descr_stats = Y, enter the directory location containing the descriptive           *;
%* statistics dataset.                                                                          *;
%* Use forward slashes for UNIX compatibility, and be sure to include the closing slash         *;
%* Example: %let descstat_dir = //sentinel/synpufs/scdm1/                                       *;
%let descstat_dir =  ;


%*----------------------------------------------------------------------------------------------*;
%* DO NOT ALTER CODE BELOW THIS LINE                                                            *;
%*----------------------------------------------------------------------------------------------*;

%*----------------------------------------------------------------------------------------------*;
%* Standard SOC environment setup code -- DO NOT EDIT                                           *;
%*----------------------------------------------------------------------------------------------*;
%* System options                                                                               *;
options nosymbolgen nomlogic;
options ls=100 nocenter ;
options obs=MAX ;
options msglevel=i ;
options mprint mprintnest ;
options errorcheck=strict errors=0 ;
options merror serror ;
options dkricond=error dkrocond=error mergenoby=warn;
options dsoptions=nonote2err noquotelenmax ;
options reuse=no ;
options fullstimer ;
options missing = .;
options validvarname = v7;


%*----------------------------------------------------------------------------------------------*;
%* Macro to export SynPUFs SCDM tables to selected file formats                                 *;
%*----------------------------------------------------------------------------------------------*;

%macro synpuf_export();

    %if %index(&subsamples,-)>0 %then %do;
        %let first_subsample=%scan(&subsamples,1,-);
        %let last_subsample=%scan(&subsamples,2,-);
        %do smp=&first_subsample %to &last_subsample;
            %if &smp=&first_subsample %then %do;
                %let subsample_list= &smp;
            %end;
            %else %do;
                %let subsample_list= &subsample_list &smp;
            %end;
        %end;
    %end;
    %else %do;
        %let subsample_list=&subsamples;
    %end;

    %* Assign input data library                                                                *;
    libname scdm "&indata_dir" access=readonly;

    %do i=1 %to %sysfunc(countw(&subsample_list));
        %let subsample=%scan(&subsample_list, &i);
        %let subsample=&subsample; %* Defensive coding *;
        %* Output requested tables and formats                                                  *;
        %do t=1 %to %sysfunc(countw(&table_list));
            %let tbl=%scan(&table_list,&t);
            %let tbl_ref=scdm.&tbl._&subsample. ;
            %* Set the output subdirectory based on table                                       *;
            %if &tbl=diagnosis or &tbl=procedure %then %do;
                %let sub_dir=&tbl._&subsample.;
            %end;
            %else %do;
                %let sub_dir=subsamples_&subsample.;
            %end;
            %* Export to selected file formats                                                  *;
            %if %upcase(&export_json) = Y %then %do;
                %* Create subsample json subdirectory if not already existing                   *;
                options dlcreatedir;
                libname tempsub "&outdata_dir./&sub_dir._json";
                libname tempsub clear;
                options nodlcreatedir;
                %* Export to json                                                               *;
                proc json out="&outdata_dir./&sub_dir._json/&tbl._&subsample..json";
                    export &tbl_ref.;
                quit;
            %end;
            %if %upcase(&export_csv) = Y %then %do;
                %* Create subsample csv subdirectory if not already existing                    *;
                options dlcreatedir;
                libname tempsub "&outdata_dir./&sub_dir._csv";
                libname tempsub clear;
                options nodlcreatedir;
                %* Export to csv                                                                *;
                proc export data=&tbl_ref.
                            outfile="&outdata_dir./&sub_dir._csv/&tbl._&subsample..csv"
                            dbms=csv
                            replace
                            ;
                quit;
            %end;
            %if %upcase(&export_txt) = Y %then %do;
                %* Create subsample txt subdirectory if not already existing                    *;
                options dlcreatedir;
                libname tempsub "&outdata_dir./&sub_dir._txt";
                libname tempsub clear;
                options nodlcreatedir;
                %* Export to txt                                                                *;
                proc export data=&tbl_ref.
                            outfile="&outdata_dir./&sub_dir._txt/&tbl._&subsample..txt"
                            dbms=tab
                            replace
                            ;
                quit;
            %end;
        %end;
    %end;

%mend synpuf_export;

%synpuf_export();

%macro synpuf_descr_stats_export();

    %if %upcase(&export_descr_stats)=Y %then %do;

        %* Set input library and verify input dataset exists                                    *;
        %let useit=0;
        libname descstat "&descstat_dir";
        %let useit=%sysfunc(exist(descstat.descriptive_stats));

        %if &useit=1 %then %do;
            %* Create descriptive_stats json subdirectory if not already existing               *;
            options dlcreatedir;
            libname tempsub "&outdata_dir./descriptive_stats_json";
            libname tempsub clear;
            options nodlcreatedir;
            %* Output descriptive stats to json                                                 *;
            proc json out="&outdata_dir./descriptive_stats_json/descriptive_stats.json";
                export descstat.descriptive_stats;
            quit;
            %* Create descriptive_stats csv subdirectory if not already existing                *;
            options dlcreatedir;
            libname tempsub "&outdata_dir./descriptive_stats_csv";
            libname tempsub clear;
            options nodlcreatedir;
            %* Output descriptive stats to csv                                                  *;
            proc export data=descstat.descriptive_stats
                        outfile="&outdata_dir./descriptive_stats_csv/descriptive_stats.csv"
                        dbms=csv
                        replace
                        ;
            quit;
            %* Create descriptive_stats txt subdirectory if not already existing                *;
            options dlcreatedir;
            libname tempsub "&outdata_dir./descriptive_stats_txt";
            libname tempsub clear;
            options nodlcreatedir;
            %* Output descriptive stats to txt                                                  *;
            proc export data=descstat.descriptive_stats
                        outfile="&outdata_dir./descriptive_stats_txt/descriptive_stats.txt"
                        dbms=tab
                        replace
                        ;
            quit;
        %end;
        %else %do;
            %put NOTE: Descriptive stats dataset was not found;
            %put NOTE: Descriptive stats could not be exported;
        %end;

    %end;
    %else %do;
        %put NOTE: Descriptive stats will not be exported;
    %end;

%mend synpuf_descr_stats_export;

%synpuf_descr_stats_export();
