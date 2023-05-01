options mprint symbolgen ;
%* Program description: ;
%* Program for reading all folder and subfolder sas7bdat and create record count;
%* Create entry of each file record_count.sas7bdat. With below format ;
%* Create one text file for rcdcnt.txt;
%* List of global macro variables, with descriptions: none;
%* List of parameters, with description ;
%* Input_Name : <Study>_<Source>_<blind type> ;
%* Output_Name : file name of files present in folder  ;
%* Number_of_Output_Records: number of records in the file ;

     data sasfile;
     input Source $ Input_Name$ 50. Output_Name $ Number_of_Output_Records;
     Datalines;
     run;
     data allsasfile;
     input Source $ Input_Name$ 50. Output_Name $ Number_of_Output_Records;
     Datalines;
     run;

%macro Data_count(dir, first=Y, outtbl=_memlist);
  %local filrf rc did memcnt name i;  
  %global  optdir sas_path cdts_inst jobid;
  %let optdir =/folders/myfolders/sasuser.v94/SAS Data;
  %let sas_path=/folders/myfolders/sasuser.v94/SAS Data;
  /* Assigns a fileref to the directory and opens the directory */
  %let rc=%sysfunc(filename(filrf,&dir));                                                                                               
  %let did=%sysfunc(dopen(&filrf));
  %let memcount=%sysfunc(dnum(&did));
           %let Input_Name = %scan(&dir,-2,%str(/),%str(b));
           %let study_name = %scan(&dir,-1,%str(/),%str(b));
           %let source_name = %scan(&Input_Name,1,%str(_),%str(b));
  /*Blinded and Unblinded in source name check ;*/
           %if %bquote(&source_name) eq bl or %bquote(&source_name) eq ubl %then %do;
                %let source_name = %scan(&dir,2,%str(_),%str(b));

           %end;

                %else %let source_name = %scan(&Input_Name,1,%str(_),%str(b));
     libname temp1234 "&dir" access=readonly;
/* Data Creation Source Input_Name Output_Name  Number_of_Output_Records with Dictionary*/
     proc sql;
     create table new as
           select distinct upcase("&source_name") as Source length=500,
           "&Input_Name" as Input_Name length=500,
           lowcase(memname) as Output_Name,
           nobs as Number_of_Output_Records
           from dictionary.tables
           where
           libname='TEMP1234'
          ;
     quit;
           proc append base=allsasfile data=new force;
     run;
           libname temp1234 "&dir/SAS" access=readonly;
/* Data Creation Source Input_Name Output_Name  Number_of_Output_Records with Dictionary*/
     proc sql;
     create table new1 as
           select distinct
           lowcase(memname) as Output_Name,
           nobs as Number_of_Output_Records
           from dictionary.tables
           where
           libname='TEMP1234'
          ;
     quit;

     proc append base=sasfile data=new1 force;
     run;
    libname temp1234 clear;
           proc datasets lib=work nolist nowarn;
      delete dummy1 new new1;
    run;

     PROC SQL;
     Create table dummy1 as
     Select y.Source,y.Input_Name,x.Output_Name,x.Number_of_Output_Records from sasfile as x left join allsasfile as y
     on x.Output_Name = y.Output_Name;
     Quit;

           data &outtbl;
           set dummy1;
           run;

   /* Loops through entire directory */ 
   %do i = 1 %to %sysfunc(dnum(&did));                                                                                                  

     /* Retrieve name of each file */

     %let name=%qsysfunc(dread(&did,&i));                                                                                              

     /* If directory name call macro again */                                                                                          

      %if %qscan(&name,2,.) = %then %do;

        data _null_;

          if findc("&dir",'/')>0 then call symputx('slash','/','l');
          else call symputx('slash','\','l');
          stop;
        run;
       %Data_count(&dir.&slash.%unquote(&name), first=N, outtbl=&outtbl);
      %end;                                                                                                                           
          %end;                                                                                                                   

 

  /* Closes the directory and clear the fileref */                                                                                     

  %let rc=%sysfunc(dclose(&did));                                                                                                       

  %let rc=%sysfunc(filename(filrf));    

           /*Creating SAS7bat file for record count */
       Libname out "&optdir";
           data out.record_count;
                  Set Data_count;
                Run;                                                                                        
           /*Creating text file for Java  */
           data read_count_file (keep=Output_Name Number_of_Output_Records Source );
           retain Output_Name Number_of_Output_Records Source ;
           set Data_count;
           Output_Name = lowcase (Output_Name);
           if scan(Input_Name,1,'_','b')='BL' then do;
                Source="BL";
                     end;
           else if scan(Input_Name,1,'_','b')='UBL' then do;
                Source="UBL";
           end;
           else Source = "N/A";
           run;

             /*Exporting text file for Java  */
                proc export Data=read_count_file outfile= "&optdir/rcdcnt.txt"
                dbms=dlm replace ;
                delimiter=",";
                PUTNAMES=NO;
                run;
                
                data _null_;
                infile  "&optdir/rcdcnt.txt" dlm=',' END= eof;
                input;
                file "&optdir/rcdcnt.txt";
                if eof then do;
                data= _infile_ ;
                put data;
                n=trim(right(_N_))||",N/A";
                put "record_count," n ;
                end;
               run;

%mend Data_count;                                                                                                                            

%macro status(dir);

  /* Assigns a fileref to the directory and opens the directory */

  %let rc=%sysfunc(filename(filrf,&dir));                                                                                              

  %let did=%sysfunc(dopen(&filrf));

  %let memcount=%sysfunc(dnum(&did));


  /* Directory Error Handling*/                                                                                                 

     %if &did eq 0 or &memcount eq 0 %then %do;                                                                                                           
     %let err_msg = "SAS Directory cannot be open or does not exist"; 
%end;
           %* update the error Failure;
%mend status;
/* First parameter is the directory of where your table are stored. */                                                                                                                                 

/* Parameter "outtbl" allows to define the name of the output table with the search result  */

%Data_count(&optdir, outtbl=Data_count); 
%status(&optdir);
proc datasets nolist nodetails lib=work kill;
run;