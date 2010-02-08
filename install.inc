<?php
// $Id$

// Oracle specific install functions

class DatabaseTasks_oracle extends DatabaseTasks {
  protected $pdoDriver = 'oci';
  
  public function name() {
    return 'Oracle';
  }
  
  public function __construct() {
    $this->tasks = array_merge(array(array(
      'function' => 'initializeDatabase',
      'arguments' => array(),
    )),$this->tasks);
    syslog(LOG_ERR,"CONSTRUCT");
  }  
  
  
  private function oracle_query($sql, $args=NULL) {
    return Database::getConnection('default')->oracleQuery($sql,$args);
  }
  
  private function failsafe_ddl($ddl) {
     $this->oracle_query("begin execute immediate '$ddl'; exception when others then null; end;");
  }
  
  /**
   * Make Oracle Drupal friendly.
   */  
  public function initializeDatabase() {
  	
  	syslog(LOG_ERR,"INITIALIZING");
    try {
    	
      $this->failsafe_ddl("create table blobs (blobid number primary key, content blob, hash varchar2(32))"); // used to store blob values
      $this->failsafe_ddl("create unique index idx_blobs_hash on blobs (hash)"); // used to index blob hash
      $this->failsafe_ddl("create sequence seq_blobs"); // blobid primary key sequence
      $this->failsafe_ddl("create table blob_column (table_name varchar2(255), column_name varchar2(255))"); // blob column catalog
      $this->failsafe_ddl("create table long_identifiers (id number primary key, identifier varchar2(4000))"); // used to store long identifier catalog
      $this->failsafe_ddl("create sequence seq_long_identifiers"); // long_identifiers primary key sequence
      $this->failsafe_ddl("create table used_blobs ( blobid number primary key ) organization index");
/*
      $this->oracle_query("create or replace trigger trg_drop_sequences
before drop
on schema
declare
 v_job number;
begin
   
   for c_col in (select column_name from user_tab_columns where table_name= ora_dict_obj_name and data_type= 'NUMBER') loop
     for c_cur in (select sequence_name from user_sequences where sequence_name = substr('SEQ_'||ora_dict_obj_name||'_'||c_col.column_name,1,30)) loop
      dbms_job.submit(v_job,'begin execute immediate ''drop sequence \"'||c_cur.sequence_name||'\"''; end;');       
     end loop;
   end loop;
   
end trg_drop_sequences;");
  */    
      $this->oracle_query("create or replace package drupal_timestamp
as

   function todate(p_timestamp number)
   return date
   deterministic;
   
   
   function totimestamp(p_date date)
   return number
   deterministic;   
      
end;");

      $this->oracle_query("create or replace package body drupal_timestamp
as

  PHP_BASE_TIME   timestamp:= (from_tz(to_timestamp('01011970000000','DDMMYYYYHH24MISS'), '+00:00') at time zone '+00:00');
  PHP_BASE_DATE   date:= to_date(to_char(PHP_BASE_TIME at time zone '00:00','DDMMYYYYHH24MISS'),'DDMMYYYYHH24MISS') at time zone '00:00';


   function todate(p_timestamp number)
   return date
   deterministic
   as
   begin
     return ((PHP_BASE_TIME at time zone '+00:00')+p_timestamp/86400) at time zone '+00:00';
   end;
   
   
   function totimestamp(p_date date)
   return number
   deterministic
   as
   begin
     return round((p_date-PHP_BASE_DATE)*86400);
   end;
      
end;");
      
      
      $this->oracle_query("create or replace package blobs_cleaner
as

  type num_table is table of number;
  
  function used_blobs
  return num_table
  pipelined;

  procedure cleanup;
  
end blobs_cleaner;");
      
      $this->oracle_query("create or replace package body blobs_cleaner as

  type refcur is ref cursor;
  
  function used_blobs
  return num_table
  pipelined
  as
    c_ids refcur;
    v_num number;
  begin
  
    for c_cur in (select distinct table_name, column_name from blob_column) loop
       
       open c_ids for 'select distinct '||c_cur.column_name||' from '||c_cur.table_name;
       <<tab>>
       loop
          fetch c_ids into v_num;
          exit tab when c_ids%notfound;
          pipe row(v_num);
       end loop;
       close c_ids;
       
    end loop;
  
    return;
  end used_blobs;
  
  procedure cleanup
  as
  begin
  
    lock table blobs in exclusive mode;
    
    begin
     execute immediate 'truncate table used_blobs';
    exception
     when others then
       null;
    end;
    
    insert into used_blobs select distinct column_value blobid from table(blobs_cleaner.used_blobs);
    
    delete blobs a
     where not exists (select 1 from used_blobs where blobid= a.blobid);
     
    commit;
  
  end;

end blobs_cleaner;");
      
      $this->oracle_query("create or replace package identifier
as

  long_identifier_prefix  constant varchar2(10):= '".ORACLE_LONG_IDENTIFIER_PREFIX."';
  identifier_max_length   constant number:= ".ORACLE_IDENTIFIER_MAX_LENGTH.";
  empty_replacer_char      constant varchar2(10):= '".ORACLE_EMPTY_STRING_REPLACER."';
  
  type serial_info is record
  (
      sequence_name    varchar2(30),
      field_name       varchar2(30),
      trigger_name     varchar2(30),
      sequence_restart number
  );
  
  type serial_info_tab is table of serial_info;

  function get_for(p_long_identifier varchar2)
  return varchar2;
  
  function get_long_name(p_long_identifier_ph varchar2)
  return varchar2;

  function sequence_for_table(p_table varchar2, p_schema varchar2)
  return varchar2;

  function get_serial(p_table varchar2, p_schema varchar2)
  return serial_info_tab
  pipelined;
  
  procedure write_blob(p_hash varchar2, p_blobid out number, p_blob out blob);

  function check_db_prefix(p_db_prefix varchar2)
  return varchar2;
    
end identifier;");      
      
      $this->oracle_query("create or replace package body identifier
as

  function get_for(p_long_identifier varchar2)
  return varchar2
  as pragma autonomous_transaction;
   v_id number;
  begin
  
     if length(p_long_identifier) < identifier_max_length+1 then
       return p_long_identifier;
     end if;
     
     select id
       into v_id
       from long_identifiers
      where identifier= upper(p_long_identifier);
     
     return long_identifier_prefix||v_id;
  
  exception
   when no_data_found then

     insert into long_identifiers (id,identifier)
          values (seq_long_identifiers.nextval,upper(p_long_identifier))
       returning id into v_id;
     commit;

     return long_identifier_prefix||v_id;
     
  end;
  
  function get_long_name(p_long_identifier_ph varchar2)
  return varchar2
  as
     v_id          number;
     v_long_name   varchar2(255);
  begin
     v_id:= to_number(substr(p_long_identifier_ph,length(long_identifier_prefix)+1));
    
     select identifier
       into v_long_name
       from long_identifiers
      where id= v_id;
    
     return v_long_name;
  exception
   when others then
      return p_long_identifier_ph;
  end;
  
  
  
  function sequence_for_table(p_table varchar2, p_schema varchar2)
  return varchar2
  as
    v_contraint_oname varchar2(31);
    v_table_oname     varchar2(31):= upper(get_for(p_table));
    v_col_name        varchar2(4000);
    v_sequence_oname  varchar2(31);
    v_schema          varchar2(31):= nvl(upper(p_schema),user);
  begin
     
     select constraint_name
       into v_contraint_oname
       from all_constraints
      where (constraint_name like 'PK_%' or constraint_name like long_identifier_prefix||'%')
        and constraint_type= 'P'
        and table_name= v_table_oname
        and owner= v_schema;
        
     select column_name
       into v_col_name
       from all_cons_columns
      where constraint_name= v_contraint_oname
        and table_name= v_table_oname
        and owner= v_schema;
        
     if instr(v_col_name,long_identifier_prefix) > 0 then
      
       declare
         v_id number;
       begin
         v_id:= to_number(substr(v_col_name,4));
         select identifier
           into v_col_name
           from long_identifiers
          where id= v_id;
       end;
       
     end if;
        
     v_sequence_oname:= get_for('SEQ_'||upper(p_table)||'_'||v_col_name);
     
     select sequence_name
       into v_sequence_oname
       from all_sequences 
      where sequence_name= v_sequence_oname
        and sequence_owner= v_schema;
      
     return '\"'||v_schema||'\".\"'||v_sequence_oname||'\"';
        
  exception
   when others then
     return null;
  end;

  function get_serial(p_table varchar2, p_schema varchar2)
  return serial_info_tab
  pipelined
  as
    v_serial          serial_info;
    v_table_oname     varchar2(30):= upper(get_for(p_table));
    v_schema          varchar2(31):= nvl(upper(p_schema),user);
    v_serial_re       varchar2(255):= '\/\* serial\((\w+),(\w+),(\w+),(\w+)\) \*\/';
  begin
  
     select regexp_replace(s.text,v_serial_re,'\\2'),
            regexp_replace(s.text,v_serial_re,'\\3'),
            regexp_replace(s.text,v_serial_re,'\\4')
       into v_serial.trigger_name,
            v_serial.sequence_name,
            v_serial.field_name
       from (select regexp_substr(text,v_serial_re) text
               from all_source
              where text like '%/* serial('||v_table_oname||',%'
                and line= 1 
                and (name like 'TRG%' or name like long_identifier_prefix||'%')
                and type= 'TRIGGER' 
                and owner= v_schema) s;

     select last_number+1
       into v_serial.sequence_restart
       from all_sequences
      where sequence_name= v_serial.sequence_name
        and sequence_owner= v_schema;       
  
     pipe row(v_serial);  -- WARN: assume 1 serial for table for now seems right
     
     return;
     
  exception
   when others then
      return;
  end;

  procedure write_blob(p_hash varchar2, p_blobid out number, p_blob out blob)
  as
  begin
    select blobid,
           content
      into p_blobid,
           p_blob
      from blobs
     where hash= p_hash;

  exception
   when no_data_found then
    insert into blobs (blobid,content,hash)
         values (seq_blobs.nextval,empty_blob,p_hash)
      returning blobid, content into p_blobid,
                                     p_blob;
  end;
  
  function check_db_prefix(p_db_prefix varchar2)
  return varchar2
  as pragma autonomous_transaction;
    v_db_prefix   varchar2(30):= upper(get_for(p_db_prefix));
  begin
  
     select username
       into v_db_prefix
       from all_users
      where username= v_db_prefix;
      
      return v_db_prefix;
  
  exception
    when no_data_found then
      execute immediate 'grant connect, resource to \"'||v_db_prefix||'\" identified by \"'||v_db_prefix||'\"';
      return v_db_prefix;
  end;
    
end identifier;");     

      // compat functions

      $this->failsafe_ddl("create or replace function greatest(p1 number, p2 number, p3 number default null)
return number 
as
begin
  if p3 is null then
    if p1 > p2 or p2 is null then
     return p1;
    else
     return p2;
    end if;
  else
   return greatest(p1,greatest(p2,p3));
  end if;
end;");
      
      $this->failsafe_ddl("create or replace function rand
return number 
as
begin
  return dbms_random.random;
end;");

      $this->failsafe_ddl("create or replace function md5(p_buff varchar2)
return varchar2
as
begin
  return lower(rawtohex(utl_raw.cast_to_raw(dbms_obfuscation_toolkit.md5(input_string => p_buff))));
end;");
      
      $this->failsafe_ddl("create or replace function substring(p_s varchar2, p_start number, p_length number default null)
return varchar2
as
begin

   if p_length is null then
     return substr(p_s,p_start);
   else
     return substr(p_s,p_start,p_length);
   end if;
   
end;");
      
      $this->failsafe_ddl("create or replace function substring_index(p_buff varchar2, p_delimiter varchar2, p_count number)
return varchar2
as
   v_end number:= 1;
begin
    
    if p_count > 0 then
    
      for i in 1..p_count loop
        v_end:= instr(p_buff, p_delimiter, v_end + 1);
        if v_end = 0 then
          v_end:= length(p_buff);
        end if;
      end loop;
    
      return substr(p_buff, 1, v_end-1);
      
    else
    
      v_end:= length(p_buff);
      
      for i in 1..(p_count*-1) loop
        v_end:= instr(p_buff, p_delimiter, (length(p_buff)-(v_end-2))*-1);
        if v_end = 0 then
          v_end:= length(p_buff);
        end if;
      end loop;
    
      return substr(p_buff, v_end+1);
    
    end if;

end;");
    
    
    }
    catch (Exception $e) {
      syslog(LOG_ERR,$e->getMessage());
      $this->fail(st('Drupal could not be correctly setup with the existing database. Revise any errors.'));
    }
  	
  	$this->pass(st('Oracle has initialized itself.'));
  }
}

