  CREATE OR REPLACE PACKAGE "ORADBA"."DOM$APP_DROP_PM" AS
  --
  PROCEDURE iterator               (p_handle       IN VARCHAR2
                                   ,p_owner        IN VARCHAR2
                                   ,p_table_name   IN VARCHAR2) ;
  PROCEDURE initial                (p_handle       IN VARCHAR2);
  PROCEDURE final                  (p_handle       IN VARCHAR2
                                  , p_after_error  IN CHAR DEFAULT 'N');
  --
  PROCEDURE  create_exchange_table  (p_handle IN VARCHAR2);
  PROCEDURE  exchange_partition     (p_handle IN VARCHAR2);
  PROCEDURE  drop_partition         (p_handle IN VARCHAR2);
  PROCEDURE  repopulate_table       (p_handle IN VARCHAR2);
  PROCEDURE  drop_exchange_table    (p_handle IN VARCHAR2);
  PROCEDURE  drop_tablespace        (p_handle IN VARCHAR2);
  --
  -- PROCEDURE  test_load  ;


END DOM$APP_DROP_PM;
/




CREATE OR REPLACE PACKAGE BODY "ORADBA"."DOM$APP_DROP_PM" is

c_EXCH_TABLE_PREFIX       CONSTANT   VARCHAR(5):='X$';
c_REINSERT_PARALLEL       CONSTANT   CHAR:='2';
c_NO                      CONSTANT   CHAR:='N';
c_YES                     CONSTANT   CHAR:='Y';

g_first_purge_month     DATE;
g_partition_position    NUMBER;
sql_str                 VARCHAR2(31000);
g_owner                 VARCHAR2(30);
g_exchange_table        VARCHAR2(50);
g_table_tablespace      VARCHAR2(30);
g_index_tablespace      VARCHAR2(30);
g_partition_name        VARCHAR2(30);
g_insert_partition      VARCHAR2(30);
g_table_name            VARCHAR2(30);
g_purge_month           DATE;
g_handle                NUMBER;

g_max_month_purged_param NUMBER;
g_trailing_months_param  NUMBER;



default_max_months_purged   NUMBER:=1;

--==============================================================================
--
-- Raises:
--==============================================================================
PROCEDURE  save_state  (p_handle   IN VARCHAR2) IS
BEGIN

  --DELETE FROM state$kenan_drop_pm WHERE run_id=TO_NUMBER(p_handle);
  
  INSERT INTO state$kenan_drop_pm(
    RUN_ID
   ,PARTITION_NAME
   ,TABLE_TABLESPACE
   ,EXCHANGE_TABLE
   ,TABLE_OWNER
   ,TABLE_NAME
   ,INSERT_PARTITION
   ,EXCHANGED
   ,REINSERTED
   ,TABLE_DROPPED
   ,TABLESPACE_DROPPED
   ,PARTITION_POSITION
   ,TABLE_CREATED
   ,PARTITION_DROPPED
   ,EXCHANGE_TABLE_DROPPED
     )
     VALUES
     (
       TO_NUMBER(p_handle)
      ,g_partition_name
      ,g_table_tablespace
      ,g_exchange_table
      ,g_owner
      ,g_table_name
      ,g_insert_partition
      ,c_NO,c_NO,c_NO,c_NO,g_partition_position,c_NO,c_NO,c_NO);
      
   COMMIT;
   
END save_state;
--==============================================================================
--
-- Raises:
--==============================================================================
PROCEDURE  get_state (p_handle IN VARCHAR2) IS
BEGIN
  DOM.initialise(p_handle);
  SELECT
    PARTITION_NAME
   ,TABLE_TABLESPACE
   ,EXCHANGE_TABLE
   ,TABLE_OWNER
   ,TABLE_NAME
   ,INSERT_PARTITION
   INTO g_partition_name,g_table_tablespace, g_exchange_table
   ,g_owner,g_table_name ,g_insert_partition
 FROM  state$kenan_drop_pm
 WHERE run_id=TO_NUMBER(p_handle);
END get_state;
--==============================================================================
--
--
-- Raises:
--==============================================================================
PROCEDURE operation_complete (p_message   IN  VARCHAR2)
AS
BEGIN
  COMMIT;  -- this commit prevents ORA-2046, distributed transcation allready begun on MAIN db.
  raise_application_error(-20100,p_message);
END operation_complete;
--==============================================================================
--
-- Raises:
--==============================================================================
PROCEDURE  initial (p_handle  IN  VARCHAR2) IS
BEGIN
  DOM.initialise(p_handle);
  get_state(p_handle);
  --
  IF g_table_name = 'CDR_DATA'
  THEN 
     DOM.execute_immediate(p_handle,'BEGIN cdc_publish_user.dba_cdc_admin.enable_delete; END;');
  END IF;
  -- 
END initial;
--==============================================================================
--
-- Raises:
--==============================================================================
PROCEDURE  final (p_handle IN VARCHAR2, p_after_error IN CHAR DEFAULT 'N') IS
BEGIN
  DOM.initialise(p_handle);
  get_state(p_handle);
  --
  IF g_table_name = 'CDR_DATA'
  THEN 
      DOM.execute_immediate(p_handle,'BEGIN cdc_publish_user.dba_cdc_admin.disable_delete; END;');
  END IF;
  --
  -- clear state when completed i.e p_afte_error is No.
  --
  IF p_after_error = 'N' 
  THEN
    DELETE FROM state$kenan_drop_pm WHERE run_id=TO_NUMBER(p_handle);
    COMMIT;
  END IF;
  
END final;
--==============================================================================
-- REPOPULTE_TABLE
--
-- Raises:
--=============================================================================
PROCEDURE  create_exchange_table (p_handle IN VARCHAR2)  AS
                                  
table_exists       EXCEPTION;
PRAGMA EXCEPTION_INIT(table_exists,-955);
BEGIN
    DOM.initialise(p_handle);
    get_state(p_handle);
 
    sql_str := 'create table '||g_exchange_table||' tablespace '|| g_table_tablespace;
    sql_str := sql_str||' as select * from '||g_owner||'.'||g_table_name||' where 1=2  ';
        
    DOM.execute_immediate(p_handle, sql_str);
    
    COMMIT; 


END create_exchange_table;
--==============================================================================
-- EXCHANGE_PARTITION
--
-- Raises:
--==============================================================================
PROCEDURE  exchange_partition  (p_handle IN VARCHAR2 ) AS
dummy_          NUMBER;
BEGIN

    DOM.initialise(p_handle);
    get_state(p_handle);
    BEGIN
    sql_str:='SELECT 1 FROM '||g_exchange_table||' WHERE ROWNUM<2'; 
    EXECUTE IMMEDIATE sql_str INTO dummy_;
    -- exchange must have taken place through manual intervention or instance crashed before state updated
    COMMIT;  
    EXCEPTION
    WHEN no_data_found THEN
        -- exchange has not taken place    
       sql_str:='alter table '||g_owner||'.'||g_table_name||' exchange partition '||g_partition_name;
       sql_str:=sql_str||' with table '||g_exchange_table;
       sql_str:=sql_str||' excluding indexes without validation update indexes';

       DOM.execute_immediate(p_handle, sql_str);
       
       COMMIT;
    END ;


END exchange_partition;
--==============================================================================
-- REPOPULTE_TABLE
--
-- Raises:
--==============================================================================
FUNCTION   segment_contains_data (p_owner_table_name IN VARCHAR2) RETURN BOOLEAN
AS
dummy_    NUMBER;
BEGIN
    sql_str:='SELECT 1 FROM '||p_owner_table_name||' WHERE ROWNUM < 2';
    EXECUTE IMMEDIATE sql_str INTO dummy_;
    
    RETURN TRUE;
    
EXCEPTION
WHEN no_data_found THEN
  RETURN FALSE;
END segment_contains_data;
--==============================================================================
-- REPOPULTE_TABLE
--
-- Raises:
--==============================================================================
FUNCTION   segment_contains_data (p_owner          VARCHAR2
                                , p_table_name     VARCHAR2
                                , p_partition_name VARCHAR2 DEFAULT NULL) RETURN BOOLEAN
AS
dummy_    NUMBER;
BEGIN
    sql_str:='SELECT 1 FROM '||p_owner||'.'||p_table_name;
    IF p_partition_name IS NOT NULL
    THEN
       sql_str:=sql_str||' partition ('||p_partition_name||')'; 
    END IF;
    sql_str:=sql_str||' WHERE ROWNUM < 2';
    EXECUTE IMMEDIATE sql_str INTO dummy_;
    
    RETURN TRUE;
    
EXCEPTION
WHEN no_data_found THEN
  RETURN FALSE;
END segment_contains_data;
--==============================================================================
-- REPOPULTE_TABLE
--
-- Raises:
--=============================================================================
PROCEDURE  drop_partition  (p_handle IN VARCHAR2) AS
dummy_      NUMBER;

partition_does_not_exist       EXCEPTION;
PRAGMA EXCEPTION_INIT(partition_does_not_exist,-2149);
BEGIN
    DOM.initialise(p_handle);
    get_state(p_handle);
    --
    -- check if partition exists
    --
    sql_str:='SELECT count(*) FROM '||g_owner||'.'||g_table_name||' partition ('||g_partition_name||') WHERE ROWNUM<2';   
    EXECUTE IMMEDIATE sql_str INTO dummy_;
    --
    -- check partition is empty as it must be after an exchange
    --
    IF segment_contains_data( g_owner, g_table_name, g_partition_name)
    THEN
          raise_application_error(-20200,'Error: inconsistency, partition to be dropped has data');
    END IF;
    
    sql_str:='alter table '||g_owner||'.'||g_table_name||' drop partition '||g_partition_name;
    
    DOM.execute_immediate(p_handle, sql_str);    
 
END drop_partition;
--==============================================================================
-- REPOPULTE_TABLE
--
-- Raises:
--==============================================================================
FUNCTION get_reinsert_parallel_param (p_handle IN VARCHAR2,p_table_name  IN VARCHAR2) RETURN VARCHAR2
IS
BEGIN
  RETURN DOM.get_string_param(p_handle,'APP PARALLEL REINSERT');
  EXCEPTION
  WHEN no_data_found THEN
    RETURN c_REINSERT_PARALLEL;
  WHEN others THEN
    RAISE;
END get_reinsert_parallel_param;
--==============================================================================
-- REPOPULTE_TABLE
--
-- Raises:
--==============================================================================
FUNCTION get_insert_hint_param (p_handle  IN VARCHAR2, p_table_name IN VARCHAR2) RETURN NUMBER IS
BEGIN

  RETURN DOM.get_string_param(p_handle,'INSERT HINT');
  EXCEPTION
  WHEN no_data_found THEN
    RETURN 'APPEND';
  WHEN others THEN
    RAISE;
END get_insert_hint_param;
--==============================================================================
-- REPOPULTE_TABLE
--
-- Raises:
--==============================================================================
PROCEDURE  repopulate_table  (p_handle IN VARCHAR2) 
AS
parallel_    CHAR;
insert_hint_ VARCHAR2(40);
read_hint_   VARCHAR2(40):='/*+ parallel(b,2) */';
db_name_     VARCHAR2(30);
predicate_   DOM$parameters.value%TYPE;
pctfree_     DOM$parameters.value%TYPE;
dummy_       CHAR;
table_does_not_exist       EXCEPTION;
PRAGMA EXCEPTION_INIT(table_does_not_exist,-942);

BEGIN
-- don't disable indexes on partition as this will/may impact queries performed during the load
    DOM.initialise(p_handle);
    get_state(p_handle);
    -- build hint 
    parallel_:=get_reinsert_parallel_param(p_handle,g_table_name);
    IF TO_NUMBER(parallel_) > 1 
    THEN 
      insert_hint_:=' /*+ APPEND parallel(A,'||parallel_||')*/' ;
      read_hint_  :=' /*+ parallel(B,'||parallel_||')*/' ;
    ELSE
      insert_hint_:=get_insert_hint_param(p_handle,g_table_name);
      read_hint_  :=' /*+ parallel(B,2)*/' ;
    END IF;
    --
    BEGIN
      predicate_:=DOM.get_string_param(p_handle,'PREDICATE');
      EXCEPTION
      WHEN others THEN
        predicate_:=NULL;
    END;
    --
    sql_str:='INSERT '||insert_hint_||' INTO '||lower(g_owner)||'.'||lower(g_table_name);
    sql_str:=sql_str||' PARTITION ('||g_insert_partition||') A ';
    sql_str:=sql_str||' SELECT '||read_hint_||' * FROM '||lower(g_exchange_table)||' B ' ||predicate_;
    --
    -- EXECUTE IMMEDIATE  'alter table '||lower(g_owner)||'.'||lower(g_table_name)||' nologging';
    --
    --  Enable compression if not enabled
    --
    BEGIN
    SELECT 'a' INTO dummy_
    FROM DBA_TAB_PARTITIONS
    WHERE table_owner    = g_owner
     AND  table_name     = g_table_name
     AND  partition_name = g_insert_partition
     AND  compression   != 'ENABLED';
    --
    DOM.execute_immediate(p_handle,'alter table '||lower(g_owner)||'.'||lower(g_table_name)||' modify partition '||g_insert_partition||' COMPRESS');
    --
    EXCEPTION
    WHEN no_data_found THEN
      NULL;
    END;
    --
    --  Set pctfree
    --
    BEGIN
    pctfree_:=DOM.get_string_param(p_handle,'PCTFREE');
    EXCEPTION
    WHEN others THEN
        pctfree_:='2';
    END;
    BEGIN
    SELECT 'a' INTO dummy_
    FROM DBA_TAB_PARTITIONS
    WHERE table_owner    = g_owner
     AND  table_name     = g_table_name
     AND  partition_name = g_insert_partition
     AND  PCT_FREE  != to_number(pctfree_);
    --
    DOM.execute_immediate(p_handle,'alter table '||lower(g_owner)||'.'||lower(g_table_name)||' modify partition '||g_insert_partition||' PCTFREE '||pctfree_);
    --
    EXCEPTION
    WHEN no_data_found THEN
      NULL;
    END;
    --
    -- insert
    --
    DOM.execute_immediate(p_handle, sql_str,'alter session enable parallel DML');
   
    COMMIT;
    
END repopulate_table;
--==============================================================================
-- submit REPOPULTE_TABLE job
--
-- Raises:
--==============================================================================
PROCEDURE  repopulate_table_ (p_handle IN VARCHAR2) 
AS
BEGIN
  -- job_action=>'BEGIN DOM$kenan_drop_pm.repopulate_table_job('''||p_handle||''','''||p_mode||'''); END;',

  DBMS_SCHEDULER.CREATE_JOB (
   job_name=>'TEST',  -- make unique
   job_type=>'PLSQL_BLOCK',
   job_action=>'BEGIN DOM$kenan_drop_pm.test_load; END;',
   number_of_arguments=> 0,
   start_date=> NULL,  -- run as soon as its enabled, as in now..
   repeat_interval=>NULL, -- run once
   end_date=> NULL,
   job_class=>'DEFAULT_JOB_CLASS',
   enabled=>TRUE,  -- run now..
   auto_drop=>TRUE, -- drop meta data associated with job   
   comments=>'A DOME submitted job');

END  repopulate_table_;

--==============================================================================
-- EXCHANGE_PARTITION
--
-- Raises:
--==============================================================================
PROCEDURE  drop_exchange_table (p_handle IN VARCHAR2 ) AS
rid_     ROWID;
table_does_not_exist       EXCEPTION;
PRAGMA EXCEPTION_INIT(table_does_not_exist,-942);
BEGIN
    DOM.initialise(p_handle);
    get_state(p_handle);
    
    sql_str:='drop table '||g_exchange_table||' purge';
     
    DOM.execute_immediate(p_handle, sql_str);
 
    COMMIT;

END drop_exchange_table;
--==============================================================================
-- REPOPULTE_TABLE
--
-- Raises:
--==============================================================================
FUNCTION  tablespace_contains_segments (p_tablespace IN  VARCHAR2) RETURN BOOLEAN
AS
dummy_    NUMBER;
BEGIN
    sql_str:='SELECT NULL FROM DBA_SEGMENTS WHERE tablespace_name = upper(:p) AND ROWNUM < 2';
    EXECUTE IMMEDIATE sql_str INTO dummy_  USING p_tablespace;
    RETURN TRUE ;  
EXCEPTION
WHEN no_data_found THEN
  RETURN FALSE;
END tablespace_contains_segments;
--==============================================================================
-- REPOPULTE_TABLE
--
-- Raises:
--==============================================================================
PROCEDURE  drop_tablespace  (p_handle IN VARCHAR2) AS
                        
dummy_                          CHAR;
tablespace_does_not_exist       EXCEPTION;
PRAGMA EXCEPTION_INIT(tablespace_does_not_exist,-959);
BEGIN
     DOM.initialise(p_handle);
     get_state(p_handle);
     --
     IF tablespace_contains_segments (g_table_tablespace)
     THEN
       RETURN;
     END IF;
     --
     sql_str:='drop tablespace '||g_table_tablespace||' including contents and datafiles'; 
     DOM.execute_immediate(p_handle, sql_str);
     --
     FOR ind_ IN (SELECT index_tablespace
                 FROM   state$kenan_drop_ind_ts_pm
                 WHERE  partition_position = g_partition_position
                  AND   run_id = TO_NUMBER(p_handle))
     LOOP
      BEGIN
       SELECT 'a' INTO dummy_
       FROM  DBA_TABLESPACES 
       WHERE TABLESPACE_NAME = ind_.index_tablespace;
       --   
       IF tablespace_contains_segments (ind_.index_tablespace)
       THEN
         EXIT;
       END IF; 
       sql_str:='drop tablespace '||ind_.index_tablespace||' including contents and datafiles';
       DOM.execute_immediate(p_handle, sql_str);
       EXCEPTION
       WHEN no_data_found THEN
         NULL; 
      END;
     END LOOP;
--
END drop_tablespace;
--==============================================================================
-- REPOPULTE_TABLE
--
-- Raises:
--==============================================================================
FUNCTION get_trailing_months_param (p_handle  IN VARCHAR2, p_table_name IN VARCHAR2) RETURN NUMBER IS
BEGIN

  RETURN DOM.get_num_param(p_handle,'APP MONTHS TRAILING');

END get_trailing_months_param;
--==============================================================================
-- REPOPULTE_TABLE
--
-- Raises:
--==============================================================================
FUNCTION get_max_months_purged_param (p_handle  IN VARCHAR2) RETURN NUMBER IS
BEGIN
  RETURN DOM.get_num_param(p_handle,'MAX MONTHS PURGED');
  EXCEPTION
  WHEN others THEN
    RETURN default_max_months_purged;
END get_max_months_purged_param;
--==============================================================================
-- GET_PARTITION_NAME
--
-- Raises:
--==============================================================================
FUNCTION get_exch_table_prefix_ (p_handle IN VARCHAR2) RETURN VARCHAR2 IS
BEGIN
  RETURN DOM.get_string_param(p_handle,'EXCHANGE TABLE PREFIX');
EXCEPTION
WHEN others THEN
  RETURN c_EXCH_TABLE_PREFIX;
END get_exch_table_prefix_;
--==============================================================================
-- REPOPULTE_TABLE
--
-- Raises:
--==============================================================================
PROCEDURE iterator  (p_handle       IN VARCHAR2
                    ,p_owner        IN VARCHAR2
                    ,p_table_name   IN VARCHAR2) AS
months_purged_      NUMBER;
p_partition_name    VARCHAR2(30);
DUMMY               CHAR;
date_mask_          VARCHAR2(20);
num_row_limit_      PLS_INTEGER;

inconsistent_datatype       EXCEPTION;
PRAGMA EXCEPTION_INIT(inconsistent_datatype,-932);
BEGIN
  DOM.initialise(p_handle);
  g_handle := to_NUMBER(p_handle);
  g_table_name:=p_table_name;
  g_owner:=p_owner;
  --g_max_month_purged_param:=get_max_months_purged_param(p_handle);
  g_purge_month:=add_months(trunc(sysdate,'MONTH')
                            ,-1*get_trailing_months_param(p_handle,g_table_name)); 
  --num_row_limit_:=DOM.get_num_param(p_handle,'MAX ROW LIMIT');
  --
  FOR hv IN (SELECT partition_position, partition_name, high_value, numrows, min_position, rowsperblock, round(max_rowsperblock) maxrowsperblock
             FROM (
                   SELECT partition_position, partition_name,nvl(num_rows,0) numrows, high_value, round(num_rows/decode(blocks,0,1,blocks)) rowsperblock
                   , max(partition_position) over () max_position
                   , min(partition_position) over () min_position
                   , max(num_rows/decode(blocks,0,1,blocks)) over () max_rowsperblock
                   FROM   dba_tab_partitions
                   WHERE  table_name  = g_table_name
                   AND    table_owner = g_owner
                  ) t
             WHERE t.partition_position!=t.max_position
             ORDER BY  partition_position DESC
)
  LOOP
    BEGIN
    -- determine if partition is less than purge_month
    sql_str:='SELECT NULL FROM dual WHERE' ;
    sql_str:=sql_str||' to_date('||''''||to_char(g_purge_month,'DD-MON-YYYY');
    sql_str:=sql_str||''''||','||'''DD-MON-YYYY'''||')';
    --
    IF g_table_name IN ('CDR_DATA','CDR_BILLED')
    THEN
       -- derive from data dictionay
       sql_str:=sql_str||' < '||hv.high_value;
    ELSE
        -- derive from partition name
        date_mask_:=DOM.get_string_param(p_handle,'PARTITION DATE MASK');
        sql_str:=sql_str||' < to_date(substr('''||hv.partition_name||''',length('''||g_table_name||''')+2),'''||date_mask_||''')';
    END IF;  
    --dbms_output.put_line(sql_str);
    EXECUTE IMMEDIATE sql_str INTO dummy;
    --
    g_insert_partition:=hv.partition_name;
    --
    EXCEPTION
    WHEN no_data_found  THEN
       BEGIN
        -- 
        g_partition_name := hv.partition_name;
        --
        -- Check if worthwhile to rollup last patition. If still too many rows then don't.
        --
        IF hv.partition_position = 1 AND hv.rowsperblock > hv.maxrowsperblock/2
        THEN
           operation_complete('All trailing months purged');
        END IF;
        SELECT tablespace_name INTO g_table_tablespace
        FROM   dba_tab_partitions
        WHERE  table_name     = g_table_name
        AND   partition_name  = g_partition_name
        AND   table_owner     = g_owner;
        --
        FOR ts IN (SELECT DISTINCT tablespace_name
                     FROM   dba_ind_partitions
                     WHERE  index_name   IN (SELECT index_name FROM dba_indexes where table_name = g_table_name and table_owner = g_owner)
                     AND   partition_name = g_partition_name)
        LOOP
               INSERT INTO state$kenan_drop_ind_ts_pm (run_id,partition_position,index_tablespace)
               VALUES (TO_NUMBER(p_handle),hv.partition_position,ts.tablespace_name);
        END LOOP;
        --
        -- Add partition to state table
        --
        g_exchange_table:=g_owner||'.'||get_exch_table_prefix_(p_handle)||g_partition_name;
        g_partition_position:=hv.partition_position;
        --
        save_state(p_handle);
        --
         RETURN;
       END;
   WHEN inconsistent_datatype THEN
        COMMIT;
        raise_application_error(-20109,'ERROR: not suitably partitioned for operation. Table must be partitioned by date');
   END;
  END LOOP;
  
  operation_complete('All trailing months purged');

END iterator;



END DOM$APP_DROP_PM;
/ 
