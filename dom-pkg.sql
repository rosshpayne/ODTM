-------------------------------------------------------

  CREATE OR REPLACE PACKAGE "ORADBA"."DOM" AS

  operation_complete           EXCEPTION;

  PROCEDURE initialise      (p_handle   IN VARCHAR2
                           , p_module   IN VARCHAR2 DEFAULT NULL
                           , p_action   IN VARCHAR2 DEFAULT NULL);

  PROCEDURE execute_immediate (  p_handle       IN VARCHAR2
                                ,p_sql_str                IN VARCHAR2
                                ,p_alter_session IN VARCHAR2 DEFAULT NULL);


  FUNCTION get_num_param    (p_handle       IN VARCHAR2
                            ,p_name         IN VARCHAR2
                            ,p_object_name  IN VARCHAR2) RETURN NUMBER;

  FUNCTION get_date_param   (p_handle       IN VARCHAR2
                            ,p_name         IN VARCHAR2
                            ,p_object_name  IN VARCHAR2) RETURN DATE;

  FUNCTION get_string_param (p_handle       IN VARCHAR2
                            ,p_name         IN VARCHAR2
                            ,p_object_name  IN VARCHAR2) RETURN VARCHAR2 ;

  FUNCTION get_num_param    (p_handle       IN VARCHAR2
                            ,p_name         IN VARCHAR2) RETURN NUMBER;

  FUNCTION get_date_param   (p_handle       IN VARCHAR2
                            ,p_name         IN VARCHAR2) RETURN DATE;

  FUNCTION get_string_param (p_handle       IN VARCHAR2
                            ,p_name         IN VARCHAR2) RETURN VARCHAR2;

END DOM;

/
  CREATE OR REPLACE PACKAGE BODY "ORADBA"."DOM" AS
--==============================================================================
--
--
--
--==============================================================================
PROCEDURE initialise      (p_handle   IN VARCHAR2
                         , p_module   IN VARCHAR2 DEFAULT NULL
                         , p_action   IN VARCHAR2 DEFAULT NULL)
AS
operation_    VARCHAR2(30);
task_         VARCHAR2(30);
sql_str_      VARCHAR2(2000);
BEGIN

COMMIT;
IF p_module IS NULL
THEN
   operation_:=DOM$MAIN.get_operation_name@MAIN(p_handle);
ELSE
   operation_:=p_module;
END IF;
IF p_action IS NULL
THEN
   task_:=DOM$MAIN.get_task_name@MAIN(p_handle);
ELSE
   task_:=p_action;
END IF;
COMMIT;

sql_str_:='BEGIN DBMS_APPLICATION_INFO.set_module( module_name => ''DOM.';
sql_str_:=sql_str_||operation_||''',action_name => '''||task_||'''); END;';
--dbms_output.put_line(sql_str_);

EXECUTE IMMEDIATE sql_Str_;

END initialise;
--==============================================================================
--
--
--
--==============================================================================
PROCEDURE execute_immediate (  p_handle       IN VARCHAR2
                              ,p_sql_str                  IN VARCHAR2
                              ,p_alter_session IN VARCHAR2 DEFAULT NULL) IS
--
-- This procedure logs all DDL statements
--
--PRAGMA AUTONOMOUS_TRANSACTION;
errmsg_         VARCHAR2(220);
handle_         VARCHAR2(30);
row_count_      INTEGER;
queries_a_      NUMBER;
dml_a_          NUMBER;
ddl_a_          NUMBER;
threads_a_      NUMBER;
queries_b_      NUMBER;
dml_b_          NUMBER;
ddl_b_          NUMBER;
threads_b_      NUMBER;
run_mode_       CHAR;
BEGIN
   handle_:=p_handle;

   COMMIT;
   DOM$MAIN.pre_execute_immediate@MAIN(handle_,p_sql_str,run_mode_);
   COMMIT;
   IF run_mode_ = 'R' -- running
   THEN
      -- execute on local database
      IF p_alter_session IS NOT NULL
      THEN
          EXECUTE IMMEDIATE p_alter_session;
      END IF;
      EXECUTE IMMEDIATE p_sql_str;
      row_count_:=SQL%rowcount;
      --
      BEGIN
      SELECT last_query INTO queries_b_ FROM v$pq_sesstat WHERE statistic='Queries Parallelized';
      SELECT last_query INTO dml_b_ FROM v$pq_sesstat WHERE statistic='DML Parallelized';
      SELECT last_query INTO ddl_b_ FROM v$pq_sesstat WHERE statistic='DDL Parallelized';
      SELECT last_query INTO threads_b_ FROM v$pq_sesstat WHERE statistic='Server Threads';
      EXCEPTION
      WHEN no_data_found THEN
         NULL;
      END;
      COMMIT;
   END IF;
     DOM$MAIN.post_execute_immediate@MAIN(handle_,row_count_,queries_b_,dml_b_,ddl_b_,threads_b_);
   COMMIT;
   --
   EXCEPTION
   WHEN others THEN
      ROLLBACK;
      dbms_output.put_line('DOM EXECUTE_IMMEDIATE -- ERROORED --');
      errmsg_:='Error '||substr(sqlerrm,1,200);
      DOM$MAIN.rollback_execute_immediate@MAIN(handle_,errmsg_);
      COMMIT;
      RAISE;
END  execute_immediate;

--==============================================================================
--
--
--
--==============================================================================


FUNCTION get_date_param   (p_handle       IN VARCHAR2
                          ,p_name         IN VARCHAR2
                          )                            RETURN DATE
IS
BEGIN
  RETURN DOM$MAIN.get_date_param@MAIN(p_handle,p_name);
END get_date_param;


FUNCTION get_string_param (p_handle       IN VARCHAR2
                          ,p_name         IN VARCHAR2
                          )                            RETURN VARCHAR2
IS
BEGIN
  RETURN DOM$MAIN.get_string_param@MAIN(p_handle, p_name);
END get_string_param;


FUNCTION get_num_param    (p_handle       IN VARCHAR2
                          ,p_name         IN VARCHAR2
                          )                            RETURN NUMBER
IS
BEGIN
  RETURN DOM$MAIN.get_num_param@MAIN(p_handle,p_name);
END get_num_param;


FUNCTION get_num_param    (p_handle       IN VARCHAR2
                          ,p_name         IN VARCHAR2
                          ,p_object_name  IN VARCHAR2
                          )                            RETURN NUMBER
IS
BEGIN
  RETURN DOM$MAIN.get_num_param@MAIN(p_handle,p_name,p_object_name);
END get_num_param;


FUNCTION get_date_param   (p_handle       IN VARCHAR2
                          ,p_name         IN VARCHAR2
                          ,p_object_name  IN VARCHAR2
                          )                            RETURN DATE
IS
BEGIN
  RETURN DOM$MAIN.get_date_param@MAIN(p_handle,p_name, p_object_name);
END get_date_param;


  FUNCTION get_string_param (p_handle       IN VARCHAR2
                            ,p_name         IN VARCHAR2
                            ,p_object_name  IN VARCHAR2) RETURN VARCHAR2
IS
BEGIN
  RETURN DOM$MAIN.get_string_param@MAIN(p_handle, p_name, p_object_name);
END get_string_param;




END DOM;

/
