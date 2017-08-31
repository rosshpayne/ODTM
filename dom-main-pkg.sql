create or replace 
PACKAGE          "DOMOWN"."DOM$MAIN" AS
  --
  --  Package:       DOM$MAIN
  --  Installed on:  DOM-Server
  --  Purpose:  This is the main package of the DOM system.  It drives off the data held
  --  in the DOM repository.  Its main task is to initiate your operations across a 
  --  network of db instances via remote SQL and provide restartability of all operations.  
  --            
  -- -------------------------------------------------------------------------------------
  --
  --  Procedure:   run_op
  --  Purpose:     this is the main API call to run an operation
  -- 
  PROCEDURE  run_op( p_operation_id                 IN NUMBER
                  , p_environment_id                IN NUMBER
                  , p_op_instance                   IN NUMBER DEFAULT 1
                  , p_mode                          IN CHAR DEFAULT 'R'
                  , p_run_constructor_after_error   IN CHAR DEFAULT 'N'
                  ) ;
   -- ----------------------------------------------------------------------------------------
--  PROCEDURE  run_op        ( p_operation_name   IN VARCHAR2
--                            ,p_environment_code IN VARCHAR2);

  PROCEDURE  initialise_remote_db       (p_operation_id  IN NUMBER);

  PROCEDURE  pre_execute_immediate      (p_handle    IN OUT VARCHAR2
                                       , p_sql_str   IN VARCHAR2
                                       , p_mode      OUT CHAR);

  PROCEDURE  post_execute_immediate     (p_handle    IN VARCHAR2
                                       , p_row_count IN VARCHAR2);

  PROCEDURE post_execute_immediate ( p_handle       IN     VARCHAR2
                                  ,p_row_count	  IN     VARCHAR2
                                  ,p_query_t      IN     NUMBER
                                  ,p_dml_t        IN     NUMBER
                                  ,p_ddl_t        IN     NUMBER
                                  ,p_server_t     IN     NUMBER);

  PROCEDURE  rollback_execute_immediate (p_handle    IN VARCHAR2
                                       , p_errmsg    IN VARCHAR2);

  FUNCTION get_operation_name    (  p_handle      IN  VARCHAR2)  RETURN VARCHAR2;

  FUNCTION get_task_name         (  p_handle      IN  VARCHAR2)  RETURN VARCHAR2;

  FUNCTION get_version  RETURN VARCHAR2;

  FUNCTION get_num_param    (p_handle       IN VARCHAR2
                            ,p_name         IN VARCHAR2
                            ,p_object_name  IN VARCHAR2) RETURN NUMBER ;

  FUNCTION get_date_param   (p_handle       IN VARCHAR2
                            ,p_name         IN VARCHAR2
                            ,p_object_name  IN VARCHAR2) RETURN DATE ;

  FUNCTION get_string_param (p_handle       IN VARCHAR2
                            ,p_name         IN VARCHAR2
                            ,p_object_name  IN VARCHAR2) RETURN VARCHAR2 ;

  FUNCTION get_num_param    (p_handle       IN VARCHAR2
                            ,p_name         IN VARCHAR2) RETURN NUMBER ;

  FUNCTION get_date_param   (p_handle       IN VARCHAR2
                            ,p_name         IN VARCHAR2) RETURN DATE ;

  FUNCTION get_string_param (p_handle       IN VARCHAR2
                            ,p_name         IN VARCHAR2) RETURN VARCHAR2 ;


END DOM$MAIN;
/
  CREATE OR REPLACE PACKAGE BODY "DOMOWN"."DOM$MAIN" AS

c_SRC_VERSION        CONSTANT  VARCHAR2(5):='2.2';
c_COMPLETED          CONSTANT  DOM$task_log.status%TYPE:='C';
c_ERRORED            CONSTANT  DOM$task_log.status%TYPE:='E';
c_RUNNING            CONSTANT  DOM$task_log.status%TYPE:='R';
c_START              CONSTANT  DOM$task_log.status%TYPE:='S';
c_FAILED             CONSTANT  DOM$task_log.status%TYPE:='F';
c_NO_UPDATES         CONSTANT  NUMBER:=0;
c_ENABLED            CONSTANT  CHAR(1):='Y';
c_MAXVARLEN          CONSTANT	 NUMBER:=32000;
c_NULL_STATUS        CONSTANT  CHAR:=NULL;
c_TEST_MODE          CONSTANT  CHAR:='T';
c_RUN_MODE           CONSTANT  CHAR:='R';
c_YES                CONSTANT  CHAR:='Y';
c_NO                 CONSTANT  CHAR:='N';
c_INITIAL            CONSTANT  VARCHAR2(8):='Initial';
c_FINAL              CONSTANT  VARCHAR2(8):='Final';
c_CLEANUP            CONSTANT  VARCHAR2(8):='Cleanup';

g_op_instance          NUMBER;
g_operation_id         DOM$operations.id%TYPE;
g_environment_id       DOM$environments.id%TYPE;
g_run_mode             CHAR;
g_package_id           NUMBER;
g_run_id               DOM$run_log.id%TYPE;
g_task_id              NUMBER;  -- task_id on restart
g_first_task_id        NUMBER;  -- the very first task
g_preview_mode         CHAR(1) := 'N';
g_current_task         NUMBER;
g_object_id            NUMBER;
g_object_name          VARCHAR2(30);
g_object_owner         VARCHAR2(30);
g_task_log_id          NUMBER;
g_database_id          NUMBER;
g_db_name              VARCHAR2(12);
g_count                NUMBER:=0;
sql_str                VARCHAR2(4000);
g_run_order            NUMBER;
g_iterator_instance    NUMBER;  -- iterator instance id

g_run_constructor_after_error CHAR;
g_iterator_pkg_id             NUMBER;
--g_constructor_used_           BOOLEAN:=TRUE;
g_iterator_pkg_name           VARCHAR2(30);
g_iterator_pkg_owner          VARCHAR2(30);
g_final_task_exists           BOOLEAN:=TRUE;
g_initial_task_exists         BOOLEAN:=TRUE;
g_iterator_task_added_        BOOLEAN:=TRUE;
g_final_task_id               NUMBER;
g_initial_task_id             NUMBER;
g_iterator_task_id            NUMBER;

operation_complete     EXCEPTION;
insufficient_time      EXCEPTION;

PROCEDURE initialise_run_state ;
PROCEDURE initialise_runtime_order ;
PROCEDURE run_op_tasks_;
PROCEDURE check_input_parameters;
PROCEDURE get_run_state;
PROCEDURE update_task_log ( p_status   IN CHAR
                           ,p_errmsg   IN VARCHAR2 DEFAULT NULL);
PROCEDURE update_log_(p_sqlcode  IN  NUMBER, p_sqlerrm IN VARCHAR2 DEFAULT NULL);
PROCEDURE update_log_(p_status IN VARCHAR2, p_status_msg IN VARCHAR2 DEFAULT NULL);
PROCEDURE initialise_remote_db_ (p_db_name IN VARCHAR2) ;
PROCEDURE raise_if_op_duration_exceeded_;
PROCEDURE reset_status_                 ;

PRAGMA EXCEPTION_INIT(operation_complete,-20100);

c_INSUFFICIENT_TIME_XCPT  CONSTANT NUMBER:=-20101;

PRAGMA EXCEPTION_INIT(insufficient_time,-20101);


PROCEDURE raise_application_error_(p_error   NUMBER
                                  ,p_message VARCHAR2)
IS
BEGIN
  update_log_(p_error, p_message);
  raise_application_error(p_error, p_message);
END raise_application_error_;

--==============================================================================
--
--
--==============================================================================
PROCEDURE DOM_DEBUG ( p_message   IN VARCHAR2) AS
PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
INSERT INTO DOM$debug_log (id,run_id,message)
VALUES (DOM$debug_seq.nextval,g_run_id,p_message);
  dbms_output.put_line(p_message);
COMMIT;
EXCEPTION
WHEN others THEN
  INSERT INTO DOM$debug_log (id,run_id,message)
  VALUES (DOM$debug_seq.nextval,-9999,p_message);
  dbms_output.put_line(p_message);
  COMMIT;
END DOM_DEBUG;
--==============================================================================
--
--
--==============================================================================
FUNCTION get_version RETURN VARCHAR2 AS
BEGIN
  RETURN c_SRC_VERSION;
END get_version;
--==============================================================================
--
--
--==============================================================================
PROCEDURE initialise_remote_db_ (p_db_name IN VARCHAR2)
IS
BEGIN
  --
  -- Copy bootstrap package to remote db
  --
  dom_debug('Transfer DOM code ..');
  COMMIT;
  EXECUTE IMMEDIATE 'begin DOM$bootstrap.transfer_code@'||p_db_name||'('''||'DOM'||'''); end;';
  COMMIT;
  dom_debug('DOM code Transfered..');
  --
  -- Create synonyms on remote db pointing back to dependent objects of package
  --
  FOR pkg IN (SELECT DISTINCT op.owner, op.package_name 
              FROM   DOM$operation_tasks_v   op
              WHERE  op.operation_id = g_operation_id
              UNION
              SELECT pk.owner,pk.package_name
              FROM   DOM$packages pk
               JOIN  DOM$operation_iterator it ON (it.package_id=pk.id)
              WHERE  it.operation_id = g_operation_id
             )
  LOOP
       --
       -- create dependant tables/views
       --
       FOR tab IN (SELECT referenced_name
                   FROM   user_dependencies
                   WHERE  name = upper(pkg.package_name)
                   AND    referenced_owner = pkg.owner
                   AND    referenced_type IN ('TABLE')
                  )
      LOOP
         BEGIN
           dom_debug('create table remotely ..'||tab.referenced_name);
           COMMIT;
           EXECUTE IMMEDIATE 'begin DOM$bootstrap.cr_table@'||p_db_name||'('''||tab.referenced_name||'''); end;';
           COMMIT;
         EXCEPTION
         WHEN others THEN
            dom_debug('create table remotely FAILED '||tab.referenced_name);
            ROLLBACK;
            NULL;
         END;
      END LOOP;
      --
      -- Transfer package(s) and dependent code to remote db
      --
     dom_debug('begin DOM$bootstrap.transfer_code@'||p_db_name||'('''||pkg.package_name||'''); end;');
      COMMIT;
      EXECUTE IMMEDIATE 'begin DOM$bootstrap.transfer_code@'||p_db_name||'('''||pkg.package_name||'''); end;';    
      COMMIT;
    dom_debug('transfer completed..');
    
  END LOOP;
  
END initialise_remote_db_;
--==============================================================================
--
--
--==============================================================================
PROCEDURE initialise_remote_db (p_operation_id IN NUMBER)
IS
BEGIN
 g_operation_id:=p_operation_id;
 FOR db IN (SELECT db_name
             FROM   DOM$operation_databases_v
             WHERE  operation_id = p_operation_id
              AND   op_instance  = g_op_instance)
 LOOP
    initialise_remote_db_ (db.db_name); 
 END LOOP;

END initialise_remote_db;
--==============================================================================
--
--
--==============================================================================
PROCEDURE get_op_id (p_operation_name IN VARCHAR2)
IS
BEGIN
 SELECT id INTO g_operation_id
 FROM   DOM$operations
 WHERE  name = upper(p_operation_name)
  AND   enabled=c_ENABLED;

 EXCEPTION
 WHEN no_data_found THEN
   raise_application_error(-20001,'Operation does not exist or is disabled');
 WHEN others THEN
    RAISE;
END get_op_id;
--==============================================================================
--
--
--==============================================================================
PROCEDURE get_db_id (p_database_name IN VARCHAR2)
IS
BEGIN
 SELECT id INTO g_database_id
 FROM   DOM$databases
 WHERE   id = upper(p_database_name)
  AND   enabled=c_ENABLED;

 EXCEPTION
 WHEN no_data_found THEN
   raise_application_error(-20001,'Database does not exist or is disabled');
 WHEN others THEN
     RAISE;
END get_db_id;
--==============================================================================
--
--
--==============================================================================
FUNCTION get_db_name RETURN VARCHAR2 AS
db_name_   VARCHAR2(30);
BEGIN
  -- get db_name from g_database_id
  --
  SELECT db_name INTO db_name_
    FROM   dom$databases_v
    WHERE  environment_id  = g_environment_id
     AND   database_id     = g_database_id;
     
  RETURN db_name_;
  
END get_db_name;
--==============================================================================
--Get the task ids associated with ITERATOR, INITIAL and FINAL procedures
--If they don't exist then they are created...if only temporarily.
--==============================================================================
FUNCTION get_task_id_(p_task_name IN VARCHAR2) RETURN NUMBER  IS
dummy_     CHAR;
task_id_   NUMBER;
BEGIN
dom_debug('Get Task id for '||initcap(p_task_name));
BEGIN
  SELECT id INTO task_id_
  FROM   DOM$tasks
  WHERE  procedure_name = initcap(p_task_name)
   AND   package_id     = g_iterator_pkg_id;
  dom_debug('task_id='||to_char(task_id_));
EXCEPTION
WHEN no_data_found THEN
  LOOP
   BEGIN
   dom_debug('insert into DOM$TASKS');
   INSERT INTO DOM$tasks(id,procedure_name,description,enabled,created,package_id)
    VALUES  (DOM$task_id_seq.nextval,initcap(p_task_name),'System added',c_YES,SYSDATE,g_iterator_pkg_id)
    RETURNING id INTO task_id_;
   EXIT;  
   EXCEPTION
    WHEN dup_val_on_index THEN
      NULL;
    WHEN others THEN
      RAISE;
    END;
  END LOOP;
END;
--
--  assign task to operation
--
BEGIN
  INSERT INTO DOM$task_run_order(operation_id,task_id,run_order,enabled,system_generated)
  VALUES (g_operation_id,task_id_,task_id_,c_YES,c_YES);
  COMMIT;
  dom_debug('Generated Get TaskId '||to_char(task_id_));
EXCEPTION
  WHEN dup_val_on_index THEN
      NULL;
  WHEN others THEN
      RAISE;
END;

RETURN task_id_;

END get_task_id_;
--==============================================================================
--
--
--==============================================================================   
PROCEDURE run_initial_task_  AS
current_task_id_       NUMBER;
BEGIN

   current_task_id_:=g_task_id;
   IF g_initial_task_id IS NULL THEN
     g_initial_task_id:=get_task_id_(c_INITIAL);
   END IF;
   g_task_id:=g_initial_task_id;
   sql_str:='BEGIN '||g_iterator_pkg_owner||'.'||g_iterator_pkg_name||'.'||c_INITIAL||'@'||g_db_name||'(''';
   sql_str:=sql_str||to_char(g_run_id)||''')'||'; END;';
   dom_debug(sql_str);
   --
   update_task_log(c_START );
   COMMIT;
   EXECUTE IMMEDIATE sql_str;
   COMMIT;
   update_task_log(c_COMPLETED );
   --
   g_task_id:=current_task_id_;
END run_initial_task_;
--==============================================================================
--
--
--==============================================================================
PROCEDURE run_final_task_ (p_db_name     IN VARCHAR2
                          ,p_after_error IN CHAR DEFAULT c_NO)  AS
current_task_id_      NUMBER;
BEGIN
   current_task_id_:=g_task_id;
   IF g_final_task_id IS NULL THEN
     g_final_task_id:=get_task_id_(c_FINAL);
   END IF;
   g_task_id:=g_final_task_id ;
   sql_str:='BEGIN '||g_iterator_pkg_owner||'.'||g_iterator_pkg_name||'.'||c_FINAL||'@'||p_db_name||'(''';
   sql_str:=sql_str||to_char(g_run_id)||''','''||p_after_error||'''); END;';
   dom_debug(sql_str);
   --
   update_task_log(c_START );
   COMMIT;
   EXECUTE IMMEDIATE sql_str;
   COMMIT;
   update_task_log(c_COMPLETED );
   g_task_id:=current_task_id_;
END run_final_task_;
--==============================================================================
--
--
--==============================================================================
PROCEDURE run_iterator_task_ (p_owner       IN VARCHAR2
                            , p_object_name IN VARCHAR2) AS
current_task_id_      NUMBER;
no_procedure_exists_  EXCEPTION;
PRAGMA EXCEPTION_INIT(no_procedure_exists_,-6550);
BEGIN

   current_task_id_:=g_task_id;
   g_task_id:=g_iterator_task_id;
   sql_str:='BEGIN '||g_iterator_pkg_owner||'.'||g_iterator_pkg_name||'.iterator@'||g_db_name||'(''';
   sql_str:=sql_str||to_char(g_run_id)||''','''||p_owner||''','''||p_object_name||''')'||'; END;';
   dom_debug('run_iterator_task_ '||sql_str);
   --
   update_task_log(c_START );
   COMMIT;
   EXECUTE IMMEDIATE sql_str;
   COMMIT;
   update_task_log(c_COMPLETED );
   g_task_id:=current_task_id_;
END run_iterator_task_;
--==============================================================================
--
--
--==============================================================================
PROCEDURE  run_op ( p_operation_id                  IN NUMBER
                  , p_environment_id                IN NUMBER
                  , p_op_instance                   IN NUMBER DEFAULT 1
                  , p_mode                          IN CHAR DEFAULT 'R'
                  , p_run_constructor_after_error   IN CHAR DEFAULT 'N'
                  )
IS
dummy_                   NUMBER;
constructor_proc_name_   VARCHAR2(30);

single_db_            BOOLEAN:=FALSE;
first_iterate         BOOLEAN:=TRUE;
iteration_count_      PLS_INTEGER:=0;
previous_db_          VARCHAR2(30);
previous_db_name_     VARCHAR2(30);
previous_obj_id_      NUMBER;
start_object_id_      NUMBER;
initial_task_run_     BOOLEAN:=FALSE;
run_final_            BOOLEAN:=FALSE;
sqlerrm_              VARCHAR2(200);
BEGIN
  g_operation_id:=p_operation_id;
  g_op_instance :=p_op_instance;
  g_environment_id:=p_environment_id;
  g_run_mode:=p_mode;  
  g_run_constructor_after_error:=p_run_constructor_after_error;
  --
  check_input_parameters;
  --
  -- get iterator pkg and task ids. This package provides the iterator, initial and final procedures
  --
  BEGIN
        SELECT  pk.id            ,              pk.owner,     pk.package_name 
        INTO    g_iterator_pkg_id,  g_iterator_pkg_owner, g_iterator_pkg_name
        FROM   DOM$packages             pk  
         JOIN  DOM$operation_iterator   it  ON (pk.id=it.package_id)
        WHERE  it.operation_id    = g_operation_id; 
        g_iterator_task_id:=get_task_id_('ITERATOR');
        EXCEPTION
        WHEN no_data_found THEN
          raise_application_error(-20122,'No iterator package defined. Set DOM$packages.operation_id');
  END;
  --
  --  populate state global variables
  --  if new run, populate: g_run_id, g_task_id, g_database_id, g_object_id, g_iterator_instance(1),g_run_mode
  --  if errored, populate: g_run_id, g_task_id, g_database_id, g_object_id, g_iterator_instance, g_run_mode
  --
  get_run_state;
  DOM.initialise(g_run_id);
  update_log_(c_RUNNING);
  --
  previous_db_:= g_database_id;
  g_db_name   := get_db_name;  -- from g_database_id
  dom_debug('Database: '||g_db_name);  
  dom_debug('database_id: '||to_char(g_database_id));
  dom_debug('run_id: '||to_char(g_run_id));
  dom_debug('task_id: '||to_char(g_task_id));
  dom_debug('object_id: '||to_char(g_object_id));
  --
  initialise_remote_db_(g_db_name);
  --
  g_initial_task_id := get_task_id_(c_INITIAL);
  g_final_task_id   := get_task_id_(c_FINAL);
  start_object_id_  := g_object_id;
  previous_obj_id_  := start_object_id_;
  previous_db_name_ := g_db_name;
  --
  -- The object run order for the operation drives DOM. The object order drives the databases.
  --
  FOR obj IN (SELECT database_id, object_id,  database_name, object_owner, object_name
              FROM   DOM$runtime_op_app_db_obj_tmp   
              WHERE  object_run_order >= (SELECT distinct object_run_order
                                          FROM   DOM$runtime_op_app_db_obj_tmp
                                          WHERE  object_id = start_object_id_)
              ORDER BY object_run_order
           ) 
  LOOP
         BEGIN
         g_object_id   := obj.object_id; 
         g_database_id := obj.database_id; 
         g_db_name     := obj.database_name;
         dom_debug('Main Loop..');
         --
         -- transfer code to db
         --
         IF g_database_id != previous_db_
         THEN
            previous_db_:=g_database_id;
            initialise_remote_db_(g_db_name);  
         END IF;
         --
         --  Loop while constructor says so. If no constructor then loop once.
         --     
         LOOP
              --
              IF g_task_id = g_final_task_id
              THEN
                initial_task_run_:=FALSE;
                COMMIT;
                run_final_task_(g_db_name);
                g_task_id := g_iterator_task_id;  -- run iterator after final.
              END IF; 
     
              --
              IF g_run_constructor_after_error IN ( 'Y','y') OR g_task_id = g_iterator_task_id
              THEN 
                 --
                -- Run Iterator
                --
                g_task_id := g_iterator_task_id;
                run_iterator_task_(obj.object_owner,obj.object_name);
                g_task_id:=g_first_task_id;
                --
              END IF;
              --
              -- run initial task once only for each loop i.e. for a object/database
              --
              IF NOT initial_task_run_ 
              THEN
                  initial_task_run_:=TRUE;
                  run_initial_task_;
              END IF;
              --
              run_op_tasks_;
              --
              g_iterator_instance:=g_iterator_instance+1;
              --
              IF g_run_mode = c_TEST_MODE
              THEN
                 raise_application_error(-20100,'Single TEST run finished..');  -- progress to next obj/db.
              END IF;
    
          END LOOP;  -- loop forever until iterator raises operation complete.
          --    
          EXCEPTION
          WHEN operation_complete THEN
              COMMIT;
              update_task_log(c_COMPLETED );
              IF initial_task_run_ 
              THEN
                initial_task_run_:=FALSE;
                run_final_task_(g_db_name);
              END IF;  
              dom_debug('RAISED Operation Finished.');
          END; 
          --
          g_iterator_instance := 1;
          iteration_count_    := 0;
          g_task_id           := g_iterator_task_id;
          --
          dom_debug('LOOP AGAIN for next object......');
          --
  END LOOP; -- Object/Database Loop
  --
  --  COMPLETED all tasks (or none to do)
  --
  update_log_(c_COMPLETED,'Finished Operation');
  --
  EXCEPTION
  WHEN others THEN
    ROLLBACK;
    --
    -- update run and task to status ot ERRORED
    --
    update_log_(sqlcode,sqlerrm);
    dom_debug('<<<<ERRORD>>>> in task, iterator, initial or final');
    --
    IF initial_task_run_ 
    THEN
       COMMIT;
       BEGIN
       run_final_task_(g_db_name,c_YES);
       EXCEPTION
       WHEN others THEN
          sqlerrm_:=substr(sqlerrm,1,200);
          -- mark as failed as a previous task must have errored. Only one errored task allowed.
          UPDATE DOM$task_log
          SET status      = c_FAILED 
             ,end_dttm    = systimestamp
             ,status_msg  = sqlerrm_
          WHERE id = g_task_log_id;
          COMMIT;
       END;
    END IF;
    RAISE;
    --
END run_op;

--==============================================================================
-- runs tasks across one database.
--==============================================================================

PROCEDURE run_op_tasks_
IS
package_name_        VARCHAR2(30);
package_owner_       VARCHAR2(30);
BEGIN
      --
      raise_if_op_duration_exceeded_ ;
      --
      --  loop thru tasks starting at g_task_id (which may or may not be the first task)
      --
      FOR task IN (SELECT task_id, owner, package_name, procedure_name
                   FROM   DOM$operation_tasks_v
                   WHERE  operation_id     = g_operation_id
                    AND   task_run_order  >= (SELECT run_order FROM DOM$task_run_order
                                              WHERE operation_id=g_operation_id AND task_id = g_task_id)
                   ORDER BY task_run_order
                   )
      LOOP
       g_task_id:=task.task_id;
       sql_str:='BEGIN '||task.owner||'.'||task.package_name||'.'||task.procedure_name||'@'||g_db_name;
       sql_str:=sql_str||'('''||to_char(g_run_id)||'''); END;';
       dom_debug(to_char(g_task_id)||' '||sql_str);
       --
       update_task_log(c_START );
       COMMIT;
       EXECUTE IMMEDIATE sql_str;
       COMMIT;
       update_task_log(c_COMPLETED );
      END LOOP;
      -- 
      -- All tasks completed on object, point task_id back to iterator 
      --
      g_task_id:=g_iterator_task_id;
      --
      -- if exception raised task status update handled by update_log
      --
END run_op_tasks_;
--==============================================================================
-- runs tasks across one database.
--==============================================================================
PROCEDURE raise_if_op_duration_exceeded_ AS
dummy_    CHAR;
BEGIN
SELECT 'a' INTO dummy_
FROM   DUAL
WHERE  SYSDATE < (SELECT nvl(restart_dttm,start_dttm) + 
                                      CASE WHEN (SELECT duration
                                                 FROM   DOM$operation_environments
                                                 WHERE  operation_id=rl.operation_id 
                                                 AND environment_id=rl.environment_id) IS NULL
                                      THEN (SELECT duration
                                            FROM   DOM$operations
                                            WHERE  id=rl.operation_id)
                                      ELSE (SELECT duration
                                            FROM   DOM$operation_environments
                                            WHERE  operation_id=rl.operation_id 
                                            AND environment_id=rl.environment_id )
                                      END                                                   
                  FROM  DOM$run_log rl
                  WHERE id=g_run_id );
                  
EXCEPTION
WHEN no_data_found THEN
  raise_application_error(c_INSUFFICIENT_TIME_XCPT,'Operation Aborted. Allowed duraton exceeded for operation');
END raise_if_op_duration_exceeded_;
--==============================================================================
-- runs tasks across one database.
--==============================================================================
PROCEDURE get_session_state_ (p_username       OUT VARCHAR2
                             ,p_client_process OUT NUMBER
                             ,p_serial         OUT NUMBER
                             ,p_sid            OUT NUMBER)
AS
BEGIN
  SELECT a.username un, a.process client_process_id,a.serial# ,sid
   INTO  p_username,p_client_process,p_serial,p_sid
  FROM v$session a
   WHERE a.sid = (SELECT sys_context('USERENV','SID') FROM dual);
END get_session_state_;
--==============================================================================
-- runs tasks across one database.
--==============================================================================
FUNCTION session_state_changed_ (p_username           IN VARCHAR2
                                ,p_client_process_id  IN NUMBER
                                ,p_serial             IN NUMBER
                                ,p_sid                IN NUMBER) RETURN BOOLEAN
IS
dummy_     CHAR;
BEGIN

   dom_debug('check if session state changed for SID '||to_char(p_sid)||' '||to_char(p_serial)||' '||to_char(p_client_process_id)||' '||p_username);
 
      SELECT 'a' INTO dummy_
      FROM v$session a
        WHERE username = p_username
         AND  process  = p_client_process_id
         AND  serial#  = p_serial
         AND  sid      = p_sid
         AND  module   = 'DOM.'||get_operation_name(g_run_id);      
    
      RETURN FALSE;
      
      EXCEPTION
      WHEN no_data_found THEN
         RETURN TRUE;
         
END session_state_changed_;
--==============================================================================
-- runs tasks across one database.
--==============================================================================
FUNCTION  db_instance_restarted_ (p_run_startup   IN  DATE) RETURN BOOLEAN 
AS
db_startup_   DATE;
BEGIN
    SELECT startup_time INTO db_startup_
    FROM V$INSTANCE;
    
    RETURN db_startup_ > p_run_startup;
    
END db_instance_restarted_;   
--==============================================================================
-- runs tasks across one database.
--==============================================================================
PROCEDURE update_run_state_after_error_  AS
username_       VARCHAR2(30);
proc_id_        NUMBER;
serial_         NUMBER;
sid_            NUMBER;
BEGIN
    get_session_state_(username_,proc_id_,serial_,sid_);
    --
    UPDATE DOM$run_log
      SET restart_dttm = SYSDATE
         ,end_dttm   = NULL
         ,status     = c_ERRORED -- only time when session state is updated
         ,status_msg = NULL
         ,username   = username_
  ,client_process_id = proc_id_
         ,serial     = serial_
         ,sid        = sid_
    WHERE  id = g_run_id;
END update_run_state_after_error_;
--==============================================================================
-- runs tasks across one database.
--==============================================================================
PROCEDURE reset_status_                  AS
username_          VARCHAR2(30);
client_process_id_ NUMBER;
serial_            NUMBER;
sid_               NUMBER;
BEGIN
  dom_debug('IN reset_run_state');
  UPDATE DOM$TASK_LOG
    SET  status     = c_ERRORED
        ,status_msg = 'Status reset after instance or session abort'
    --       ,start_dttm = SYSDATE
  WHERE  status IN( c_RUNNING, c_ERRORED)
   AND   run_id = g_run_id;
  -- raise if more than 1 row update;
  UPDATE DOM$SQL_LOG 
    SET  status     = c_ERRORED
        ,status_msg = 'Status reset after instance or session abort'
      --  ,start_dttm = SYSDATE
  WHERE status     = c_RUNNING
   AND task_log_id = (SELECT task_log_id
                      FROM   DOM$TASK_LOG
                      WHERE  run_id=g_run_id
                       AND   status in (c_ERRORED, c_RUNNING));
  COMMIT;
END reset_status_                 ;
--==============================================================================
--  Operation state is kept in two table = s:
--         DOM$run_log
--         DOM$task_log
-- 
-- State consists of the following:
--         Table.column      -> populates --> Global variable
--         ------------------------------     -------------------
--         DOM$run_log.id                 g_run_id
--         DOM$task_log.id                g_task_log_id (not for new state)
--         DOM$task_log.database_id       g_database_id
--         DOM$task_log.runtime_task_id   g_task_id
--============================================================================== 
PROCEDURE get_run_state
IS
run_status_            CHAR(1);
dummy_                 CHAR;
username_              VARCHAR2(30);
instance_startup_      DATE;
client_process_id_     NUMBER;
serial_                NUMBER;
sid_                   NUMBER;
status_                CHAR;
tasks_                 NUMBER;
BEGIN
 --
 --  get first task for this operation.
 --
 dom_debug('Enter get_run_state..');
 BEGIN  
 SELECT t.task_id INTO g_first_task_id 
 FROM (
        SELECT task_id, ot.run_order  ,  min(run_order) over () min_run_order
        FROM  DOM$tasks           t 
         JOIN DOM$task_run_order ot ON (ot.task_id=t.id)
        WHERE ot.operation_id = g_operation_id
         AND  ot.run_order        > 0 
         AND  t.enabled           = c_ENABLED
         AND  ot.system_generated = c_NO
        ORDER by ot.run_order
        ) t
 WHERE t.run_order = t.min_run_order;
 EXCEPTION
  WHEN no_data_found THEN
     raise_application_error_(-20014,'No tasks assigned to operation');
 END;
 --
 g_iterator_instance:=1;
 --
 -- Get last run log id for the input arguments.
 --
 SELECT  tab.id, decode(tab.run_mode,'T','C',tab.status)  , tab.run_mode , instance_startup, username, client_process_id, serial, sid               
         INTO g_run_id, run_status_, g_run_mode, instance_startup_, username_, client_process_id_, serial_, sid_
   FROM 
      (SELECT id, status ,run_mode,instance_startup,username,client_process_id,serial ,sid, max(id) over () max_id
       FROM   DOM$run_log 
       WHERE  operation_id       = g_operation_id
        AND   operation_instance = g_op_instance
        AND   environment_id     = g_environment_id
        AND   run_mode           = g_run_mode
      ) tab
  WHERE tab.id=tab.max_id;
  dom_debug('Get_state: '||to_char(g_run_id)||' '||run_status_||' '||g_run_mode); 
  --
  -- Check session still exists if status is RUNNING otherwise set state to ERRORED
  --
  IF run_status_ IN (c_RUNNING)
  THEN
    IF db_instance_restarted_(instance_startup_)
    THEN 
         dom_debug('Instance restarted: assign status to E');  
         run_status_:=c_ERRORED;
         reset_status_;
    ELSIF session_state_changed_(username_,client_process_id_,serial_,sid_)
    THEN
         dom_debug('Session nolonger exists: assign status to E');  
         run_status_:=c_ERRORED;
         reset_status_;
    END IF;
  END IF;
  --
  IF run_status_ = c_ERRORED 
  THEN
    --
    update_run_state_after_error_;
    --
    initialise_runtime_order;
    --
    --  Check task integrity
    --
    SELECT count(*) INTO tasks_
    FROM DOM$task_log
    WHERE  run_id  =  g_run_id
     AND    status IN (c_RUNNING) ;    
    IF tasks_ > 1 
    THEN 
      raise_application_error(-20002,'More than one task with Running  status');  
    END IF;
    --
    SELECT count(*) INTO tasks_
    FROM DOM$task_log
    WHERE  run_id  =  g_run_id
     AND    status IN (c_ERRORED) ;     
    IF tasks_ > 2 
    THEN 
      raise_application_error(-20002,'More than two tasks with Errored status');  
    END IF;
    --
    --  get state of last error'd or running task.  Find the first errored task for the run.
    --
    BEGIN
      SELECT tl.id      , tl.database_id, tl.task_id, tl.object_id , tl.repeat_instance, tl.status
      INTO g_task_log_id, g_database_id ,g_task_id  , g_object_id  , g_iterator_instance, status_
      FROM
         (SELECT id, run_id, status , database_id, object_id, task_id, repeat_instance,  max(id) over () max_log_task_id
           FROM   DOM$task_log
           WHERE  run_id  =  g_run_id
           AND    status IN (c_RUNNING, c_ERRORED)
          )     tl
      WHERE tl.id= tl.max_log_task_id ;
      --
      dom_debug('CURRENT TASK task_log_id '||to_char(g_task_log_id)||' Status '||to_char(status_)||' task_id '||to_char(g_task_id));
      --
      --  Set status of any associated sql_log entries to 'E' if never reset from 'R'. 
      --
      UPDATE DOM$sql_log
        SET status = 'E'
      WHERE task_log_id = g_task_log_id
      AND STATUS = c_RUNNING;
    
      COMMIT;
        
    EXCEPTION
    WHEN no_data_found THEN
       dom_debug('No Errored or Running task found for an errored run. Grab last completed task');
       raise_application_error(-20110,'Inconsistency: no errored tasks found for an errored run. This can only happen on a restart.');
    END;
    ---
    dom_debug('Last run errored.. Next task_id -> '||to_char(g_task_id));
    dom_debug('Last run errored.. Next object_id -> '||to_char(g_object_id));
    --
    -- set all other errrored tasks to Failed so they are not selected in the future.
    --
    UPDATE DOM$task_log
       SET status = c_FAILED
    WHERE  run_id = g_run_id
     AND   status IN (c_RUNNING,c_ERRORED)
     AND   id != g_task_log_id;
    --
    COMMIT;
    --
 ELSIF run_status_ = c_COMPLETED
 THEN
   --
   -- new run id
   --
   initialise_run_state;
   --
 ELSIF run_status_ = c_RUNNING
 THEN
   raise_application_error(-20001,'Operation is already running');  
 ELSE
   raise_application_error_(-20002,'Data inconsistency in status of task log');
 END IF; 
 
 dom_debug('Exit get_run_state..');
 
 EXCEPTION 
 WHEN no_data_found THEN
   -- 
   --  First ever run for Operation
   --
   initialise_run_state;
   
   dom_debug('Exit get_run_state..');
   
END get_run_state;
--==============================================================================
-- initialises the following global state variables
-- performed when operation first performed or last operation completed
-- and and new run is about to begin.
--
--  g_database_id - first enabled database assigned to operation
--  g_task_id     - first runtime task assigned to object
--  g_run_id       
--  g_task_log_id
--============================================================================== 
PROCEDURE initialise_run_state 
IS
first_row_         BOOLEAN:=TRUE;
username_          VARCHAR2(30);
client_process_id_ NUMBER;
serial_            NUMBER;
sid_               NUMBER;
BEGIN
  dom_debug('initialise_Run_state..mode='||g_run_mode);
  --
  -- get session state
  --
  get_session_state_(username_,client_process_id_,serial_,sid_);
  --
  --  create a run_log entry
  --
  INSERT INTO DOM$run_log (id,operation_id,operation_instance,start_dttm,status,environment_id,run_mode
       ,instance_startup,username,client_process_id,serial ,sid                   )
  VALUES (DOM$run_seq.nextval,g_operation_id,g_op_instance,SYSDATE,c_RUNNING,g_environment_id,g_run_mode,
          (SELECT startup_time FROM V$INSTANCE),username_, client_process_id_, serial_,sid_)
  RETURNING id INTO g_run_id;
  
  dom_debug('RUnID '||to_char(g_run_id));
  
  initialise_runtime_order;
          
  g_task_id:=g_iterator_task_id;
  g_iterator_instance:=1;
  
  COMMIT;
  
END initialise_run_state;
--==============================================================================
--
--
--
--=============================================================================
PROCEDURE initialise_runtime_order
IS
dummy_   CHAR;
BEGIN
  --
  --  create entry in TMP table for each database involved in the operation in operation order.
  --    
  dom_debug('Initialise_runTime_order..');
    DELETE FROM DOM$runtime_op_app_db_obj_tmp;
    BEGIN
    SELECT distinct '1' INTO dummy_
    FROM   DOM$operation_run_order_V 
    WHERE  operation_id   = g_operation_id
      AND  op_instance    = g_op_instance
      AND  environment_id = g_environment_id;
    EXCEPTION
    WHEN no_data_found THEN
       raise_application_error(-20145,'No rows found in DOM$operation_run_order_v');
    END; 

    INSERT INTO DOM$runtime_op_app_db_obj_tmp 
          (run_id,database_id,database_name,object_id,object_owner,object_name
           ,operation_id ,object_run_order)
    SELECT g_run_id, database_id, db_name, object_id, object_owner, object_name
          ,operation_id ,object_run_order
    FROM   DOM$operation_run_order_V 
    WHERE  operation_id   = g_operation_id
      AND  op_instance    = g_op_instance
      AND  environment_id = g_environment_id;
      
    SELECT database_id , object_id INTO g_database_id, g_object_id
    FROM (
          SELECT database_id,object_id, rank() over (order by object_run_order) rk
          FROM   DOM$runtime_op_app_db_obj_tmp
         ) t
    WHERE t.rk = 1;
    dom_debug('End: Initialise_runTime_order..');

END initialise_runtime_order;
--==============================================================================
--
--
--
--=============================================================================
PROCEDURE check_input_parameters
IS
enabled_         DOM$operations.enabled%TYPE;
BEGIN
 -- g_operation_id:=p_operation_id;
 -- g_op_instance :=p_op_instance;
 -- g_environment_id:=p_environment_id;
 -- g_run_mode:=p_mode; 
 -- g_run_constructor_after_error_:=p_run_constructor_after_error;
   --
  IF g_run_mode NOT IN (c_TEST_MODE, c_RUN_MODE)
  THEN
      raise_application_error(-20022,'Run Mode must be either T (Test) or R (Run)');
  END IF;
  IF g_run_mode='T' AND g_run_constructor_after_error IN ( 'Y','y')
  THEN
    raise_application_error(-20021,'Cannot use TEST mode when requiring to run constructor after error');
  END IF;
  
  BEGIN
    SELECT enabled INTO enabled_
    FROM   DOM$operations      
    WHERE  id   = g_operation_id;
    IF enabled_ != c_YES THEN
      raise_application_error(-20020,'Operation disabled');
    END IF;
    EXCEPTION
    WHEN no_data_found THEN
      raise_application_error(-20020,'Operation does not exist');
  END;
  BEGIN
     SELECT enabled INTO enabled_
     FROM   DOM$environments      
     WHERE  id   = g_environment_id;
     IF enabled_ != c_YES THEN
        raise_application_error(-20020,'Environment disabled');
     END IF;
     EXCEPTION
     WHEN no_data_found THEN
        raise_application_error(-20020,'Environment does not exist');
  END;
  BEGIN
     SELECT enabled INTO enabled_
     FROM   DOM$operation_environments     
     WHERE  operation_id   = g_operation_id
      AND   environment_id = g_environment_id;
     IF enabled_ != c_YES THEN
        raise_application_error(-20020,'Operation is disabled for this Environment');
     END IF;
  EXCEPTION
  WHEN no_data_found THEN
      raise_application_error(-20020,'Operation not assigned to this Environment.');
  END;
  BEGIN
    SELECT enabled INTO enabled_
    FROM   DOM$operation_instance  oi 
    WHERE  operation_id   = g_operation_id
     AND   instance       = g_op_instance;
    IF enabled_ = c_NO
    THEN
      raise_application_error(-20020,'The instance of this Operation is disabled');
    END IF;
  EXCEPTION
  WHEN no_data_found THEN
      raise_application_error(-20020,'Thie instance of this operation does not exist');
  END;
END check_input_parameters;
--==============================================================================
--
-- NOT USED
--NOT USED
--=============================================================================
PROCEDURE check_object_enabled (p_obj_id        IN NUMBER
                               ,p_op_id         IN NUMBER) IS

enabled_         DOM$operations.enabled%TYPE;
BEGIN
  --
  -- check if object is completely disabled
  --
  SELECT enabled INTO enabled_
  FROM   DOM$objects
  WHERE  id = p_obj_id;

  IF enabled_ = c_NO
  THEN
      raise_application_error_(-20020,'Object disabled');
  ELSE
    --
    -- check if object is disabled at operation level
    --
    SELECT enabled INTO enabled_
    FROM   DOM$operation_Objects
    WHERE  operation_id=p_op_id
    AND    object_id = p_obj_id;

    IF enabled_ = c_NO
    THEN
      raise_application_error_(-20020,'object disabled for operation');
    END IF;
  END IF;
  EXCEPTION
  WHEN no_data_found THEN
      raise_application_error_(-20020,'Object does not exist');
END check_object_enabled;
--==============================================================================
-- Updates DOM$TASK_LOG & DOM$RUN_LOG
--
-- Raises: 
--==============================================================================
PROCEDURE update_task_log ( p_status   IN CHAR
                           ,p_errmsg   IN VARCHAR2 DEFAULT NULL) IS
BEGIN

  IF p_status = c_START THEN
    --
    --  Unique index on run-id,runtime_task_id,database_id,object_id,instance_id
    --
    --  In the cases where an operation runs its component tasks multiple times each run
    --  of the tasks is assigned its own istance id.
    --
    -- alter table DOM$task_log add constraint DOM$task_log_uk1 UNIQUE (operation_id,run_id,task_id,database_id,object_id);
    --
    BEGIN 
    dom_debug('AT new/update task log ');
    SELECT id INTO g_task_log_id
    FROM DOM$task_log
    WHERE task_id= g_task_id
     AND  run_id = g_run_id
     AND  database_id = g_database_id
     AND  object_id   = g_object_id
     AND  repeat_instance = g_iterator_instance
     AND  operation_id=g_operation_id;
    dom_debug('UPDATE task log for '||to_char(g_task_log_id));     
    UPDATE DOM$task_log
       SET  status     = c_RUNNING
           ,start_dttm = systimestamp --CASE WHEN status = c_ERRORED THEN start_dttm ELSE systimestamp END
           ,end_dttm   = NULL -- CASE WHEN p_status IN (c_COMPLETED,c_ERRORED) THEN systimestamp ELSE NULL END
          -- ,status_msg = NULL
    WHERE id = g_task_log_id;
      
    EXCEPTION 
      WHEN no_data_found THEN
        INSERT INTO DOM$task_log (id,task_id,start_dttm,run_id,database_id,status,object_id, operation_id,repeat_instance,database_name)
        VALUES (DOM$task_seq.nextval ,g_task_id ,SYSTIMESTAMP ,g_run_id,g_database_id,c_RUNNING,g_object_id,g_operation_id,g_iterator_instance,g_db_name)
        RETURNING id INTO g_task_log_id;
        dom_debug('Create new Task log .. '||to_char(g_task_log_id));
     END;
      
  ELSIF p_status = c_COMPLETED THEN
  
     UPDATE DOM$task_log
       SET  status     = p_status
           ,end_dttm   = systimestamp       
      WHERE id = g_task_log_id;

  ELSE
         raise_application_error_(-20008,'Program inconsistency - invalid status');
  END IF; 

  COMMIT;
  
  EXCEPTION
  WHEN others THEN
     RAISE;
END update_task_log;
--==============================================================================
-- UPDATE RUN LOG
--
-- Raises:
--==============================================================================
PROCEDURE update_log_(p_status IN VARCHAR2, p_status_msg IN VARCHAR2 DEFAULT NULL)
IS
BEGIN
  UPDATE DOM$run_log
    SET status      = p_status
        ,status_msg = p_status_msg
        ,end_dttm   = CASE p_status WHEN c_RUNNING THEN NULL ELSE systimestamp END
  WHERE id = g_run_id;
 
  COMMIT;
 
END update_log_;
--==============================================================================
-- UPDATE RUN LOG
--
-- Raises:
--==============================================================================
PROCEDURE update_log_(p_sqlcode  IN  NUMBER, p_sqlerrm IN VARCHAR2 DEFAULT NULL)
IS
error_msg_    VARCHAR2(200);
run_status_   VARCHAR2(30);
updated_      NUMBER;
BEGIN
error_msg_:=substr(p_sqlerrm,1,200);
--
-- NOTE: don't change status to c_ERROR as there is dependency
-- that a task must have failed to generate this status.
--
IF abs(p_sqlcode) between 20000 and 20999
THEN
  IF abs(p_sqlcode) = 20100 -- operation completed.
  THEN
    UPDATE DOM$run_log
        SET status   = c_COMPLETED
           ,end_dttm =  systimestamp
         ,status_msg = 'Operation Completed'
       --  ,database_id=g_database_id
    WHERE              id = g_run_id
    AND      operation_id = g_operation_id;
    --
  ELSIF abs(p_sqlcode) = 20007
  THEN
    UPDATE DOM$run_log
        SET status   = c_COMPLETED
           ,end_dttm =  systimestamp
         ,status_msg = 'Operation had nothing to perform'
        -- ,database_id=g_database_id
    WHERE              id = g_run_id
    AND      operation_id = g_operation_id;

    UPDATE DOM$task_log
        SET status = c_COMPLETED
           ,end_dttm =  systimestamp
       , status_msg  =  'Operation had nothing to perform'
       ,database_id  =  g_database_id
    WHERE  id=g_task_log_id;    
    --
  ELSIF abs(p_sqlcode) > 20001 
  THEN
  -- raise application error received
    UPDATE DOM$run_log
        SET   status = c_ERRORED
           ,end_dttm =  systimestamp
       , status_msg  =  error_msg_
      -- ,database_id=g_database_id
    WHERE       id = g_run_id;

    UPDATE DOM$task_log
    SET status = c_ERRORED
           ,end_dttm =  systimestamp
       , status_msg  =  error_msg_
      -- ,database_id  = g_database_id
    WHERE  id=g_task_log_id;
  END IF;
ELSE
  -- Oracle Internal error
  UPDATE DOM$run_log
        SET   status = c_ERRORED
           ,end_dttm =  systimestamp
       , status_msg  =  error_msg_
  WHERE         id = g_run_id;

  UPDATE DOM$task_log
        SET status = c_ERRORED
           ,end_dttm =  systimestamp
       , status_msg  =  error_msg_
    WHERE id = g_task_log_id;
END IF;

COMMIT;

END update_log_;
--==============================================================================
--
--
--
--==============================================================================
PROCEDURE pre_execute_immediate (  p_handle       IN  OUT VARCHAR2
                                  ,p_sql_str		  IN      VARCHAR2
                                  ,p_mode        OUT      CHAR)
IS
id_             NUMBER;
task_log_id_    NUMBER;
BEGIN
    
   INSERT INTO DOM$sql_log
   ( id, sql_str, start_dttm, end_dttm,task_log_id)
   VALUES
   (ddl_log_seq.nextval, substr(p_sql_str,1,4000), SYSTIMESTAMP, NULL,
    (SELECT id FROM DOM$task_log WHERE run_id=TO_NUMBER(p_handle) AND status = c_RUNNING))
   RETURNING id INTO id_;
   
   SELECT run_mode INTO p_mode
   FROM   DOM$run_log 
   WHERE  id = TO_NUMBER(p_handle);

   p_handle:=to_char(id_);

END  pre_execute_immediate;
--==============================================================================
--
--
--
--==============================================================================
PROCEDURE post_execute_immediate ( p_handle       IN     VARCHAR2
                                  ,p_row_count	  IN     VARCHAR2
                                  ,p_query_t      IN     NUMBER
                                  ,p_dml_t        IN     NUMBER
                                  ,p_ddl_t        IN     NUMBER
                                  ,p_server_t     IN     NUMBER)
IS
BEGIN
   --
   UPDATE DOM$sql_log
      SET row_count=p_row_count
         ,status=c_COMPLETED
         ,end_dttm = systimestamp
         ,query_threads = p_query_t
         ,dml_threads = p_dml_t
         ,ddl_threads = p_ddl_t
         ,server_threads = p_server_t
   WHERE ID = TO_NUMBER(p_handle);

END  post_execute_immediate;
--==============================================================================
--
--
--
--==============================================================================
PROCEDURE post_execute_immediate ( p_handle       IN     VARCHAR2
                                  ,p_row_count	  IN     VARCHAR2 )
IS
BEGIN
   --
   UPDATE DOM$sql_log
      SET row_count=p_row_count
         ,status=c_COMPLETED
         ,end_dttm = SYSDATE
   WHERE ID = TO_NUMBER(p_handle);

END  post_execute_immediate;
--==============================================================================
--
--
--
--==============================================================================
PROCEDURE rollback_execute_immediate (  p_handle      IN  VARCHAR2
                                       ,p_errmsg		  IN  VARCHAR2)
IS
BEGIN
   --
   UPDATE DOM$sql_log
        SET status   = c_ERRORED
        ,status_msg  = p_errmsg
           ,end_dttm = SYSDATE
   WHERE ID= to_number(p_handle);

END  rollback_execute_immediate;
--==============================================================================
--
--
--
--==============================================================================
FUNCTION get_operation_name    (  p_handle      IN  VARCHAR2)
                                       RETURN VARCHAR2
IS
op_name_     VARCHAR2(30);
BEGIN
   --
  SELECT o.name INTO op_name_
  FROM  DOM$run_log     r
   JOIN DOM$operations  o ON (o.id=r.operation_id)
  WHERE  r.id      = TO_NUMBER(p_handle);
   
  RETURN op_name_;

END  get_operation_name;
--==============================================================================
--
--
--
--==============================================================================
FUNCTION get_task_name    (  p_handle      IN  VARCHAR2)  RETURN VARCHAR2
IS
task_name_     VARCHAR2(30);
BEGIN
   --
   SELECT procedure_name INTO task_name_
   FROM  DOM$task_log   tlog
    JOIN DOM$tasks      t     ON (tlog.task_id=t.id)
   WHERE  tlog.run_id = TO_NUMBER(p_handle)
   AND    tlog.status = c_RUNNING;
   
  RETURN task_name_;
  
  EXCEPTION
  WHEN no_data_found THEN
    RETURN 'MAIN-'||p_handle;

END  get_task_name;
--==============================================================================
--
--
--
--=============================================================================
FUNCTION get_num_param (p_handle       IN VARCHAR2
                       ,p_name         IN VARCHAR2
                        )     RETURN NUMBER IS
BEGIN
   RETURN TO_NUMBER(get_string_param(p_handle,p_name));
END get_num_param;

FUNCTION get_num_param (p_handle       IN VARCHAR2
                       ,p_name         IN VARCHAR2
                       ,p_object_name  IN VARCHAR2 
                       ) RETURN NUMBER IS
BEGIN
   -- ignore object_name. Function exists for legacy code.
   RETURN TO_NUMBER(get_string_param(p_handle,p_name));
END get_num_param;
--==============================================================================
-- get_string_param:  returns the parameter value 
--
--
--==============================================================================
FUNCTION get_date_param    (p_handle       IN VARCHAR2
                           ,p_name         IN VARCHAR2) RETURN DATE IS
BEGIN
 RETURN TO_DATE(get_string_param(p_handle,p_name),'DD-MON-YYYY');
END get_date_param;


FUNCTION get_date_param (p_handle       IN VARCHAR2
                        ,p_name         IN VARCHAR2
                        ,p_object_name  IN VARCHAR2
                        )
                        RETURN DATE IS

BEGIN
   RETURN TO_DATE(get_string_param(p_handle,p_name),'DD-MON-YYYY');
END get_date_param;
--==============================================================================
-- get_string_param:  returns the parameter value 
--
--
--==============================================================================
FUNCTION get_string_param (p_handle       IN VARCHAR2
                          ,p_name         IN VARCHAR2) RETURN VARCHAR2 IS

value_        DOM$parameters.value%TYPE;
BEGIN
   --
   --  IF object_id and operation_instance defined
   --
   SELECT a.value INTO value_
   FROM   DOM$parameters a
   WHERE  a.operation_id=(SELECT operation_id
                          FROM   DOM$run_Log
                          WHERE id = TO_NUMBER(p_handle))
   AND    upper(a.name)  = upper(p_name)
   AND    a.object_id    = (SELECT object_id FROM  DOM$running_task_v WHERE  run_id=TO_NUMBER(p_handle))
   AND    environment_id = (SELECT environment_id
                            FROM   DOM$run_log
                            WHERE id = TO_NUMBER(p_handle))
   AND    operation_instance = (SELECT operation_instance
                                   FROM   DOM$run_log 
                                   WHERE  id = TO_NUMBER(p_handle)
                               );
   
   RETURN value_;

   EXCEPTION
   WHEN no_data_found THEN  
     --
     --  If object_id is defined but operation_instance is null
     -- 
     BEGIN
     SELECT a.value INTO value_
     FROM   DOM$parameters a
     WHERE  a.operation_id=(SELECT operation_id
                            FROM   DOM$run_Log
                            WHERE id = TO_NUMBER(p_handle))
     AND    upper(a.name)  = upper(p_name)
     AND    a.object_id    = (SELECT object_id FROM  DOM$running_task_v WHERE  run_id=TO_NUMBER(p_handle))
     AND    environment_id = (SELECT environment_id
                              FROM   DOM$run_Log
                              WHERE id = TO_NUMBER(p_handle))
     AND    operation_instance IS NULL;
    
     RETURN value_;
  
     EXCEPTION 
     WHEN no_data_found THEN
       --
       --  object_id is NULL but operation instance is defined
       --
       BEGIN
       SELECT a.value INTO value_
       FROM   DOM$parameters a
       WHERE  a.operation_id=(SELECT operation_id
                              FROM   DOM$run_Log
                              WHERE id = TO_NUMBER(p_handle))
       AND    upper(a.name)  = upper(p_name)
       AND    object_id IS NULL
       AND    environment_id = (SELECT environment_id
                                FROM   DOM$run_Log
                                WHERE id = TO_NUMBER(p_handle))
       AND    operation_instance    = (SELECT operation_instance
                                       FROM   DOM$run_log 
                                       WHERE  id = TO_NUMBER(p_handle)
                                       );
       RETURN value_;
      
       EXCEPTION
       WHEN no_data_found THEN
            BEGIN
                --
                --  both object_id and operation_instance is NULL
                --
                SELECT value INTO value_
                FROM DOM$parameters
                WHERE operation_id=(SELECT operation_id
                                    FROM   DOM$run_Log
                                    WHERE id = TO_NUMBER(p_handle))
                AND   upper(name) = upper(p_name)
                AND   object_id  IS NULL
                AND   environment_id     = (SELECT environment_id
                                            FROM   DOM$run_Log
                                            WHERE id = TO_NUMBER(p_handle))
                 AND   operation_instance  IS NULL;
                
                RETURN value_;
                
            EXCEPTION 
            WHEN no_data_found THEN
              raise_application_error(-20115,'Parameter '||p_name||' not defined.');
            END;
        END;
    END;
            
END get_string_param;



FUNCTION get_string_param (p_handle       IN VARCHAR2
                          ,p_name         IN VARCHAR2
                          ,p_object_name  IN VARCHAR2 
                          )   RETURN VARCHAR2 IS
BEGIN
    RETURN get_string_param(p_handle, p_name);
END get_string_param;




END DOM$MAIN;

/
