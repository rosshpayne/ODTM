create or replace 
PACKAGE         DOM$BOOTSTRAP AS
  --
  --  Package:       DOM$BOOTSTRAP
  --  Installed on:  DOM-Client
  --  Purpose:       Provides initialisation services to the DOM-Server.  
  --                 It is the only code that is manually installed on the DOM-Client.  
  --                 All other code is installed by DOM at runtime during initialisation
  --                 of an operation.
  --                 Requires a db-link to DOM-Server called MAIN.
  --
  --
  procedure initialise ;

  procedure cr_synonym (p_synonym IN VARCHAR2);

  procedure cr_table   (p_tabname IN VARCHAR2);

  procedure transfer_code (p_name IN VARCHAR2);

end dom$bootstrap;
/
create or replace 
PACKAGE BODY         DOM$BOOTSTRAP AS

MAXVARLEN     CONSTANT	 NUMBER:=32000;


PROCEDURE load (p_name IN VARCHAR2);
--==============================================================================
--
--
--
--==============================================================================
PROCEDURE initialise
AS
dummy_   PLS_INTEGER;
BEGIN
      COMMIT;
      --
      load('DOM');
      --
END initialise;
--==============================================================================
--
--
--==============================================================================


PROCEDURE cr_synonym (p_synonym   IN VARCHAR2)
AS
BEGIN
      COMMIT;
      --
      EXECUTE IMMEDIATE 'create or replace synonym '||p_synonym||' for DOMOWN.'||p_synonym||'@MAIN';
      COMMIT;

END cr_synonym;

--==============================================================================
--
--
--==============================================================================
PROCEDURE cr_table (p_tabname   IN VARCHAR2)
AS
dummy_   CHAR;
BEGIN
      COMMIT;
      --
      -- create table
      --
      EXECUTE IMMEDIATE 'create table '||p_tabname||' as select * from '||p_tabname||'@MAIN';
      COMMIT;

      EXCEPTION
      WHEN others THEN
          ROLLBACK;
          NULL;

END cr_table;
--==============================================================================
--
--
--==============================================================================
PROCEDURE transfer_code(p_name	IN VARCHAR2)
IS
BEGIN

  COMMIT;
  load(p_name);
  COMMIT;

END transfer_code;
--==============================================================================
--
--
--==============================================================================
PROCEDURE load (p_name IN VARCHAR2)
IS
TYPE cv is REF CURSOR;
this_cv     cv;
txt_1	    VARCHAR2(32000);
txt_2	    VARCHAR2(32000);
line_	    VARCHAR2(32000);
sql_str     VARCHAR2(2000);
dummy_	    NUMBER;
BEGIN
  --
  --  Transfer only if package does not exist on remote db
  --  or remote version is older than version on MAIN server.

  FOR obj IN ( SELECT 1, object_type type
              FROM   USER_OBJECTS
              WHERE  object_type = 'PACKAGE'
               AND   object_name = upper(p_name)
               AND   last_ddl_time < (SELECT last_ddl_time
                                      FROM   USER_OBJECTS@MAIN
                                      WHERE  object_type = 'PACKAGE'
                                       AND   object_name = upper(p_name))
              UNION
              SELECT 3, object_type
              FROM   USER_OBJECTS
              WHERE  object_type = 'PACKAGE BODY'
               AND   object_name = upper(p_name)
               AND   last_ddl_time < (SELECT last_ddl_time
                                      FROM   USER_OBJECTS@MAIN
                                      WHERE  object_type = 'PACKAGE BODY'
                                       AND   object_name = upper(p_name))
              UNION
              SELECT 2, object_type
              FROM   USER_OBJECTS@MAIN
              WHERE  object_type = 'PACKAGE'
               AND   object_name = upper(p_name)
               AND   NOT EXISTS  (SELECT 'a'
                                  FROM   USER_OBJECTS
                                  WHERE  object_type = 'PACKAGE'
                                   AND   object_name = upper(p_name))
              UNION
              SELECT 4, object_type
              FROM   USER_OBJECTS@MAIN
              WHERE  object_type = 'PACKAGE BODY'
               AND   object_name = upper(p_name)
               AND   NOT EXISTS (SELECT 'a'
                                 FROM   USER_OBJECTS
                                 WHERE  object_type = 'PACKAGE BODY'
                                  AND   object_name = upper(p_name))
              ORDER BY 1)
  LOOP
     sql_str:='SELECT 1 '||
	      'FROM   user_objects@main a '||
	      'WHERE  object_type = '''||obj.type||''''||
	      ' AND object_name = '''||upper(p_name)||''''||
	      ' AND last_ddl_time < (SELECT last_ddl_time '||
				 ' FROM   user_objects '||
				 ' WHERE   object_type ='''||obj.type||''''||
				 ' AND	   object_name = '''||upper(p_name)||''')';


     OPEN this_cv FOR sql_str;
     FETCH this_cv INTO dummy_;
     IF this_cv%NOTFOUND THEN
       --
       -- Transfer the code
       --
       txt_1:='Create or replace '; txt_2:=NULL;
	     sql_str:='SELECT text '||
		            'FROM user_source@MAIN '||
		            'WHERE type = '''||obj.type||''''||
		            ' AND name = '''||p_name||''' ORDER BY line';
	     OPEN this_cv FOR sql_str;
	     LOOP
	        FETCH this_cv INTO line_;
	        EXIT WHEN this_cv%NOTFOUND;
	        IF length(txt_1) + length(line_) + nvl(length(txt_2),0) < MAXVARLEN
	        THEN
	           txt_1:=txt_1||line_;
	        ELSE
	           txt_2:=txt_2||line_;
	        END IF;
	     END LOOP;
       COMMIT;
             dbms_output.put_line(txt_1||txt_2);
	     EXECUTE IMMEDIATE txt_1||txt_2;
       COMMIT;
       --
     END IF;
     CLOSE this_cv;

  END LOOP;

  COMMIT;

END load;

END DOM$BOOTSTRAP;
/
