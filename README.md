# README #

Oracle Distributed Task Manager (aka DOM) 

ODTM provides a simple framework to manage and execute complex database maintenance operations from a central database instance, called the DTM-server, across a network of remote Oracle database instances, in a safe, restartable and scalable fashion.

Complex database operations, such as purging hundreds if not billions of records in a single operation, are usually comprised of mulitple SQL statements that must be executed in a specific order and in the event of a failure of any one SQL statement should immediately abort the whole operation. While this is easy to do making the operation restartable is slightly more difficult. It requires the operation to be broken down into separate repeatable units, called tasks, which in most but not all cases, executes  one SQL statement. The other requirement is the operation must execute in a framework that enables it to be self-aware, meaning it must know what tasks to run in series or parallel and be able to detect a failed task immediately and abort that stream of the operation and then be in a position to recommence the operation from the point of the failed task -  once the issue has been resolved by the DBA.  This is essentially what ODTM does but on a grand scale while providing the centralised management of code and all logging data. ODTM will automatically install all the code required to execute each task across each instance involved in an operation. You do not have to manage code distribution across potentially hundreds of of Oracle instance. ODTM allows you to manage all the code from one central server and ODTM does the rest. 

, managing long running complex operations across a plethora of Oracle database instances while installing the necessary task software on each remote instance automatically prior to execute the operation. 

while providing detailed logging of each operation, its tasks and the individual SQL statements. When applicable, the logging records the number of rows impacted for each SQL statement. 

You can halt an operation at any stage and resume it at a later time.

The DTM framework requires each of your maintenance opeations be packaged into a single PLSQL package where each task and its associated SQL, is represented as a stored proc call.

* Usage example	

The following demonstrates three of the basic features of DTM, the centralised configuration and execution management, realtime logging and the automatic restartability of an operation.


    0.  Configure your database environment, install ALL your DTM packages, state tables and database links
        into a nominated DTM-Oracle instance. This is a one off task. 
	The DTM-Oracle instance is where you issue DTM commands and contains the log data of all operations and tasks across
	each database instance involved in an operation.
	It is not necessary to log into any remote database involved in an operation to monitor the operations progress or
	issue commands to start or stop a task. Everything is managed from the DTM-Oracle instance.
        
    1.  Start operation using PLSQL API
		
		SQL> execute DOM$MAIN.run_op ( 12, 1)

            where 12 represents the operation id which you have defined, in this example, to be a 7 step operation .
            The second argument is the environment id, such as PRODUCTION, UAT or TEST.
            
       The above API is typically executed via a scheduler of your choice.

	2. Unfortunately the operation fails with a space issue (say) at step/task 4. 
	   DTM aborts and writes the error to various log tables on the central DTM-server instance.  
	   There remains task 4 plus 3 other tasks to complete however.
       
           Review the reason for the failure by refering to one or all of the following log tables in the DTM-server:

		SQL>  select * from DOM$run_log  where id = DOM$MAIN.get_run_id(17,1)
		SQL>  select * from DOM$task_log where run_id = DOM$MAIN.get_run_id(17,1)
		SQL>  select * from DOM$sql_log  where task_log_id = g_task_log_id
	

 	3.   Fix the space issue 
    
   	4.   Restart the operation using the SQL from step 1.   
    
  	     DTM will automatically run the remaining tasks starting at task 4.
         
    This example shows just one operation which maybe one amoungst hundreds that are being executed concurrently by DTM
    
* The DTM architecture:

DTM employees a single server with multiple clients model. Further:

        + DTM-server  -  schema which contains the DTM configuration repository, all your code and state tables.
                         DTM executes operation from this server and initiates all operations remotely 
                         across DTM-clients.
                         (In the code this is referred as the MAIN server) 
        + DTM-clients -  one client for each database instance that executes a task in an operation.  
                         Requires a DTM-client schema and DOM$bootstrap package.
                         All DTM-clients should be accessible to the DTM-Server via db-links. 
                         The DTM-server should also be accessible to the DTM-client via a db-link.
                         The DTM-server will remotely install your operation package and state table on the client 
                         at runtime during the initialisation phase of each operation.
                         
DOM features include:

* central Data Repository

The repository defines the data required to drive DTMs execution of database operation across the enterprise. Such information includes but is not limited to:

    + database environments types (dev,test,prod etc)
    + the Oracle instances that belong to those environments
    + the package and SPs that defines the database operation
    + which operations run across which Oracle instances
    + user defined parameters (key-value pairs) to drive your code logic 
    + detailed runtime logging of all operations, tasks and SQL

* Self installing

The DTM-server is responsible for installing your package code, representing the repeatable tasks in an operation, and associated state table across each DTM-client associated with each operation during the initialisation phase of each operation execution.

* secure implementation

The DTM configuration repository and runtime operations are conducted in their own dedicated database schemas which should follow a minimum privileges model. The DTM server schema has privileges to maintain the repository while each remote instance has a DOM schema with sufficient privileges (usually at a DBA level) to perform the database operations required.

* a single code repository for your database packages.

All your pacakges are installed or upgraded on the DOM repository only.  At runtime DTM will remotely copy the relevant package to each database instance involved in the operation. Note: the repository holds only the package code not any of its dependent objects.

* central runtime logging of your operation down to the SQL level

Each operation generates detailed logging back to the central DTM-server down to the SQL level.  Such metrics include the execution time for the operation, its tasks and associated SQL, status of each, the SQL text and number of parallel threads used and number of rows processed where appropriate.

* Simple Framework 

DTM's framework requires that for each operation you must create a "state" table and define three stored procedures in your operatino package. 
The table is used to hold operation state between execution of each repeatable task defined in the operation package. 
The three manadatory procedures are:

        + initial
        + final
        + iterator
        
See code examples for further descriptions.

* Simple and safe restartability

Should an operation fail DOM will log the error in the central repository. Once you have fixed the reason for the error the DOM administrator merely issues the same command used to start the operation.  DOM will restart the operation from the failed task. 

* Version 1.0 

* [Learn Markdown](https://bitbucket.org/tutorials/markdowndemo)

### How do I get set up? ###

* Summary of set up
* Configuration
* Dependencies
* Database configuration
* How to run tests
* Deployment instructions

### Contribution guidelines ###

* Writing tests
* Code review
* Other guidelines

### Who do I talk to? ###

* Repo owner or admin
* Other community or team contact
