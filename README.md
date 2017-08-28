# README #

DOM provides a simple framework and an execution engine, known as the DOM-server, which centrally manages the execution of your database operations remotely across a network of Oracle database instances in a safe, restartable and scalable fashion.

Each database operation is represented as an ordered set of stored procedures calls defined in a single PL/SQL package that you develop utilising DOM’s simple framework to guarantee safe restarting of a failed operation while providing logging of all operations to the single DOM server.

DOM features include:

* a central Data Repository

The repository defines the data required by DOM to drive the execution of each database operation across your enterprise. Such information includes but is not limited to:

  ** database environments types (dev,test,prod etc)
  ** the Oracle instances that belong to those environments
  ** the package and SP that defines the database operation
  ** which operations run across which Oracle instances
  ** user defined parameters (key-value pairs) to drive your code logic 
  ** detailed runtime logging of all operations, tasks and SQL

* secure implementation

The DOM repository and runtime operations are conducted in their own dedicated database schemas with minimum privileges. The DOM server schema has privileges to maintain the repository while each remote instance has a DOM schema with sufficient privileges (usually at a DBA level) to perform the database operations required.

* a single code repository for your database packages.

Each SP package is initially saved to the DOM repository.  At runtime DOM will copy the package to each remote database instance involved in the operation. Note: the repository holds only the package code not any of its dependent objects.

* a central runtime logging of your operation down to the SQL level

DOM logs all runtime metrics for each operation across each remote instance and for each of the tasks associated with an operation down to the SQL level.  Such metrics include the execution time for the operation, its tasks and associated SQL, status of each, the SQL text and number of parallel threads used and number of rows processed where appropriate.

* Concurrent processing

For each operation DOM issues a separate Oracle scheduler job on the DOM server for each of the database instances assigned to the operation.  Each job in turn executes the DOM code responsible for remotely executing the uncompleted SP on its assigned Oracle instance

* Simple Framework 

DOM provides a framework which your packages should use to get all the benefits of flexibility of operation, re-startability and logging.  

* Simple and safe restartability

Should an operation fail DOM will log the error in the central repository. Once you have fixed the reason for the error the DOM administrator merely has to issue the same command used to start the operation.  DOM will restart the operation from the failed task. 

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
