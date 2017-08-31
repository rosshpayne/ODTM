# README #

Distributed Operation Manager (DOM) for Oracle

DOM provides a simple framework and execution engine to manage the execution of your complex database maintenance operations across a network of Oracle database instances from a central server, called the DOM-server, in a safe, restartable and scalable fashion.

Complex database operations are usually composed of mulitple SQL statements and/or stored procedure calls applied to one or more database objects (tables, indexes, partitions etc) where each step must complete successfuly before proceeding to the next step. Alternatively, an operation may involve running identical steps over multiple Oracle instances concurrently.  Eitherway, the more complex an operation the easier it becomes to justify the cost of configuring DOM to do it.

The DOM framework requires each database operation be represented as a number of repeatable tasks, each implemented as a stored procedure contained in a single PL/SQL package, combined with a table containing state information to guarantee safe restarting of a failed operation.

* The DOM architecture:

DOM employees a singler server with multiple clients model. Further:

        + DOM-server  -  single database instance which contains the DOM repository, all your code and initiates all operations.
                         (In the code base this is referred as the MAIN server) 
        + DOM-clients -  one client for each database instance that executes an operation.  Requires a DOM-client schema and DOM$bootstrap package.
                         The DOM-server will remotely install all code on the client during each operation run as part of the initialisation phase.
                         
DOM features include:

* central Data Repository

The repository defines the data required to drive DOMs execution of database operation across the enterprise. Such information includes but is not limited to:

    + database environments types (dev,test,prod etc)
    + the Oracle instances that belong to those environments
    + the package and SPs that defines the database operation
    + which operations run across which Oracle instances
    + user defined parameters (key-value pairs) to drive your code logic 
    + detailed runtime logging of all operations, tasks and SQL

* Self installing

The DOM-server is responsible for installing on each DOM-client your package code required to run the operation. This happens before each operation run.

* secure implementation

The DOM repository and runtime operations are conducted in their own dedicated database schemas which follow minimum privileges model. The DOM server schema has privileges to maintain the repository while each remote instance has a DOM schema with sufficient privileges (usually at a DBA level) to perform the database operations required.

* a single code repository for your database packages.

Each SP package is initially saved to the DOM repository.  At runtime DOM will copy the package to each remote database instance involved in the operation. Note: the repository holds only the package code not any of its dependent objects.

* central runtime logging of your operation down to the SQL level

Each operation generates detailed logging back to the central DOM-server down to the SQL level.  Such metrics include the execution time for the operation, its tasks and associated SQL, status of each, the SQL text and number of parallel threads used and number of rows processed where appropriate.

* Simple Framework 

DOM's framework requires that for each operation you must create a "state" table and define three stored procedures in your operatino package. 
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
