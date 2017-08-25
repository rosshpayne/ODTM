# README #

DOM is a framework as well as a centralised execution environment designed to run your complex Oracle database operations (e.g. purge data, reorg tables/indexes)
across a network of hundreds of Oracle instances in a safe and completely restartable and scalable fashion.

An Operation can be any short or long running database administration task, that you want to make safely restartabe should an error occur at any stage in the process.

An opertion typically executes many SQL statements that must all be successfully executed in a particular order. 
Should anyone SQL statement fail you want the operation to be immediately aborted and a log of the error produced.  Once you have resolved the reason 
for the failure you want to be able to restart the operation by simply executing the same command you used to start the operation and have DOM complete the operation from where it last failed.

DOM provides you this facility among other things like complete logging with performance metrics of all SQL.  
All you need to do is develop a set of stored procedures which represent restartable steps in the operation. Each SP generates one or more SQL statemnts which are 
executed via a DOM API.

DOM executes each task concurrently across as many database instances you have associated with the operation.  


### What is this repository for? ###

* Quick summary
* Version
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
