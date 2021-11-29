
PROJECT NAME: 
	
	Building and Analysing Data Warehouse Prototype for METRO Shopping Store in Pakistan

PROJECT DESCRIPTION: 
	
	This project extracts operational data from databases through the incoming stream, transforms it in a format
	suitable to be loaded in a data warehouse, and loads it into the warehouse using an efficient join algorithm
	called MESHJOIN. Several OLAP queries are executed to find out answers to several business oriented problems.
	
TOOLS AND TECHNOLOGIES USED:

	1. Java - Extract, load and transform data using MESHJOIN
	2. Google Guava - Multimap (hashtable) library
	3. MYSQL - Store the warehouse data in start schema format
	4. SQL - Perform OLAP queries 

HOW TO OPERATE PROJECT:

	1. Have Java installed on your system
	2. Install Google Guava jar file and place it into the java project directory
	3. Replace MYSQL connection in meshjoin.java with your MYSQL connection
	4. Have the star schema tables setup using createDW.sql file
	5. Run meshjoin.java
	6. In under a minute, the java file should have populated all the data into the dwh star schema
	7. Run OLAP queries using queriesDW.sql through MYSQL
	
