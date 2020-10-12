### Introduction

Sparkify is a sports startup company. They want to analyze the data, being collected for the music industry. The analytics team is interested in understanding what songs users are listening to.currently they don't have the setup to do it. The data is in the JSON format located in different folders.

### Database schema design 
The whole data will be stored in multiple tables. this will avoid the duplication of data.
* Users table contains all the data related to users
* songplays contains all the songs played by the artist.
* songs contains all the songs played and the artist id who palyed it
* artists contains all the artist details.
* time contains the time details.

_Here is the schema diagram_
![schema design](Schema_design.png)



### ETL Process
Here are the files used:
###### README.md
    It contains a summary of the project

###### sql_queries.py
    This file contains all the scripts for all the tables assigned to varaibles.
 * To drop the tables 
 * To create the tables 
 * To insert the data
    
###### create_tables.py
   This file contains python scripts for creating/dropping database. It calls the SQL script in SQL_queries.py file to create tables.
 
###### etl.py
This file contains script to read the json files using python. Then insert them to the tables.

###### RunCreate_db_tables.ipynb
  Use this file to call the "create_tables.py" for creating tables and database. call "etl.py" to run the ETL proess

These below two are for testing ETL and testing the inserted data.
* test.ipynb
* etl.ipynb