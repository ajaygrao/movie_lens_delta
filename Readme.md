# MovieLens Data Ingestion and Transformation

### Description
This is a scala spark based code to ingest MovieLens data in csv format and store it as delta lake tables.


Library/SDK | version 
--- | --- 
java | 1.8 openjdk
scala | 2.12.15
spark | 3.2.0
delta | 1.2.1

Note: Spark ans Scala libraries are marked provided so please be sure to include provided package while executing

#### This program is able to ingest these data:
* Movies
* Ratings
* Tags

#### Transformations:
* Split movies with mulriple genres to multiple rows
* Fetch the top 10 movies with maximum average rating

The program takes 2 mandatory arguments in this order:
* Path to the directory where all input csv are stored
* Path where the delta table needs to be stored

Note: input files are read based on names from given directory
So please store them as movies.csv, ratings.csv and tags.csv respectively
