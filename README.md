# Sparkify Streaming Database for Song Play Analysis
The team over at sparkify has an issue with disparate data currently.  Their analytics team can't easily access the data to understand what actions they should take to optimize usage of their platform.  Our goal is to collect the data from these disparate sources and colocate them into a Postgres database to aide in their useage of it.  We will generate 1 fact table, of songplays, and then 5 dimension tables, which will be our users, songs, artists, and time.  The data is centered around when a user utilized the "NextSong" action.

## Project Components
### Create Tables
This component is the first to run in the pipeline/project.  This create the inital database, if tables currently exist it drops them, and then recreates them.  This ensures we start the pipeline on a fresh start.

### ETL
This component runs after create tables.  This crawls over the files that exist in the directories, and parses them to produce the necessary data to populate the tables.

### SQL Queries
This contains the queries that create tables uses to generate the tables initially.  Additionally, it contains the insert statements that ETL uses to populate the tables.  It also contains 1 query, that the songplay table relies on, due to needing to join data from other tables to generate it.

### Database Regeneration
This is an end to end script that can be ran to generate everything in one go.  This combines the ETL and Create Tables script, to have one easy to utilize location to run.

### Test Database Regeneration
This is a basic test script, to ensure that we can run the end to end pipeline.  It runs the ETL/Create tables, and then runs some very basic test queries, and just ensures that there is data returned.  This can be ran through the terminal with pytest.

### Analytic Queries Notebook
This runs some basic queries, and analytics to understand the data.  This is a good place to go for basic insight into data.