# Project 1

## 2020 Covid-19 Cases by Country Analysis

A simple project that utilizes mapreduce on a dataset of daily confirmed covid-19 cases(per country) for analysis

## Technologies

Project was created with:

- SBT version 1.4.4
- Scala version 2.13.2
- Apache Hadoop Common 3.2.1
- Apache Hadoop MapReduce Core 3.2.1

## Requirements

- JDK version 8 or 11
- Scala and SBT
- SBT-assembly plugin

## Data Analysis Questions

- [x] **[Question #1]** - How many confirmed Covid-19 cases did each country have by 12/14/2020

  - Create a customized mapper class to select only Countries and cases.
  - Create a customized reducer class to combine results

- [ ] **[Question #2]** - List the top 20 countries that had the highest cumulative cases by 12/14/2020

  - Create a hive query (via docker)

- [ ] **[Question #3]** - List the top 20 countries that had the fewest cumulative cases by 12/14/2020
  - Create a hive query (via docker)

## Dataset Stats

> **Original Dataset Format** (619,000 records)  
> `Date Reported` `Day` `Month` `Year` `Cases` `Deaths` `Countries and Territories`  
> `GeoID` `Country Code` `2019 Population` `Continent` `# of cases per/100000`  
**MapReduced Data Result** (193 records)  
> `Countries and Territories` `Total Cumulative Cases`

## Todo List/ Features

- [x] Create custom Mapreduce with scala
- [x] Create jar file
- [x] Upload dataset and jar file to docker container
- [x] Move dataset to hdfs
- [x] Run jar file on dataset
- [ ] Install Hive on docker
- [ ] Run Hive queries on dataset

## references:

[Dataset](https://www.ecdc.europa.eu/en/publications-data/download-todays-data-geographic-distribution-covid-19-cases-worldwide)  
[Big Data Europe's dockerized Hadoop cluster](https://github.com/big-data-europe/docker-hadoop)
