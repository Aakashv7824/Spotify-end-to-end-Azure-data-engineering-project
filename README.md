# Spotify Analytics using Azure tech stack

## Overview
This project aims to constantly fetch data from spotify API and provide analytics on various paramerters.


## Project Goals
1. Data Ingestion — Use ADF and Python to injest data.
2. ETL System — We are getting data in JSON format, transforming this data into the proper format using Python.
3. CSV  — We will be getting data from spotify API in json format , we will convert it into csv using pandas.
4. Cloud — Its a Timer triggered project , we will use ADLS and timer based Azure functions to clean and tranform the data using pandas.
6. Reporting — Use Azure Synaspe for analysis.

## Services we will be using
1. ADLS: ADLS is an object storage service that provides manufacturing scalability, data availability, security, and performance.
2. ADF : Azure data Factory to load the data from ALDS to Azure Synapse
3. Azure Synapse : once the data is loaded to Syanpse , we need to perform analytics on that. 
4. Azure Functions : Creat a timer based trigger to constantly retrieve data from spotify API.

## Architecture Diagram
<img src="architecture.jpeg">
