## How to use
Open terminal and run - python ETL.py

## Datasets
1. I94 Immigration Data: This data comes from the US National Tourism and Trade Office. 
```
+--------+
|VISATYPE|
+--------+
|      F2|F2 visas are used by the dependents of F1 visa holders
|     GMB|
|      B2|B1 visa is for business while the B2 visa is for pleasure
|      F1|F1 visa is issued to international students who are attending an academic program
|     CPL|
|      I1| Press / Media Representatives
|      WB|Waiver
|      M1|international students who are attending an academic program, Can work on campus
|      B1|B1 visa is for business while the B2 visa is for pleasure
|      WT|Waiver
|      M2|used by the dependents of M1 visa holders
|      CP|
|     GMT|
|      E1|International Trade
|       I|Information media representative (media, journalists)
|      E2| Treaty investors
|     SBP|
+--------+

A List of NIV Types - http://www.ustraveldocs.com/in/in-niv-typeall.asp
Purpose of Travel to U.S. and Nonimmigrant Visas	Visa Type
Athletes, amateur and professional (competing for prize money only)	B-1
Athletes, artists, entertainers	P
Australian worker - professional specialty	E-3
Border Crossing Card: Mexico	BCC
Business visitors	B-1
Crewmembers (serving aboard a sea vessel or aircraft in the U.S.)	D
Diplomats and foreign government officials	A
Domestic employees or nannies (must be accompanying a foreign national employer)	B-1
Employees of a designated international organization, and NATO	G1-G5, NATO
Exchange visitors	J
Exchange visitors - au pairs	J-1
Exchange visitors - children (under age 21) or spouse of a J-1 holder	J-2
Exchange visitors - professors, scholars, teachers	J-1
Exchange visitors - international cultural	J, Q
Fiancé(e)	K-1
Foreign military personnel stationed in the U.S.	A-2, NATO1-6
Foreign nationals with extraordinary ability in sciences, arts, education, business or athletics	O-1
Free Trade Agreement (FTA) professionals: Chile	H-1B1
Free Trade Agreement (FTA) professionals: Singapore	H-1B1
Information media representative (media, journalists)	I
Intra-company transferees	L
Medical treatment, visitors for	B-2
NAFTA professional workers: Mexico, Canada	TN/TD
Nurses traveling to areas short of health care professionals	H-1C
Physicians	J1, H-1B
Religious workers	R
Specialty occupations in fields requiring highly specialized knowledge	H-1B
Students - academic and language students	F-1
Student dependents - dependent of an F-1 holder	F-2
Students - vocational	M-1
Student dependents - dependent of an M-1 holder	M-2
Temporary workers - seasonal agricultural	H-2A
Temporary workers - nonagricultural	H-2B
Tourism, vacation, pleasure visitors	B-2
Training in a program not primarily for employment	H-3
Treaty investors	E-2
Treaty traders	E-1
Transiting the United States	C
Victims of human trafficking	T-1
Victims of criminal activity	U-1
Visa renewals in the U.S. - A, G, and NATO	A1-2, G1-4, NATO1-6
```

2. World Temperature Data: This dataset came from Kaggle. 
3. U.S. City Demographic Data: This data comes from OpenSoft. 
4. Airport Code Table: This is a simple table of airport codes and corresponding cities. 

## Data Model 

The purpose of the final data model is to provide easier access to immigration data. It can be used to answer immigration traffic coming during a particular any particular season. This can later be used to create a ML forecasting model to better manage the complete process.

```
+++++++++++++++++++++++++++++++++++++
time	--	tourism	--	airport
            |		
            temperature		
            |		
            demographics		
+++++++++++++++++++++++++++++++++++++        
```

1. airport_codes - Filtered for US as iso-country. Immmigration table can be joined with this table by pulling the iata_code to get the airport information
2. us_cities_demographics - Assumed static population
3. GlobalLandTemperaturesByCity - Assumed 8 years seasonality in temperature fluctuations
4. TourismByAirChannel 
5. Time - Time table created from arrival data in immigration data

## Steps:
Outline of the steps taken in the project.

1. All the files from ../../data/18-83510-I94-Data-2016/ are read. A custom function is written to parse the sas datasets to parquest files. These parquest are filtered for removing missing values and aggregated to improve performance
2. SAS numeric dates are converted to python dates and time related fields are extracted to create 'Time' table
3. From Global temperature, Seasons are added- spring, summer, autumn and winter.
4. US demographics data is loaded
5. Airport data is filtered for US, so that it can be joined with the corresponding i94 airports
6. Checks for joins are added in a separate function - Assumed 8 years seasonality in Temperature Fluctuations, Static Population in a city


## Scaling Scenarios
1. The data was increased by 100x. - Move to Cassandra as reading and and writing speed will become the top priority
2. The pipelines would be run on a daily basis by 7am everyday. - Move to airflow and create DAGs with triggers on failing pipelines
3. The database needed to be access by 100+ people. - Create a Data Lake with smaller analytical datasets which can be used to run business reports

## Quality Checks
A custom function is written that reads the parquet files and shows a sample of data loaded; checks for joins between the tables



