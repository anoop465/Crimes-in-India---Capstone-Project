Create database SQLphase3
Use SQLphase3
show databases

/*  3.1 Insert records from 42_District_wise_crimes_committed_against_women_2001_2012.csv into a table.*/

CREATE TABLE crimes_committed_against_women (
    STATE_UT VARCHAR(50),
    DISTRICT VARCHAR(50),
    Year INT,
    Rape INT,
    Kidnapping_and_Abduction INT,
    Dowry_Deaths INT,
    Assault_on_women_with_intent_to_outrage_her_modesty INT,
    Insult_to_modesty_of_Women INT,
    Cruelty_by_Husband_or_his_Relatives INT,
    Importation_of_Girls INT
)
select * from crimes_committed_against_women

LOAD DATA INFILE '42_District_wise_crimes_committed_against_women_2001_2012.csv' 
INTO TABLE crimes_committed_against_women
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
select * from crimes_committed_against_women

/* 3.2 Write SQL query to find the highest number of rapes & Kidnappings that happened in which state, District, and year.*/

SELECT State_UT, District, Year, MAX(Rape) AS Max_Rapes, MAX(Kidnapping_and_Abduction) AS Max_Kidnappings
FROM crimes_committed_against_women
WHERE District NOT LIKE '%total%'
GROUP BY State_UT, District, Year
ORDER BY Max_Rapes DESC, Max_Kidnappings DESC
LIMIT 1;

/* 3.3	Write SQL query to find All the lowest number of rapes & Kidnappings that happened in which state, District, and year.*/
WITH MinValues AS (
    SELECT 
        MIN(Rape) AS Min_Rapes, 
        MIN(Kidnapping_and_Abduction) AS Min_Kidnappings
    FROM 
        crimes_committed_against_women  WHERE 
        District NOT LIKE '%total%'
)
SELECT  STATE_UT,  DISTRICT,  Year,  Rape AS Min_Rapes,  Kidnapping_and_Abduction AS Min_Kidnappings
FROM  crimes_committed_against_women,  MinValues
WHERE 
    District NOT LIKE '%total%'
    AND (Rape = MinValues.Min_Rapes OR Kidnapping_and_Abduction = MinValues.Min_Kidnappings);

/*3.4	Insert records from 02_District_wise_crimes_committed_against_ST_2001_2012.csv into a new table*/

CREATE TABLE District_wise_crimes_committed_against_ST (
    STATE_UT VARCHAR(50), DISTRICT VARCHAR(50), Year INT, Murder INT, Rape INT, Kidnapping_and_Abduction INT, Dacoity INT,
    Robbery INT, Arson INT, Hurt INT, `Protection_of_Civil_Rights_(PCR)_Act` INT, `Prevention_of_atrocities_(POA)_Act` INT,
    Other_Crimes_Against_STs INT );
Select * from District_wise_crimes_committed_against_ST
LOAD DATA INFILE '02_District_wise_crimes_committed_against_ST_2001_2012.csv' 
INTO TABLE District_wise_crimes_committed_against_ST
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
Select * from District_wise_crimes_committed_against_ST

/* 3.5	Write SQL query to find the highest number of dacoity/robbery in which district.*/

SELECT  STATE_UT,  DISTRICT,  Year,  Dacoity,  Robbery
FROM  District_wise_crimes_committed_against_ST
WHERE 
    (Dacoity = (SELECT MAX(Dacoity) FROM District_wise_crimes_committed_against_ST) 
    OR Robbery = (SELECT MAX(Robbery) FROM District_wise_crimes_committed_against_ST))
    AND District NOT LIKE '%total%';
    
    /* 3.6	Write SQL query to find in which districts(All) the lowest number of murders happened.*/
SELECT MIN(Murder) AS min_murders
FROM District_wise_crimes_committed_against_ST;
SELECT DISTRICT, STATE_UT, Year, Murder
FROM District_wise_crimes_committed_against_ST
WHERE Murder = (SELECT MIN(Murder) FROM District_wise_crimes_committed_against_ST);


/* 3.7	Write SQL query to find the number of murders in ascending order in district and year wise.*/

SELECT DISTRICT, STATE_UT, Year, Murder
FROM District_wise_crimes_committed_against_ST
ORDER BY DISTRICT ASC, Year ASC, Murder ASC;

/* 3.8.1	Insert records of STATE/UT, DISTRICT, YEAR, MURDER, ATTEMPT TO MURDER, and RAPE columns only 
from 01_District_wise_crimes_committed_IPC_2001_2012.csv into a new table.*/

CREATE TABLE District_wise_crimes_committed_IPC (
    STATE_UT VARCHAR(50), DISTRICT VARCHAR(50), Year INT, MURDER INT, ATTEMPT_TO_MURDER INT, Rape INT );

Select * from District_wise_crimes_committed_IPC


LOAD DATA INFILE '01_District_wise_crimes_committed_IPC_2001_2012.csv'
INTO TABLE District_wise_crimes_committed_IPC
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(STATE_UT, DISTRICT, Year, MURDER, ATTEMPT_TO_MURDER, Rape,@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,
@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,
@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,@dummy);

/* 3.8.2	Write SQL query to find which District in each state/UT has the highest number of murders year
 wise. Your output should show STATE/UT, YEAR, DISTRICT, and MURDERS.*/
 
 WITH RankedMurders AS (
    SELECT STATE_UT, DISTRICT, Year, MURDER,
        ROW_NUMBER() OVER (PARTITION BY STATE_UT, Year ORDER BY MURDER DESC) AS rn
    FROM  District_wise_crimes_committed_IPC  )
SELECT STATE_UT, Year, DISTRICT, MURDER FROM RankedMurders WHERE rn = 1
ORDER BY STATE_UT, Year;

/* 3.8.3	Store the above data (the result of 3.2) in DataFrame and analyze districts that appear 3 or more than 3 years and print the corresponding state/UT, district,
 murders, and year in descending order.*/
 -- Step 1: Create a temporary table to store the initial results
CREATE TEMPORARY TABLE IF NOT EXISTS highest_murders AS WITH RankedMurders AS (
    SELECT STATE_UT, DISTRICT, Year, MURDER, ROW_NUMBER() OVER (PARTITION BY STATE_UT, Year ORDER BY MURDER DESC) AS rn
    FROM District_wise_crimes_committed_IPC )
SELECT STATE_UT, Year, DISTRICT, MURDER FROM RankedMurders WHERE rn = 1;
-- Step 2: Identify districts that appear 3 or more times
CREATE TEMPORARY TABLE IF NOT EXISTS frequent_districts AS
SELECT DISTRICT FROM highest_murders GROUP BY DISTRICT HAVING COUNT(*) >= 3
-- Step 3: Select the relevant data and order it
SELECT hm.STATE_UT, hm.Year, hm.DISTRICT, hm.MURDER FROM highest_murders hm JOIN frequent_districts fd ON hm.DISTRICT = fd.DISTRICT
WHERE hm.DISTRICT != 'total'  -- Exclude rows where district is 'total'
ORDER BY hm.STATE_UT, hm.DISTRICT, hm.Year DESC;
