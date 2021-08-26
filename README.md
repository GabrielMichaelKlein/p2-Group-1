# Spark-Scala: Covid Analysis
## General Info
The purpose of this project is to make use of Scala, Spark, and Hadoop in order to find various trends within covid data gathered by Johns Hopkins
## Technologies Used
* Intellij Community
* Scala 2.11.12
* SBT Build Tool 1.5.5
* Hadoop 3.3.0
* Spark
* Git
* GitHub
* Excel (For graphics/visuals)
## Features
* **Trend 1: U.S. Case and Death analytics**
  * Calculates the amount of confirmed cases out of population for each state within the United States
  * Calculates the amount of deaths out of population for each state within the U.S.
  * Calculates the amount of deaths out of confirmed cases for each state within the U.S.
  * There is also an option to find the maximum and minimum for each of the above calculations
* **Trend 2: U.S. cases and deaths over time**
  * Queries the covid_19_data to group covid cases/deaths with their respective state and date measured
  * Plots the case total per state as it grows over time
  * Plots the death total per state as it grows over time
* **Trend 3: Comparing cases and deaths globally**
## Usage
* Save the Johns Hopkins input data to an input folder within the project's root folder
* To show max and min calculations for Trend 1 uncomment 'Extra queries' under the caseAndDeathTrendsUS method
