# Domain-Api-Resi-Results.

## Note 
**As of August 2023 Domain has removed access to their free tier of APIs resulting in this project being on permanent hiatus.**

## About:
The basis of this project began in Febuary 2022 while looking to purchase a house and wanting to keep a track of sold prices of houses I had looked at without needing to go through the website search UI. The project evolved from just the properties I was personally interested in, to all sold listings for the available cities every week. I was able to query areas I was interested in with criteria for bedrooms and bathrooms and get an idea of historical prices and what had sold in the area. This proved to be easier than using the typical search filters on real estate websites. 

This is the second iteration of the script - the first was run locally on a sqlite database. The current version was rewritten to make use of Amazon RDS and a hosted Postgres server. The project is now entirely hosted on AWS running via Lambda + EventsBridge to a postgreSQL database.

It must be noted that this was never designed or intended to provide a complete list of sales as many of the listings will have their sold price omitted and appear as *null*. 

## Objective: 
- Use the Domain API to get weekly auction results for the supported major cities in Australia to further analyse in PowerBI
- Develop and improve understanding of:
  - Python
  - SQL
  - AWS


## Data Pipeline
Basic data pipeline

![Untitled Diagram drawio](https://github.com/AJardelH/Resi-Results/assets/113073854/00f7465b-f485-4c6a-b4f1-5bbbe109b193)

## Example Data

![image](https://github.com/AJardelH/Resi-Results/assets/113073854/ee52ad27-7b96-4b5f-ab3f-62b9f5ea93dd)






## Example Visualisation:
An example of the data visualisation below. Colour density indicating median Price, the number indicating number of sales within each Postcode.
Note: The data gathered from Domain is not complete and there will be instances where the purchases has chosen to withhold the sale price which will result in a _null_ value for Price. The above visualisation is for demonstrative purposes and may differ from other sources. 
![Tableau_example](https://user-images.githubusercontent.com/113073854/206088732-b924e4b1-7cd4-4350-af72-eb5d15a2086e.PNG)

![Tableau_example_2](https://user-images.githubusercontent.com/113073854/206089729-b35fb7f7-abd1-4916-ba3c-ff13256f3313.PNG)

