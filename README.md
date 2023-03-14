# Domain-Api-Resi-Results

# Domain-Auctions
Access the Domain API to get auction results for support major cities. 

The basis of this project began in Febuary 2022 while looking to purchase a house. (WIP) 

I soon discovered there were some limitations on the front end searching of the two dominant real estate websites in Australia.

I came across this project by Alex D'Ambra which utilised the Domain API. 
https://medium.com/@alexdambra/how-to-get-aussie-property-price-guides-using-python-the-domain-api-afe871efac96

This is the second version of the script - the first was run locally on a sqlite database. This current version was rewritten to make use of Amazon RDS and a hosted postgres server. 
The intenetion that it be run via an Amazon lambda instance and be completely hands off.

An example of the data visualisation below. Colour density indicating median Price, the number indicating number of sales within each Postcode.
Note: The data gathered from Domain is not complete and there will be instances where the purchases has chosen to withhold the sale price which will result in a _null_ value for Price. The above visualisation is for demonstrative purposes and may differ from other sources. 
![Tableau_example](https://user-images.githubusercontent.com/113073854/206088732-b924e4b1-7cd4-4350-af72-eb5d15a2086e.PNG)

![Tableau_example_2](https://user-images.githubusercontent.com/113073854/206089729-b35fb7f7-abd1-4916-ba3c-ff13256f3313.PNG)

