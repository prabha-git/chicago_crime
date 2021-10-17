# Chicago Crime Analysis


[Chicago crime data](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2) contains the reported incidents of crime that occured in Chicago since 2001.

## Maintaining a history
Data in the portal does not have any history, latest info for each case is updated in the dataset. I decided to maintain the history of the data using google cloud.

I created a python script to pull the data using sodapy api and stores the data in Google Big Query database. if there is an update to an existing case record it will inactivate the record in teh GBQ and inserts the latest record.

![](https://si.wsj.net/public/resources/images/BN-XJ808_CARJAC_P_20180208130657.jpg)


*Photo from wsj.com*

I dockerize the script and publish to google container registry and deployed in google cloud run. Used Google scheduler to run the python script every day at 9 PM. 


Below is the architecture cloud diagram.
![](https://github.com/prabha-git/chicago_crime/blob/master/gbq_data_history/Chicago_crime_data_history.png)
