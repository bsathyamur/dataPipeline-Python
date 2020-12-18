Data Pipeline Automation using Python

Use Python to automate the data acquisition, cleaning/transformation, and storage process.

The python script will use the config.yaml file to read the source and target file locations to download the data file from the URL 
[link] https://archive.ics.uci.edu/ml/datasets.php

The python script will perform the following process in a step by step procedure:
* Download the retail dataset from the specified URL
 * Clean the retail dataset 
 * Convert null fields in user id to Guest
          b. Convert null fields in description to Unlisted
          c. Extract the invoice date column and add new columns - Year and Quarter
          d. Based on the Quantity column add a new column - InvoiceType with values either Purchase or Return
     3. Split the dataset into retail dataset for United Kingdom and Other countries
     4. Save the dataset as two csv files
     
  The logging process will log each of the success/failure message into the datapipeline_log.log file
