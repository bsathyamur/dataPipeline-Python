import requests
import yaml
import pandas as pd
import numpy as np
import logging

class dataPipeline:
    
    def __init__(self):
        
        #Open the config.yaml file and load the config in a class object
        with open('config.yaml') as f:
            self.config = yaml.load(f, Loader=yaml.FullLoader)
        
        # To get the total number of the files downloaded
        self.file_download_cnt = 0
        
        self.dataset = None
        self.dataPrimaryCountry = None
        self.datasetOtherCountry = None
        
        # Adding information related to logger
        self.logger = logging.getLogger('dev')
        self.logger.setLevel(logging.INFO)
        self.fileHandler = logging.FileHandler('datapipeline_log.log')
        self.fileHandler.setLevel(logging.INFO)
        self.logger.addHandler(self.fileHandler)
        self.formatter = logging.Formatter('%(asctime)s  %(name)s  %(levelname)s: %(message)s')
        self.fileHandler.setFormatter(self.formatter)
        
    def logTransaction(self,info):
        self.logger.info(info)
        
    def downloadFile(self)->bool:
        """
        Download a file from the source path configured in yaml config file
        Parameters
        ----------
        None
        Returns
        -------
        bool
            True if successful, False otherwise
        """ 
        
        try:
            
            fileURL = self.config['sourcePath'] + "/" + self.config['sourceFile']
            mydatafile = requests.get(fileURL)
            
            targetfile = self.config['targetPath'] + "/" + self.config['sourceFile']
            
            open(targetfile, 'wb').write(mydatafile.content)
            self.file_download_cnt += 1
            
            self.logTransaction('successfully downloaded file' + fileURL)
            
            return True
        
        except Exception as e:
            print("Exception occured during download {Message} ",e)
            self.logTransaction("Exception - file download - " + str(e))
            return False
        
    def cleanData(self)->bool:
        """
        Cleans the data in the file downloaded from the source path. 
        Following transformation occurs in this step:
         1. Identify the null customer ID field and change the values to Guest
         2. Identify the null stock description field and change the values to Not-Specified
         3. Create 3 more columns for InvoiceYear, Quarter
        Parameters
        ----------
        None
        Returns
        -------
        bool
            True if successful, False otherwise
        """ 
        
        try:        
            file = self.config['sourceFile']
            
            # Load the dataset from the two worksheets
            data_wsheet1 = pd.read_excel(file,sheet_name=self.config['worksheet1'])
            data_wsheet2 = pd.read_excel(file,sheet_name=self.config['worksheet2'])
            
            self.dataset = pd.concat([data_wsheet1,data_wsheet2])
            
            # Handling the null Customer ID field as Guest
            self.dataset["Customer ID"] = self.dataset["Customer ID"].fillna('Guest')
            
            # Handling the null description field as Unlisted
            self.dataset["Description"] = self.dataset["Description"].fillna('Unlisted')
            
            # Creating a new column as Invoice Year
            self.dataset['InvoiceYear'] = pd.DatetimeIndex(self.dataset['InvoiceDate']).year
            
            # Creating a new column for Quarter
            self.dataset['Quarter'] = pd.PeriodIndex(self.dataset['InvoiceDate'], freq='Q-MAR').strftime('Q%q')
            
            self.dataset['InvoiceType'] = self.dataset["Quantity"].apply(lambda row: 'Return' if row < 0 else 'Purchase')
            
            fileURL = self.config['sourcePath'] + "/" + self.config['sourceFile']
            
            self.logTransaction('successfully data cleaned file' + fileURL)
            
            return True
        
        except Exception as e:
            print("Exception occured during data cleaning {Message} ",e)
            self.logTransaction("Exception - data cleaning - " + str(e))            
            return False       
        
    def splitData(self)->bool:
        """
        Splits the dataset between the primary country and other countries
        Parameters
        ----------
        None
        Returns
        -------
        bool
            True if successful, False otherwise
        """ 
        
        try:         
            primCountry = self.config['PrimCountry']
        
            self.dataPrimaryCountry = self.dataset[self.dataset["Country"] == primCountry]
        
            self.datasetOtherCountry = self.dataset[self.dataset["Country"] != primCountry]
            
            fileURL = self.config['sourcePath'] + "/" + self.config['sourceFile']
            
            self.logTransaction('successfully split data' + fileURL)
            
            return True
        except Exception as e:
            print("Exception occured during splitting data {Message} ")
            self.logTransaction("Exception - data split - " + str(e))            
            return False          
        
    def writeData(self)->bool:
        
        """
        Write the dataset as two seperate files to target location
        Parameters
        ----------
        None
        Returns
        -------
        bool
            True if successful, False otherwise
        """ 
        try:        
            primCountry = self.config['PrimCountry']
        
            self.dataPrimaryCountry.to_csv("retail-" + primCountry + ".csv" , index=False)
            self.datasetOtherCountry.to_csv("retail-" + "others.csv" , index=False)
            
            self.logTransaction('successfully write data file ' + "retail-" + \
                                    primCountry + ".csv," + "retail-" + "others.csv")

            return True
        except Exception as e:
            print("Exception occured during splitting data {Message} ",e)
            self.logTransaction("Exception - file write - " + str(e))            
            return False        


def main():
    d = dataPipeline()
    print("File Download Process status ",d.downloadFile())
    print("File Data cleaning Process status ",d.cleanData())
    print("File Split Data Process status ",d.splitData())
    print("File write Process status ",d.writeData())

if __name__ == "__main__":
    main()            