import gzip
import pandas as pd
import requests
import os
import ijson
import sys
from pyspark.sql import SparkSession


class parseRecords:
    def __init__(self):
        pass

    def loadFileList(self, csv_path):
        df = pd.read_csv(csv_path)
        return df['gz_url'].tolist()

    def parseFirstFile(self, csv_path):
        ''' Used to sanity check the ETL process ''' 
        file_list = self.loadFileList(csv_path)
        first_file_url = file_list[0]
        local_file_path = self.downloadFile(first_file_url)
        df = self.parseLargeRecord(local_file_path)
        os.remove(local_file_path)
        return df

    def parseAllFiles(self, csv_path):
        ''' Returns a dataframe with a very large amount of price transparency data. Work in progress ''' 
        file_list = self.loadFileList(csv_path)
        spark = SparkSession.builder.appName('ParseAllFiles').getOrCreate()
        final_df = None
        for file_url in file_list:
            local_file_path = self.downloadFile(file_url)
            df = self.parseLargeRecord(local_file_path)
            os.remove(local_file_path)
            spark_df = spark.createDataFrame(df)
            if final_df is None:
                final_df = spark_df
            else:
                final_df = final_df.union(spark_df)
        return final_df
        return None 

    def downloadFile(self, url):
        '''Download gzipped data from insurance company and unzip.''' 
        gz_file_path = url.split('/')[-1]
        response = requests.get(url)
        with open(gz_file_path, 'wb') as file:
            file.write(response.content)

        # Unzipping the downloaded file
        unzipped_file_path = gz_file_path.replace('.gz', '')
        with gzip.open(gz_file_path, 'rb') as gz_file:
            with open(unzipped_file_path, 'wb') as unzipped_file:
                unzipped_file.write(gz_file.read())

        # Deleting the downloaded .gz file
        os.remove(gz_file_path)

        return unzipped_file_path

    def parseLargeRecord(self,recordPath):
        '''Retrieve the relevant parts of the price transparency file'''

        in_network_data = []

        with open(recordPath, "rb") as json_file:
            parser = ijson.parse(json_file)
            current_item = {}
            current_negotiated_prices = {}
            essential_fields = {}   

            for prefix, event, value in parser:
                if prefix == 'in_network.item':
                    if event == 'start_map':
                        current_item = {}
                        essential_fields = {}
                    elif event == 'end_map':
                        pass   

                elif 'in_network.item.' in prefix:
                    key = prefix[len('in_network.item.'):]

                    if event == 'start_map':
                        if key == 'negotiated_rates.item.negotiated_prices.item':
                            current_negotiated_prices = {}

                    elif event == 'end_map':
                        if key == 'negotiated_rates.item.negotiated_prices.item':
                            new_entry = essential_fields.copy()  # Shallow copy
                            new_entry.update(current_negotiated_prices)
                            in_network_data.append(new_entry)

                    elif event in ('string', 'number'):
                        if '.' in key:
                            sub_key, actual_key = key.rsplit('.', 1)
                            if sub_key == 'negotiated_rates.item.negotiated_prices.item':
                                current_negotiated_prices[actual_key] = value
                            elif sub_key == 'negotiated_rates.item':
                                essential_fields[f'negotiated_rates_{actual_key}'] = value  
                            else:
                                
                                pass
                        else:
                            essential_fields[key] = value
        df = pd.json_normalize(in_network_data)
        print(".",end="")
        return df 

if __name__ == '__main__':
    pr = parseRecords()
    if len(sys.argv) > 1 and sys.argv[1] == '-f':
        pr.parseFirstFile('transparency_json_zipped_links.csv')
    else:
        pr.parseAllFiles('transparency_json_zipped_links.csv')
