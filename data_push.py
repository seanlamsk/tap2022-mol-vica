import pandas as pd
import numpy as np
import ast

from pymongo import MongoClient
import pymongo

def main():
    path = 'insurance_data.csv'
    print('Reading CSV...')
    raw_df = pd.read_csv(path,delimiter=';',decimal=',')
    print('Cleaning raw data...')
    clean_df = clean_data(raw_df)
    print('Mapping data to destination structure...')
    mapped = map_data(clean_df)
    #mongo db
    print('Preparing for Data Push')
    client = connect_mongo()
    print("Inserting Data...")
    insert_to_db(mapped,client)
    print("Data Inserted. End of script")

def clean_data(df):
    clean_df = df.copy()

    # Remap free text values to No
    free_text_life_policies = {'no term life policy taken','term life policy not taken'}
    clean_df['multipleTermLifePolicies'] = clean_df['multipleTermLifePolicies'].apply(lambda x: 'No' if x in free_text_life_policies else x)

    #Convert comma to decimal for dollar figure columns
    def clean_totalPremium(x):
        if x == np.nan or len(x.strip()) == 0:
            return np.nan
        else:
            return float(x.replace(',', '.'))

    clean_df['totalPremium'] = clean_df['totalPremium'].apply(clean_totalPremium)

    #map binary columns as category to preserve NaNs
    free_text_yesno_map = {'Yes':True,'No':False,np.nan:np.nan}
    bool_map = {
        'is45OrOlder':{'1.0':True,'0.0':False,np.nan:np.nan},
        'isMarried':free_text_yesno_map,
        'hasKids':free_text_yesno_map,
        'termLifeInsurance':free_text_yesno_map,
        'multipleTermLifePolicies':free_text_yesno_map,
        'eStatements':free_text_yesno_map,
        'renewal':{'Y':True,'N':False,np.nan:np.nan}
    }

    for col,d in bool_map.items():
        clean_df[col] = clean_df[col].map(d).astype('category')

    #convert healthrider column to tuple 
    def convert_to_tuple(x):
        if pd.isnull(x):
            return []
        else:
            x = ast.literal_eval(x)
        if isinstance(x,tuple):
            return list(x)
        else:
            return [x]
    clean_df['healthRiders'] = clean_df['healthRiders'].apply(convert_to_tuple)
    

    return clean_df

def map_data(df):
    result = []
    def rnan(x):
        if x!=x:
            return None
        else:
            return x
    for index, row in df.iterrows():
        mapped_row = {}
        mapped_row['insuree#'] = rnan(row['insuree#'])
        mapped_row['gender'] = rnan(row['gender'])
        mapped_row['is45OrOlder'] = rnan(row['is45OrOlder'])
        mapped_row['hasKids'] = rnan(row['hasKids'])
        mapped_row['insuredMonths'] = rnan(row['insuredMonths'])

        mapped_row['termLifeInsurance'] = {}
        mapped_row['termLifeInsurance']['hasPolicy'] = rnan(row['termLifeInsurance'])
        mapped_row['termLifeInsurance']['hasMultiplePolicies'] = rnan(row['multipleTermLifePolicies'])

        mapped_row['healthInsurance'] = {}
        mapped_row['healthInsurance']['hasPolicy'] = rnan(row['healthInsurance']) != 'No'
        mapped_row['healthInsurance']['riders'] = rnan(row['healthRiders'])

        mapped_row['premiumFrequency'] = None if np.isnan(row['premiumFrequency']) else int(row['premiumFrequency'])
        mapped_row['eStatements'] = rnan(row['eStatements'])
        mapped_row['monthlyPremium'] = rnan(row['monthlyPremium'])
        mapped_row['totalPremium'] = rnan(row['totalPremium'])
        mapped_row['renewal'] = rnan(row['renewal'])

        result.append(mapped_row)
    
    return result
def connect_mongo():
    user = 'mongouser'
    pw = 'mongopw'
    DB_PORT = 27017
    DB_URL = f'mongodb://{user}:{pw}@localhost:{DB_PORT}'

    try:
        client = MongoClient(DB_URL)
        print('Connecting to database...')
        client.server_info()
        print('Connected successfully')
        return client
    except Exception as e:
        print(e)
        print("insertion failed, exiting script ...")
        exit()

def insert_to_db(mapped,client):
    try:
        destination_db='insurance'
        db = client[destination_db]
        #create collection
        collection = db['insurees']
        #insert to collection
        collection.insert_many(mapped)
    except Exception as e:
        print(e)
        print("connection failed, exiting script ...")
        exit()

def test():
    client = connect_mongo()
    db = client['insurance']
    print(list(db.insurees.find().limit(1)))
if __name__ == "__main__":
    test()