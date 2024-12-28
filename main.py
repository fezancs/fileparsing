from fastapi import FastAPI
from pymongo import MongoClient
import pandas as pd
import os
from collections import OrderedDict
from pydantic import BaseModel
from typing import List

app = FastAPI()

# MongoDB setup
client = MongoClient('mongodb+srv://data_science:8h9tT402Fr56AW3X@db-mongodb-lon1-blacklion-2e2bb381.mongo.ondigitalocean.com/admin?tls=true&authSource=admin&replicaSet=db-mongodb-lon1-blacklion')
db = client['blacklion-dev']
artistroyaltyfundinghistories = db['artistfunding-gcp-service']


# Pydantic model for file paths
class FilePaths(BaseModel):
    file_paths: List[str]  # List of S3 file paths
    distributor: str  # Name of the source (e.g., kolbort, BMI)
    period: str  # Month or Quarter (e.g., "January", "Q1", "March 2024")
    year:str
    

@app.post("/fileparsing/")
async def combine_files(file_paths: FilePaths):

    
    # file_path = '274a86c4-34eb-45fa-8035-9ba385151a1e.csv/(01 July 2024-30 September 2024) - Gunna Music (Publishing Rights)_AGR686182.1.csv'

    # Ensure the file exists
    # if not os.path.exists(file_path):
    #     return {"error": f"File not found: {file_path}"}
    
    df_list = [pd.read_csv(file,low_memory=False) for file in file_paths.file_paths]
    df = pd.concat(df_list, ignore_index=True)


    # Read the CSV file
    # df = pd.read_csv(file_path, low_memory=False)

    # Income by source
    income_by_source = df.groupby('REVENUE_SOURCE_NAME').agg(
        qty=('DIRECT_COLLECTED_AMOUNT', 'size'),
        earnings=('DIRECT_COLLECTED_AMOUNT', 'sum')
    ).reset_index()

    income_dict = income_by_source.set_index('REVENUE_SOURCE_NAME').to_dict(orient='index')

    tracks = {}
    result_dict = {}

    # Grouped income streams by work title and revenue source
    grouped_income_streams = df.groupby(['WORK_TITLE', 'REVENUE_SOURCE_NAME']).agg(
        total_earnings=('DISTRIBUTED_AMOUNT', 'sum')
    ).reset_index()

    # Populate result_dict with earnings data
    for _, row in grouped_income_streams.iterrows():
        work_title = row['WORK_TITLE']
        revenue_source = row['REVENUE_SOURCE_NAME']
        earnings = row['total_earnings']
        
        if work_title not in result_dict:
            result_dict[work_title] = {}
        
        result_dict[work_title][revenue_source] = earnings

    # Populate tracks data and calculate sum of earnings
    for work_title, revenue_sources in result_dict.items():
        tracks[work_title] = {
            file_paths.year: {
                file_paths.period: OrderedDict(),  # Use OrderedDict to maintain order
            }
        }
        
        sum_earnings = 0  # Initialize sum of earnings for the work title
        
        # Calculate earnings for each revenue source and accumulate the sum of earnings
        for revenue_source, earnings in revenue_sources.items():
            tracks[work_title]["2024"]["1"][revenue_source] = {
                "qty": 1,  # Example value, change as per your data
                "earnings": earnings
            }
            sum_earnings += earnings  # Add to sum earnings inside "1"
        
        # Add sum earnings at the top of "1" using OrderedDict
        tracks[work_title]["2024"]["1"] = OrderedDict([
            ("sum", sum_earnings),  # Ensure sum appears first
            *tracks[work_title]["2024"]["1"].items()  # Keep other items intact
        ])

    # Platform sorted by revenue
    platform_sorted_by_revenue = []
    revenue_summary_sorted = df.groupby('REVENUE_SOURCE_NAME').agg(
        sum_of_revenue=('DIRECT_COLLECTED_AMOUNT', 'sum')
    ).reset_index()

    for idx, row in revenue_summary_sorted.iterrows():
        if row['sum_of_revenue'] != 0:
            platform_sorted_by_revenue.append([
                row['REVENUE_SOURCE_NAME'],
                {
                    "sum": row['sum_of_revenue']
                }
            ])

    # **New Code**: Calculate the sum of earnings across all tracks for the "2024" -> "1" section
    total_earnings_2024 = 0
    for work_title, year_data in tracks.items():
        total_earnings_2024 += year_data["2024"]["1"]["sum"]

    # Final structure to save
    save = {
        "file_url": file_paths.file_paths[0],
        "file_key": file_paths.file_paths[0],
        "distributor": "Empire",
        "insights": {
            file_paths.year: {
                file_paths.period: {
                    "sum": total_earnings_2024,  # Add the sum of all earnings for 2024 -> 1
                    **income_dict  # Include the existing income data
                }
            }
        },
        "tracks": tracks,
        "platform_sorted_by_revenue": platform_sorted_by_revenue
    }

    # Insert into MongoDB
    result = artistroyaltyfundinghistories.insert_one(save)
    return {"inserted_id": str(result.inserted_id)}

if __name__ == "__main__":
    import uvicorn 
    uvicorn.run(app, host="0.0.0.0", port=8080)
