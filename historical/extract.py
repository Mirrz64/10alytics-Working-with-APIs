import asyncio # The Asychoronous Operation Library
import aiohttp # The Request if Asychoronous Operation
import os 
import json
from datetime import date, timedelta

# Define the Bronze Layer: A folder
BRONZE_DIR = "data/bronze"
os.makedirs(BRONZE_DIR, exist_ok=True)

# limit the concurrent execution to the defined number
limitter = asyncio.Semaphore(3)

async def api_request(session, target_date):
    url = f"https://api.carbonintensity.org.uk/regional/intensity/{target_date}/pt24h"
    raw_data_path = os.path.join(BRONZE_DIR, f"{target_date}.json")

    if os.path.exists(raw_data_path):
        print(f"{target_date} already exists! Skip")
        return # skip
    
    async with limitter:
        for attempts in range(3): # this is to enable the operation make the requests thrice
            try:
                async with session.get(url, timeout=20) as response:
                    if response.status == 200:
                        resp_json = await response.json()
                        with open(raw_data_path, "w") as file:
                            data = resp_json['data']
                            json.dump(data, file, indent=4)
                        print(f"Saved data for {target_date}")
                        return
                    elif response.status == 429: # if the there too many requests
                        print(f"Encoutered rate limit on {target_date}. waiting to retry")
                        await asyncio.sleep(5 * (attempts + 1))
                    else:
                        print(f"Failed Status: {response.status} for {target_date}")
            except Exception as e:
                print(f"Tried making request for {target_date} in {attempts + 1} attempts due to {e}")
                await asyncio.sleep(2)
        print(f"Failed after 3 attempts: {target_date}")

# To run the asychronous operation
async def run_operation():
    start_date = date(2022, 1, 1)
    end_date = date(2024, 12, 31)

    connector = aiohttp.TCPConnector(limit=10, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [] # this gather all the date and requests tasks
        current = start_date # setting up markers
        while current <= end_date:
            tasks.append(api_request(session, target_date=current))
            current += timedelta(days=1)
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(run_operation())