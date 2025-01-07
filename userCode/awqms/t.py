import os
import csv
import asyncio
import aiohttp
import xml.etree.ElementTree as ET

async def fetch_url(session, url):
    try:
        headers = {
            "Accept": "application/xml"
        }

        async with session.get(url, headers=headers, timeout=120) as response:
            text = await response.text()
            return False if text == "No records were found which match your search criteria" else True
    except Exception:
        # Long timeout implies there is content
        print('Timeout')
        return False

async def fetch_features(session, base_url, value):
    params = {
        "service": "wfs",
        "request": "GetFeature",
        "typeName": "MonitoringLocation",
        "storedQuery_id": "GetFeaturesByParameters",
        "MonitoringLocationIdentifiersCsv": value,
    }

    try:
        async with session.get(base_url, params=params, timeout=10) as response:
            response.raise_for_status()
            root = ET.fromstring(await response.text())
            namespaces = {
                "awqms": "http://awqms.goldsystems.com/api/wfs"
            }  # Updated namespaces for parsing

            for feature in root.findall(".//awqms:ContinuousResultsUrl", namespaces):
                url = feature.text 
                if url:
                    is_valid = await fetch_url(session, url)
                    if is_valid:
                        return value  # Only return the value if the URL is valid
    except Exception as e:
        pass
    return None

async def combine_unique_values_from_csvs_and_query(directory="."):
    unique_values = set()  # Use a set to ensure uniqueness

    # Iterate through files in the directory
    for filename in os.listdir(directory):
        if filename.endswith(".csv") and not filename.startswith('valid_features'):
            filepath = os.path.join(directory, filename)

            with open(filepath, "r", newline='', encoding="utf-8") as csvfile:
                reader = csv.reader(csvfile)
                next(reader)
                for row in reader:
                    if row:  # Ensure the row is not empty
                        unique_values.add(row[0])  # Add the first column value to the set
                        # break

    base_url = "https://ordeq.gselements.com/api/wfs"

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_features(session, base_url, value) for value in unique_values]

        results = await asyncio.gather(*tasks)

    # Filter out None results
    valid_features = [result for result in results if result is not None]

    output_file = "valid_features.csv"
    with open(output_file, "w", newline='', encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["MonitoringLocationIdentifier"])
        for feature in valid_features:
            writer.writerow([feature])

    return valid_features

if __name__ == "__main__":
    valid_features_list = asyncio.run(combine_unique_values_from_csvs_and_query())
    print("Valid features with non-404 ContinuousResultsUrl:")
    print(valid_features_list)
