import os
import csv
import asyncio
import aiohttp

base_url = "https://ordeq.gselements.com/api/ContinuousResultsVer1"
COUNT = 0
# Function to construct the ContinuousResultsUrl directly
def construct_continuous_results_url(value):
    # Construct the URL with the provided MonitoringLocationIdentifier
    return f"{base_url}?MonitoringLocationIdentifiersCsv={value}"

async def fetch_url(session, url, semaphore):
    try:
        headers = {
            "Accept": "application/xml"
        }
        # Acquire the semaphore to ensure limited concurrent requests
        async with semaphore:
            async with session.get(url, headers=headers, timeout=30) as response:
                text = await response.text()
                return False if text == "No records were found which match your search criteria" else True
    except Exception:
        # Long timeout implies there is content
        return True

async def process_value(session, value, semaphore):
    global COUNT
    # Construct the ContinuousResultsUrl directly
    url = f"{base_url}?MonitoringLocationIdentifiersCsv={value}"
    
    # Check if the URL is valid (i.e., if it returns content)
    is_valid = await fetch_url(session, url, semaphore)
    print(f'Count {COUNT}\r', end='')
    COUNT += 1
    if is_valid:
        return value  # Only return the value if the URL is valid

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

    # Limit the number of concurrent requests
    semaphore = asyncio.Semaphore(100)

    async with aiohttp.ClientSession() as session:
        # Process each value asynchronously with semaphore
        tasks = [process_value(session, value, semaphore) for value in unique_values]

        results = await asyncio.gather(*tasks)

    # Filter out None results
    valid_features = set(filter(None, results))

    # Write valid features to CSV
    output_file = "valid_features.csv"
    with open(output_file, "w", newline='', encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["MonitoringLocationIdentifier"])
        for feature in valid_features:
            writer.writerow([feature])

    return valid_features

if __name__ == "__main__":
    valid_features_list = asyncio.run(combine_unique_values_from_csvs_and_query())
    # print("Valid features with non-404 ContinuousResultsUrl:")
    # print(valid_features_list)
