import requests
import json
import pandas as pd


API_KEY = "xKxMlKmdA4LP2hawyHB9Ch7fIvyKGesVof5MqDAh"
START_DATE = "2025-03-28"
END_DATE = "2025-03-30"

url = f"https://api.nasa.gov/neo/rest/v1/feed?start_date={START_DATE}&end_date={END_DATE}&api_key={API_KEY}"

response = requests.get(url)
data = response.json()

print(data.keys())

# Get near_earth_objects
neo_objects = data.get("near_earth_objects", {})

# Print the keys (which are dates)
print(neo_objects.keys())

for date, objects in neo_objects.items():
    if objects:  # Ensure there are objects in the list
        print(f"Keys for NEO on {date}: {objects[0].keys()}")
        break  # Print only for the first available date

# Prepare a list for DataFrame conversion
neo_list = []

for date, objects in neo_objects.items():
    for obj in objects:
        neo_list.append(
            {
                "date": date,
                "id": obj.get("id"),
                "name": obj.get("name"),
                "magnitude": obj.get("absolute_magnitude_h"),
                "diameter_min": obj.get("estimated_diameter", {})
                .get("meters", {})
                .get("estimated_diameter_min"),
                "diameter_max": obj.get("estimated_diameter", {})
                .get("meters", {})
                .get("estimated_diameter_max"),
                "hazardous": obj.get("is_potentially_hazardous_asteroid"),
                "close_approach_date": obj.get("close_approach_data", [{}])[0].get(
                    "close_approach_date"
                ),
                "velocity_km_per_s": obj.get("close_approach_data", [{}])[0]
                .get("relative_velocity", {})
                .get("kilometers_per_second"),
                "miss_distance_km": obj.get("close_approach_data", [{}])[0]
                .get("miss_distance", {})
                .get("kilometers"),
                "orbiting_body": obj.get("close_approach_data", [{}])[0].get(
                    "orbiting_body"
                ),
            }
        )

# Convert to DataFrame
df = pd.DataFrame(neo_list)

# Save to CSV
df.to_csv("/opt/airflow/data/nasa_neo_data.csv", index=False)

print("Data saved successfully as nasa_neo_data.csv")
