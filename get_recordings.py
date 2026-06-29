import os

import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("WHEREBY_API_KEY")
BASE_URL = "https://api.whereby.dev/v1"

print(API_KEY)

headers = {"Authorization": f"Bearer {API_KEY}"}


def get_all_recordings():
    recordings = []
    cursor = None

    while True:
        params = {"limit": 50}
        if cursor:
            params["cursor"] = cursor

        response = requests.get(
            f"{BASE_URL}/recordings", headers=headers, params=params
        )

        if response.status_code != 200:
            print(f"Error {response.status_code}: {response.text}")
            break

        data = response.json()
        recordings.extend(data["results"])
        print(
            f"Fetched {len(data['results'])} recordings, total so far: {len(recordings)}"
        )

        cursor = data.get("cursor")
        if cursor is None:
            break

    return recordings


recordings = get_all_recordings()
print(f"\nTotal recordings fetched: {len(recordings)}")
for r in recordings:
    print(r)
