import os

import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("WHEREBY_API_KEY")
headers = {"Authorization": f"Bearer {API_KEY}"}

r = requests.get(
    "https://api.whereby.dev/v1/recordings", headers=headers, params={"limit": 1}
)
import json

print(json.dumps(r.json(), indent=2))
