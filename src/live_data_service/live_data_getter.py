import os
import traceback

import aiohttp

KIA_API_BASE = os.getenv("KIA_BMTC_API_URL", "https://bmtcmobileapi.karnataka.gov.in/WebAPI")
HEADERS = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Content-Type': 'application/json',
    'User-Agent': 'insomnia/1.0.0',
    'lan': 'en',
    'deviceType': 'WEB',
    'Connection': 'keep-alive'
}
async def fetch_route_data(parent_id: int, connector) -> list:
    url = f"{KIA_API_BASE}/SearchByRouteDetails_v4"
    payload = {
        "routeid": parent_id,
        "servicetypeid": 0
    }
    try:
        async with aiohttp.ClientSession(connector=connector, connector_owner=False, timeout=aiohttp.ClientTimeout(total=120)) as session:
            async with session.post(url, json=payload, headers=HEADERS, timeout=120) as resp:
                if resp.status != 200:
                    print(f"[Getter] Error {resp.status} for parent_id {parent_id}")
                    return []

                json_data = await resp.json()

                if not json_data.get("issuccess", False):
                    print(f"[Getter] API error: {json_data.get('message')}")
                    return []

                combined_data = []
                for direction in ["up", "down"]:
                    if direction in json_data:
                        combined_data.extend(json_data[direction].get("data", []))

                return combined_data

    except Exception as e:
        traceback.print_exc()
        print(f"[Getter] Exception fetching live data for route {parent_id}: {e}")
        return []
