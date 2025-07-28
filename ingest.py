def ingest(count=None):
    import requests
    import os,json
    import time
    from datetime import datetime

    print("Starting data ingestion...")
    max_retries = 5
    url = "https://pokeapi.co/api/v2/pokemon"
    retries = 0
    delay = 1

    while retries < max_retries:
        try:
            print(f"Attempt {retries + 1} to fetch data...")
            response = requests.get(url)
            response.raise_for_status()
            pokemon_list = response.json()["results"]
            if count:
                pokemon_list = pokemon_list[:count]
            detailed_data = []

            for pokemon in pokemon_list:
                poke_response = requests.get(pokemon["url"])
                if poke_response.status_code == 200:
                    detailed_data.append(poke_response.json())

            os.makedirs("data/raw", exist_ok=True)
            timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M")
            with open(f"data/raw/pokemon_raw_{timestamp}.json", "w") as f:
                json.dump(detailed_data, f, indent=2)
            return len(pokemon_list)

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
            retries += 1
            delay *= 2

    raise Exception("Failed to fetch data after multiple retries.")
