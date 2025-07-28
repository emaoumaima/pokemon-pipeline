def transform(filter_type=None):
    import pandas as pd
    from datetime import datetime
    import glob
    import os
    
    # read users to dataframe
    def get_latest_raw_file():
        files = glob.glob("data/raw/pokemon_raw_*.json")
        if not files:
            raise FileNotFoundError("No raw Pokémon data files found in data/raw/")
        return max(files, key=os.path.getctime)
    raw_path = get_latest_raw_file()
    df = pd.read_json(raw_path)

    df["pokemon_types"] = df["types"].apply(lambda types: [t["type"]["name"] for t in types])
    # select & rename columns
    df = df[["id", "name", "weight", "height", "pokemon_types"]]
    df.columns = ["pokemon_id", "pokemon_name", "pokemon_weight", "pokemon_height", "pokemon_types"]

    # assert no duplicates in user_id
    if df["pokemon_id"].duplicated().any():
        raise ValueError("Duplicate user_ids found")
    #filter pokemon based on type
    if filter_type:
        df = df[df["pokemon_types"].apply(lambda types: filter_type in types)]

    timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M")
    # save the cleaned data
    os.makedirs("data/processed", exist_ok=True)
    out_path = f"data/processed/pokemons_clean_{timestamp}.csv"
    df.to_csv(out_path, index=False)
   

    return len(df)
