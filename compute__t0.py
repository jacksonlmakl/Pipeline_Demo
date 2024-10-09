from core import Pipeline

import requests
import pandas as pd

def get_kanto_pokemon():
    """
    Fetches all Pokémon from the Kanto region using the PokeAPI.
    
    Returns:
        DataFrame: A Pandas DataFrame containing name, height, weight, abilities, and base stats of all Kanto Pokémon.
    """
    # Kanto region ID is 2 (according to PokeAPI)
    kanto_pokedex_url = "https://pokeapi.co/api/v2/pokedex/2/"
    
    # Fetch the Kanto region Pokémon list
    response = requests.get(kanto_pokedex_url)
    
    if response.status_code != 200:
        print(f"Error: Could not fetch Kanto Pokedex. Status code {response.status_code}")
        return None
    
    kanto_pokemon = response.json()['pokemon_entries']
    
    # List to hold Pokémon data
    pokemon_data_list = []
    
    # Iterate through each Pokémon in the Kanto region
    for entry in kanto_pokemon:
        pokemon_name = entry['pokemon_species']['name']
        pokemon_data = get_pokemon_data(pokemon_name)
        
        if pokemon_data:
            pokemon_data_list.append(pokemon_data)
    
    # Create a DataFrame from the list of Pokémon data
    df = pd.DataFrame(pokemon_data_list)
    return df

def get_pokemon_data(pokemon_name):
    """
    Fetches data for a given Pokémon from the PokeAPI.
    
    Parameters:
        pokemon_name (str): The name of the Pokémon to fetch.
        
    Returns:
        dict: A dictionary containing the Pokémon's name, height, weight, abilities, and base stats.
    """
    url = f"https://pokeapi.co/api/v2/pokemon/{pokemon_name.lower()}/"
    
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        
        # Extract relevant data
        pokemon_info = {
            'name': data['name'],
            'height': data['height'],
            'weight': data['weight'],
            'abilities': ', '.join([ability['ability']['name'] for ability in data['abilities']]),
            'base_stats': {stat['stat']['name']: stat['base_stat'] for stat in data['stats']}
        }
        
        # Flatten base stats into separate columns
        for stat, value in pokemon_info['base_stats'].items():
            pokemon_info[stat] = value
        
        # Remove base_stats key now that we have flattened it
        del pokemon_info['base_stats']
        
        return pokemon_info
    else:
        print(f"Error: Could not fetch data for {pokemon_name}. Status code {response.status_code}")
        return None

# Main execution
def main():
    kanto_pokemon_df = get_kanto_pokemon()
    return kanto_pokemon_df

p=Pipeline('sample.xml')



t0 = main()

curr_table=[i for i in p.tables if i.id=='t0'][0]
 


[i.connection for i in p.tables if i.id == 't0'][0].Session()

curr_table.connection.df_to_table(t0, curr_table.table, curr_table.database, curr_table.schema, curr_table.materialization, schema_change_behavior=curr_table.schema_change, primary_key=curr_table.primary_key)