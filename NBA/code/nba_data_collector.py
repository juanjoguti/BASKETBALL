import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd
from nba_api.stats.static import players
from nba_api.stats.endpoints import playercareerstats, commonplayerinfo
import time

# Constants
HOOPSHYPE_SALARIES_URL = 'https://hoopshype.com/salaries/players/'
HOOPSHYPE_PLAYER_URL = 'https://hoopshype.com/player/{}/salary/'


def create_session_with_retries():
    """
    Creates a requests session with retry logic.
    Returns:
        session (requests.Session): A session with retry logic.
    """
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session


def fetch_active_players():
    """
    Fetches the list of active NBA players using nba_api.
    Returns:
        DataFrame: A DataFrame containing active player information.
    """
    active_players = players.get_active_players()
    return pd.DataFrame(active_players)


def fetch_player_stats(player_id):
    """
    Fetches the career stats for a given player using nba_api.
    Args:
        player_id (int): The player's ID.
    Returns:
        DataFrame: A DataFrame containing the player's career stats.
    """
    career_stats = playercareerstats.PlayerCareerStats(player_id=player_id)
    return career_stats.get_data_frames()[0]


def fetch_all_player_stats(players_df):
    """
    Fetches the stats for all active players.
    Args:
        players_df (DataFrame): A DataFrame containing active player information.
    Returns:
        DataFrame: A DataFrame containing all players' stats.
    """
    all_stats = []
    for _, player in players_df.iterrows():
        try:
            stats = fetch_player_stats(player['id'])
            if not stats.empty:
                stats['player_id'] = player['id']
                all_stats.append(stats)
            time.sleep(1)  # Sleep to avoid hitting the rate limit
        except Exception as e:
            print(f"Failed to fetch stats for player {player['full_name']}: {e}")
    return pd.concat(all_stats, ignore_index=True) if all_stats else pd.DataFrame()


def fetch_player_awards(player_id):
    """
    Fetches the awards for a given player using nba_api.
    Args:
        player_id (int): The player's ID.
    Returns:
        DataFrame: A DataFrame containing the player's awards.
    """
    player_info = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
    awards = player_info.player_headline_stats.get_data_frame()
    return awards


def fetch_all_player_awards(players_df):
    """
    Fetches the awards for all active players.
    Args:
        players_df (DataFrame): A DataFrame containing active player information.
    Returns:
        DataFrame: A DataFrame containing all players' awards.
    """
    all_awards = []
    for _, player in players_df.iterrows():
        try:
            awards = fetch_player_awards(player['id'])
            if not awards.empty:
                awards['player_id'] = player['id']
                all_awards.append(awards)
            time.sleep(1)  # Sleep to avoid hitting the rate limit
        except Exception as e:
            print(f"Failed to fetch awards for player {player['full_name']}: {e}")
    return pd.concat(all_awards, ignore_index=True) if all_awards else pd.DataFrame()


def fetch_player_salaries(player_name):
    """
    Fetches the projected and past salaries for a given player from HoopsHype.
    Args:
        player_name (str): The player's name.
    Returns:
        DataFrame: A DataFrame containing the player's projected and past salaries.
    """
    session = create_session_with_retries()
    url = HOOPSHYPE_PLAYER_URL.format(player_name.replace(' Jr.', '').lower().replace(' ', '-'))
    response = session.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    all_salaries = []

    past_table = soup.find('span', string='Past Salaries').find_next('table')
    if past_table:
        for row in past_table.tbody.find_all('tr'):
            year = row.find('td', {'class': 'table-key'}).text.strip()
            salary = row.find_all('td')[2].text.strip().split(' ')[0].replace('$', '').replace(',', '')
            all_salaries.append({'season': year, 'salary': salary})
    else:
        print(f"Past salaries table not found for player {player_name}")

    return pd.DataFrame(all_salaries)


def fetch_all_player_salaries(players_df):
    """
    Scrapes the salary data for all active NBA players from HoopsHype.
    Args:
        players_df (DataFrame): A DataFrame containing active player information.
    Returns:
        DataFrame: A DataFrame containing salary information.
    """
    all_salaries = []
    for _, player in players_df.iterrows():
        try:
            salaries = fetch_player_salaries(player['full_name'])
            if not salaries.empty:
                salaries['full_name'] = player['full_name']
                all_salaries.append(salaries)
            time.sleep(1)  # Sleep to avoid hitting the rate limit
        except Exception as e:
            print(f"Failed to fetch salaries for player {player['full_name']}: {e}")
    return pd.concat(all_salaries, ignore_index=True) if all_salaries else pd.DataFrame()


def merge_data(players_df, stats_df, awards_df, salaries_df):
    """
    Merges player information, stats, awards, and salary data into a single DataFrame.
    Args:
        players_df (DataFrame): DataFrame containing player information.
        stats_df (DataFrame): DataFrame containing player stats.
        awards_df (DataFrame): DataFrame containing player awards.
        salaries_df (DataFrame): DataFrame containing player salaries.
    Returns:
        DataFrame: A merged DataFrame containing all player data.
    """
    merged_df = players_df.merge(stats_df, left_on='id', right_on='player_id', how='left')
    merged_df = merged_df.merge(awards_df, on='player_id', how='left')
    merged_df = merged_df.merge(salaries_df, left_on='full_name', right_on='full_name', how='left')
    return merged_df


def save_data(df, filename):
    """
    Saves the DataFrame to a CSV file.
    Args:
        df (DataFrame): The DataFrame to save.
        filename (str): The filename for the CSV file.
    """
    df.to_csv(filename, index=False)


def main():
    """
    Main function to fetch, process, and save NBA player data.
    """
    print("Fetching active players...")
    players_df = fetch_active_players()

    print("Fetching player stats...")
    stats_df = fetch_all_player_stats(players_df)

    print("Fetching player awards...")
    awards_df = fetch_all_player_awards(players_df)

    print("Scraping player salaries...")
    salaries_df = fetch_all_player_salaries(players_df)

    print("Merging data...")
    merged_df = merge_data(players_df, stats_df, awards_df, salaries_df)

    print("Saving data to CSV...")
    save_data(merged_df, filename='nba_active_players_dataset.csv')

    print("Data collection complete.")


if __name__ == "__main__":
    main()
