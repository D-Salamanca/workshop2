import pandas as pd

class Transformations:

    def __init__(self, file):
        self.df = pd.read_csv(file, sep=',', encoding='utf-8')

    def insert_id(self) -> None:
        self.df.insert(0, 'id', self.df.index + 1)
    
    def convert_to_datetime(self, column_name):
        """
        Converts a column in the DataFrame to datetime type.

        Parameters:
            column_name (str): The name of the column to convert.

        Returns:
            pandas.DataFrame: The DataFrame with the column converted to datetime type.
        """
        self.df[column_name] = pd.to_datetime(self.df[column_name], format='%Y').dt.year
        return self.df


def categorize_column(df : pd.DataFrame, column_name : str, bins : list, labels : list) -> None:
    """
    Categorizes a column in the DataFrame based on specified bins and labels.

    Parameters:
        df (pandas.DataFrame): The DataFrame.
        column_name (str): The name of the column to categorize.
        bins (list): The bin edges for categorization.
        labels (list): The labels for the categories.

    Returns:
        None
    """
    df[column_name + '_category'] = pd.cut(df[column_name], bins=bins, labels=labels, right=False)



def convert_duration(df : pd.DataFrame, new_column_name : str, column_name : str) -> None:
    """
    Converts a duration column in milliseconds to a new column with format MM:SS.

    Parameters:
        df (pandas.DataFrame): The DataFrame.
        new_column_name (str): The name for the new duration column.
        column_name (str): The name of the original duration column.

    Returns:
        None
    """
    df[new_column_name] = pd.to_datetime(df[column_name], unit='ms').dt.strftime('%M:%S')


def map_genre_to_category(df : pd.DataFrame) -> None:
    """
    Maps specific genres to broader categories and adds a new 'genre' column to the DataFrame.

    Parameters:
        df (pandas.DataFrame): The DataFrame containing the 'track_genre' column.

    Returns:
        None
    """
    genre_mapping = {
        'mood': ['ambient', 'chill', 'happy', 'sad', 'sleep', 'study', 'comedy'],
        'electronic': ['afrobeat', 'breakbeat', 'chicago-house', 'club', 'dance', 'deep-house', 'detroit-techno', 'dub', 'dubstep', 'edm', 'electro', 'electronic', 'house', 'idm', 'techno', 'minimal-techno', 'trance', 'hardstyle'],
        'pop': ['anime', 'cantopop', 'j-pop', 'k-pop', 'pop', 'power-pop', 'synth-pop', 'indie-pop', 'pop-film'],
        'urban': ['hip-hop', 'j-dance', 'j-idol', 'r-n-b', 'trip-hop'],
        'latino': ['brazil', 'latin', 'latino', 'reggaeton', 'salsa', 'samba', 'spanish', 'pagode', 'sertanejo', 'mpb'],
        'global sounds': ['indian', 'iranian', 'malay', 'mandopop', 'reggae', 'turkish', 'ska', 'dancehall', 'tango'],
        'jazz and soul': ['blues', 'bluegrass', 'funk', 'gospel', 'jazz', 'soul'],
        'varied themes': ['children', 'disney', 'forro', 'grindcore', 'kids', 'party', 'romance', 'show-tunes'],
        'instrumental': ['acoustic', 'classical', 'folk', 'guitar', 'piano', 'singer-songwriter', 'songwriter', 'world-music', 'opera', 'new-age'],
        'single genre': ['country', 'progressive-house', 'swedish', 'emo', 'honky-tonk', 'french', 'german', 'drum-and-bass', 'groove', 'disco'],
        'rock and metal': ['alt-rock', 'alternative', 'british', 'grunge', 'hard-rock', 'indie', 'metal', 'metalcore', 'punk-rock', 'rock', 'rock-n-roll', 'black-metal', 'death-metal', 'hardcore', 'heavy-metal', 'industrial', 'psych-rock', 'rockabilly', 'goth', 'punk', 'j-rock', 'garage']
    }

    genre_to_category = {genre: category for category, genres in genre_mapping.items() for genre in genres}

    df['genre'] = df['track_genre'].map(genre_to_category)