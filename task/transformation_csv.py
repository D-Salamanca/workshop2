import pandas as pd  # Importar pandas

class Transformations:
    def __init__(self, file):
        # Inicializa el DataFrame a partir del archivo CSV
        self.df = pd.read_csv(file, sep=',', encoding='utf-8')

    def insert_id(self) -> None:
        # Inserta una columna 'id' en el DataFrame
        self.df.insert(0, 'id', self.df.index + 1)

    def convert_to_datetime(self, column_name):
        """
        Convierte una columna en el DataFrame al tipo datetime.

        Parámetros:
            column_name (str): El nombre de la columna a convertir.

        Returns:
            pandas.DataFrame: El DataFrame con la columna convertida a tipo datetime.
        """
        self.df[column_name] = pd.to_datetime(self.df[column_name], format='%Y').dt.year
        return self.df

def categorize_column(df: pd.DataFrame, column_name: str, bins: list, labels: list) -> None:
    """
    Categoriza una columna en el DataFrame según los bins y etiquetas especificados.

    Parámetros:
        df (pandas.DataFrame): El DataFrame.
        column_name (str): El nombre de la columna a categorizar.
        bins (list): Los bordes de los bins para la categorización.
        labels (list): Las etiquetas para las categorías.

    Returns:
        None
    """
    df[column_name + '_category'] = pd.cut(df[column_name], bins=bins, labels=labels, right=False)

def convert_duration(df: pd.DataFrame, new_column_name: str, column_name: str) -> None:
    """
    Convierte una columna de duración en milisegundos a un nuevo formato MM:SS.

    Parámetros:
        df (pandas.DataFrame): El DataFrame.
        new_column_name (str): El nombre para la nueva columna de duración.
        column_name (str): El nombre de la columna original de duración.

    Returns:
        None
    """
    df[new_column_name] = pd.to_datetime(df[column_name], unit='ms').dt.strftime('%M:%S')

def map_genre_to_category(df: pd.DataFrame) -> None:
    """
    Mapea géneros específicos a categorías más amplias y agrega una nueva columna 'genre' al DataFrame.

    Parámetros:
        df (pandas.DataFrame): El DataFrame que contiene la columna 'track_genre'.

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