import datetime
from sqlalchemy import Column, Integer, String, Boolean, Date, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class GrammyAwards(Base):
    __tablename__ = 'grammy_awards'

    id = Column(Integer, primary_key=True)  # ID único para cada nominación
    year = Column(Integer, nullable=False)  # Año de la nominación
    category = Column(String(255), nullable=False)  # Categoría de la nominación
    artist = Column(String(255), nullable=False)  # Artista nominado
    nominee = Column(String(255), nullable=False)  # Nombre del nominado
    winner = Column(Boolean, default=False)  # Indica si es ganador
    record_label = Column(String(255))  # Sello discográfico
    album_name = Column(String(255))  # Nombre del álbum
    song_name = Column(String(255))  # Nombre de la canción
    featured_artists = Column(String(255))  # Artistas destacados
    songwriters = Column(String(255))  # Compositores
    producers = Column(String(255))  # Productores
    release_date = Column(Date)  # Fecha de lanzamiento
    duration = Column(Float)  # Duración de la canción (en minutos)


class SongsData(Base):
    __tablename__ = 'songs_data'
    
    id = Column(Integer, primary_key=True, autoincrement=True)  # ID autoincrementable
    track_name = Column(String)  # Nombre de la canción
    artists = Column(String)  # Artistas de la canción
    album = Column(String)  # Álbum de la canción
    release_date = Column(datetime)  # Fecha de lanzamiento
    duration_min_sec = Column(String)  # Duración en formato MM:SS
    popularity = Column(Integer)  # Popularidad de la canción
    genre = Column(String)  # Género de la canción
    nominee_status = Column(Boolean)  # Estado de nominación