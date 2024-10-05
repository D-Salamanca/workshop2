from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

# Configurar GoogleAuth
gauth = GoogleAuth()
gauth.CommandLineAuth()  # O usa otro método según tu flujo

# Autoriza a Google Drive
drive = GoogleDrive(gauth)

# Ahora puedes usar 'drive' para interactuar con Google Drive