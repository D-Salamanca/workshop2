# Proyecto de ETL para Datos de Grammy y Spotify

### Herramientas utilizadas

- **Python** <img src="https://cdn-icons-png.flaticon.com/128/3098/3098090.png" alt="Python" width="21px" height="21px">
- **Jupyter Notebooks** <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/3/38/Jupyter_logo.svg/883px-Jupyter_logo.svg.png" alt="Jupyter" width="21px" height="21px">
- **PostgreSQL** <img src="https://cdn-icons-png.flaticon.com/128/5968/5968342.png" alt="Postgres" width="21px" height="21px">
- **SQLAlchemy** <img src="https://quintagroup.com/cms/python/images/sqlalchemy-logo.png/@@images/eca35254-a2db-47a8-850b-2678f7f8bc09.png" alt="SQLAlchemy" width="50px" height="21px">
- **Apache Airflow** <img src="https://static-00.iconduck.com/assets.00/airflow-icon-512x512-tpr318yf.png" alt="Airflow" width="30px" height="25px">
- **Google Drive** para almacenar datasets.

---

### Sobre los datos

Los datasets utilizados en este proyecto fueron obtenidos de:

- [Premios Grammy](https://www.kaggle.com/datasets/unanimad/grammy-awards) "Grammy Awards, 1958 - 2019"
- [Dataset de Canciones de Spotify](https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset) "Un dataset de canciones de Spotify con diferentes géneros y sus características de audio"

---

### Organización del Proyecto

![Estructura del Proyecto]

---

### Requisitos Previos

1. **Instalar Git** si no lo tienes instalado en tu máquina virtual Ubuntu:
    
    ```bash
    sudo apt update
    sudo apt install git
    ```
    
2. **Configurar Git** con tu nombre y correo electrónico:
    
    ```bash
    git config --global user.name "Tu Nombre"
    git config --global user.email "tuemail@example.com"
    ```

### Paso 1: Abrir Visual Studio Code

1. **Abre Visual Studio Code** en tu máquina local (o dentro de la VM si lo tienes instalado allí).
2. **Instala la extensión de Git** si no la tienes instalada ya. Busca "GitLens" o "GitHub" en las extensiones y asegúrate de que estén instaladas.

### Paso 2: Clonar el repositorio desde VS Code

1. **Abre VS Code** y presiona `Ctrl + Shift + P` para abrir la paleta de comandos.
2. Escribe **Git: Clone** y selecciona la opción `Git: Clone`.
3. En el campo que aparece, pega la URL del repositorio:
    
    ```bash
    git@github.com:D-Salamanca/workshop2.git
    ```
    
4. **Selecciona una carpeta** donde quieras clonar el repositorio en tu máquina.

### Paso 3: Configurar tu clave SSH

1. Si no tienes configurada una clave SSH para GitHub, debes crear una. En tu máquina virtual Ubuntu, ejecuta:
    
    ```bash
    ssh-keygen -t rsa -b 4096 -C "tuemail@example.com"
    ```
    
    Presiona `Enter` para todos los prompts y no configures una contraseña para simplificar el proceso.
    
2. Luego, agrega tu clave SSH a tu agente de SSH:
    
    ```bash
    eval "$(ssh-agent -s)"
    ssh-add ~/.ssh/id_rsa
    ```
    
3. **Copia la clave SSH** a tu portapapeles:
    
    ```bash
    cat ~/.ssh/id_rsa.pub
    ```
    
4. **Agrega la clave SSH** a tu cuenta de GitHub:
    - Ve a [GitHub SSH Settings](https://github.com/settings/keys).
    - Haz clic en **New SSH Key**.
    - Pega tu clave pública y dale un nombre (por ejemplo, "VM Ubuntu").

### Paso 4: Autenticación y clonación

1. Después de agregar la clave SSH a GitHub, vuelve a Visual Studio Code y continúa con el proceso de clonación.
2. Si todo está correctamente configurado, no deberías necesitar ingresar tu usuario y contraseña, ya que GitHub usará tu clave SSH.

### Paso 5: Verificar la conexión

1. En el terminal integrado de Visual Studio Code (o el terminal de Ubuntu), navega hasta el repositorio clonado:
    
    ```bash
    cd workshop2
    ```
    
2. Verifica la conexión con GitHub:
    
    ```bash
    git remote -v
    ```
    
    Deberías ver algo como:
    
    ```
    origin  git@github.com:D-Salamanca/workshop2.git (fetch)
    origin  git@github.com:D-Salamanca/workshop2.git (push)
    ```

---

### ¿Cómo ejecutar este proyecto?  

1. Clona el proyecto
```bash
  git clone https://github.com/SamuelEscalante/Workshop-02-ETL.git
```

2. Ve al directorio del proyecto
```bash
  cd Workshop-02-ETL
```

3. En la raíz del proyecto, crea un archivo `db_settings.json` para establecer las credenciales de la base de datos:
```json
{
  "DIALECT": "El dialecto o tipo de base de datos. En este caso, se establece en 'postgres' para PostgreSQL.",
  "PGUSER": "Tu nombre de usuario de la base de datos PostgreSQL.",
  "PGPASSWD": "Tu contraseña de la base de datos PostgreSQL.",
  "PGHOST": "La dirección del host o IP donde se está ejecutando tu base de datos PostgreSQL.",
  "PGPORT": "El puerto en el que PostgreSQL está escuchando.",
  "PGDB": "El nombre de tu base de datos PostgreSQL."
}
```

4. Crea un entorno virtual para Python
```bash
  python -m venv venv
```

5. Activa el entorno
```bash
  source venv/bin/activate 
```

6. Instala las librerías
```bash
  pip install -r requirements.txt
```

7. Crea un archivo `.env` y agrega estas variables:
   
    - WORK_PATH <- Establece el directorio de trabajo para la aplicación, indicando la ruta base para realizar operaciones y gestionar archivos.
    - EMAIL <- Establece tu dirección de correo electrónico, esto es para el correo que enviará las notificaciones.

9. __Crea tu base de datos__, este paso es opcional si estás ejecutando localmente, pero si estás en la nube, ya debes tener tu base de datos.

10. Comienza con los notebooks:
- 001_eda_spotify_dataset.ipynb
- 002_eda_grammys_dataset.ipynb

11. Inicia Airflow:
    
    - Exporta a Airflow tu ruta actual
      ```bash
      export AIRFLOW_HOME=${pwd}
      ```
    - Ejecuta Apache Airflow
      ```bash
      airflow standalone
      ```
    - Luego ve a tu navegador y busca 'localhost:8080'
   
    - Finalmente, activa el DAG.

12. Luego, ve a PostgreSQL y verifica si las tablas se han creado correctamente.

---

## Despedida y Agradecimientos

¡Gracias por visitar el repositorio!
