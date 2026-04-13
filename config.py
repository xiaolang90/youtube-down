import os
import json
import shutil

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def _load_dotenv(path):
    if not os.path.exists(path):
        return
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, v = line.split('=', 1)
            k, v = k.strip(), v.strip().strip('"').strip("'")
            os.environ.setdefault(k, v)


_load_dotenv(os.path.join(BASE_DIR, '.env'))
DOWNLOAD_DIR = os.path.join(BASE_DIR, 'downloads')
DB_PATH = os.path.join(BASE_DIR, 'data.db')
SETTINGS_PATH = os.path.join(BASE_DIR, 'settings.json')
MAX_CONCURRENT = int(os.environ.get('MAX_CONCURRENT', '2'))
YT_DLP_PATH = os.path.expanduser(os.environ.get('YT_DLP_PATH', 'yt-dlp'))
HOST = os.environ.get('HOST', '0.0.0.0')
PORT = int(os.environ.get('PORT', '8080'))
HTTP_CHUNK_SIZE = os.environ.get('HTTP_CHUNK_SIZE', '100M')

_aria2c_env = os.environ.get('ARIA2C_PATH', '').strip()
ARIA2C_PATH = os.path.expanduser(_aria2c_env) if _aria2c_env else (shutil.which('aria2c') or '')
ARIA2C_CONNECTIONS = int(os.environ.get('ARIA2C_CONNECTIONS', '16'))

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Settings are locked: always use Chrome browser cookies.
FIXED_SETTINGS = {
    'cookie_mode': 'browser',
    'cookie_browser': 'chrome',
    'cookie_file': '',
}


def load_settings():
    return dict(FIXED_SETTINGS)


def save_settings(settings):
    pass
