import glob
import logging
import os
from datetime import date, datetime, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, 'log')
os.makedirs(LOG_DIR, exist_ok=True)

LOG_PREFIX = 'app'
LOG_RETENTION_DAYS = 30

_logger = None


class DailyFileHandler(logging.FileHandler):
    """Writes to log/app-YYYY-MM-DD.log; reopens a new file when the date changes."""

    def __init__(self, log_dir, prefix, encoding='utf-8'):
        self.log_dir = log_dir
        self.prefix = prefix
        self._current_date = date.today()
        super().__init__(self._filename(self._current_date), encoding=encoding, delay=False)

    def _filename(self, d):
        return os.path.join(self.log_dir, f'{self.prefix}-{d.isoformat()}.log')

    def emit(self, record):
        today = date.today()
        if today != self._current_date:
            self.close()
            self._current_date = today
            self.baseFilename = self._filename(today)
            self.stream = self._open()
        super().emit(record)


def _cleanup_old_logs(log_dir, prefix, keep_days):
    cutoff = date.today() - timedelta(days=keep_days)
    for path in glob.glob(os.path.join(log_dir, f'{prefix}-*.log')):
        name = os.path.basename(path)
        stem = name[len(prefix) + 1:-4]  # strip "app-" and ".log"
        try:
            d = datetime.strptime(stem, '%Y-%m-%d').date()
        except ValueError:
            continue
        if d < cutoff:
            try:
                os.remove(path)
            except OSError:
                pass


def get_logger():
    global _logger
    if _logger is not None:
        return _logger

    _cleanup_old_logs(LOG_DIR, LOG_PREFIX, LOG_RETENTION_DAYS)

    logger = logging.getLogger('ytdlp')
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    fmt = logging.Formatter(
        '%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    file_handler = DailyFileHandler(LOG_DIR, LOG_PREFIX)
    file_handler.setFormatter(fmt)
    file_handler.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    console.setLevel(logging.DEBUG)
    logger.addHandler(console)

    _logger = logger
    return logger
