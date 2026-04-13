import sqlite3
import uuid
from datetime import datetime
from config import DB_PATH


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = get_db()
    conn.execute('''
        CREATE TABLE IF NOT EXISTS downloads (
            id TEXT PRIMARY KEY,
            url TEXT NOT NULL,
            title TEXT,
            thumbnail TEXT,
            uploader TEXT,
            duration INTEGER,
            format_id TEXT,
            format_note TEXT,
            filesize INTEGER,
            filename TEXT,
            status TEXT DEFAULT 'pending',
            progress REAL DEFAULT 0,
            speed TEXT,
            eta TEXT,
            error TEXT,
            created_at TEXT DEFAULT (datetime('now','localtime')),
            completed_at TEXT
        )
    ''')
    conn.commit()
    conn.close()


def create_task(url, title, thumbnail, uploader, duration, format_id, format_note, filesize):
    task_id = str(uuid.uuid4())[:8]
    conn = get_db()
    conn.execute(
        '''INSERT INTO downloads (id, url, title, thumbnail, uploader, duration,
           format_id, format_note, filesize, status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')''',
        (task_id, url, title, thumbnail, uploader, duration,
         format_id, format_note, filesize)
    )
    conn.commit()
    conn.close()
    return task_id


def update_task(task_id, **kwargs):
    conn = get_db()
    sets = ', '.join(f'{k} = ?' for k in kwargs)
    values = list(kwargs.values()) + [task_id]
    conn.execute(f'UPDATE downloads SET {sets} WHERE id = ?', values)
    conn.commit()
    conn.close()


def get_task(task_id):
    conn = get_db()
    row = conn.execute('SELECT * FROM downloads WHERE id = ?', (task_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def list_tasks(page=1, per_page=20, search='', status_filter=''):
    conn = get_db()
    where_clauses = []
    params = []
    if search:
        where_clauses.append('title LIKE ?')
        params.append(f'%{search}%')
    if status_filter:
        where_clauses.append('status = ?')
        params.append(status_filter)
    where = ('WHERE ' + ' AND '.join(where_clauses)) if where_clauses else ''

    total = conn.execute(f'SELECT COUNT(*) FROM downloads {where}', params).fetchone()[0]
    rows = conn.execute(
        f'SELECT * FROM downloads {where} ORDER BY created_at DESC LIMIT ? OFFSET ?',
        params + [per_page, (page - 1) * per_page]
    ).fetchall()
    conn.close()
    return {'items': [dict(r) for r in rows], 'total': total, 'page': page, 'per_page': per_page}


def delete_task(task_id):
    conn = get_db()
    conn.execute('DELETE FROM downloads WHERE id = ?', (task_id,))
    conn.commit()
    conn.close()


def delete_all_tasks():
    """Return filenames of completed downloads so caller can delete files."""
    conn = get_db()
    rows = conn.execute("SELECT filename FROM downloads WHERE filename IS NOT NULL AND filename != ''").fetchall()
    filenames = [r['filename'] for r in rows]
    conn.execute('DELETE FROM downloads')
    conn.commit()
    conn.close()
    return filenames


def mark_stale_downloads():
    """Mark any 'downloading' tasks as 'failed' on startup (crash recovery)."""
    conn = get_db()
    conn.execute("UPDATE downloads SET status = 'failed', error = '服务器重启，下载中断' WHERE status = 'downloading'")
    conn.commit()
    conn.close()
