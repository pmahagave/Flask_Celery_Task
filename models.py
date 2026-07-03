import sqlite3
from config import Config

class Database:
    def __init__(self):
        self.db_path = Config.DATABASE_PATH
        self.init_db()
    
    def init_db(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item TEXT NOT NULL,
                status TEXT DEFAULT 'pending'
            )
        ''')
        conn.commit()
        conn.close()
    
    def insert_item(self, item_name):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO items (item, status) VALUES (?, ?)',
            (item_name, 'pending')
        )
        item_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return item_id
    
    def update_item_status(self, item_name, status):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        # First find the latest pending item
        cursor.execute('''
            SELECT id FROM items 
            WHERE item = ? AND status = 'pending' 
            ORDER BY id DESC 
            LIMIT 1
        ''', (item_name,))
        result = cursor.fetchone()
        
        if result:
            item_id = result[0]
            cursor.execute('''
                UPDATE items 
                SET status = ? 
                WHERE id = ?
            ''', (status, item_id))
            conn.commit()
            conn.close()
            return True
        else:
            conn.close()
            return False

db = Database()