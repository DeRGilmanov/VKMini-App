import sqlite3
import json
from datetime import datetime
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_path: str = "chat_history.db"):
        self.db_path = db_path
        self.init_database()
    
    def get_connection(self):
        """Получение соединения с БД"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def init_database(self):
        """Инициализация базы данных"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Таблица сообщений
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    peer_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    text TEXT,
                    transcribed_text TEXT,
                    message_type TEXT DEFAULT 'text',
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    is_bot BOOLEAN DEFAULT 0,
                    attachments TEXT
                )
            ''')
            
            # Таблица пользователей
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    first_name TEXT,
                    last_name TEXT,
                    screen_name TEXT,
                    messages_count INTEGER DEFAULT 0,
                    last_active DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Таблица бесед (чатов)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS chats (
                    peer_id INTEGER PRIMARY KEY,
                    chat_name TEXT,
                    last_active DATETIME DEFAULT CURRENT_TIMESTAMP,
                    messages_count INTEGER DEFAULT 0,
                    is_active BOOLEAN DEFAULT 1
                )
            ''')
            
            # Индексы для быстрого поиска
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_messages_peer_time ON messages(peer_id, timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_messages_user ON messages(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_chats_last_active ON chats(last_active)')
            
            conn.commit()
            logger.info("Database initialized successfully")
    
    def save_message(self, peer_id: int, user_id: int, text: str = None, 
                    message_type: str = "text", transcribed_text: str = None,
                    attachments: List[Dict] = None, is_bot: bool = False) -> bool:
        """Сохранение сообщения в базу"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO messages (peer_id, user_id, text, message_type, transcribed_text, attachments, is_bot)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (peer_id, user_id, text, message_type, transcribed_text, 
                      json.dumps(attachments, ensure_ascii=False) if attachments else None, is_bot))
                conn.commit()
                
                # Обновляем информацию о беседе
                self.update_chat_info(peer_id)
                
                message_id = cursor.lastrowid
                logger.debug(f"Message saved: id={message_id}, peer={peer_id}, user={user_id}")
                return True
        except Exception as e:
            logger.error(f"Error saving message: {e}", exc_info=True)
            return False
    
    def update_chat_info(self, peer_id: int):
        """Обновление информации о беседе"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Получаем количество сообщений в беседе
                cursor.execute('SELECT COUNT(*) FROM messages WHERE peer_id = ?', (peer_id,))
                messages_count = cursor.fetchone()[0]
                
                # Получаем последнюю активность
                cursor.execute('SELECT MAX(timestamp) FROM messages WHERE peer_id = ?', (peer_id,))
                last_active = cursor.fetchone()[0]
                
                # Вставляем или обновляем запись о беседе
                cursor.execute('''
                    INSERT INTO chats (peer_id, messages_count, last_active, is_active)
                    VALUES (?, ?, ?, 1)
                    ON CONFLICT(peer_id) DO UPDATE SET
                        messages_count = excluded.messages_count,
                        last_active = excluded.last_active,
                        is_active = 1
                ''', (peer_id, messages_count, last_active))
                conn.commit()
        except Exception as e:
            logger.error(f"Error updating chat info: {e}", exc_info=True)
    
    def get_all_chats(self) -> List[Dict]:
        """Получение списка всех бесед"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT 
                        peer_id,
                        messages_count,
                        last_active,
                        is_active
                    FROM chats
                    WHERE peer_id > 2000000000
                    ORDER BY last_active DESC
                ''')
                
                chats = []
                for row in cursor.fetchall():
                    # Получаем последние 3 сообщения для превью
                    cursor.execute('''
                        SELECT 
                            COALESCE(m.transcribed_text, m.text) as text,
                            u.first_name,
                            u.last_name
                        FROM messages m
                        LEFT JOIN users u ON m.user_id = u.user_id
                        WHERE m.peer_id = ? AND m.is_bot = 0
                        ORDER BY m.timestamp DESC
                        LIMIT 3
                    ''', (row['peer_id'],))
                    
                    last_messages = []
                    for msg in cursor.fetchall():
                        if msg['text']:
                            user_name = f"{msg['first_name']} {msg['last_name']}".strip() if msg['first_name'] else "Пользователь"
                            last_messages.append({
                                "user": user_name,
                                "text": msg['text'][:100]
                            })
                    
                    chats.append({
                        "peer_id": row['peer_id'],
                        "chat_id": row['peer_id'] - 2000000000,
                        "name": f"Беседа {row['peer_id'] - 2000000000}",
                        "messages_count": row['messages_count'],
                        "last_active": row['last_active'],
                        "last_messages": last_messages
                    })
                
                return chats
        except Exception as e:
            logger.error(f"Error getting chats: {e}", exc_info=True)
            return []
    
    def get_chat_by_peer_id(self, peer_id: int) -> Optional[Dict]:
        """Получение информации о конкретной беседе"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT 
                        peer_id,
                        messages_count,
                        last_active,
                        is_active
                    FROM chats
                    WHERE peer_id = ?
                ''', (peer_id,))
                row = cursor.fetchone()
                
                if row:
                    return {
                        "peer_id": row['peer_id'],
                        "chat_id": row['peer_id'] - 2000000000,
                        "name": f"Беседа {row['peer_id'] - 2000000000}",
                        "messages_count": row['messages_count'],
                        "last_active": row['last_active']
                    }
                return None
        except Exception as e:
            logger.error(f"Error getting chat: {e}", exc_info=True)
            return None
    
    def get_messages(self, peer_id: int, limit: int = 100, offset: int = 0,
                     include_bot: bool = False) -> List[Dict]:
        """Получение сообщений из чата"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            query = '''
                SELECT id, user_id, text, transcribed_text, message_type, timestamp, attachments, is_bot
                FROM messages
                WHERE peer_id = ?
            '''
            
            if not include_bot:
                query += " AND is_bot = 0"
            
            query += " ORDER BY timestamp DESC LIMIT ? OFFSET ?"
            
            cursor.execute(query, (peer_id, limit, offset))
            
            messages = []
            for row in cursor.fetchall():
                msg = dict(row)
                if msg['attachments']:
                    msg['attachments'] = json.loads(msg['attachments'])
                
                # Используем транскрибированный текст если есть
                if msg['transcribed_text']:
                    msg['full_text'] = msg['transcribed_text']
                else:
                    msg['full_text'] = msg['text'] or ''
                
                messages.append(msg)
            
            return messages[::-1]  # Возвращаем в хронологическом порядке
    
    def get_user_messages(self, peer_id: int, user_id: int, limit: int = 200) -> List[Dict]:
        """Получение сообщений конкретного пользователя"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT text, transcribed_text, message_type, timestamp
                FROM messages
                WHERE peer_id = ? AND user_id = ? AND is_bot = 0
                ORDER BY timestamp DESC
                LIMIT ?
            ''', (peer_id, user_id, limit))
            
            messages = []
            for row in cursor.fetchall():
                msg = dict(row)
                msg['text'] = msg['transcribed_text'] if msg['transcribed_text'] else msg['text']
                messages.append(msg)
            
            return messages[::-1]
    
    def save_user_info(self, user_id: int, first_name: str, last_name: str = "", screen_name: str = ""):
        """Сохранение информации о пользователе"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO users (user_id, first_name, last_name, screen_name, messages_count)
                    VALUES (?, ?, ?, ?, 1)
                    ON CONFLICT(user_id) DO UPDATE SET
                        first_name = COALESCE(?, first_name),
                        last_name = COALESCE(?, last_name),
                        screen_name = COALESCE(?, screen_name),
                        messages_count = messages_count + 1,
                        last_active = CURRENT_TIMESTAMP
                ''', (user_id, first_name, last_name, screen_name, 
                      first_name, last_name, screen_name))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Error saving user info: {e}", exc_info=True)
            return False
    
    def get_formatted_messages(self, peer_id: int, limit: int = 100) -> str:
        """Получение форматированных сообщений для анализа"""
        messages = self.get_messages(peer_id, limit)
        
        formatted = []
        for msg in messages:
            user_id = msg['user_id']
            text = msg['full_text']
            if text and text.strip():
                formatted.append(f"User_{user_id}: {text}")
        
        return "\n".join(formatted)
    
    def clear_history(self, peer_id: int, keep_last: int = 1000):
        """Очистка старой истории"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    DELETE FROM messages
                    WHERE peer_id = ? AND id NOT IN (
                        SELECT id FROM messages
                        WHERE peer_id = ?
                        ORDER BY timestamp DESC
                        LIMIT ?
                    )
                ''', (peer_id, peer_id, keep_last))
                conn.commit()
                
                # Обновляем информацию о беседе
                self.update_chat_info(peer_id)
        except Exception as e:
            logger.error(f"Error clearing history: {e}", exc_info=True)
    
    def get_chat_stats(self, peer_id: int) -> Dict:
        """Получение статистики чата"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Общее количество сообщений
            cursor.execute('SELECT COUNT(*) FROM messages WHERE peer_id = ? AND is_bot = 0', (peer_id,))
            total_messages = cursor.fetchone()[0] or 0
            
            # Количество уникальных пользователей
            cursor.execute('SELECT COUNT(DISTINCT user_id) FROM messages WHERE peer_id = ? AND is_bot = 0', (peer_id,))
            unique_users = cursor.fetchone()[0] or 0
            
            # Активность по дням (последние 7 дней)
            cursor.execute('''
                SELECT DATE(timestamp) as date, COUNT(*) as count
                FROM messages
                WHERE peer_id = ? AND is_bot = 0 AND timestamp >= DATE('now', '-7 days')
                GROUP BY DATE(timestamp)
                ORDER BY date DESC
            ''', (peer_id,))
            daily_activity = {row['date']: row['count'] for row in cursor.fetchall()}
            
            return {
                'total_messages': total_messages,
                'unique_users': unique_users,
                'daily_activity': daily_activity
            }

# Глобальный экземпляр базы данных
db = Database()