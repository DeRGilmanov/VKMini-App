# main.py - положи в корневую папку с ботом
import sqlite3
import os
import requests
from datetime import datetime
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
import io
from dotenv import load_dotenv

load_dotenv()

# ===== НАСТРОЙКА =====
DATABASE_PATH = os.path.join(os.path.dirname(__file__), "chat_history.db")
VK_TOKEN = os.getenv("VK_TOKEN", "")
VK_API_VERSION = "5.199"
GROUP_ID = os.getenv("GROUP_ID", "236213880")

print(f"📁 База данных: {DATABASE_PATH}")
print(f"📁 Файл существует: {os.path.exists(DATABASE_PATH)}")
print(f"🔑 VK Token: {'✅ Найден' if VK_TOKEN else '❌ Не найден'}")
print(f"👥 Group ID: {GROUP_ID}")

# ===== FASTAPI =====
app = FastAPI(title="AI Assistant Bot API")

# CORS настройки
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===== ФУНКЦИИ РАБОТЫ С БД =====
def get_db_connection():
    if not os.path.exists(DATABASE_PATH):
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute('''
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
        conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                screen_name TEXT,
                messages_count INTEGER DEFAULT 0,
                last_active DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS chat_names (
                peer_id INTEGER PRIMARY KEY,
                name TEXT,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        conn.close()
        print("✅ База данных создана автоматически")
    
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def get_chat_name(peer_id: int) -> str:
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM chat_names WHERE peer_id = ?", (peer_id,))
        row = cursor.fetchone()
        
        if row and row['name']:
            conn.close()
            return row['name']
        conn.close()
        
        if VK_TOKEN:
            chat_id = peer_id - 2000000000
            url = "https://api.vk.com/method/messages.getChat"
            params = {
                "chat_id": chat_id,
                "access_token": VK_TOKEN,
                "v": VK_API_VERSION
            }
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            if "response" in data:
                chat = data["response"]
                if "title" in chat and chat["title"]:
                    chat_name = chat["title"]
                    conn = get_db_connection()
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT OR REPLACE INTO chat_names (peer_id, name, updated_at)
                        VALUES (?, ?, CURRENT_TIMESTAMP)
                    """, (peer_id, chat_name))
                    conn.commit()
                    conn.close()
                    return chat_name
        
        return f"Беседа {peer_id - 2000000000}"
    except Exception as e:
        print(f"Ошибка: {e}")
        return f"Беседа {peer_id - 2000000000}"

def get_all_chats_from_db():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                peer_id,
                COUNT(*) as messages_count,
                MAX(timestamp) as last_active
            FROM messages
            WHERE peer_id > 2000000000
            GROUP BY peer_id
            ORDER BY last_active DESC
        """)
        
        chats = []
        for row in cursor.fetchall():
            chat_name = get_chat_name(row['peer_id'])
            
            cursor.execute("""
                SELECT COALESCE(m.transcribed_text, m.text) as text, u.first_name, u.last_name
                FROM messages m
                LEFT JOIN users u ON m.user_id = u.user_id
                WHERE m.peer_id = ? AND m.is_bot = 0
                ORDER BY m.timestamp DESC
                LIMIT 2
            """, (row['peer_id'],))
            
            last_messages = []
            for msg in cursor.fetchall():
                if msg['text']:
                    user_name = f"{msg['first_name']} {msg['last_name']}".strip() if msg['first_name'] else "Пользователь"
                    last_messages.append(f"{user_name}: {msg['text'][:60]}")
            
            chats.append({
                "peer_id": row['peer_id'],
                "chat_id": row['peer_id'] - 2000000000,
                "name": chat_name,
                "messages_count": row['messages_count'],
                "last_active": row['last_active'],
                "last_messages": last_messages
            })
        
        conn.close()
        return chats
    except Exception as e:
        print(f"Ошибка: {e}")
        return []

# ===== API ЭНДПОИНТЫ =====

@app.get("/api/chats")
async def get_chats():
    try:
        chats = get_all_chats_from_db()
        return {"chats": chats}
    except Exception as e:
        return {"chats": []}

@app.get("/api/stats")
async def get_stats(peer_id: int):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM messages WHERE peer_id = ? AND is_bot = 0", (peer_id,))
        total_messages = cursor.fetchone()[0] or 0
        
        cursor.execute("SELECT COUNT(DISTINCT user_id) FROM messages WHERE peer_id = ? AND is_bot = 0", (peer_id,))
        unique_users = cursor.fetchone()[0] or 0
        
        cursor.execute("SELECT COUNT(*) FROM messages WHERE peer_id = ? AND is_bot = 1", (peer_id,))
        bot_messages = cursor.fetchone()[0] or 0
        
        cursor.execute("SELECT COUNT(*) FROM messages WHERE peer_id = ? AND message_type IN ('voice', 'image')", (peer_id,))
        recognized_media = cursor.fetchone()[0] or 0
        
        cursor.execute("""
            SELECT user_id, COUNT(*) as count
            FROM messages 
            WHERE peer_id = ? AND is_bot = 0 
            GROUP BY user_id 
            ORDER BY count DESC 
            LIMIT 10
        """, (peer_id,))
        top_users = []
        for row in cursor.fetchall():
            cursor.execute("SELECT first_name, last_name FROM users WHERE user_id = ?", (row['user_id'],))
            user = cursor.fetchone()
            top_users.append({
                "user_id": row['user_id'],
                "name": f"{user['first_name']} {user['last_name']}".strip() if user else f"ID{row['user_id']}",
                "messages_count": row['count']
            })
        
        cursor.execute("""
            SELECT DATE(timestamp) as date, COUNT(*) as count
            FROM messages
            WHERE peer_id = ? AND is_bot = 0 AND timestamp >= DATE('now', '-30 days')
            GROUP BY DATE(timestamp)
            ORDER BY date
        """, (peer_id,))
        daily_activity = [{"date": row['date'], "count": row['count']} for row in cursor.fetchall()]
        
        cursor.execute("""
            SELECT strftime('%H', timestamp) as hour, COUNT(*) as count
            FROM messages
            WHERE peer_id = ? AND is_bot = 0 AND timestamp >= DATE('now', '-7 days')
            GROUP BY hour
            ORDER BY hour
        """, (peer_id,))
        hourly_activity = [{"hour": row['hour'], "count": row['count']} for row in cursor.fetchall()]
        
        cursor.execute("""
            SELECT message_type, COUNT(*) as count
            FROM messages
            WHERE peer_id = ?
            GROUP BY message_type
        """, (peer_id,))
        message_types = [{"message_type": row['message_type'] or 'text', "count": row['count']} for row in cursor.fetchall()]
        
        conn.close()
        
        return {
            "total_messages": total_messages,
            "unique_users": unique_users,
            "bot_messages": bot_messages,
            "recognized_media": recognized_media,
            "top_users": top_users,
            "daily_activity": daily_activity,
            "hourly_activity": hourly_activity,
            "message_types": message_types
        }
    except Exception as e:
        print(f"Ошибка: {e}")
        return {
            "total_messages": 0,
            "unique_users": 0,
            "bot_messages": 0,
            "recognized_media": 0,
            "top_users": [],
            "daily_activity": [],
            "hourly_activity": [],
            "message_types": []
        }

@app.get("/api/messages")
async def get_messages(peer_id: int, limit: int = 50, offset: int = 0):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT m.id, m.user_id, m.text, m.transcribed_text, m.message_type, 
                   m.timestamp, m.is_bot, u.first_name, u.last_name
            FROM messages m
            LEFT JOIN users u ON m.user_id = u.user_id
            WHERE m.peer_id = ?
            ORDER BY m.timestamp DESC
            LIMIT ? OFFSET ?
        """, (peer_id, limit, offset))
        
        messages = []
        for row in cursor.fetchall():
            messages.append({
                "id": row['id'],
                "user_id": row['user_id'],
                "user_name": f"{row['first_name']} {row['last_name']}".strip() if row['first_name'] else f"ID{row['user_id']}",
                "text": row['transcribed_text'] if row['transcribed_text'] else (row['text'] or ''),
                "type": row['message_type'] or 'text',
                "timestamp": row['timestamp'],
                "is_bot": bool(row['is_bot'])
            })
        
        cursor.execute("SELECT COUNT(*) FROM messages WHERE peer_id = ?", (peer_id,))
        total = cursor.fetchone()[0] or 0
        
        conn.close()
        
        return {
            "messages": messages,
            "total": total,
            "limit": limit,
            "offset": offset
        }
    except Exception as e:
        print(f"Ошибка: {e}")
        return {"messages": [], "total": 0, "limit": limit, "offset": offset}

@app.post("/api/ask")
async def ask_question(request: Request):
    try:
        data = await request.json()
        question = data.get('question')
        peer_id = data.get('peer_id')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT COALESCE(m.transcribed_text, m.text) as text
            FROM messages m
            WHERE m.peer_id = ? AND m.is_bot = 0
            ORDER BY m.timestamp DESC
            LIMIT 100
        """, (peer_id,))
        
        messages = [row['text'] for row in cursor.fetchall() if row['text']]
        conn.close()
        
        if not messages:
            return {
                "question": question,
                "answer": "В этом чате пока нет сообщений для анализа.",
                "timestamp": datetime.now().isoformat()
            }
        
        context = "\n".join(reversed(messages[-30:]))
        
        return {
            "question": question,
            "answer": f"📝 *Вопрос:* {question}\n\n📚 *На основе истории чата:*\n\n{context[:500]}...",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "question": question if 'question' in locals() else "",
            "answer": f"Ошибка: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }

@app.get("/api/export")
async def export_data(peer_id: int):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT m.timestamp, 
                   COALESCE(u.first_name || ' ' || u.last_name, 'ID' || m.user_id) as user_name,
                   COALESCE(m.transcribed_text, m.text, '') as message,
                   m.message_type as type
            FROM messages m
            LEFT JOIN users u ON m.user_id = u.user_id
            WHERE m.peer_id = ?
            ORDER BY m.timestamp
        """, (peer_id,))
        
        rows = cursor.fetchall()
        conn.close()
        
        output = io.StringIO()
        output.write("Дата и время,Пользователь,Сообщение,Тип\n")
        
        for row in rows:
            message = row['message'].replace('\n', ' ').replace(',', ';') if row['message'] else ''
            output.write(f"{row['timestamp']},{row['user_name']},{message},{row['type']}\n")
        
        output.seek(0)
        
        return StreamingResponse(
            iter([output.getvalue().encode('utf-8-sig')]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=chat_export_{peer_id}.csv"}
        )
    except Exception as e:
        print(f"Ошибка экспорта: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
async def serve_frontend():
    # Ищем index.html в корневой папке
    frontend_path = os.path.join(os.path.dirname(__file__), "index.html")
    if os.path.exists(frontend_path):
        return FileResponse(frontend_path)
    else:
        return {"message": "Frontend not found", "path": frontend_path}

if __name__ == "__main__":
    import uvicorn
    print("\n" + "="*50)
    print("🚀 Запуск FastAPI сервера...")
    print(f"📁 База данных: {DATABASE_PATH}")
    print(f"📊 API доступно: http://localhost:8000")
    print("="*50 + "\n")
    uvicorn.run(app, host="0.0.0.0", port=8000)
