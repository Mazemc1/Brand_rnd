#!/usr/bin/env python3
import os
import json
import requests
import socket
import sys

print("=" * 60)
print("🔍 ТЕСТ ДОСТУПНОСТИ MAX.RU API")
print("=" * 60)

# Получаем токен и канал из переменных окружения
MAX_BOT_TOKEN = os.getenv('MAX_BOT_TOKEN')
MAX_CHANNEL_ID = os.getenv('MAX_CHANNEL_ID')

print(f"\n📊 Переменные окружения:")
print(f"   MAX_BOT_TOKEN: {'✅ задан' if MAX_BOT_TOKEN else '❌ НЕТ'}")
print(f"   MAX_CHANNEL_ID: {MAX_CHANNEL_ID if MAX_CHANNEL_ID else '❌ НЕТ'}")

if not MAX_BOT_TOKEN or not MAX_CHANNEL_ID:
    print("\n⚠️  Ошибка: не заданы переменные окружения MAX_BOT_TOKEN или MAX_CHANNEL_ID")
    sys.exit(1)

# === ШАГ 1: Проверка DNS-разрешения ===
print("\n" + "=" * 60)
print("ШАГ 1: Проверка DNS (разрешается ли домен api.max.ru?)")
print("=" * 60)

try:
    ip = socket.gethostbyname('api.max.ru')
    print(f"✅ DNS OK: api.max.ru → {ip}")
except socket.gaierror as e:
    print(f"❌ DNS ERROR: {e}")
    sys.exit(1)

# === ШАГ 2: Проверка доступности порта 443 ===
print("\n" + "=" * 60)
print("ШАГ 2: Проверка доступности порта 443 (HTTPS)")
print("=" * 60)

try:
    sock = socket.create_connection(('api.max.ru', 443), timeout=5)
    sock.close()
    print("✅ Порт 443 доступен")
except (socket.timeout, ConnectionRefusedError, OSError) as e:
    print(f"❌ Порт 443 недоступен: {e}")
    sys.exit(1)

# === ШАГ 3: Отправка тестового запроса к API ===
print("\n" + "=" * 60)
print("ШАГ 3: Отправка тестового сообщения через MAX API")
print("=" * 60)

url = "https://api.max.ru/v1/messages/send"
headers = {
    "Authorization": f"Bearer {MAX_BOT_TOKEN}",
    "Content-Type": "application/json; charset=utf-8",
    "User-Agent": "Mozilla/5.0 (compatible; GitHub-Actions/1.0)"
}
payload = {
    "chat_id": MAX_CHANNEL_ID,
    "text": "🧪 Тестовое сообщение из GitHub Actions",
    "parse_mode": "html"
}

print(f"   URL: {url}")
print(f"   Chat ID: {MAX_CHANNEL_ID}")
print(f"   Текст: {payload['text']}")

try:
    response = requests.post(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode('utf-8'),
        headers=headers,
        timeout=15
    )
    
    print(f"\n📊 Ответ от сервера:")
    print(f"   HTTP Status: {response.status_code}")
    print(f"   Response body: {response.text}")
    
    if response.status_code == 200:
        print("\n✅ УСПЕХ! API работает, сообщение отправлено.")
        sys.exit(0)
    elif response.status_code == 401:
        print("\n❌ ОШИБКА: Неверный токен (401 Unauthorized)")
    elif response.status_code == 403:
        print("\n❌ ОШИБКА: Бот не имеет прав на публикацию в этом канале (403 Forbidden)")
    elif response.status_code == 400:
        print("\n❌ ОШИБКА: Неверный формат запроса или неверный chat_id (400 Bad Request)")
    else:
        print(f"\n❌ ОШИБКА: Неизвестный код ответа {response.status_code}")
        
except requests.exceptions.Timeout:
    print("\n❌ ТАЙМАУТ: Сервер не отвечает в течение 15 секунд")
    print("   Это означает, что api.max.ru недоступен из вашей сети/облака.")
    
except requests.exceptions.ConnectionError as e:
    print(f"\n❌ ОШИБКА СОЕДИНЕНИЯ: {e}")
    print("   Сервер недоступен или блокирует запросы.")
    
except Exception as e:
    print(f"\n❌ НЕИЗВЕСТНАЯ ОШИБКА: {e}")

print("\n" + "=" * 60)
print("Тест завершён")
print("=" * 60)
