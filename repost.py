import time
import urllib3
import base64
import json
import uuid
import re
import os
from telethon.sync import TelegramClient
from telegram import Bot, InlineKeyboardMarkup, InlineKeyboardButton
import urllib.parse
import asyncio

# --- 🔧 Настройки (теперь из переменных окружения) ---
API_ID = os.getenv('API_ID')
API_HASH = os.getenv('API_HASH')
SESSION_NAME = 'gigachat_telegram_reposter'

SOURCE_CHANNEL_ENTITIES = [
    'brand_shop_usa',
    'brand_shop_in_russia',
    '@shoppogolikhm'  # ← ДОБАВЛЕН НОВЫЙ КАНАЛ
]
MAX_MESSAGES_TO_CHECK = 20

BOT_TOKEN = os.getenv('BOT_TOKEN')
TARGET_CHANNEL = '@rnduseu'
YOUR_TG_LINK = 'https://t.me/mazemc'  # ✅ пробел в конце УДАЛЁН

API_KEY = os.getenv('GIGACHAT_API_KEY')
PRICE_INCREMENT = 1000

GIGACHAT_PROMPT_TEMPLATE = """
Ты — помощник по созданию хештегов для товаров в Telegram-канале.
Получив описание товара, ты должен сгенерировать краткий набор релевантных хештегов на русском языке.
Формат: только хештеги через пробел, начиная с решётки.
Обязательно включи:
- Название бренда (если в тексте "Tommy Hilfiger", то #tommyhilfiger).
- Категорию товара (если в тексте "сумка" или "рюкзак", то #сумка).
- Статус товара (#в_наличии, если в тексте "В НАЛИЧИИ").
- Не добавляй пояснения, только хештеги.
- Не используй хештеги про видео, YouTube, личные истории.

Текст описания товара:
{text}
"""

LAST_PROCESSED_FILE = 'last_processed.json'
FAILED_POSTS_FILE = 'failed_posts.txt'

_access_token = None
_token_expires_at = 0

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Вспомогательные функции ---
def load_last_processed():
    if os.path.exists(LAST_PROCESSED_FILE):
        try:
            with open(LAST_PROCESSED_FILE, 'r') as f:
                data = json.load(f)
                return {str(k): int(v) for k, v in data.items()}
        except Exception as e:
            print(f"⚠️ Ошибка загрузки {LAST_PROCESSED_FILE}: {e}")
    return {}

def save_last_processed(channel, msg_id):
    data = load_last_processed()
    data[channel] = msg_id
    with open(LAST_PROCESSED_FILE, 'w') as f:
        json.dump(data, f, indent=2)

def is_post_failed(channel, msg_id):
    if os.path.exists(FAILED_POSTS_FILE):
        with open(FAILED_POSTS_FILE, 'r') as f:
            for line in f:
                if line.strip() == f"{channel}:{msg_id}":
                    return True
    return False

def mark_post_as_failed(channel, msg_id):
    with open(FAILED_POSTS_FILE, 'a') as f:
        f.write(f"{channel}:{msg_id}\n")

# --- GigaChat ---
def get_gigachat_token():
    global _access_token, _token_expires_at
    if _access_token and (time.time() * 1000) < _token_expires_at - 60000:
        return _access_token

    decoded = base64.b64decode(API_KEY.strip()).decode("utf-8")
    client_id, client_secret = decoded.split(":", 1)
    credentials = f"{client_id}:{client_secret}"
    basic_token = base64.b64encode(credentials.encode("ascii")).decode("ascii")

    url = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"
    body = b"scope=GIGACHAT_API_PERS"
    headers = {
        "Authorization": f"Basic {basic_token}",
        "Content-Type": "application/x-www-form-urlencoded",
        "Content-Length": str(len(body)),
        "RqUID": str(uuid.uuid4()),
        "Accept": "application/json",
        "User-Agent": "Python/3.x"
    }

    http = urllib3.PoolManager(cert_reqs="CERT_NONE")
    resp = http.request("POST", url, body=body, headers=headers)

    if resp.status == 200:
        data = json.loads(resp.data)
        _access_token = data["access_token"]
        _token_expires_at = data["expires_at"]
        print("✅ Новый токен GigaChat получен.")
        return _access_token
    else:
        raise Exception(f"Не удалось получить токен GigaChat: {resp.status} {resp.data.decode()}")

def call_gigachat_for_hashtags(text: str) -> str:
    token = get_gigachat_token()
    url = "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    body = {
        "model": "GigaChat-2-Max",
        "messages": [
            {"role": "system", "content": "Ты — помощник по созданию хештегов для товаров в Telegram-канале."},
            {"role": "user", "content": GIGACHAT_PROMPT_TEMPLATE.format(text=text)}
        ],
        "temperature": 0.3,
        "max_tokens": 60
    }

    http = urllib3.PoolManager(cert_reqs="CERT_NONE")
    resp = http.request("POST", url, body=json.dumps(body).encode(), headers=headers)

    if resp.status == 200:
        result = json.loads(resp.data)
        raw_response = result["choices"][0]["message"]["content"].strip()
        hashtags = " ".join(re.findall(r'#\S+', raw_response))
        return hashtags
    elif resp.status == 401 and "Token has expired" in resp.data.decode():
        global _access_token
        _access_token = None
        return call_gigachat_for_hashtags(text)
    else:
        raise Exception(f"Ошибка вызова GigaChat: {resp.status} {resp.data.decode()}")

def extract_and_increase_price(text):
    match = re.search(r'(\d{3,})\s*₽?', text)
    if match:
        base_price = int(match.group(1))
        return base_price + PRICE_INCREMENT
    return None

def remove_contacts(text):
    contact_pattern = r'@\w+'
    cleaned_text = re.sub(contact_pattern, '', text)
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
    return cleaned_text

# --- Публикация (только async) ---
async def publish_via_bot(bot_token, channel, text, media_paths, button_text, button_url):
    bot = Bot(token=bot_token)
    keyboard = [[InlineKeyboardButton(button_text, url=button_url)]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if media_paths:
        for i, media_path in enumerate(media_paths):
            with open(media_path, 'rb') as media_file:
                if i == 0:
                    await bot.send_photo(chat_id=channel, photo=media_file, caption=text, reply_markup=reply_markup)
                else:
                    await bot.send_photo(chat_id=channel, photo=media_file)
    else:
        await bot.send_message(chat_id=channel, text=text, reply_markup=reply_markup)

# --- Основной блок ---
if __name__ == "__main__":
    # Проверка наличия всех переменных
    required_vars = ['API_ID', 'API_HASH', 'BOT_TOKEN', 'GIGACHAT_API_KEY']
    for var in required_vars:
        if not os.getenv(var):
            raise EnvironmentError(f"❌ Переменная окружения {var} не задана!")

    os.makedirs('downloads', exist_ok=True)

    last_processed = load_last_processed()
    print(f"Последние ID по каналам: {last_processed}")

    posts_with_media = []

    with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
        for entity in SOURCE_CHANNEL_ENTITIES:
            last_id = last_processed.get(entity, 0)
            print(f"🔍 Проверяю {entity}, пропускаю ID ≤ {last_id}")
            try:
                for msg in client.iter_messages(entity, limit=MAX_MESSAGES_TO_CHECK):
                    if msg.id <= last_id:
                        break
                    if is_post_failed(entity, msg.id):
                        continue
                    original_text = (msg.raw_text or msg.message or msg.text or "").strip()
                    if not original_text:
                        continue

                    media_path = None
                    if msg.media:
                        try:
                            path = client.download_media(
                                msg.media,
                                file=f"downloads/{msg.id}_media"
                            )
                            if path and os.path.exists(path):
                                if os.path.getsize(path) <= 10 * 1024 * 1024:
                                    media_path = path
                                    print(f"✅ Медиа сохранено: {path}")
                                else:
                                    print(f"⏭️ Медиа >10 МБ — пропускаем (ID: {msg.id})")
                        except Exception as e:
                            print(f"⚠️ Ошибка скачивания медиа для {msg.id}: {e}")

                    posts_with_media.append({
                        'entity': entity,
                        'msg_id': msg.id,
                        'text': original_text,
                        'media_path': media_path
                    })
                    print(f"✅ Найден пост {msg.id} в {entity}")
            except Exception as e:
                print(f"❌ Ошибка при работе с {entity}: {e}")
                continue

    if not posts_with_media:
        print("❌ Нет новых постов для публикации.")
        exit()

    posts_with_media.sort(key=lambda x: x['msg_id'])
    new_max_ids = {}

    for item in posts_with_media:
        entity = item['entity']
        msg_id = item['msg_id']
        text = item['text']
        media_path = item['media_path']

        print(f"\n🔄 Публикую пост {msg_id} из {entity}")
        try:
            cleaned_text = remove_contacts(text)
            extracted_price = extract_and_increase_price(cleaned_text)
            button_text = f"Заказать за {extracted_price} >>" if extracted_price else "Заказать >>"
            price_for_message = extracted_price if extracted_price else "не указана"

            try:
                hashtags = call_gigachat_for_hashtags(cleaned_text)
                if not hashtags.strip():
                    hashtags = "#товар"
            except Exception as e:
                print(f"⚠️ GigaChat ошибка: {e}")
                hashtags = "#товар"

            base_url = f"https://t.me/{entity}"  # ✅ Исправлено: убраны пробелы
            if isinstance(price_for_message, int):
                pre_text = f"хочу заказать товар из поста в {base_url}\n{hashtags} за {price_for_message}р"
            else:
                pre_text = f"хочу заказать товар из поста в {base_url}\n{hashtags}"
            encoded_text = urllib.parse.quote(pre_text)
            button_url = f"{YOUR_TG_LINK}?text={encoded_text}"

            media_paths = [media_path] if media_path else []

            asyncio.run(publish_via_bot(
                BOT_TOKEN, TARGET_CHANNEL, hashtags, media_paths, button_text, button_url
            ))
            print(f"✅ Успешно опубликован пост {msg_id}")

            if entity not in new_max_ids or msg_id > new_max_ids[entity]:
                new_max_ids[entity] = msg_id

        except Exception as e:
            print(f"❌ Ошибка публикации {msg_id}: {e}")
            mark_post_as_failed(entity, msg_id)

    for entity, max_id in new_max_ids.items():
        save_last_processed(entity, max_id)

    print(f"\n✅ Всего опубликовано постов: {len(posts_with_media)}")
