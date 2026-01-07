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
from datetime import datetime, timedelta

# --- 🔧 Настройки ---
API_ID = os.getenv('API_ID')
API_HASH = os.getenv('API_HASH')
SESSION_NAME = 'gigachat_telegram_reposter'

SOURCE_CHANNEL_ENTITIES = [
    'brand_shop_usa',
    'brand_shop_in_russia',
    'shoppogolikhm'
]
MAX_MESSAGES_TO_CHECK = 20

BOT_TOKEN = os.getenv('BOT_TOKEN')
TARGET_CHANNEL = '@rnduseu'
YOUR_TG_LINK = 'https://t.me/mazemc'

API_KEY = os.getenv('GIGACHAT_API_KEY')
PRICE_INCREMENT = 1000

# --- Сокращения для каналов ---
CHANNEL_SHORTCODES = {
    'shoppogolikhm': 'sh',
    'brand_shop_usa': 'bu',
    'brand_shop_in_russia': 'br',
}

# --- Настройки факт-постов ---
BRAND_FACTS_TOPICS = ['Calvin Klein', 'Levi’s', 'Tommy Hilfiger', 'Karl Lagerfeld', 'Ralph Lauren']
BRAND_FACT_LAST_POST_FILE = 'last_brand_fact_post.txt'
BRAND_FACT_INTERVAL_DAYS = 3

GIGACHAT_PROMPT_TEMPLATE = """
Ты — помощник по созданию хештегов для товаров в Telegram-канале.
Получив описание товара, ты должен сгенерировать краткий набор релевантных хештегов на русском языке.
Формат: только хештеги через пробел, начиная с решётки.
Обязательно включи:
- Название бренда (если в тексте "Tommy Hilfiger", то #tommyhilfiger).
- Категорию товара (если в тексте "сумка" или "рюкзак", то #сумка).
- Статус товара (#в_наличии, если в тексте "В НАЛИЧИИ", #доставка, в остальных случаях, если про то что товар "в наличии" ничего не сообщено).
- Не добавляй пояснения, только хештеги.
- Не используй хештеги про видео, YouTube, личные истории.
- Игнорируйте посты в которых что-то иное а не обьявление с товаром.

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

def generate_brand_fact(brand_name: str) -> str:
    prompt = f"""
Ты — редактор модного журнала. Напиши один интересный исторический факт о бренде {brand_name}, особенно связанный с Россией или СНГ, если такой есть. 
Если факта про Россию нет — расскажи об интересном моменте из мировой истории бренда.
Формат:
- Текст должен быть живым, с эмодзи (1–2 штуки).
- Без вводных слов вроде «Вот факт:».
- Максимум 2–3 предложения.
- На русском языке.
- Не упоминай, что это факт.
"""
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
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.7,
        "max_tokens": 100
    }

    http = urllib3.PoolManager(cert_reqs="CERT_NONE")
    resp = http.request("POST", url, body=json.dumps(body).encode(), headers=headers)

    if resp.status == 200:
        result = json.loads(resp.data)
        text = result["choices"][0]["message"]["content"].strip()
        return text
    else:
        raise Exception(f"Ошибка генерации факта: {resp.status} {resp.data.decode()}")

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

def find_photo_of_brand_in_target_channel(client, brand_name: str):
    """Ищет в TARGET_CHANNEL последний пост с упоминанием бренда и возвращает путь к фото."""
    try:
        for msg in client.iter_messages(TARGET_CHANNEL, limit=50):
            text = (msg.raw_text or "").lower()
            brand_words = [w.lower() for w in re.split(r'\s+', brand_name)]
            if any(word in text for word in brand_words):
                if msg.media:
                    path = client.download_media(
                        msg.media,
                        file=f"downloads/fact_{brand_name.replace(' ', '_')}"
                    )
                    if path and os.path.exists(path) and os.path.getsize(path) <= 10 * 1024 * 1024:
                        return path
    except Exception as e:
        print(f"⚠️ Ошибка поиска фото для бренда {brand_name}: {e}")
    return None

def get_last_brand_fact_date():
    if os.path.exists(BRAND_FACT_LAST_POST_FILE):
        with open(BRAND_FACT_LAST_POST_FILE, 'r') as f:
            date_str = f.read().strip()
            try:
                return datetime.fromisoformat(date_str)
            except:
                return None
    return None

def set_last_brand_fact_date():
    with open(BRAND_FACT_LAST_POST_FILE, 'w') as f:
        f.write(datetime.now().isoformat())

# --- Публикация (только async) ---
async def publish_via_bot(bot_token, channel, text, media_paths, button_text=None, button_url=None):
    bot = Bot(token=bot_token)
    reply_markup = None
    if button_text and button_url:
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
    required_vars = ['API_ID', 'API_HASH', 'BOT_TOKEN', 'GIGACHAT_API_KEY']
    for var in required_vars:
        if not os.getenv(var):
            raise EnvironmentError(f"❌ Переменная окружения {var} не задана!")

    os.makedirs('downloads', exist_ok=True)

    # Загружаем состояние
    last_processed = load_last_processed()
    print(f"Последние ID по каналам: {last_processed}")

    # Проверяем режимы
    force_full_repost = os.getenv('FORCE_FULL_REPOST') == '1'
    if force_full_repost:
        print("🔄 Принудительный режим: перечитываем последние посты (но избегаем дублей)")

    posts_with_media = []

    with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
        for entity in SOURCE_CHANNEL_ENTITIES:
            last_id = last_processed.get(entity, 0)

            if force_full_repost:
                print(f"🔄 Запрашиваю последние {MAX_MESSAGES_TO_CHECK} постов из {entity}")
                all_messages = list(client.iter_messages(entity, limit=MAX_MESSAGES_TO_CHECK))
            else:
                print(f"🔍 Проверяю {entity}, пропускаю ID ≤ {last_id}")
                all_messages = []
                for msg in client.iter_messages(entity, limit=MAX_MESSAGES_TO_CHECK):
                    if msg.id <= last_id:
                        break
                    all_messages.append(msg)

            for msg in all_messages:
                # Пропускаем, если уже обработан и не в режиме forced
                if not force_full_repost and msg.id <= last_id:
                    continue
                # Пропускаем, если в списке проваленных (чтобы не зациклиться)
                if is_post_failed(entity, msg.id):
                    print(f"⏭️ Пропускаем ранее упавший пост {msg.id} (в failed_posts)")
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

    if posts_with_media:
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

                short_code = CHANNEL_SHORTCODES.get(entity, entity[:2])
                post_ref = f"{short_code}-{msg_id}"

                if isinstance(price_for_message, int):
                    pre_text = f"хочу заказать товар {post_ref}\n{hashtags} за {price_for_message}р"
                else:
                    pre_text = f"хочу заказать товар {post_ref}\n{hashtags}"

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
    else:
        print("❌ Нет новых постов для публикации.")

    # --- Факт-посты ---
    force_fact = os.getenv('FORCE_BRAND_FACT') == '1'
    last_fact_date = get_last_brand_fact_date()
    now = datetime.now()
    should_post_fact = (
        force_fact or
        last_fact_date is None or
        (now - last_fact_date) >= timedelta(days=BRAND_FACT_INTERVAL_DAYS)
    )

    if should_post_fact:
        print("🔄 Планируется публикация факт-поста о бренде...")
        try:
            with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
                import random
                brand = random.choice(BRAND_FACTS_TOPICS)

                fact_text = generate_brand_fact(brand)
                print(f"📝 Сгенерирован факт: {fact_text}")

                photo_path = find_photo_of_brand_in_target_channel(client, brand)

                caption = f"✨ {fact_text}\n\n#мода #бренды #{brand.replace(' ', '').lower()} #fact"
                asyncio.run(publish_via_bot(
                    BOT_TOKEN, TARGET_CHANNEL, caption,
                    [photo_path] if photo_path else [],
                    "Смотреть товары этого бренда 👀",
                    YOUR_TG_LINK + "?text=Хочу%20посмотреть%20товары%20" + urllib.parse.quote(brand)
                ))

                if not force_fact:
                    set_last_brand_fact_date()
                print(f"✅ Опубликован факт-пост о бренде: {brand}")

        except Exception as e:
            print(f"❌ Ошибка публикации факт-поста: {e}")
