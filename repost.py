import time
import base64
import json
import uuid
import requests
import re
import os
import asyncio
import random
from datetime import datetime, timedelta

from telethon.sync import TelegramClient
from telegram import Bot, InlineKeyboardMarkup, InlineKeyboardButton
import urllib.parse

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

# ==================== МОДЕЛИ GIGACHAT (ИСПРАВЛЕНО!) ====================
# ✅ По результатам теста: только GigaChat имеет токены (47,955)
# ❌ GigaChat-Pro и GigaChat-Max: 0 токенов (402 Payment Required)
MODEL_HASHTAGS = 'GigaChat'      # Для генерации хештегов
MODEL_FACTS = 'GigaChat'         # Для фактов о брендах

# --- Сокращения для каналов ---
CHANNEL_SHORTCODES = {
    'shoppogolikhm': 'sh',
    'brand_shop_usa': 'bu',
    'brand_shop_in_russia': 'br',
}

# --- Единые хештеги брендов ---
BRAND_HASHTAGS = {
    'Nike': 'nike',
    'Adidas': 'adidas',
    'Puma': 'puma',
    'Reebok': 'reebok',
    'Tommy Hilfiger': 'tommyhilfiger',
    'Calvin Klein': 'calvinklein',
    'Ralph Lauren': 'ralphlauren',
    "Levi's": 'levis',
    'Gucci': 'gucci',
    'Prada': 'prada',
    'Zara': 'zara',
    'H&M': 'hm',
    'Louis Vuitton': 'louisvuitton',
    'Chanel': 'chanel',
    'Dior': 'dior',
    'MAC': 'maccosmetics',
    'The North Face': 'northface',
    'Oysho': 'oysho',
    'Guess': 'guess',
    'DKNY': 'dkny',
    "Victoria's Secret": 'victoriassecret',
    'Armani': 'armani',
    'Valentino': 'valentino',
    'Karl Lagerfeld': 'karllagerfeld',
    'Moschino': 'moschino',
    'Vikolo': 'vikolo',
    'Michael Kors': 'michaelkors',
    'Coach': 'coach',
    'Fendi': 'fendi',
    'Versace': 'versace',
    'Dolce & Gabbana': 'dolcegabbana',
    'Burberry': 'burberry',
    'Givenchy': 'givenchy',
    'Balenciaga': 'balenciaga',
    'Saint Laurent': 'saintlaurent',
    'Off-White': 'offwhite',
    'Supreme': 'supreme',
    'Abercrombie & Fitch': 'abercrombie',
    'Hollister': 'hollister',
    'Gap': 'gap',
    'Mango': 'mango',
    'Diesel': 'diesel',
    'Benetton': 'benetton',
    'Max Mara': 'maxmara',
    'Furla': 'furla',
    "Tod's": 'tods',
    'Salvatore Ferragamo': 'ferragamo',
}

BRAND_FACTS_TOPICS = list(BRAND_HASHTAGS.keys())
BRAND_FACT_LAST_POST_FILE = 'last_brand_fact_post.txt'
BRAND_FACT_INTERVAL_DAYS = 3
LAST_PUBLISHED_BRAND_FILE = 'last_published_brand.txt'
FACTS_HISTORY_FILE = 'facts_history.json'
MAX_FACTS_HISTORY = 50

# --- Аспекты для разнообразия фактов ---
FACT_ASPECTS = [
    "история создания бренда",
    "основатель и его биография",
    "первый продукт или коллекция",
    "скандальный или спорный момент",
    "инновация или прорыв",
    "связь с искусством или культурой",
    "благотворительность и социальные проекты",
    "неожиданный факт о производстве",
    "влияние на моду и тренды",
    "редкий или малоизвестный факт",
    "сотрудничество с другими брендами",
    "изменение логотипа или айдентики",
    "расширение в новые страны",
    "культовый продукт или бестселлер",
    "экологические инициативы",
    "технологии и материалы",
    "знаменитые амбассадоры бренда",
    "архитектура магазинов",
    "упаковка и фирменный стиль",
    "связь с кино или музыкой"
]

GIGACHAT_PROMPT_TEMPLATE = """
Ты — помощник по созданию хештегов для товаров в Telegram-канале.
Получив описание товара, ты должен сгенерировать краткий набор релевантных хештегов на русском языке.
Формат: только хештеги через пробел, начиная с решётки.
Обязательно включи:
- Название бренда — используй ТОЛЬКО: {allowed_hashtags}.
- Категорию товара (если "сумка" → #сумка).
- Статус (#в_наличии если "В НАЛИЧИИ", иначе #доставка).
- Никаких пояснений, только хештеги.
- Игнорируй посты без товара.

Текст:
{{text}}
""".format(allowed_hashtags=' '.join(f'#{v}' for v in BRAND_HASHTAGS.values()))

LAST_PROCESSED_FILE = 'last_processed.json'
FAILED_POSTS_FILE = 'failed_posts.txt'
PUBLISHED_SOURCE_POSTS_FILE = 'published_source_posts.txt'

_access_token = None
_token_expires_at = 0

requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

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

def load_published_source_posts():
    if os.path.exists(PUBLISHED_SOURCE_POSTS_FILE):
        with open(PUBLISHED_SOURCE_POSTS_FILE, 'r') as f:
            return set(line.strip() for line in f if line.strip())
    return set()

def is_source_post_published(entity, msg_id):
    return f"{entity}:{msg_id}" in load_published_source_posts()

def mark_source_post_as_published(entity, msg_id):
    with open(PUBLISHED_SOURCE_POSTS_FILE, 'a') as f:
        f.write(f"{entity}:{msg_id}\n")

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

def save_last_published_brand(brand_name: str):
    with open(LAST_PUBLISHED_BRAND_FILE, 'w') as f:
        f.write(brand_name)

def load_last_published_brand() -> str:
    if os.path.exists(LAST_PUBLISHED_BRAND_FILE):
        with open(LAST_PUBLISHED_BRAND_FILE, 'r') as f:
            return f.read().strip()
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

def load_facts_history():
    if os.path.exists(FACTS_HISTORY_FILE):
        try:
            with open(FACTS_HISTORY_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_facts_history(history):
    if len(history) > MAX_FACTS_HISTORY:
        sorted_history = dict(sorted(history.items(), key=lambda x: x[1], reverse=True)[:MAX_FACTS_HISTORY])
        history = sorted_history
    with open(FACTS_HISTORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(history, f, ensure_ascii=False, indent=2)

def normalize_text(text):
    return ' '.join(text.lower().split())

def is_duplicate_fact(new_fact, brand, history, threshold=0.6):
    new_normalized = normalize_text(new_fact)
    new_words = set(new_normalized.split())
    
    for key, old_fact in history.items():
        if key.startswith(brand.lower() + "_"):
            old_normalized = normalize_text(old_fact)
            old_words = set(old_normalized.split())
            if len(new_words) == 0 or len(old_words) == 0:
                continue
            similarity = len(new_words & old_words) / len(new_words | old_words)
            if similarity > threshold:
                return True
    return False

# ==================== GIGACHAT API ====================

def get_gigachat_token():
    global _access_token, _token_expires_at
    if _access_token and time.time() < _token_expires_at - 60:
        return _access_token
    
    try:
        decoded = base64.b64decode(API_KEY.strip()).decode('utf-8')
        client_id, client_secret = decoded.split(':', 1)
        credentials = f"{client_id}:{client_secret}"
        basic_auth = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
        
        url = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"
        headers = {
            'Authorization': f'Basic {basic_auth}',
            'Content-Type': 'application/x-www-form-urlencoded',
            'RqUID': str(uuid.uuid4()),
            'Accept': 'application/json'
        }
        data = {'scope': 'GIGACHAT_API_PERS'}
        
        response = requests.post(url, headers=headers, data=data, verify=False, timeout=30)
        
        if response.status_code == 200:
            token_data = response.json()
            _access_token = token_data['access_token']
            _token_expires_at = token_data['expires_at'] / 1000
            print("✅ Токен GigaChat получен/обновлён")
            return _access_token
        else:
            raise Exception(f"GigaChat OAuth error {response.status_code}: {response.text}")
    except Exception as e:
        print(f"❌ Ошибка получения токена GigaChat: {e}")
        raise

def call_gigachat(prompt: str, model: str, max_tokens: int = 100, temperature: float = 0.3) -> str:
    token = get_gigachat_token()
    url = "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"
    
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    payload = {
        'model': model,
        'messages': [{'role': 'user', 'content': prompt}],
        'temperature': temperature,
        'max_tokens': max_tokens,
        'stream': False
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, verify=False, timeout=60)
        
        if response.status_code == 200:
            result = response.json()
            return result['choices'][0]['message']['content'].strip()
        elif response.status_code == 401:
            global _access_token
            _access_token = None
            return call_gigachat(prompt, model, max_tokens, temperature)
        elif response.status_code == 402:
            raise Exception("💰 Payment Required — закончились токены!")
        else:
            raise Exception(f"GigaChat API error {response.status_code}: {response.text}")
    except requests.exceptions.Timeout:
        raise Exception("Таймаут запроса к GigaChat")
    except requests.exceptions.ConnectionError:
        raise Exception("Ошибка подключения к GigaChat")
    except Exception as e:
        print(f"❌ Ошибка вызова GigaChat: {e}")
        raise

def call_gigachat_for_hashtags(text: str) -> str:
    try:
        prompt = GIGACHAT_PROMPT_TEMPLATE.format(text=text)
        response = call_gigachat(prompt=prompt, model=MODEL_HASHTAGS, max_tokens=80, temperature=0.2)
        hashtags = " ".join(re.findall(r'#\S+', response))
        return hashtags if hashtags else "#товар"
    except Exception as e:
        print(f"⚠️ GigaChat (хештеги) ошибка: {e}")
        return "#товар"

def generate_brand_fact(brand_name: str) -> str:
    """Генерирует УНИКАЛЬНЫЙ факт о бренде с защитой от повторов"""
    history = load_facts_history()
    aspect = random.choice(FACT_ASPECTS)
    request_id = str(uuid.uuid4())[:8]
    
    print(f"🎲 Аспект факта: {aspect}")
    
    prompt = f"""Ты — эксперт по истории моды. Придумай УНИКАЛЬНЫЙ факт о бренде {brand_name}.
Сфокусируйся на: {aspect}

Требования:
1. КОНКРЕТНЫЙ факт (даты, имена, цифры если возможно)
2. Избегай общих фраз вроде "бренд основан в..."
3. Найди НЕОБЫЧНЫЙ или МАЛОИЗВЕСТНЫЙ момент
4. 1-2 эмодзи по теме
5. 2-3 предложения максимум
6. Пиши живо и интересно

ID запроса: {request_id}

Факт:"""
    
    for attempt in range(3):
        try:
            temp = 0.8 + (attempt * 0.1)  # 0.8 → 0.9 → 1.0
            response = call_gigachat(prompt=prompt, model=MODEL_FACTS, max_tokens=150, temperature=temp)
            
            if attempt > 0 and is_duplicate_fact(response, brand_name, history):
                print(f"⚠️ Факт похож на предыдущие, пробую снова (попытка {attempt + 2})...")
                continue
            
            history_key = f"{brand_name.lower()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            history[history_key] = response
            save_facts_history(history)
            
            print(f"✅ Уникальный факт сгенерирован (попытка {attempt + 1})")
            return response
            
        except Exception as e:
            print(f"⚠️ Ошибка генерации факта: {e}")
            if attempt == 2:
                return f"Бренд {brand_name} — один из самых влиятельных в мире моды. 💫"
    
    return f"Бренд {brand_name} — легенда мировой моды. ✨"

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================

def extract_and_increase_price(text):
    match = re.search(r'(\d{3,})\s*₽?', text)
    if match:
        return int(match.group(1)) + PRICE_INCREMENT
    return None

def remove_contacts(text):
    cleaned_text = re.sub(r'@\w+', '', text)
    return re.sub(r'\s+', ' ', cleaned_text).strip()

# --- Публикация в Telegram ---
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
    only_brand_fact = os.getenv('ONLY_BRAND_FACT') == '1'

    if only_brand_fact:
        print("🎯 Режим: ТОЛЬКО факт-пост.")
        posts_with_media = []
    else:
        last_processed = load_last_processed()
        print(f"Последние ID по каналам: {last_processed}")
        force_full_repost = os.getenv('FORCE_FULL_REPOST') == '1'
        if force_full_repost:
            print("🔄 Принудительный режим")

        posts_with_media = []
        with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
            for entity in SOURCE_CHANNEL_ENTITIES:
                last_id = last_processed.get(entity, 0)
                if force_full_repost:
                    print(f"🔄 Запрашиваю последние {MAX_MESSAGES_TO_CHECK} постов из {entity}")
                    all_messages = list(client.iter_messages(entity, limit=MAX_MESSAGES_TO_CHECK))
                else:
                    print(f"🔍 Проверяю {entity}, пропускаю ID ≤ {last_id}")
                    all_messages = [msg for msg in client.iter_messages(entity, limit=MAX_MESSAGES_TO_CHECK) if msg.id > last_id]

                for msg in all_messages:
                    if is_post_failed(entity, msg.id) or is_source_post_published(entity, msg.id):
                        continue
                    original_text = (msg.raw_text or "").strip()
                    if not original_text:
                        continue

                    media_path = None
                    if msg.media:
                        try:
                            path = client.download_media(msg.media, file=f"downloads/{msg.id}_media")
                            if path and os.path.exists(path) and os.path.getsize(path) <= 10 * 1024 * 1024:
                                media_path = path
                        except Exception as e:
                            print(f"⚠️ Ошибка скачивания медиа для {msg.id}: {e}")

                    posts_with_media.append({'entity': entity, 'msg_id': msg.id, 'text': original_text, 'media_path': media_path})
                    print(f"✅ Найден НОВЫЙ пост {msg.id} в {entity}")

        # Публикация товарных постов
        if posts_with_media:
            posts_with_media.sort(key=lambda x: x['msg_id'])
            for i, item in enumerate(posts_with_media):
                if i > 0:
                    time.sleep(3)  # ⏱ Защита от flood control
                
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

                    hashtags = call_gigachat_for_hashtags(cleaned_text)

                    # === ОПРЕДЕЛЕНИЕ БРЕНДА ===
                    detected_brand = None
                    hashtags_lower = hashtags.lower()
                    for brand, hashtag in BRAND_HASHTAGS.items():
                        if f"#{hashtag}" in hashtags_lower:
                            detected_brand = brand
                            break
                    if not detected_brand:
                        cleaned_text_lower = cleaned_text.lower()
                        for brand, hashtag in BRAND_HASHTAGS.items():
                            brand_clean = brand.lower().replace('’', '').replace('&', '').replace('.', '')
                            if brand_clean in cleaned_text_lower:
                                detected_brand = brand
                                break

                    # === ФОРМИРОВАНИЕ ЗАКАЗА ===
                    short_code = CHANNEL_SHORTCODES.get(entity, entity[:2])
                    order_lines = [f"хочу заказать товар {short_code}-{msg_id}", hashtags]
                    if isinstance(price_for_message, int):
                        order_lines.append(f"Цена: {price_for_message} ₽")
                    encoded_text = urllib.parse.quote("\n".join(order_lines))
                    button_url = f"{YOUR_TG_LINK}?text={encoded_text}"

                    media_paths = [media_path] if media_path else []

                    # === ПУБЛИКАЦИЯ С ПОВТОРНЫМИ ПОПЫТКАМИ ===
                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            asyncio.run(publish_via_bot(BOT_TOKEN, TARGET_CHANNEL, hashtags, media_paths, button_text, button_url))
                            print(f"✅ Успешно опубликован пост {msg_id}")
                            break
                        except Exception as e:
                            error_msg = str(e)
                            if "Flood control" in error_msg and attempt < max_retries - 1:
                                match = re.search(r"Retry in (\d+) seconds", error_msg)
                                wait_time = int(match.group(1)) + 1 if match else 5
                                print(f"⏳ Flood control, жду {wait_time}с (попытка {attempt+1}/{max_retries})")
                                time.sleep(wait_time)
                            else:
                                raise

                    if detected_brand:
                        save_last_published_brand(detected_brand)
                        print(f"🔖 Сохранён бренд: {detected_brand}")
                    mark_source_post_as_published(entity, msg_id)
                    save_last_processed(entity, msg_id)

                except Exception as e:
                    print(f"❌ Ошибка публикации {msg_id}: {e}")
                    mark_post_as_failed(entity, msg_id)

            print(f"\n✅ Всего опубликовано постов: {len(posts_with_media)}")
        else:
            print("❌ Нет новых постов для публикации.")

    # --- Факт-посты ---
    force_fact = os.getenv('FORCE_BRAND_FACT') == '1' or only_brand_fact
    last_fact_date = get_last_brand_fact_date()
    now = datetime.now()
    should_post_fact = force_fact or last_fact_date is None or (now - last_fact_date) >= timedelta(days=BRAND_FACT_INTERVAL_DAYS)

    if should_post_fact:
        print("🔄 Планируется публикация факт-поста о бренде...")
        brand = load_last_published_brand()
        if not brand or brand not in BRAND_HASHTAGS:
            brand = random.choice(BRAND_FACTS_TOPICS)
            print(f"⚠️ Последний бренд не найден, используем случайный: {brand}")
        else:
            print(f"🧠 Факт-пост о бренде: {brand}")

        fact_text = generate_brand_fact(brand)
        brand_hashtag = BRAND_HASHTAGS.get(brand, brand.lower().replace(' ', '').replace('&', 'and').replace('’', ''))
        caption = f"✨ {fact_text}\n\n#мода #бренды #{brand_hashtag} #fact"
        print(f"📤 Публикуем факт-пост: {caption[:60]}...")

        time.sleep(5)

        try:
            asyncio.run(publish_via_bot(
                BOT_TOKEN, TARGET_CHANNEL, caption, media_paths=[],
                button_text="Смотреть товары этого бренда 👀",
                button_url=YOUR_TG_LINK + "?text=Хочу%20посмотреть%20товары%20" + urllib.parse.quote(brand)
            ))
            print("✅ Факт-пост опубликован в Telegram")
            if not only_brand_fact:
                set_last_brand_fact_date()
            print(f"✅ Успешно опубликован факт-пост о бренде: {brand}")
        except Exception as e:
            print(f"⚠️ Факт-пост не опубликован: {e}")
