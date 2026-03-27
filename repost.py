import time
import base64
import json
import uuid
import requests
import re
import os
import asyncio
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

# --- Модели GigaChat ---
# Используем GigaChat-Pro для генерации хештегов (качество)
# и GigaChat-Lite для фактов (скорость + экономия)
MODEL_HASHTAGS = 'GigaChat-Pro'
MODEL_FACTS = 'GigaChat-Lite'  # или 'GigaChat-Pro' если нужно больше качества

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

# Шаблон промпта для хештегов — используем .format() с {{}} для экранирования
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

# Глобальные переменные для кэширования токена
_access_token = None
_token_expires_at = 0  # в секундах (unix timestamp)

# Отключаем предупреждения о самоподписанных сертификатах
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

# ==================== GIGACHAT API ====================

def get_gigachat_token():
    """Получает или возвращает закэшированный токен доступа к GigaChat API"""
    global _access_token, _token_expires_at
    
    # Если токен есть и не истёк (с запасом 60 секунд)
    if _access_token and time.time() < _token_expires_at - 60:
        return _access_token
    
    try:
        # Декодируем client_id:client_secret из Base64
        decoded = base64.b64decode(API_KEY.strip()).decode('utf-8')
        client_id, client_secret = decoded.split(':', 1)
        
        # Формируем Basic Auth заголовок
        credentials = f"{client_id}:{client_secret}"
        basic_auth = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
        
        # Запрос на получение токена
        url = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"
        headers = {
            'Authorization': f'Basic {basic_auth}',
            'Content-Type': 'application/x-www-form-urlencoded',
            'RqUID': str(uuid.uuid4()),
            'Accept': 'application/json'
        }
        data = {'scope': 'GIGACHAT_API_PERS'}
        
        response = requests.post(
            url,
            headers=headers,
            data=data,
            verify=False,  # Отключаем проверку SSL для совместимости
            timeout=30
        )
        
        if response.status_code == 200:
            token_data = response.json()
            _access_token = token_data['access_token']
            # expires_at приходит в миллисекундах — конвертируем в секунды
            _token_expires_at = token_data['expires_at'] / 1000
            print("✅ Токен GigaChat получен/обновлён")
            return _access_token
        else:
            raise Exception(f"GigaChat OAuth error {response.status_code}: {response.text}")
            
    except Exception as e:
        print(f"❌ Ошибка получения токена GigaChat: {e}")
        raise

def call_gigachat(prompt: str, model: str, max_tokens: int = 100, temperature: float = 0.3) -> str:
    """Универсальная функция вызова GigaChat API"""
    token = get_gigachat_token()
    url = "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"
    
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    payload = {
        'model': model,
        'messages': [
            {'role': 'user', 'content': prompt}
        ],
        'temperature': temperature,
        'max_tokens': max_tokens,
        'stream': False
    }
    
    try:
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            verify=False,
            timeout=60
        )
        
        if response.status_code == 200:
            result = response.json()
            content = result['choices'][0]['message']['content'].strip()
            return content
        elif response.status_code == 401:
            # Токен истёк — сбрасываем кэш и пробуем ещё раз
            global _access_token
            _access_token = None
            return call_gigachat(prompt, model, max_tokens, temperature)
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
    """Генерирует хештеги через GigaChat-Pro"""
    try:
        prompt = GIGACHAT_PROMPT_TEMPLATE.format(text=text)
        response = call_gigachat(
            prompt=prompt,
            model=MODEL_HASHTAGS,
            max_tokens=80,
            temperature=0.2
        )
        # Извлекаем только хештеги из ответа
        hashtags = " ".join(re.findall(r'#\S+', response))
        return hashtags if hashtags else "#товар"
    except Exception as e:
        print(f"⚠️ GigaChat (хештеги) ошибка: {e}")
        return "#товар"  # Fallback

def generate_brand_fact(brand_name: str) -> str:
    """Генерирует факт о бренде через GigaChat-Lite"""
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
    try:
        response = call_gigachat(
            prompt=prompt,
            model=MODEL_FACTS,
            max_tokens=120,
            temperature=0.7
        )
        return response.strip()
    except Exception as e:
        print(f"⚠️ GigaChat (факты) ошибка: {e}")
        return f"Бренд {brand_name} — один из самых влиятельных в мире моды. 💫"

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================

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
                    all_messages = []
                    for msg in client.iter_messages(entity, limit=MAX_MESSAGES_TO_CHECK):
                        if msg.id <= last_id:
                            break
                        all_messages.append(msg)

                for msg in all_messages:
                    if not force_full_repost and msg.id <= last_id:
                        continue
                    if is_post_failed(entity, msg.id):
                        print(f"⏭️ Пропускаем ранее упавший пост {msg.id}")
                        continue

                    original_text = (msg.raw_text or "").strip()
                    if not original_text:
                        continue

                    if is_source_post_published(entity, msg.id):
                        print(f"⏭️ Пропускаем уже опубликованный пост {msg.id} из {entity}")
                        continue

                    media_path = None
                    if msg.media:
                        try:
                            path = client.download_media(msg.media, file=f"downloads/{msg.id}_media")
                            if path and os.path.exists(path) and os.path.getsize(path) <= 10 * 1024 * 1024:
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
                    print(f"✅ Найден НОВЫЙ пост {msg.id} в {entity}")

        # Публикация товарных постов
        if posts_with_media:
            posts_with_media.sort(key=lambda x: x['msg_id'])
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

                    # Генерация хештегов
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
                    order_lines = [
                        f"хочу заказать товар {short_code}-{msg_id}",
                        hashtags
                    ]
                    if isinstance(price_for_message, int):
                        order_lines.append(f"Цена: {price_for_message} ₽")
                    pre_text = "\n".join(order_lines)
                    encoded_text = urllib.parse.quote(pre_text)
                    button_url = f"{YOUR_TG_LINK}?text={encoded_text}"

                    media_paths = [media_path] if media_path else []

                    # === ПУБЛИКАЦИЯ В TELEGRAM ===
                    asyncio.run(publish_via_bot(
                        BOT_TOKEN, TARGET_CHANNEL, hashtags, media_paths, button_text, button_url
                    ))
                    print(f"✅ Успешно опубликован пост {msg_id}")

                    # === СОХРАНЕНИЕ СОСТОЯНИЯ ===
                    if detected_brand:
                        save_last_published_brand(detected_brand)
                        print(f"🔖 Сохранён бренд последней публикации: {detected_brand}")
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
    should_post_fact = (
        force_fact or
        last_fact_date is None or
        (now - last_fact_date) >= timedelta(days=BRAND_FACT_INTERVAL_DAYS)
    )

    if should_post_fact:
        print("🔄 Планируется публикация факт-поста о бренде...")
        
        brand = load_last_published_brand()
        if not brand or brand not in BRAND_HASHTAGS:
            import random
            brand = random.choice(BRAND_FACTS_TOPICS)
            print(f"⚠️ Последний бренд не найден, используем случайный: {brand}")
        else:
            print(f"🧠 Факт-пост о бренде: {brand}")

        fact_text = generate_brand_fact(brand)

        brand_hashtag = BRAND_HASHTAGS.get(brand, brand.lower().replace(' ', '').replace('&', 'and').replace('’', ''))
        caption = f"✨ {fact_text}\n\n#мода #бренды #{brand_hashtag} #fact"

        print(f"📤 Публикуем факт-пост: {caption[:60]}...")

        # Задержка перед публикацией
        time.sleep(5)

        try:
            # Telegram
            asyncio.run(publish_via_bot(
                BOT_TOKEN, TARGET_CHANNEL, caption,
                media_paths=[],
                button_text="Смотреть товары этого бренда 👀",
                button_url=YOUR_TG_LINK + "?text=Хочу%20посмотреть%20товары%20" + urllib.parse.quote(brand)
            ))
            print("✅ Факт-пост опубликован в Telegram")

            if not only_brand_fact:
                set_last_brand_fact_date()
            print(f"✅ Успешно опубликован факт-пост о бренде: {brand}")

        except Exception as e:
            print(f"⚠️ Факт-пост не опубликован: {e}")
