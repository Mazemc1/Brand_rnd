#!/usr/bin/env python3
"""
Telegram Reposter с факт-постами о брендах
Версия: 2.0 (GigaChat-2 Lite + улучшенные промпты)
"""

import os
import sys
import json
import time
import random
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple

# Telegram
from telethon import TelegramClient
from telethon.tl.types import Message
from telethon.tl.functions.messages import SendMessageRequest
from telethon import Button

# HTTP requests
import requests

# === Логирование ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# === Константы ===
SOURCE_CHANNELS = ['@brand_shop_usa', '@brand_shop_in_russia', '@shoppogolikhm']
TARGET_CHANNEL = '@rnduseu'
MANAGER_USERNAME = 'mazemc'

# Префиксы для коротких ID
CHANNEL_PREFIXES = {
    '@brand_shop_usa': 'bu',
    '@brand_shop_in_russia': 'br',
    '@shoppogolikhm': 'sh'
}

# Файлы состояния
STATE_FILES = {
    'last_processed': 'last_processed.json',
    'published_posts': 'published_source_posts.txt',
    'failed_posts': 'failed_posts.txt',
    'last_brand': 'last_published_brand.txt',
    'last_fact': 'last_brand_fact_post.txt'
}

# === Бренды и хештеги ===
BRAND_HASHTAGS = {
    'Tommy Hilfiger': 'tommyhilfiger',
    'Tommy': 'tommyhilfiger',
    'Levi\'s': 'levis',
    'Levis': 'levis',
    'Adidas': 'adidas',
    'Nike': 'nike',
    'Puma': 'puma',
    'Ralph Lauren': 'ralphlauren',
    'Polo': 'ralphlauren',
    'Calvin Klein': 'calvinklein',
    'CK': 'calvinklein',
    'Hugo Boss': 'hugoboss',
    'Boss': 'hugoboss',
    'Armani': 'armani',
    'Emporio Armani': 'armani',
    'Gucci': 'gucci',
    'Prada': 'prada',
    'Versace': 'versace',
    'Dolce & Gabbana': 'dolcegabbana',
    'D&G': 'dolcegabbana',
    'Burberry': 'burberry',
    'Louis Vuitton': 'louisvuitton',
    'LV': 'louisvuitton',
    'Chanel': 'chanel',
    'Dior': 'dior',
    'Fendi': 'fendi',
    'Givenchy': 'givenchy',
    'Balenciaga': 'balenciaga',
    'Off-White': 'offwhite',
    'Supreme': 'supreme',
    'Stone Island': 'stoneisland',
    'The North Face': 'thenorthface',
    'Patagonia': 'patagonia',
    'Columbia': 'columbia',
    'Timberland': 'timberland',
    'Dr. Martens': 'drmartens',
    'Converse': 'converse',
    'Vans': 'vans',
    'New Balance': 'newbalance',
    'Reebok': 'reebok',
    'Under Armour': 'underarmour',
    'Lacoste': 'lacoste',
    'Fred Perry': 'fredperry',
    'Benetton': 'benetton',
    'Zara': 'zara',
    'H&M': 'hm',
    'Uniqlo': 'uniqlo',
    'Massimo Dutti': 'massimodutti',
    'Mango': 'mango',
    'Guess': 'guess',
    'Michael Kors': 'michaelkors',
    'Coach': 'coach',
    'Kate Spade': 'katespade',
    'Tory Burch': 'toryburch',
    'Ray-Ban': 'rayban',
    'Oakley': 'oakley',
    'Persol': 'persol',
    'Montblanc': 'montblanc',
    'Tissot': 'tissot',
    'Casio': 'casio',
    'Seiko': 'seiko',
    'Citizen': 'citizen',
}

# === Промпт для генерации хештегов товаров ===
HASHTAG_PROMPT = """Ты — эксперт по SEO и продвижению в Telegram.
Подбери 5-7 релевантных хештегов для товара.

Требования:
- Только латиница, без пробелов
- Без # (добавлю сам)
- Через запятую
- Микс: бренд + категория + стиль + аудитория

Пример ответа:
tommyhilfiger, hoodie, streetwear, mensfashion, casual, style, outfit

Товар:"""

# === Промпт для факт-постов о брендах (улучшенный) ===
def get_brand_fact_prompt(brand_name: str) -> str:
    """Генерирует промпт с вариативностью для уникальных постов"""
    
    # Случайный "угол" подачи (чтобы посты не повторялись)
    angles = [
        f"Расскажи НЕОЖИДАННЫЙ факт о {brand_name}, который удивит даже фанатов бренда.",
        f"Какой самый БЕЗУМНЫЙ момент в истории {brand_name}? Расскажи кратко и с юмором.",
        f"Что в {brand_name} есть такого, чего нет у конкурентов? Факт, а не маркетинг.",
        f"Представь, что {brand_name} — это человек. Какой у него самый крутой поступок в карьере?",
        f"За что {brand_name} реально стоит уважать? Конкретика, без воды.",
    ]
    
    angle = random.choice(angles)
    
    prompt = f"""Ты — остроумный эксперт по моде и брендам, который знает историю индустрии и умеет рассказывать о ней с лёгкой иронией. 😎

{angle}

🎯 Твоя задача:
Напиши короткий пост (ровно 1-2 предложения) о бренде {brand_name}.

✅ Обязательно включи:
- Конкретный факт: год основания, технология, коллаборация, награда, скандал, рекорд, необычная история
- Чем бренд реально отличается от других (не "качество", а конкретика)
- 1-2 эмодзи по теме для настроения

🎭 Стиль:
- Живой русский язык, как в Телеграме
- Лёгкая ирония, удивление или тёплый сарказм — приветствуются
- Можно использовать скобки для комментариев: "(спойлер: это сработало)"

🚫 Строго запрещено:
- Фразы-клише: "один из самых влиятельных", "ведущий бренд", "инновационные решения", "стиль и качество", "узнаваемый во всём мире"
- Общие слова без конкретики: "популярный", "знаменитый", "уважаемый"
- Вода и маркетинговый шум
- Повторы шаблонов из примеров ниже

💡 Примеры ХОРОШИХ постов (ориентируйся на этот стиль):
"🧵 Levi's изобрела джинсы с заклёпками в 1873 для золотоискателей — и они пережили золотую лихорадку. Спойлер: джинсы оказались прочнее золота."

"👟 В 2013 Adidas выпустила Boost — пену, которая вернула ногам чувство молодости. Теперь все остальные просто догоняют."

"🕶️ Ray-Ban создали авиаторы для лётчиков США в 1937. Сейчас их носят все — от пилотов до котов в инстаграме."

"👜 Коко Шанель освободила женщин от корсетов в 1910-х. И да, маленькое чёрное платье — тоже её идея."

❌ Примеры ПЛОХИХ постов (НИКОГДА так не пиши):
"✨ Бренд {brand_name} — один из самых влиятельных в мире моды. 💫"
"{brand_name} — это стиль, качество и инновации уже много лет."
"Узнаваемый бренд с богатой историей и преданными поклонниками."

📋 Формат ответа:
- Только текст поста (1-2 предложения)
- Без заголовков, без объяснений, без "Вот пост:"
- В конце — только хештеги, которые я добавлю отдельно

Напиши про {brand_name} так, чтобы хотелось сделать репост. Удиви меня:"""
    
    return prompt


# === Класс для работы с состоянием ===
class StateManager:
    def __init__(self):
        self.last_processed = self._load_json(STATE_FILES['last_processed'], {})
        self.published_posts = self._load_set(STATE_FILES['published_posts'])
        self.failed_posts = self._load_set(STATE_FILES['failed_posts'])
        self.last_brand = self._load_text(STATE_FILES['last_brand'])
        self.last_fact_date = self._load_text(STATE_FILES['last_fact'])
    
    def _load_json(self, path: str, default: dict) -> dict:
        try:
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Не удалось загрузить {path}: {e}")
        return default
    
    def _load_set(self, path: str) -> set:
        try:
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f:
                    return set(line.strip() for line in f if line.strip())
        except Exception as e:
            logger.warning(f"Не удалось загрузить {path}: {e}")
        return set()
    
    def _load_text(self, path: str) -> str:
        try:
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f:
                    return f.read().strip()
        except Exception as e:
            logger.warning(f"Не удалось загрузить {path}: {e}")
        return ''
    
    def save(self):
        """Сохраняет все файлы состояния"""
        try:
            with open(STATE_FILES['last_processed'], 'w', encoding='utf-8') as f:
                json.dump(self.last_processed, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения last_processed: {e}")
        
        try:
            with open(STATE_FILES['published_posts'], 'w', encoding='utf-8') as f:
                f.write('\n'.join(self.published_posts))
        except Exception as e:
            logger.error(f"Ошибка сохранения published_posts: {e}")
        
        try:
            with open(STATE_FILES['failed_posts'], 'w', encoding='utf-8') as f:
                f.write('\n'.join(self.failed_posts))
        except Exception as e:
            logger.error(f"Ошибка сохранения failed_posts: {e}")
        
        try:
            with open(STATE_FILES['last_brand'], 'w', encoding='utf-8') as f:
                f.write(self.last_brand)
        except Exception as e:
            logger.error(f"Ошибка сохранения last_brand: {e}")
        
        try:
            with open(STATE_FILES['last_fact'], 'w', encoding='utf-8') as f:
                f.write(self.last_fact_date)
        except Exception as e:
            logger.error(f"Ошибка сохранения last_fact: {e}")
        
        logger.info("✅ Файлы состояния сохранены")


# === GigaChat API ===
class GigaChatClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://gigachat.devices.sberbank.ru/api/v2"
        self.access_token = None
        self.token_expires = 0
    
    def _get_access_token(self) -> str:
        """Получает токен доступа"""
        if self.access_token and time.time() < self.token_expires:
            return self.access_token
        
        try:
            response = requests.post(
                "https://ngw.devices.sberbank.ru:9443/api/v2/oauth",
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "RqUID": hashlib.md5(os.urandom(16)).hexdigest(),
                    "Authorization": f"Basic {self.api_key}"
                },
                data={"scope": "GIGACHAT_API_PERS"},
                timeout=30,
                verify=False  # Для самоподписанных сертификатов
            )
            response.raise_for_status()
            data = response.json()
            self.access_token = data['access_token']
            self.token_expires = time.time() + data['expires_in'] - 60
            logger.info("✅ Токен GigaChat получен")
            return self.access_token
        except Exception as e:
            logger.error(f"Ошибка получения токена GigaChat: {e}")
            raise
    
    def generate_text(self, prompt: str, temperature: float = 0.65, max_tokens: int = 256) -> str:
        """Генерирует текст через GigaChat"""
        try:
            token = self._get_access_token()
            
            payload = {
                "model": "GigaChat-2",  # Lite-версия
                "messages": [{"role": "user", "content": prompt}],
                "temperature": temperature,
                "top_p": 0.9,
                "max_tokens": max_tokens,
            }
            
            response = requests.post(
                f"{self.base_url}/chat/completions",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {token}"
                },
                json=payload,
                timeout=60,
                verify=False
            )
            response.raise_for_status()
            
            result = response.json()
            text = result['choices'][0]['message']['content'].strip()
            logger.info(f"✅ GigaChat ответил ({len(text)} символов)")
            return text
            
        except Exception as e:
            logger.error(f"Ошибка GigaChat: {e}")
            raise


# === Основной класс репостера ===
class TelegramReposter:
    def __init__(self):
        self.api_id = int(os.getenv('API_ID', 0))
        self.api_hash = os.getenv('API_HASH', '')
        self.bot_token = os.getenv('BOT_TOKEN', '')
        self.gigachat_key = os.getenv('GIGACHAT_API_KEY', '')
        
        self.client = TelegramClient('reposter_session', self.api_id, self.api_hash)
        self.gigachat = GigaChatClient(self.gigachat_key)
        self.state = StateManager()
        
        # Флаги из environment
        self.only_brand_fact = os.getenv('ONLY_BRAND_FACT', '0') == '1'
        self.force_brand_fact = os.getenv('FORCE_BRAND_FACT', '0') == '1'
        self.force_full_repost = os.getenv('FORCE_FULL_REPOST', '0') == '1'
    
    async def start(self):
        """Запуск клиента"""
        await self.client.start(bot_token=self.bot_token)
        logger.info("✅ Telegram клиент запущен")
    
    async def stop(self):
        """Остановка клиента"""
        await self.client.disconnect()
        self.state.save()
        logger.info("✅ Telegram клиент остановлен")
    
    def detect_brand(self, text: str) -> Optional[str]:
        """Определяет бренд по тексту"""
        text_lower = text.lower()
        
        # Сначала по хештегам
        for brand, hashtag in BRAND_HASHTAGS.items():
            if f'#{hashtag}' in text_lower or f'#{hashtag.lower()}' in text_lower:
                return brand
        
        # Затем по названию в тексте
        for brand in BRAND_HASHTAGS.keys():
            if brand.lower() in text_lower:
                return brand
        
        return None
    
    def generate_short_id(self, channel: str, post_id: int) -> str:
        """Генерирует короткий ID товара"""
        prefix = CHANNEL_PREFIXES.get(channel, 'br')
        return f"{prefix}-{post_id}"
    
    def generate_hashtags(self, text: str) -> str:
        """Генерирует хештеги через GigaChat"""
        try:
            prompt = f"{HASHTAG_PROMPT}\n\n{text[:500]}"
            response = self.gigachat.generate_text(prompt, temperature=0.3, max_tokens=100)
            hashtags = response.replace('#', '').strip()
            return hashtags
        except Exception as e:
            logger.warning(f"Не удалось сгенерировать хештеги: {e}")
            return "мода,бренды,shopping"
    
    def generate_brand_fact(self, brand_name: str) -> str:
        """Генерирует факт о бренде"""
        try:
            prompt = get_brand_fact_prompt(brand_name)
            fact = self.gigachat.generate_text(prompt, temperature=0.65, max_tokens=256)
            # Очищаем от лишних символов
            fact = fact.replace('"', '').strip()
            if fact.startswith('Вот') or fact.startswith('Пост:'):
                fact = fact.split('\n', 1)[-1].strip()
            return fact
        except Exception as e:
            logger.error(f"Ошибка генерации факта: {e}")
            return f"✨ Бренд {brand_name} — легенда в своём деле. 💫"
    
    def process_message_text(self, text: str, channel: str, message_id: int) -> Tuple[str, str]:
        """Обрабатывает текст сообщения"""
        # Удаляем @username
        import re
        text = re.sub(r'@\w+', '', text)
        
        # Увеличиваем цену на 1000₽
        import re
        def increase_price(match):
            price = int(match.group(1).replace(' ', '').replace('\u202f', ''))
            new_price = price + 1000
            return f"{new_price} ₽"
        
        text = re.sub(r'(\d[\d\s\u202f]*)\s*₽', increase_price, text)
        
        # Генерируем хештеги
        hashtags = self.generate_hashtags(text[:300])
        hashtag_list = [h.strip() for h in hashtags.split(',') if h.strip()]
        hashtag_str = ' '.join(f'#{h}' for h in hashtag_list[:7])
        
        # Добавляем системные хештеги
        brand = self.detect_brand(text)
        if brand:
            brand_hashtag = BRAND_HASHTAGS.get(brand, brand.lower().replace(' ', ''))
            hashtag_str += f' #{brand_hashtag}'
        
        hashtag_str += ' #fact' if self.only_brand_fact else ''
        
        # Генерируем короткий ID
        short_id = self.generate_short_id(channel, message_id)
        
        # Формируем финальный текст
        final_text = f"{text}\n\n{hashtag_str}\n\n📦 ID: {short_id}"
        
        return final_text, short_id
    
    def create_order_button(self, short_id: str) -> list:
        """Создаёт кнопку «Заказать»"""
        url = f"https://t.me/{MANAGER_USERNAME}?text=хочу%20заказать%20товар%20{short_id}"
        return [[Button.url("🛒 Заказать", url)]]
    
    async def publish_post(self, text: str, media: Optional[Message] = None, buttons: Optional[list] = None):
        """Публикует пост в целевом канале"""
        try:
            if media and media.media:
                # С медиа
                await self.client.send_file(
                    TARGET_CHANNEL,
                    media.media,
                    caption=text,
                    buttons=buttons,
                    parse_mode='markdown'
                )
            else:
                # Текст
                await self.client.send_message(
                    TARGET_CHANNEL,
                    text,
                    buttons=buttons
                )
            
            logger.info("✅ Пост опубликован")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка публикации: {e}")
            return False
    
    async def process_source_channel(self, channel: str, limit: int = 20):
        """Обрабатывает канал-источник"""
        logger.info(f"📡 Обработка канала: {channel}")
        
        last_id = self.state.last_processed.get(channel, 0)
        processed_count = 0
        
        async for message in self.client.iter_messages(channel, limit=limit):
            if message.id <= last_id and not self.force_full_repost:
                break
            
            # Проверка на дубли
            post_key = f"{channel}:{message.id}"
            if post_key in self.state.published_posts:
                continue
            
            # Пропускаем без медиа (для товаров)
            if not self.only_brand_fact and not message.media:
                continue
            
            logger.info(f"📝 Обработка поста {message.id} из {channel}")
            
            try:
                # Обрабатываем текст
                text = message.text or message.caption or ""
                if not text and not self.only_brand_fact:
                    continue
                
                if self.only_brand_fact:
                    continue  # Факт-посты обрабатываются отдельно
                
                final_text, short_id = self.process_message_text(text, channel, message.id)
                buttons = self.create_order_button(short_id)
                
                # Публикуем
                if await self.publish_post(final_text, message, buttons):
                    self.state.published_posts.add(post_key)
                    self.state.last_processed[channel] = message.id
                    
                    # Сохраняем бренд для факт-постов
                    brand = self.detect_brand(text)
                    if brand:
                        self.state.last_brand = brand
                    
                    processed_count += 1
                    await asyncio.sleep(2)  # Anti-flood
                    
            except Exception as e:
                logger.error(f"Ошибка обработки поста {message.id}: {e}")
                self.state.failed_posts.add(f"{post_key}: {e}")
        
        logger.info(f"✅ Обработано {processed_count} постов из {channel}")
    
    async def publish_brand_fact(self):
        """Публикует факт о бренде"""
        # Проверяем, не публиковали ли недавно
        if not self.force_brand_fact and self.state.last_fact_date:
            try:
                last_date = datetime.fromisoformat(self.state.last_fact_date)
                if datetime.now() - last_date < timedelta(days=3):
                    logger.info("⏭️ Факт-пост был недавно — пропускаем")
                    return
            except:
                pass
        
        # Получаем бренд
        brand = self.state.last_brand
        if not brand:
            logger.warning("⚠️ Нет сохранённого бренда — пропускаем факт-пост")
            return
        
        logger.info(f"🎯 Генерация факта о бренде: {brand}")
        
        try:
            # Генерируем факт
            fact_text = self.generate_brand_fact(brand)
            
            # Добавляем хештеги
            brand_hashtag = BRAND_HASHTAGS.get(brand, brand.lower().replace(' ', ''))
            hashtags = f"#мода #бренды #{brand_hashtag} #fact"
            
            full_text = f"{fact_text}\n\n{hashtags}"
            
            # Публикуем
            if await self.publish_post(full_text):
                self.state.last_fact_date = datetime.now().isoformat()
                logger.info("✅ Факт-пост опубликован")
                
                # Задержка перед следующим действием
                await asyncio.sleep(5)
                
        except Exception as e:
            logger.error(f"Ошибка публикации факта: {e}")
            # Не прерываем основной workflow
    
    async def run(self):
        """Основной цикл"""
        try:
            await self.start()
            
            if self.only_brand_fact:
                # Только факт-пост
                await self.publish_brand_fact()
            else:
                # Репостинг товаров
                for channel in SOURCE_CHANNELS:
                    limit = 20 if self.force_full_repost else 10
                    await self.process_source_channel(channel, limit)
                
                # Факт-пост после товаров (если пора)
                if self.force_brand_fact:
                    await self.publish_brand_fact()
                else:
                    try:
                        await self.publish_brand_fact()
                    except Exception as e:
                        logger.warning(f"Факт-пост не опубликован: {e}")
            
        except Exception as e:
            logger.error(f"Критическая ошибка: {e}")
            raise
        finally:
            await self.stop()


# === Точка входа ===
if __name__ == '__main__':
    import asyncio
    
    # Проверка переменных окружения
    required_vars = ['API_ID', 'API_HASH', 'BOT_TOKEN', 'GIGACHAT_API_KEY']
    missing = [v for v in required_vars if not os.getenv(v)]
    
    if missing:
        logger.error(f"❌ Отсутствуют переменные окружения: {missing}")
        sys.exit(1)
    
    logger.info("🚀 Запуск Telegram Reposter v2.0")
    logger.info(f"📊 Режим: only_brand_fact={os.getenv('ONLY_BRAND_FACT', '0')}")
    logger.info(f"⚡ force_brand_fact={os.getenv('FORCE_BRAND_FACT', '0')}")
    logger.info(f"🔄 force_full_repost={os.getenv('FORCE_FULL_REPOST', '0')}")
    
    reposter = TelegramReposter()
    asyncio.run(reposter.run())
