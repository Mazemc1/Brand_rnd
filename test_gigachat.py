import os
import base64
import json
import uuid
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

API_KEY = os.getenv('GIGACHAT_API_KEY')
if not API_KEY:
    raise Exception("❌ GIGACHAT_API_KEY не задан")

def get_token():
    decoded = base64.b64decode(API_KEY.strip()).decode("utf-8")
    client_id, client_secret = decoded.split(":", 1)
    credentials = f"{client_id}:{client_secret}"
    basic_token = base64.b64encode(credentials.encode("ascii")).decode("ascii")

    url = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"
    body = b"scope=GIGACHAT_API_PERS"
    headers = {
        "Authorization": f"Basic {basic_token}",
        "Content-Type": "application/x-www-form-urlencoded",
        "RqUID": str(uuid.uuid4()),
        "Accept": "application/json"
    }

    http = urllib3.PoolManager(cert_reqs="CERT_NONE")
    resp = http.request("POST", url, body=body, headers=headers)
    if resp.status == 200:
        data = json.loads(resp.data)
        print("✅ Токен получен")
        return data["access_token"]
    else:
        raise Exception(f"❌ Ошибка токена: {resp.status} {resp.data.decode()}")

def call_gigachat(token, text):
    url = "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    body = {
        "model": "GigaChat-2-Max",
        "messages": [
            {"role": "system", "content": "Ты — помощник по созданию хештегов для товаров в Telegram-канале."},
            {"role": "user", "content": f'''Ты — помощник по созданию хештегов для товаров в Telegram-канале.
Получив описание товара, ты должен сгенерировать краткий набор релевантных хештегов на русском языке.
Формат: только хештеги через пробел, начиная с решётки.
Обязательно включи:
- Название бренда (если в тексте "Tommy Hilfiger", то #tommyhilfiger).
- Категорию товара (если в тексте "сумка" или "рюкзак", то #сумка).
- Статус товара (#в_наличии, если в тексте "В НАЛИЧИИ").
- Не добавляй пояснения, только хештеги.
- Не используй хештеги про видео, YouTube, личные истории.

Текст описания товара:
{text}'''}
        ],
        "temperature": 0.3,
        "max_tokens": 60
    }

    http = urllib3.PoolManager(cert_reqs="CERT_NONE")
    resp = http.request("POST", url, body=json.dumps(body).encode(), headers=headers)
    if resp.status == 200:
        result = json.loads(resp.data)
        raw = result["choices"][0]["message"]["content"].strip()
        print(f"🔍 Сырой ответ от GigaChat:\n{raw}")
        hashtags = " ".join([tag for tag in raw.split() if tag.startswith('#')])
        print(f"✅ Хештеги: {hashtags or '#товар'}")
        return hashtags or "#товар"
    else:
        err = resp.data.decode()
        print(f"❌ Ошибка GigaChat: {resp.status} {err}")
        return "#товар"

# --- Тест с реальным текстом из вашего канала ---
test_text = "VALENTINO BAGS\nСумка женская, красная, кожа, цена 16400₽"
print(f"Тестируем текст: {test_text}")
token = get_token()
hashtags = call_gigachat(token, test_text)
print(f"\n🏁 Итоговый результат: {hashtags}")
