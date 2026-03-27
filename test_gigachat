# test_gigachat.py
import os
import sys
import base64
import uuid
import requests

# Отключаем предупреждения о сертификатах
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

def get_token(api_key: str) -> str:
    """Получает токен доступа"""
    decoded = base64.b64decode(api_key.strip()).decode('utf-8')
    client_id, client_secret = decoded.split(':', 1)
    basic_auth = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    
    response = requests.post(
        "https://ngw.devices.sberbank.ru:9443/api/v2/oauth",
        headers={
            "Authorization": f"Basic {basic_auth}",
            "Content-Type": "application/x-www-form-urlencoded",
            "RqUID": str(uuid.uuid4()),
            "Accept": "application/json"
        },
        data={"scope": "GIGACHAT_API_PERS"},
        verify=False,
        timeout=30
    )
    
    if response.status_code != 200:
        raise Exception(f"OAuth error {response.status_code}: {response.text}")
    
    data = response.json()
    return data["access_token"]

def chat_request(token: str, model: str, prompt: str) -> str:
    """Делает простой запрос к модели"""
    response = requests.post(
        "https://gigachat.devices.sberbank.ru/api/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        },
        json={
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.3,
            "max_tokens": 50,
            "stream": False
        },
        verify=False,
        timeout=60
    )
    
    if response.status_code != 200:
        raise Exception(f"API error {response.status_code}: {response.text}")
    
    return response.json()["choices"][0]["message"]["content"].strip()

def main():
    api_key = os.getenv("GIGACHAT_API_KEY")
    if not api_key:
        print("❌ Ошибка: переменная GIGACHAT_API_KEY не задана")
        sys.exit(1)
    
    print("🔄 Получаю токен...")
    try:
        token = get_token(api_key)
        print("✅ Токен получен")
    except Exception as e:
        print(f"❌ Ошибка получения токена: {e}")
        sys.exit(1)
    
    # Тестируем доступные модели
    models_to_test = ["GigaChat-Pro", "GigaChat-Max", "GigaChat-Lite"]
    test_prompt = "Ответь одним словом: какой сегодня день?"
    
    print(f"\n🔄 Тестирую модели (промпт: '{test_prompt}')...\n")
    
    for model in models_to_test:
        try:
            print(f"📡 {model}... ", end="", flush=True)
            response = chat_request(token, model, test_prompt)
            print(f"✅ ОК → '{response}'")
        except Exception as e:
            print(f"❌ FAIL → {str(e)[:60]}")
    
    print("\n✨ Тест завершён")

if __name__ == "__main__":
    main()
