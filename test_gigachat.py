# test_gigachat.py — обновлённая версия с проверкой баланса и доступных моделей
import os
import sys
import base64
import uuid
import requests

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

def get_available_models(token: str) -> list:
    """Получает список доступных моделей"""
    response = requests.get(
        "https://gigachat.devices.sberbank.ru/api/v1/models",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        },
        verify=False,
        timeout=30
    )
    
    if response.status_code == 200:
        models = response.json().get("data", [])
        return [m["id"].split(":")[0] for m in models]  # Возвращаем имена без версий
    return []

def check_balance(token: str) -> dict:
    """Проверяет остаток токенов (если доступно)"""
    response = requests.get(
        "https://gigachat.devices.sberbank.ru/api/v1/balance",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        },
        verify=False,
        timeout=30
    )
    
    if response.status_code == 200:
        return response.json().get("balance", [])
    elif response.status_code == 403:
        return {"error": "pay-as-you-go"}  # Схема оплаты без пакетов
    return {}

def chat_request(token: str, model: str, prompt: str) -> str:
    """Делает запрос к модели"""
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
    
    if response.status_code == 402:
        raise Exception("💰 Payment Required — закончились токены или нужна подписка")
    elif response.status_code == 404:
        raise Exception("🔍 Model not found")
    elif response.status_code != 200:
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
    
    # Проверяем баланс
    print("\n💳 Проверка баланса...")
    balance = check_balance(token)
    if isinstance(balance, list) and balance:
        for item in balance:
            print(f"   • {item.get('usage', 'N/A')}: {item.get('value', 'N/A')} токенов")
    elif balance.get("error") == "pay-as-you-go":
        print("   ⚠️ Схема оплаты: pay-as-you-go (баланс не отображается)")
    else:
        print("   ⚠️ Не удалось получить информацию о балансе")
    
    # Получаем доступные модели
    print("\n📋 Доступные модели:")
    available = get_available_models(token)
    if available:
        for m in sorted(set(available)):
            print(f"   ✅ {m}")
    else:
        print("   ⚠️ Не удалось получить список моделей")
    
    # Тестируем модели в приоритетном порядке
    test_prompt = "Ответь одним словом: какой сегодня день?"
    models_to_test = ["GigaChat", "GigaChat-Pro", "GigaChat-Max", "GigaChat-Lite"]
    
    print(f"\n🔄 Тест генерации (промпт: '{test_prompt}')...\n")
    
    working_models = []
    
    for model in models_to_test:
        if model not in available and available:
            print(f"⏭️ {model}: не в списке доступных — пропускаю")
            continue
            
        try:
            print(f"📡 {model}... ", end="", flush=True)
            response = chat_request(token, model, test_prompt)
            print(f"✅ ОК → '{response}'")
            working_models.append(model)
        except Exception as e:
            error = str(e)
            if "Payment Required" in error:
                print(f"❌ 💰 НЕТ ТОКЕНОВ (402)")
            elif "not found" in error.lower():
                print(f"❌ 🔍 Не найдена (404)")
            else:
                print(f"❌ {error[:50]}")
    
    # Итог
    print(f"\n{'='*50}")
    if working_models:
        print(f"🎯 РАБОЧИЕ МОДЕЛИ: {', '.join(working_models)}")
        print(f"\n📝 В основном скрипте укажите:")
        print(f"   MODEL_HASHTAGS = '{working_models[0]}'")
        print(f"   MODEL_FACTS = '{working_models[0]}'")
    else:
        print("❌ Ни одна модель не работает")
        print("\n🔧 Возможные решения:")
        print("   1. Пополните баланс в личном кабинете: https://developers.sber.ru/")
        print("   2. Проверьте, что в проекте включён доступ к GigaChat API")
        print("   3. Убедитесь, что scope = GIGACHAT_API_PERS (для физлиц)")
    
    print(f"{'='*50}\n✨ Тест завершён")

if __name__ == "__main__":
    main()
