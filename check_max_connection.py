#!/usr/bin/env python3
"""
Диагностика подключения к api.max.ru
Запуск: python check_max_connection.py
"""

import socket
import ssl
import requests
import time
import sys
from urllib.parse import urljoin

# Настройки
HOST = "api.max.ru"
PORT = 443
TIMEOUT = 10
TEST_URL = "https://api.max.ru/"  # Базовый эндпоинт для теста

# Цвета для вывода (работают в большинстве терминалов)
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'

def print_status(status: str, message: str):
    color = {"OK": Colors.GREEN, "FAIL": Colors.RED, "WARN": Colors.YELLOW}.get(status, Colors.RESET)
    print(f"{color}[{status}]{Colors.RESET} {message}")

def check_dns():
    """Проверка DNS-разрешения"""
    try:
        ip = socket.gethostbyname(HOST)
        print_status("OK", f"DNS: {HOST} → {ip}")
        return True
    except socket.gaierror as e:
        print_status("FAIL", f"DNS: не удалось разрешить {HOST} — {e}")
        return False

def check_tcp_connection():
    """Проверка TCP-соединения на порт 443"""
    try:
        sock = socket.create_connection((HOST, PORT), timeout=TIMEOUT)
        sock.close()
        print_status("OK", f"TCP: порт {PORT} открыт")
        return True
    except socket.timeout:
        print_status("FAIL", f"TCP: таймаут подключения к {HOST}:{PORT}")        return False
    except ConnectionRefusedError:
        print_status("FAIL", f"TCP: соединение отклонено {HOST}:{PORT}")
        return False
    except Exception as e:
        print_status("FAIL", f"TCP: ошибка — {e}")
        return False

def check_ssl():
    """Проверка SSL-сертификата"""
    try:
        context = ssl.create_default_context()
        with socket.create_connection((HOST, PORT), timeout=TIMEOUT) as sock:
            with context.wrap_socket(sock, server_hostname=HOST) as ssock:
                cert = ssock.getpeercert()
                subject = dict(x[0] for x in cert['subject'])
                print_status("OK", f"SSL: сертификат валиден, выдан для: {subject.get('commonName', 'N/A')}")
                return True
    except ssl.SSLError as e:
        print_status("FAIL", f"SSL: ошибка сертификата — {e}")
        return False
    except Exception as e:
        print_status("FAIL", f"SSL: ошибка — {e}")
        return False

def check_https_request():
    """Проверка базового HTTPS-запроса"""
    try:
        start = time.time()
        response = requests.get(TEST_URL, timeout=TIMEOUT, headers={"User-Agent": "MAX-HealthCheck/1.0"})
        elapsed = round((time.time() - start) * 1000)
        
        if response.status_code == 200:
            print_status("OK", f"HTTPS: {TEST_URL} → {response.status_code} ({elapsed} мс)")
            return True
        elif response.status_code in [401, 403]:
            print_status("WARN", f"HTTPS: {TEST_URL} → {response.status_code} (требуется авторизация, но сервер отвечает)")
            return True  # Сервер жив, просто нужен токен
        else:
            print_status("WARN", f"HTTPS: {TEST_URL} → {response.status_code}")
            return True
    except requests.exceptions.Timeout:
        print_status("FAIL", f"HTTPS: таймаут запроса к {TEST_URL}")
        return False
    except requests.exceptions.ConnectionError as e:
        print_status("FAIL", f"HTTPS: ошибка соединения — {e}")
        return False
    except requests.exceptions.SSLError as e:
        print_status("FAIL", f"HTTPS: SSL-ошибка — {e}")
        return False    except Exception as e:
        print_status("FAIL", f"HTTPS: непредвиденная ошибка — {e}")
        return False

def check_from_github_actions():
    """Подсказка для запуска в GitHub Actions"""
    print(f"\n{Colors.BLUE}💡 Запуск из GitHub Actions?{Colors.RESET}")
    print("Добавьте этот шаг в ваш workflow для теста:")
    print("""
    - name: Test MAX.ru connectivity
      run: |
        python -m pip install requests
        python check_max_connection.py
    """)

def main():
    print(f"{Colors.BLUE}🔍 Диагностика подключения к {HOST}{Colors.RESET}\n")
    
    results = []
    results.append(("DNS", check_dns()))
    results.append(("TCP", check_tcp_connection()))
    results.append(("SSL", check_ssl()))
    results.append(("HTTPS", check_https_request()))
    
    # Итог
    print(f"\n{Colors.BLUE}📊 Итог:{Colors.RESET}")
    all_ok = all(r[1] for r in results)
    for name, ok in results:
        status = "✅" if ok else "❌"
        print(f"  {status} {name}")
    
    if all_ok:
        print(f"\n{Colors.GREEN}✓ api.max.ru доступен из текущей среды{Colors.RESET}")
        return 0
    else:
        print(f"\n{Colors.RED}✗ Есть проблемы с подключением{Colors.RESET}")
        print(f"{Colors.YELLOW}Возможные причины:{Colors.RESET}")
        print("  • API недоступен из облачных сетей (ограничение по IP)")
        print("  • Требуется белый список IP для GitHub Actions")
        print("  • API временно не работает")
        check_from_github_actions()
        return 1

if __name__ == "__main__":
    sys.exit(main())
