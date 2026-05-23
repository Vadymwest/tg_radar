# Список Telegram-чатов для мониторинга (юзернеймы или ID)
# Для теста можешь добавить сюда чаты, где часто просят помощь, или свои тестовые группы
# TARGET_CHATS = ["@test_radar_2026"]  # обязательно с @
# или числовой ID (надёжнее):
TARGET_CHATS = [-1003965469818]  # -100 + raw channel id

# Ключевые слова на английском для первичного быстрого фильтра скрипта
KEYWORDS = [
    # Автоматизация и парсинг
    "script", "bot", "scraper", "scraping", "automation", "playwright", "selenium", "puppeteer",
    # Помощь и ошибки
    "error", "help with", "need developer", "hire", "fix bug", "bypass", "cloudflare", "captcha",
    # Сервера и крипта
    "vps", "server", "deploy", "proxies", "proxy", "api integration", "bybit api", "crypto bot"
]

# ID чата или твой собственный юзернейм/ID, куда бот будет слать горячие лиды
# Значение "me" означает, что бот будет отправлять уведомления в твои "Избранные сообщения" (Saved Messages)
NOTIFICATION_TARGET = "me"