"""
radar/main.py — Userbot-радар на Pyrogram v2
Архитектура: активный polling (get_chat_history) вместо пассивных хендлеров.
Решает проблему с замьюченными форумами 100к+ участников, которым Telegram
не пушит WebSocket-апдейты на пассивные сессии.
"""

import asyncio
import json
import logging
import os
import random
import re
from pathlib import Path

from dotenv import load_dotenv
from openai import AsyncOpenAI
from pyrogram import Client, enums
from pyrogram.errors import FloodWait, ChannelPrivate, ChatForbidden
from pyrogram.types import Message

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("radar")

# ---------------------------------------------------------------------------
# Env
# ---------------------------------------------------------------------------
load_dotenv()

API_ID:               int = int(os.environ["API_ID"])
API_HASH:             str = os.environ["API_HASH"]
PROXY_IP:             str = os.environ["PROXY_IP"]
PROXY_PORT:           int = int(os.environ["PROXY_PORT"])
PROXY_USER:           str = os.environ["PROXY_USER"]
PROXY_PASS:           str = os.environ["PROXY_PASS"]
OPENAI_API_KEY:       str = os.environ["OPENAI_API_KEY"]
NOTIFICATION_CHAT_ID: int = int(os.environ["NOTIFICATION_CHAT_ID"])

# ---------------------------------------------------------------------------
# Настройки поллинга
# ---------------------------------------------------------------------------
POLL_INTERVAL_SEC     = 30    # пауза между полными обходами всех чатов
CHAT_REQUEST_DELAY    = 0.3   # пауза между запросами к отдельным чатам
PAGE_SIZE             = 50    # размер одной страницы get_chat_history

# ИЗМЕНЕНИЕ 1: Поднят до 500 — хватит вытянуть глубокую историю за 12-15 мин
# отсутствия на 500-чатовом аккаунте, не упираясь в аварийный лимит.
MAX_MESSAGES_PER_CHAT = 500

# ---------------------------------------------------------------------------
# Долгосрочная память: блэклист обработанных пользователей
#
# Архитектура персистентности:
#   1. _load_processed_users() — строгая загрузка при старте.
#      Проверяет существование файла И ненулевой размер, чтобы не
#      споткнуться об пустой файл после краша. Все ID из файла
#      добавляются к сету через .update() — старые записи не стираются.
#
#   2. _mark_user_processed() — добавляет ID в сет в памяти + взводит
#      флаг BLACKLIST_UPDATED. Диск не трогает — нет I/O в горячем пути.
#
#   3. _flush_blacklist_if_needed() — вызывается ОДИН РАЗ в конце каждого
#      цикла поллинга. Если флаг взведён — сохраняет весь сет на диск
#      атомарно (через временный файл + rename), сбрасывает флаг.
#      Атомарность гарантирует: краш во время записи не испортит файл.
# ---------------------------------------------------------------------------
PROCESSED_USERS_FILE = Path("processed_users.json")
PROCESSED_USERS: set[int] = set()
BLACKLIST_UPDATED: bool = False          # флаг: были ли новые записи за цикл


def _load_processed_users() -> None:
    """
    Загружает блэклист при старте. Два защитных барьера:
      - файл должен существовать
      - файл должен быть ненулевого размера (защита от пустого файла после краша)
    """
    if not PROCESSED_USERS_FILE.exists() or PROCESSED_USERS_FILE.stat().st_size == 0:
        logger.info(
            "processed_users.json %s — начинаем с чистого листа.",
            "не найден" if not PROCESSED_USERS_FILE.exists() else "пустой",
        )
        return
    try:
        data = json.loads(PROCESSED_USERS_FILE.read_text(encoding="utf-8"))
        PROCESSED_USERS.update(int(uid) for uid in data)
        logger.info(
            "✅ Блэклист загружен: %d пользователей из %s",
            len(PROCESSED_USERS), PROCESSED_USERS_FILE,
        )
    except Exception as exc:
        logger.warning("Не удалось прочитать processed_users.json: %s", exc)


def _mark_user_processed(user_id: int) -> None:
    """
    Добавляет user_id в сет в памяти и взводит флаг записи на диск.
    Если ID уже есть — set-семантика игнорирует дубликат, старые записи
    не пострадают. Диск не трогаем здесь — запись батчем в конце цикла.
    """
    global BLACKLIST_UPDATED
    PROCESSED_USERS.add(user_id)
    BLACKLIST_UPDATED = True


def _flush_blacklist_if_needed() -> None:
    """
    Сохраняет весь блэклист на диск, если за текущий цикл были новые записи.
    Вызывается один раз в конце каждого цикла поллинга — не нагружает диск.

    Атомарная запись через временный файл + os.replace():
      - Пишем во временный файл рядом с целевым.
      - os.replace() делает атомарный rename на уровне ОС.
      - Краш во время записи оставит старый файл нетронутым.
    """
    global BLACKLIST_UPDATED
    if not BLACKLIST_UPDATED:
        return

    tmp_path = PROCESSED_USERS_FILE.with_suffix(".tmp")
    try:
        tmp_path.write_text(
            json.dumps(sorted(PROCESSED_USERS), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        os.replace(tmp_path, PROCESSED_USERS_FILE)
        BLACKLIST_UPDATED = False
        logger.debug("💾 Блэклист сохранён: %d записей.", len(PROCESSED_USERS))
    except Exception as exc:
        logger.warning("Не удалось сохранить processed_users.json: %s", exc)
        # tmp-файл мог остаться — чистим, чтобы не мусорить
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Семантическое ядро
# ---------------------------------------------------------------------------

_TRAFFIC = [
    'фарминг', 'фарм', 'авторегер', 'регер', 'прогрев', 'прогреватор', 'гугл адс', 'google ads', 'fb ads',
    'facebook ads', 'tiktok ads', 'мультиакк', 'мультиаккаунт', 'мультиакаунт', 'зрд', 'логи', 'куки',
    'cookies', 'спенд', 'первобил', 'гембла', 'нутра', 'фармилка', 'прогрів', 'обхід', 'клоака', 'cloaking',
    'keitaro', 'binom', 'трекер', 'бм', 'bm', 'business manager', 'фп', 'fanpage', 'кабы', 'лички',
    'агентские', 'реклама', 'трафик', 'залив', 'запуск рк', 'кампании', 'пиксель', 'pixel', 'креативы',
    'крео', 'сетапы', 'связка', 'прокла', 'лендинг', 'прилы', 'приложения', 'криптоофферы', 'gambling',
    'betting', 'шторм', 'микроспенд', 'биллинг', 'вайты', 'whitepage', 'twitter ads', 'x ads', 'рк',
    'акки', 'фармленные', 'дейли лимит', 'daily limit', 'spend', 'безлимит', 'инсталлы', 'траф', 'лидген',
    'регєр', 'прогрівання', 'трафік', 'кабінети', 'прилки', 'застосунки', 'біллінг', 'облікові записи',
    'аккаунти',
]

_ANTIDETECT = [
    'playwright', 'selenium', 'puppeteer', 'zennoposter', 'zenno', 'bas', 'browser automation studio',
    'multilogin', 'multilogin x', 'dolphin', 'anty', 'adspower', 'gologin', 'incogniton', 'octo', 'окто',
    'антидетект', 'антик', 'индиго', 'сфера', 'прокси', 'proxy', 'мобильные прокси', 'резидентские',
    'proxy-seller', 'iproyal', 'decodo', 'cloudflare', 'капча', 'captcha', 'recaptcha', 'turnstile',
    'эмулятор', 'bluestacks', 'memu', 'nox', 'проксі', 'мобільні проксі', 'клаудфлаєр', 'bstweaker',
    'ldplayer', 'genymotion', 'android emulator', 'ios emulator', 'fingerprint', 'отпечатток', 'подмена',
    'canvas', 'webgl', 'webrtc', 'useragent', 'юзерагент', 'кукисы', 'локальные прокси', 'ipv4', 'ipv6',
    'socks', 'socks5', 'http proxy', 'https proxy', 'гео прокси', 'ротация', 'сменные айпи', 'ip',
    'менять ip', 'linken sphere', 'ghostbrowser', 'loginways', 'undici', 'tls', 'ssl', 'curl-impersonate',
    'cloudflare bypass', 'akamai', 'datadome', 'kaspersky', 'imperva', 'sucuri', 'perimeterx', 'incapsula',
    'hcaptcha', 'funcaptcha', 'geetest', 'разгадать', 'антикапча', 'anticaptcha', '2captcha', 'rucaptcha',
    'capmonster', 'капмонстр', 'відбиток', 'підміна', 'емулятор',
]

_CRYPTO = [
    'снайпер', 'sniper bot', 'p2p', 'п2п', 'trading bot', 'торговый бот', 'торговий бот', 'биржа', 'bybit',
    'gate.io', 'bingx', 'bitget', 'mexc', 'digifinex', 'binance', 'okx', 'metascalp', 'скальпинг',
    'арбитраж крипты', 'арбітраж', 'связки', "зв'язки", 'спред', 'межбиржа', 'dex', 'cex', 'web3',
    'смарт-контракт', 'ретродроп', 'абуз', 'solana', 'ethereum', 'токен', 'монета', 'листинг', 'пресейл',
    'лаунчпад', 'launchpad', 'фьючерсы', 'спот', 'стакан', 'ордера', 'лимитки', 'маркет ордер',
    'тейк профит', 'стоп лосс', 'маржиналка', 'кредитное плечо', 'api key', 'апи ключ', 'secret key',
    'сигналы', 'kucoin', 'huobi', 'htx', 'gate', 'гейт', 'байбит', 'бинанс', 'мекс', 'битгет', 'бингх',
    'метаскальп', 'cscalp', 'сискальп', 'tiger trade', 'алготрейдинг', 'робот', 'скринер', 'screener',
    'волатильность', 'ликвидность', 'пул', 'пулы', 'uniswap', 'pancakeswap', 'raydium', 'jup', 'jupiter',
    'phantom', 'metamask', 'кошелек', 'сид фраза', 'приватник', 'транзакция', 'gas', 'газ', 'нода', 'node',
    'валидатор', 'тестнет', 'майннет', 'дефи', 'defi', 'nft', 'нфт', 'стейкинг', 'лістинг', "ф'ючерси",
    'ордери', 'гаманець', 'транзакція',
]

_DEV = [
    'python', 'питон', 'пайтон', 'api', 'апи', 'апі', 'бэкенд', 'backend', 'fastapi', 'база данных',
    'база даних', 'sql', 'telegram bot', 'тг бот', 'телеграм бот', 'aiogram', 'pyrogram', 'спаммер',
    'инвайтер', 'рассыльщик', 'розсильник', 'парсер тг', 'граббер', 'chatgpt', 'openai', 'claude', 'llm',
    'нейронка', 'интеграция', 'інтеграція', 'django', 'flask', 'asyncio', 'асинхронный', 'threading',
    'multiprocessing', 'парсинг сайтов', 'веб-скрапинг', 'scraping', 'скрапить', 'спарсить', 'выкачать',
    'собрать данные', 'автоматизировать', 'автоматизация', 'nodejs', 'javascript', 'typescript', 'js', 'ts',
    'cheerio', 'axios', 'requests', 'beautifulsoup', 'bs4', 'postgresql', 'mysql', 'sqlite', 'mongodb',
    'redis', 'docker', 'докер', 'docker-compose', 'git', 'github', 'gitlab', 'cicd', 'vps', 'vultr', 'vds',
    'сервер', 'хостинг', 'деплой', 'linux', 'ubuntu', 'bash', 'логирование', 'логгер', 'вебхук', 'webhook',
    'websocket', 'сокеты', 'telethon', 'телетон', 'aiogram3', 'телеграм-бот', 'юзербот', 'userbot',
    'кликер', 'автокликер', 'нейросеть', 'gpt4', 'gpt4o', 'deepseek', 'дипсик', 'langchain', 'rag',
    'промпты', 'асинхронний', 'розробка', 'програмування', 'парсинг сайтів', 'налаштування сервера',
    'вебхуки', 'нейромережа',
]

_INTENT = [
    'ищу', 'нужен', 'требуется', 'ищем', 'разработчик', 'кодер', 'прогер', 'программист', 'кто напишет',
    'кто сделает', 'создать', 'разработать', 'заказать', 'плачу', 'оплата', 'бюджет', 'тз', 'задача',
    'проект', 'срочно', 'заказ', 'баг', 'ошибка', 'не работает', 'починить', 'доработать', 'фикс',
    'шукаю', 'потрібен', 'потрібно', 'треба', 'шукаємо', 'розробник', 'хто напише', 'хто зробить',
    'створити', 'розробити', 'замовити', 'вартість', 'ціна', 'помилка', 'не працює', 'полагодити',
    'виправити', 'терміново', 'замовлення', 'допоможіть', 'допомога', 'hire', 'looking for', 'pay',
    'task', 'developer', 'engineer', 'error', 'bug', 'freelance', 'project', 'remote', 'urgent',
    'need help', 'хелп', 'help', 'спасайте', 'поднять', 'затык', 'крашится', 'падает', 'отваливается',
    'горят сроки', 'вакансия', 'работа', 'подработка', 'фриланс', 'фрилансер', 'заказчик', 'нужен человек',
    'ищу специалиста', 'ищу мастера', 'нужен профи', 'исполнитель', 'подрядчик', 'аутсорс', 'аутсорсинг',
    'в команду', 'долгосрок', 'сдельно', 'оплата по результату', 'плачу в usdt', 'плачу криптой', 'гарант',
    'через гаранта', 'готов платить', 'сколько стоит', 'какая цена', 'оцените задачу', 'напишите софт',
    'написать программу', 'сделать расширение', 'сделать плагин', 'написать бота', 'написать парсер',
    'сделать сайт', 'дописать код', 'исправить баг', 'решить проблему', 'не открывается', 'выдает ошибку',
    'сломался', 'упал', 'лег сервер', 'забанили', 'выдает капчу', 'блокирует', 'ошибка авторизации',
    'не могу войти', 'слетает', 'падает скрипт', 'вылетает', 'краш', 'crash', 'error log', 'не парсит',
    'не собирает', 'не отправляет', 'не работает кнопка', 'переделать', 'ускорить', 'масштабировать',
    'настроить под ключ', 'техзадание', 'техническое задание', 'бриф', 'нужен софт', 'куплю софт',
    'куплю скрипт', 'ищу софт', 'ищу скрипт', 'отпишите в лс', 'пишите в личку', 'жду в лс', 'лс', 'pm',
    'contact me', 'hiring', 'job post', 'open for work', 'budget allocation', 'custom script', 'шукаю кодера',
    'потрібен програміст', 'хто може зробити', 'розробити бота', 'написати скрипт', 'заплачу',
    'оплата в юсдт', 'техзавдання', 'помилка в коді', 'зламався', 'впав', 'термінова задача',
    'горить дедлайн', 'пишіть в пп', 'чекаю в лс', 'приватні повідомлення',
]

KEYWORDS: set[str] = {
    kw.lower()
    for kw in (_TRAFFIC + _ANTIDETECT + _CRYPTO + _DEV + _INTENT)
}

# ---------------------------------------------------------------------------
# Regex-матчинг ключевых слов
#
# Короткие и омонимичные слова (≤3 символа или встречаются внутри других слов)
# проверяем через lookaround — они не должны быть частью более длинного слова.
# Остальные — простой поиск подстроки (быстрее, достаточно для длинных слов).
# ---------------------------------------------------------------------------

_BOUNDARY_WORDS: set[str] = {
    # Латинские аббревиатуры
    'ip', 'bm', 'js', 'ts', 'pm', 'p2p', 'api', 'sql', 'rag',
    'gas', 'nft', 'dex', 'cex', 'vds', 'vps', 'tls', 'ssl',
    # Кириллические короткие
    'бм', 'рк', 'тз', 'лс', 'апи', 'апі',
}

_SUBSTRING_KEYWORDS: set[str] = KEYWORDS - _BOUNDARY_WORDS

# Один скомпилированный паттерн для всех boundary-слов.
# Lookaround вместо \b — корректно работает на границе кириллица/латиница.
_BOUNDARY_RE = re.compile(
    r'(?<![а-яёА-ЯЁіїєґА-ЯІЇЄҐa-zA-Z0-9_])('
    + '|'.join(re.escape(w) for w in sorted(_BOUNDARY_WORDS, key=len, reverse=True))
    + r')(?![а-яёА-ЯЁіїєґА-ЯІЇЄҐa-zA-Z0-9_])',
    re.IGNORECASE | re.UNICODE,
)


def _find_keywords(text: str) -> list[str]:
    """
    Возвращает список найденных ключевых слов.
    - Длинные слова: простой поиск подстроки — O(n), нет ложных срабатываний.
    - Короткие/омонимичные: regex с lookaround — 'апи' не найдёт в 'напиши',
      'bm' не найдёт в 'bmw', 'ip' не найдёт в 'типичный'.
    """
    text_lower = text.lower()
    found: list[str] = []
    found.extend(kw for kw in _SUBSTRING_KEYWORDS if kw in text_lower)
    found.extend(m.group(0).lower() for m in _BOUNDARY_RE.finditer(text_lower))
    return found

# ---------------------------------------------------------------------------
# OpenAI
# ---------------------------------------------------------------------------
openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)

SYSTEM_PROMPT = """
Ты — опытный русскоязычный разработчик и арбитражник, который мониторит Telegram-чаты в поисках реальных IT-заказов.

Тебе приходит сообщение из Telegram-группы. Твоя задача:
1. Определить, является ли это РЕАЛЬНЫМ ЛИДОМ — человек с конкретной задачей, болью или бюджетом, которому нужна разработка, автоматизация, настройка инструментов или консультация.
2. НЕ считать лидом: флуд, мемы, общую болтовню, новости, споры, рекламу чужих услуг, вопросы без коммерческого интента.

ПРАВИЛО ЯЗЫКА для draft_reply:
- Определи язык оригинального сообщения (русский / украинский / английский).
- Напиши черновик СТРОГО на том же языке, что и оригинал.
- Стиль: уверенный технический специалист, лаконично, сразу к делу.
- Пример (RU): "Привет, увидел твою задачу с [боль] — решаемо, делал подобное. Давай обсудим детали."
- Пример (UA): "Привіт, бачив твою задачу з [біль] — вирішувано, робив подібне. Давай обговоримо деталі."
- Пример (EN): "Hey, saw your issue with [pain] — done similar stuff before. Let's discuss the details."

Верни СТРОГО валидный JSON без markdown-блоков, без пояснений, только JSON:

{
  "is_lead": true или false,
  "problem_summary": "Краткая суть задачи на русском (1-2 предложения)",
  "perspective": "Оценка коммерческого потенциала и стоит ли писать в личку (на русском)",
  "draft_reply": "Готовое сообщение клиенту строго на языке оригинала"
}
""".strip()


async def analyze_message(text: str) -> dict:
    response = await openai_client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0.2,
        max_tokens=600,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": text},
        ],
    )
    raw_text = response.choices[0].message.content or ""
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        logger.warning("OpenAI вернул невалидный JSON: %s", raw_text[:200])
        return {"is_lead": False}


# ---------------------------------------------------------------------------
# Pyrogram Client
# ---------------------------------------------------------------------------
app = Client(
    "radar_session",
    api_id=2040,
    api_hash="b18441a1ff607e10a989891a5462e627",
    device_model="Desktop",
    system_version="Windows 10",
    app_version="4.8.1 x64",
    proxy={
        "scheme": "socks5",
        "hostname": PROXY_IP,
        "port": PROXY_PORT,
        "username": PROXY_USER,
        "password": PROXY_PASS,
    },
)

# ---------------------------------------------------------------------------
# Хелперы форматирования
# ---------------------------------------------------------------------------

def _build_message_link(message: Message) -> str | None:
    if not message.chat:
        return None
    is_forum = getattr(message.chat, "is_forum", False)
    topic_id = None
    if is_forum and getattr(message, "topic_top_message_id", None):
        topic_id = message.topic_top_message_id

    chat_id_str   = str(message.chat.id)
    clean_chat_id = chat_id_str[4:] if chat_id_str.startswith("-100") else chat_id_str.replace("-", "")

    if message.chat.username:
        base = f"https://t.me/{message.chat.username}"
        return f"{base}/{topic_id}/{message.id}" if topic_id else f"{base}/{message.id}"
    else:
        base = f"https://t.me/c/{clean_chat_id}"
        return f"{base}/{topic_id}/{message.id}" if topic_id else f"{base}/{message.id}"


def _user_link(message: Message) -> str:
    user = message.from_user
    if user is None:
        return "Аноним / канал"
    name_parts = [user.first_name or "", user.last_name or ""]
    name = " ".join(p for p in name_parts if p).strip() or str(user.id)
    if user.username:
        return f'<a href="https://t.me/{user.username}">{name}</a>'
    return name


def _format_notification(message: Message, data: dict) -> str:
    msg_link = _build_message_link(message)
    user     = message.from_user

    if msg_link:
        action_line = f'👉 <a href="{msg_link}">Перейти к сообщению</a>'
    elif user and user.username:
        action_line = f'👉 <a href="https://t.me/{user.username}">Написать клиенту</a>'
    else:
        action_line = "👉 Ссылка недоступна (приватный чат)"

    chat      = message.chat
    chat_name = chat.title if chat else str(message.chat.id)

    return (
        "🔥 <b>Новый лид!</b>\n\n"
        f"💬 <b>Чат:</b> {chat_name}\n"
        f"👤 <b>Пользователь:</b> {_user_link(message)}\n\n"
        f"📝 <b>Суть:</b>\n{data.get('problem_summary', '—')}\n\n"
        f"🎯 <b>Перспектива:</b>\n{data.get('perspective', '—')}\n\n"
        f"✉️ <b>Черновик</b> (тап → буфер):\n"
        f"<code>{data.get('draft_reply', '—')}</code>\n\n"
        f"{action_line}"
    )


# ---------------------------------------------------------------------------
# Flood guard
# ---------------------------------------------------------------------------

async def _send_notification(text: str) -> None:
    for attempt in range(2):
        try:
            await app.send_message(
                NOTIFICATION_CHAT_ID,
                text=text,
                parse_mode=enums.ParseMode.HTML,
                disable_web_page_preview=True,
            )
            return
        except FloodWait as exc:
            if attempt == 0:
                logger.warning("FloodWait при отправке: ждём %ss", exc.value)
                await asyncio.sleep(exc.value)
            else:
                logger.error("FloodWait повторился — уведомление пропущено.")
                raise


# ---------------------------------------------------------------------------
# ИЗМЕНЕНИЕ 2: фоновая задача для OpenAI + отправки
#
# Весь тяжёлый I/O (запрос к GPT + отправка в Telegram) выполняется в
# отдельном asyncio.Task. polling_loop не ждёт результата и немедленно
# переходит к следующему сообщению. Это устраняет OpenAI-бутылочное горлышко.
# ---------------------------------------------------------------------------

async def _qualify_and_notify(message: Message) -> None:
    """Фоновая задача: OpenAI → уведомление. Вызывается через create_task."""
    chat_name = message.chat.title if message.chat else str(message.chat.id)
    user_id   = message.from_user.id  # from_user уже проверен до create_task

    try:
        data = await analyze_message(message.text or message.caption or "")
    except Exception:
        logger.exception("Ошибка OpenAI для user_id=%s", user_id)
        return

    if not data.get("is_lead"):
        logger.debug("Не лид | чат=%s | user_id=%s", chat_name, user_id)
        return

    logger.info("✅ ЛИД | чат=%s | user_id=%s", chat_name, user_id)

    try:
        await _send_notification(_format_notification(message, data))
    except Exception:
        logger.exception("Ошибка отправки уведомления")


# ---------------------------------------------------------------------------
# Обработка одного сообщения — синхронная часть (без I/O)
# ---------------------------------------------------------------------------

def _process_message(message: Message) -> None:
    """
    Быстрая синхронная часть: фильтры без сетевых вызовов.
    Если сообщение проходит — создаёт фоновую задачу для GPT + уведомления.
    Главный цикл не блокируется.
    """
    if message.from_user is None:
        return

    user_id = message.from_user.id

    if user_id in PROCESSED_USERS:
        return

    text = message.text or message.caption
    if not text:
        return

    matched = _find_keywords(text)
    if not matched:
        return

    chat_name = message.chat.title if message.chat else str(message.chat.id)
    username  = message.from_user.username or message.from_user.first_name
    logger.info("🔑 %s | чат=%s | от=%s (id=%s)", matched[:3], chat_name, username, user_id)

    # Фиксируем в памяти ДО запуска задачи — гарантия no-dup при параллельных тасках.
    # На диск не пишем здесь — батчевая запись в конце каждого цикла поллинга.
    _mark_user_processed(user_id)

    # Запускаем GPT + отправку в фоне — основной цикл не ждёт
    asyncio.create_task(
        _qualify_and_notify(message),
        name=f"qualify-{user_id}-{message.id}",
    )


# ---------------------------------------------------------------------------
# Активный поллинг
# ---------------------------------------------------------------------------

async def _build_chat_list() -> tuple[list[int], dict[int, str]]:
    chat_ids:   list[int]       = []
    chats_meta: dict[int, str]  = {}

    monitored_types = (
        enums.ChatType.GROUP,
        enums.ChatType.SUPERGROUP,
        enums.ChatType.CHANNEL,
    )

    async for dialog in app.get_dialogs():
        chat = dialog.chat
        if chat.type in monitored_types:
            chat_ids.append(chat.id)
            chats_meta[chat.id] = chat.title or str(chat.id)

    return chat_ids, chats_meta


async def _init_last_ids(chat_ids: list[int], chats_meta: dict[int, str]) -> dict[int, int]:
    last_processed_id: dict[int, int] = {}
    logger.info("📍 Инициализация точек отсчёта для %d чатов…", len(chat_ids))

    for chat_id in chat_ids:
        await asyncio.sleep(CHAT_REQUEST_DELAY)
        try:
            async for msg in app.get_chat_history(chat_id, limit=1):
                last_processed_id[chat_id] = msg.id
                break
            else:
                last_processed_id[chat_id] = 0
        except FloodWait as exc:
            logger.warning("FloodWait при init chat_id=%s: ждём %ss", chat_id, exc.value)
            await asyncio.sleep(exc.value)
            last_processed_id[chat_id] = 0
        except (ChannelPrivate, ChatForbidden):
            logger.warning("Нет доступа к chat_id=%s (%s) — пропускаем.", chat_id, chats_meta.get(chat_id))
            last_processed_id[chat_id] = 0
        except Exception as exc:
            logger.warning("Ошибка init chat_id=%s: %s", chat_id, exc)
            last_processed_id[chat_id] = 0

    logger.info("✅ Точки отсчёта установлены для %d чатов.", len(last_processed_id))
    return last_processed_id


async def _fetch_new_messages(chat_id: int, last_id: int) -> list[Message]:
    """
    Листает историю постранично через offset_id, собирая всё новее last_id.
    Аварийный лимит MAX_MESSAGES_PER_CHAT защищает от спам-рейдов.
    """
    collected: list[Message] = []
    offset_id = 0
    hit_limit = False

    while True:
        page: list[Message] = []

        try:
            await asyncio.sleep(CHAT_REQUEST_DELAY)
            async for msg in app.get_chat_history(
                chat_id,
                limit=PAGE_SIZE,
                offset_id=offset_id,
            ):
                if msg.id <= last_id:
                    return collected

                page.append(msg)

                if len(collected) + len(page) >= MAX_MESSAGES_PER_CHAT:
                    collected.extend(page)
                    hit_limit = True
                    break

            if hit_limit:
                break

        except FloodWait as exc:
            logger.warning("FloodWait (pagination) chat_id=%s: ждём %ss", chat_id, exc.value)
            await asyncio.sleep(exc.value)
            continue

        if not page:
            break

        collected.extend(page)

        if len(page) < PAGE_SIZE:
            break

        offset_id = page[-1].id

    return collected


async def polling_loop(chat_ids: list[int], chats_meta: dict[int, str]) -> None:
    last_processed_id = await _init_last_ids(chat_ids, chats_meta)

    cycle = 0
    while True:
        cycle += 1

        # ИЗМЕНЕНИЕ 3: случайный порядок обхода чатов — имитация живого юзера
        random.shuffle(chat_ids)

        new_total = 0
        logger.info("🔄 Цикл #%d | чатов=%d | блэклист=%d", cycle, len(chat_ids), len(PROCESSED_USERS))

        for chat_id in list(chat_ids):
            last_id   = last_processed_id.get(chat_id, 0)
            chat_name = chats_meta.get(chat_id, str(chat_id))

            try:
                batch = await _fetch_new_messages(chat_id, last_id)

                if not batch:
                    continue

                fetched_count = len(batch)
                hit_cap       = fetched_count >= MAX_MESSAGES_PER_CHAT

                if hit_cap:
                    logger.warning(
                        "⚠️  %s: достигнут лимит %d сообщений за цикл.",
                        chat_name, MAX_MESSAGES_PER_CHAT,
                    )

                # Хронологический порядок: старое → новое
                batch.reverse()

                for msg in batch:
                    try:
                        # Синхронная часть — мгновенно, без await
                        _process_message(msg)
                    except Exception:
                        logger.exception(
                            "Ошибка при обработке msg.id=%s в chat_id=%s",
                            msg.id, chat_id,
                        )

                last_processed_id[chat_id] = batch[-1].id
                new_total += fetched_count

                logger.info(
                    "  ✓ %s: +%d новых (last_id=%s%s)",
                    chat_name,
                    fetched_count,
                    batch[-1].id,
                    ", лимит!" if hit_cap else "",
                )

            except (ChannelPrivate, ChatForbidden):
                logger.warning("Потеряли доступ к %s — исключаем.", chat_name)
                chat_ids.remove(chat_id)
            except Exception as exc:
                logger.warning("Ошибка poll %s: %s", chat_name, exc)

        logger.info(
            "✅ Цикл #%d завершён | новых=%d | блэклист=%d | след. через %ss",
            cycle, new_total, len(PROCESSED_USERS), POLL_INTERVAL_SEC,
        )

        # Батчевая запись блэклиста — один раз в конце цикла, не на каждое сообщение
        _flush_blacklist_if_needed()

        await asyncio.sleep(POLL_INTERVAL_SEC)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    _load_processed_users()

    logger.info(
        "Запуск radar (polling) | NOTIFICATION_CHAT_ID=%s | keywords=%d | blacklist=%d",
        NOTIFICATION_CHAT_ID, len(KEYWORDS), len(PROCESSED_USERS),
    )

    await app.start()

    me = await app.get_me()
    logger.info("Авторизован: %s (@%s) id=%s", me.first_name, me.username, me.id)

    try:
        notify_chat = await app.get_chat(NOTIFICATION_CHAT_ID)
        logger.info("🎯 Чат уведомлений: %r", notify_chat.title or str(NOTIFICATION_CHAT_ID))
    except Exception as exc:
        logger.error("❌ Чат уведомлений недоступен: %s", exc)
        await app.stop()
        return

    logger.info("📋 Получаем список чатов через get_dialogs()…")
    chat_ids, chats_meta = await _build_chat_list()
    logger.info("📋 Будем мониторить %d чатов.", len(chat_ids))

    for cid, title in chats_meta.items():
        logger.info("  • %s (id=%s)", title, cid)

    try:
        await polling_loop(chat_ids, chats_meta)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await app.stop()
        logger.info("Остановлен.")


if __name__ == "__main__":
    asyncio.run(main())