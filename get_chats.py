import asyncio
from pyrogram import Client

# Подключаемся к нашей рабочей сессии
app = Client("radar_session")


async def main():
    async with app:
        print("📥 Выкачиваем последние чаты с сервера...")
        # Проходимся по 15 последним перепискам аккаунта
        async for dialog in app.get_dialogs(limit=15):
            name = dialog.chat.title or dialog.chat.first_name or "Без имени"
            print(f"Чат: {name} | ID: {dialog.chat.id}")


app.run(main())
