import asyncio, os

from dotenv import load_dotenv
from pyrogram import Client

load_dotenv()


async def main():
    async with Client(
        name="tmp_saver",
        api_id=int(os.environ["API_ID"]),
        api_hash=os.environ["API_HASH"],
    ) as app:
        s = await app.export_session_string()
        print(f"\nSESSION_STRING={s}\n")


asyncio.run(main())
