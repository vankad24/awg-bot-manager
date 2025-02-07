import db
import aiohttp
import logging
import asyncio
import aiofiles
import os
import re
import tempfile
import json
import subprocess
import sys
import pytz
import zipfile
import ipaddress
import humanize
import shutil
from aiogram import Bot, types
from aiogram.dispatcher import Dispatcher
from aiogram.utils import exceptions as aiogram_exceptions
from aiogram.dispatcher.middlewares import BaseMiddleware
from aiogram.utils import executor
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

config = db.get_config()
bot_token = config.get('bot_token')
admin_id = config.get('admin_id')

if not all([bot_token, admin_id]):
    logger.error("Отсутствуют обязательные настройки бота (bot_token или admin_id).")
    sys.exit(1)

servers = db.load_servers()
if not servers:
    logger.warning("Не найдено ни одного сервера в конфигурации")

bot = Bot(bot_token)
admin = int(admin_id)

current_server = None
WG_CONFIG_FILE = None
DOCKER_CONTAINER = None
ENDPOINT = None

def update_server_settings(server_id=None):
    global current_server, WG_CONFIG_FILE, DOCKER_CONTAINER, ENDPOINT
    if server_id:
        servers = db.load_servers()
        if server_id in servers:
            server_config = servers[server_id]
            WG_CONFIG_FILE = server_config.get('wg_config_file')
            DOCKER_CONTAINER = server_config.get('docker_container')
            ENDPOINT = server_config.get('endpoint')
            current_server = server_id
            logger.info(f"Настройки сервера {server_id} обновлены")
            return True
        else:
            logger.error(f"Сервер {server_id} не найден")
            return False
    else:
        WG_CONFIG_FILE = None
        DOCKER_CONTAINER = None
        ENDPOINT = None
        current_server = None
        return True

class AdminMessageDeletionMiddleware(BaseMiddleware):
    async def on_process_message(self, message: types.Message, data: dict):
        if message.from_user.id == admin:
            asyncio.create_task(delete_message_after_delay(message.chat.id, message.message_id, delay=2))

dp = Dispatcher(bot)
scheduler = AsyncIOScheduler(timezone=pytz.UTC)
scheduler.start()

dp.middleware.setup(AdminMessageDeletionMiddleware())

main_menu_markup = InlineKeyboardMarkup(row_width=1).add(
    InlineKeyboardButton("Добавить пользователя", callback_data="add_user"),
    InlineKeyboardButton("Список клиентов", callback_data="list_users"),
    InlineKeyboardButton("Создать бекап", callback_data="create_backup"),
    InlineKeyboardButton("Управление серверами", callback_data="manage_servers")
)

current_server = None

user_main_messages = {}
isp_cache = {}
ISP_CACHE_FILE = 'files/isp_cache.json'
CACHE_TTL = timedelta(hours=24)

TRAFFIC_LIMITS = ["5 GB", "10 GB", "30 GB", "100 GB", "Неограниченно"]

def get_interface_name():
    return os.path.basename(WG_CONFIG_FILE).split('.')[0]

async def load_isp_cache():
    global isp_cache
    if os.path.exists(ISP_CACHE_FILE):
        async with aiofiles.open(ISP_CACHE_FILE, 'r') as f:
            try:
                isp_cache = json.loads(await f.read())
                for ip in list(isp_cache.keys()):
                    isp_cache[ip]['timestamp'] = datetime.fromisoformat(isp_cache[ip]['timestamp'])
            except:
                isp_cache = {}

async def save_isp_cache():
    async with aiofiles.open(ISP_CACHE_FILE, 'w') as f:
        cache_to_save = {ip: {'isp': data['isp'], 'timestamp': data['timestamp'].isoformat()} for ip, data in isp_cache.items()}
        await f.write(json.dumps(cache_to_save))

async def get_isp_info(ip: str) -> str:
    now = datetime.now(pytz.UTC)
    if ip in isp_cache:
        if now - isp_cache[ip]['timestamp'] < CACHE_TTL:
            return isp_cache[ip]['isp']
    try:
        ip_obj = ipaddress.ip_address(ip)
        if ip_obj.is_private:
            return "Private Range"
    except:
        return "Invalid IP"
    url = f"http://ip-api.com/json/{ip}?fields=status,message,isp"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('status') == 'success':
                        isp = data.get('isp', 'Unknown ISP')
                        isp_cache[ip] = {'isp': isp, 'timestamp': now}
                        await save_isp_cache()
                        return isp
    except:
        pass
    return "Unknown ISP"

async def cleanup_isp_cache():
    now = datetime.now(pytz.UTC)
    for ip in list(isp_cache.keys()):
        if now - isp_cache[ip]['timestamp'] >= CACHE_TTL:
            del isp_cache[ip]
    await save_isp_cache()

async def cleanup_connection_data(username: str):
    file_path = os.path.join('files', 'connections', f'{username}_ip.json')
    if os.path.exists(file_path):
        async with aiofiles.open(file_path, 'r') as f:
            try:
                data = json.loads(await f.read())
            except:
                data = {}
        sorted_ips = sorted(data.items(), key=lambda x: datetime.strptime(x[1], '%d.%m.%Y %H:%M'), reverse=True)
        limited_ips = dict(sorted_ips[:100])
        async with aiofiles.open(file_path, 'w') as f:
            await f.write(json.dumps(limited_ips))

async def load_isp_cache_task():
    await load_isp_cache()
    scheduler.add_job(cleanup_isp_cache, 'interval', hours=1)

def create_zip(backup_filepath):
    with zipfile.ZipFile(backup_filepath, 'w') as zipf:
        for main_file in ['awg-decode.py', 'newclient.sh', 'removeclient.sh']:
            if os.path.exists(main_file):
                zipf.write(main_file, main_file)
        for root, dirs, files in os.walk('files'):
            for file in files:
                filepath = os.path.join(root, file)
                arcname = os.path.relpath(filepath, os.getcwd())
                zipf.write(filepath, arcname)
        for root, dirs, files in os.walk('users'):
            for file in files:
                filepath = os.path.join(root, file)
                arcname = os.path.relpath(filepath, os.getcwd())
                zipf.write(filepath, arcname)

async def delete_message_after_delay(chat_id: int, message_id: int, delay: int):
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id, message_id)
    except:
        pass

def parse_relative_time(relative_str: str) -> datetime:
    try:
        parts = relative_str.lower().replace(' ago', '').split(', ')
        delta = timedelta()
        for part in parts:
            number, unit = part.split(' ')
            number = int(number)
            if 'minute' in unit:
                delta += timedelta(minutes=number)
            elif 'second' in unit:
                delta += timedelta(seconds=number)
            elif 'hour' in unit:
                delta += timedelta(hours=number)
            elif 'day' in unit:
                delta += timedelta(days=number)
            elif 'week' in unit:
                delta += timedelta(weeks=number)
            elif 'month' in unit:
                delta += timedelta(days=30 * number)
            elif 'year' in unit:
                delta += timedelta(days=365 * number)
        return datetime.now(pytz.UTC) - delta
    except Exception as e:
        logger.error(f"Ошибка при парсинге относительного времени '{relative_str}': {e}")
        return None

@dp.message_handler(commands=['start', 'help'])
async def help_command_handler(message: types.Message):
    if message.chat.id == admin:
        sent_message = await message.answer("Выберите действие:", reply_markup=main_menu_markup)
        user_main_messages[admin] = {'chat_id': sent_message.chat.id, 'message_id': sent_message.message_id}
        try:
            await bot.pin_chat_message(chat_id=message.chat.id, message_id=sent_message.message_id, disable_notification=True)
        except:
            pass
    else:
        await message.answer("У вас нет доступа к этому боту.")

@dp.message_handler()
async def handle_messages(message: types.Message):
    if message.chat.id != admin:
        await message.answer("У вас нет доступа к этому боту.")
        return
    
    user_state = user_main_messages.get(admin, {}).get('state')
    
    if user_state == 'waiting_for_server_id':
        server_id = message.text.strip()
        if not all(c.isalnum() or c in "-_" for c in server_id):
            main_chat_id = user_main_messages.get(admin, {}).get('chat_id')
            main_message_id = user_main_messages.get(admin, {}).get('message_id')
            if main_chat_id and main_message_id:
                await bot.edit_message_text(
                    chat_id=main_chat_id,
                    message_id=main_message_id,
                    text="Идентификатор сервера может содержать только буквы, цифры, дефисы и подчёркивания.\nВведите идентификатор нового сервера:",
                    reply_markup=InlineKeyboardMarkup().add(
                        InlineKeyboardButton("Отмена", callback_data="manage_servers")
                    )
                )
            asyncio.create_task(delete_message_after_delay(message.chat.id, message.message_id, delay=5))
            return
        
        user_main_messages[admin]['server_id'] = server_id
        user_main_messages[admin]['state'] = 'waiting_for_server_host'
        
        main_chat_id = user_main_messages.get(admin, {}).get('chat_id')
        main_message_id = user_main_messages.get(admin, {}).get('message_id')
        if main_chat_id and main_message_id:
            await bot.edit_message_text(
                chat_id=main_chat_id,
                message_id=main_message_id,
                text="Введите IP-адрес сервера:",
                reply_markup=InlineKeyboardMarkup().add(
                    InlineKeyboardButton("Отмена", callback_data="manage_servers")
                )
            )
        asyncio.create_task(delete_message_after_delay(message.chat.id, message.message_id, delay=5))
        
    elif user_state == 'waiting_for_server_host':
        host = message.text.strip()
        user_main_messages[admin]['host'] = host
        user_main_messages[admin]['state'] = 'waiting_for_server_port'
        
        main_chat_id = user_main_messages.get(admin, {}).get('chat_id')
        main_message_id = user_main_messages.get(admin, {}).get('message_id')
        if main_chat_id and main_message_id:
            await bot.edit_message_text(
                chat_id=main_chat_id,
                message_id=main_message_id,
                text="Введите SSH порт (по умолчанию 22):",
                reply_markup=InlineKeyboardMarkup().add(
                    InlineKeyboardButton("Отмена", callback_data="manage_servers")
                )
            )
        asyncio.create_task(delete_message_after_delay(message.chat.id, message.message_id, delay=5))
        
    elif user_state == 'waiting_for_server_port':
        try:
            port = int(message.text.strip() or "22")
            user_main_messages[admin]['port'] = port
            user_main_messages[admin]['state'] = 'waiting_for_server_username'
            
            main_chat_id = user_main_messages.get(admin, {}).get('chat_id')
            main_message_id = user_main_messages.get(admin, {}).get('message_id')
            if main_chat_id and main_message_id:
                await bot.edit_message_text(
                    chat_id=main_chat_id,
                    message_id=main_message_id,
                    text="Введите имя пользователя SSH:",
                    reply_markup=InlineKeyboardMarkup().add(
                        InlineKeyboardButton("Отмена", callback_data="manage_servers")
                    )
                )
            asyncio.create_task(delete_message_after_delay(message.chat.id, message.message_id, delay=5))
        except ValueError:
            main_chat_id = user_main_messages.get(admin, {}).get('chat_id')
            main_message_id = user_main_messages.get(admin, {}).get('message_id')
            if main_chat_id and main_message_id:
                await bot.edit_message_text(
                    chat_id=main_chat_id,
                    message_id=main_message_id,
                    text="Порт должен быть числом.\nВведите SSH порт (по умолчанию 22):",
                    reply_markup=InlineKeyboardMarkup().add(
                        InlineKeyboardButton("Отмена", callback_data="manage_servers")
                    )
                )
            asyncio.create_task(delete_message_after_delay(message.chat.id, message.message_id, delay=5))
            
    elif user_state == 'waiting_for_server_username':
        username = message.text.strip()
        user_main_messages[admin]['username'] = username
        user_main_messages[admin]['state'] = 'waiting_for_auth_type'
        
        auth_markup = InlineKeyboardMarkup(row_width=2)
        auth_markup.add(
            InlineKeyboardButton("Пароль", callback_data="auth_password"),
            InlineKeyboardButton("SSH ключ", callback_data="auth_key")
        )
        
        main_chat_id = user_main_messages.get(admin, {}).get('chat_id')
        main_message_id = user_main_messages.get(admin, {}).get('message_id')
        if main_chat_id and main_message_id:
            await bot.edit_message_text(
                chat_id=main_chat_id,
                message_id=main_message_id,
                text="Выберите тип аутентификации:",
                reply_markup=auth_markup
            )
        asyncio.create_task(delete_message_after_delay(message.chat.id, message.message_id, delay=5))
        
    elif user_state == 'waiting_for_password':
        password = message.text.strip()
        server_data = user_main_messages[admin]
        
        success = db.add_server(
            server_data['server_id'],
            server_data['host'],
            server_data['port'],
            server_data['username'],
            'password',
            password=password
        )
        
        main_chat_id = user_main_messages.get(admin, {}).get('chat_id')
        main_message_id = user_main_messages.get(admin, {}).get('message_id')
        
        if success:
            if main_chat_id and main_message_id:
                await bot.edit_message_text(
                    chat_id=main_chat_id,
                    message_id=main_message_id,
                    text="Сервер успешно добавлен!",
                    reply_markup=main_menu_markup
                )
            await asyncio.sleep(2)
            await bot.edit_message_text(
                chat_id=main_chat_id,
                message_id=main_message_id,
                text="Управление серверами:",
                reply_markup=InlineKeyboardMarkup(row_width=2).add(
                    *[InlineKeyboardButton(
                        f"{'✅ ' if server == current_server else ''}{server}",
                        callback_data=f"select_server_{server}"
                    ) for server in db.get_server_list()],
                    InlineKeyboardButton("Добавить сервер", callback_data="add_server"),
                    InlineKeyboardButton("Удалить сервер", callback_data="delete_server"),
                    InlineKeyboardButton("Домой", callback_data="home")
                )
            )
        else:
            if main_chat_id and main_message_id:
                await bot.edit_message_text(
                    chat_id=main_chat_id,
                    message_id=main_message_id,
                    text="Ошибка при добавлении сервера.",
                    reply_markup=InlineKeyboardMarkup().add(
                        InlineKeyboardButton("Назад", callback_data="manage_servers")
                    )
                )
        
        asyncio.create_task(delete_message_after_delay(message.chat.id, message.message_id, delay=5))
            
    elif user_state == 'waiting_for_key_path':
        key_path = message.text.strip()
        server_data = user_main_messages[admin]
        
        success = db.add_server(
            server_data['server_id'],
            server_data['host'],
            server_data['port'],
            server_data['username'],
            'key',
            key_path=key_path
        )
        
        main_chat_id = user_main_messages.get(admin, {}).get('chat_id')
        main_message_id = user_main_messages.get(admin, {}).get('message_id')
        
        if success:
            if main_chat_id and main_message_id:
                await bot.edit_message_text(
                    chat_id=main_chat_id,
                    message_id=main_message_id,
                    text="Сервер успешно добавлен!",
                    reply_markup=main_menu_markup
                )
            await asyncio.sleep(2)
            await bot.edit_message_text(
                chat_id=main_chat_id,
                message_id=main_message_id,
                text="Управление серверами:",
                reply_markup=InlineKeyboardMarkup(row_width=2).add(
                    *[InlineKeyboardButton(
                        f"{'✅ ' if server == current_server else ''}{server}",
                        callback_data=f"select_server_{server}"
                    ) for server in db.get_server_list()],
                    InlineKeyboardButton("Добавить сервер", callback_data="add_server"),
                    InlineKeyboardButton("Удалить сервер", callback_data="delete_server"),
                    InlineKeyboardButton("Домой", callback_data="home")
                )
            )
        else:
            if main_chat_id and main_message_id:
                await bot.edit_message_text(
                    chat_id=main_chat_id,
                    message_id=main_message_id,
                    text="Ошибка при добавлении сервера.",
                    reply_markup=InlineKeyboardMarkup().add(
                        InlineKeyboardButton("Назад", callback_data="manage_servers")
                    )
                )
        
        asyncio.create_task(delete_message_after_delay(message.chat.id, message.message_id, delay=5))
            
    elif user_state == 'waiting_for_user_name':
        user_name = message.text.strip()
        if not all(c.isalnum() or c in "-_" for c in user_name):
            await message.reply("Имя пользователя может содержать только буквы, цифры, дефисы и подчёркивания.")
            asyncio.create_task(delete_message_after_delay(sent_message.chat.id, sent_message.message_id, delay=5))
            return
        user_main_messages[admin]['client_name'] = user_name
        user_main_messages[admin]['state'] = 'waiting_for_duration'
        duration_buttons = [
            InlineKeyboardButton("1 час", callback_data=f"duration_1h_{user_name}_noipv6"),
            InlineKeyboardButton("1 день", callback_data=f"duration_1d_{user_name}_noipv6"),
            InlineKeyboardButton("1 неделя", callback_data=f"duration_1w_{user_name}_noipv6"),
            InlineKeyboardButton("1 месяц", callback_data=f"duration_1m_{user_name}_noipv6"),
            InlineKeyboardButton("Без ограничений", callback_data=f"duration_unlimited_{user_name}_noipv6"),
            InlineKeyboardButton("Домой", callback_data="home")
        ]
        duration_markup = InlineKeyboardMarkup(row_width=1).add(*duration_buttons)
        main_chat_id = user_main_messages[admin].get('chat_id')
        main_message_id = user_main_messages[admin].get('message_id')
        if main_chat_id and main_message_id:
            await bot.edit_message_text(
                chat_id=main_chat_id,
                message_id=main_message_id,
                text=f"Выберите время действия конфигурации для пользователя **{user_name}**:",
                parse_mode="Markdown",
                reply_markup=duration_markup
            )
        else:
            await message.answer("Ошибка: главное сообщение не найдено.")
    else:
        await message.reply("Неизвестная команда или действие.")
        asyncio.create_task(delete_message_after_delay(sent_message.chat.id, sent_message.message_id, delay=5))

@dp.callback_query_handler(lambda c: c.data.startswith('add_user'))
async def prompt_for_user_name(callback_query: types.CallbackQuery):
    if callback_query.from_user.id != admin:
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
        
    if not current_server:
        await callback_query.answer("Сначала выберите сервер в разделе 'Управление серверами'", show_alert=True)
        return
    main_chat_id = user_main_messages.get(admin, {}).get('chat_id')
    main_message_id = user_main_messages.get(admin, {}).get('message_id')
    if main_chat_id and main_message_id:
        await bot.edit_message_text(
            chat_id=main_chat_id,
            message_id=main_message_id,
            text="Введите имя пользователя для добавления:",
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("Домой", callback_data="home")
            )
        )
        user_main_messages[admin]['state'] = 'waiting_for_user_name'
    else:
        await callback_query.answer("Ошибка: главное сообщение не найдено.", show_alert=True)
    await callback_query.answer()

def parse_traffic_limit(traffic_limit: str) -> int:
    mapping = {'B':1, 'KB':10**3, 'MB':10**6, 'GB':10**9, 'TB':10**12}
    match = re.match(r'^(\d+(?:\.\d+)?)\s*(B|KB|MB|GB|TB)$', traffic_limit, re.IGNORECASE)
    if match:
        value = float(match.group(1))
        unit = match.group(2).upper()
        return int(value * mapping.get(unit, 1))
    else:
        return None

@dp.callback_query_handler(lambda c: c.data.startswith('duration_'))
async def set_config_duration(callback: types.CallbackQuery):
    if callback.from_user.id != admin:
        await callback.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
    parts = callback.data.split('_')
    if len(parts) < 4:
        await callback.answer("Некорректные данные.", show_alert=True)
        return
    duration_choice = parts[1]
    client_name = parts[2]
    ipv6_flag = parts[3]
    user_main_messages[admin]['duration_choice'] = duration_choice
    user_main_messages[admin]['state'] = 'waiting_for_traffic_limit'
    traffic_buttons = [
        InlineKeyboardButton(limit, callback_data=f"traffic_limit_{limit}_{client_name}")
        for limit in TRAFFIC_LIMITS
    ]
    traffic_markup = InlineKeyboardMarkup(row_width=1).add(*traffic_buttons)
    await bot.edit_message_text(
        chat_id=callback.message.chat.id,
        message_id=callback.message.message_id,
        text=f"Выберите лимит трафика для пользователя **{client_name}**:",
        parse_mode="Markdown",
        reply_markup=traffic_markup
    )
    await callback.answer()

def format_vpn_key(vpn_key, num_lines=8):
    line_length = len(vpn_key) // num_lines
    if len(vpn_key) % num_lines != 0:
        line_length += 1
    lines = [vpn_key[i:i+line_length] for i in range(0, len(vpn_key), line_length)]
    formatted_key = '\n'.join(lines)
    return formatted_key

@dp.callback_query_handler(lambda c: c.data.startswith('traffic_limit_'))
async def set_traffic_limit(callback_query: types.CallbackQuery):
    if callback_query.from_user.id != admin:
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
    parts = callback_query.data.split('_', 3)
    if len(parts) < 4:
        await callback_query.answer("Некорректные данные.", show_alert=True)
        return
    traffic_limit = parts[2]
    client_name = parts[3]
    traffic_bytes = parse_traffic_limit(traffic_limit)
    if traffic_limit != "Неограниченно" and traffic_bytes is None:
        await callback_query.answer("Некорректный формат лимита трафика.", show_alert=True)
        return
    user_main_messages[admin]['traffic_limit'] = traffic_limit
    user_main_messages[admin]['state'] = None
    duration_choice = user_main_messages.get(admin, {}).get('duration_choice')
    if duration_choice == '1h':
        duration = timedelta(hours=1)
    elif duration_choice == '1d':
        duration = timedelta(days=1)
    elif duration_choice == '1w':
        duration = timedelta(weeks=1)
    elif duration_choice == '1m':
        duration = timedelta(days=30)
    elif duration_choice == 'unlimited':
        duration = None
    else:
        duration = None
    if duration:
        expiration_time = datetime.now(pytz.UTC) + duration
        db.set_user_expiration(client_name, expiration_time, traffic_limit, server_id=current_server)
        scheduler.add_job(
            deactivate_user,
            trigger=DateTrigger(run_date=expiration_time),
            args=[client_name],
            id=client_name
        )
        confirmation_text = f"Пользователь **{client_name}** добавлен. \nКонфигурация истечет через **{duration_choice}**."
    else:
        db.set_user_expiration(client_name, None, traffic_limit, server_id=current_server)
        confirmation_text = f"Пользователь **{client_name}** добавлен с неограниченным временем действия."
    if traffic_limit != "Неограниченно":
        confirmation_text += f"\nЛимит трафика: **{traffic_limit}**."
    else:
        confirmation_text += f"\nЛимит трафика: **♾️ Неограниченно**."
    success = db.root_add(client_name, server_id=current_server, ipv6=False)
    if success:
        try:
            conf_path = os.path.join('users', client_name, f'{client_name}.conf')
            vpn_key = ""
            if os.path.exists(conf_path):
                vpn_key = await generate_vpn_key(conf_path)
            if vpn_key:
                instruction_text = (
                    "\nAmneziaVPN [Google Play](https://play.google.com/store/apps/details?id=org.amnezia.vpn&hl=ru), "
                    "[GitHub](https://github.com/amnezia-vpn/amnezia-client)"
                )
                formatted_key = format_vpn_key(vpn_key)
                key_message = f"```\n{formatted_key}\n```"
                caption = f"{instruction_text}\n{key_message}"
            else:
                caption = "VPN ключ не был сгенерирован."
            if os.path.exists(conf_path):
                with open(conf_path, 'rb') as config:
                    sent_doc = await bot.send_document(
                        admin,
                        config,
                        caption=caption,
                        parse_mode="Markdown",
                        disable_notification=True
                    )
                    asyncio.create_task(delete_message_after_delay(admin, sent_doc.message_id, delay=15))
        except FileNotFoundError:
            confirmation_text = "Не удалось найти файлы конфигурации для указанного пользователя."
            sent_message = await bot.send_message(admin, confirmation_text, parse_mode="Markdown", disable_notification=True)
            asyncio.create_task(delete_message_after_delay(admin, sent_message.message_id, delay=15))
            await callback_query.answer()
            return
        except Exception as e:
            logger.error(f"Ошибка при отправке конфигурации: {e}")
            confirmation_text = "Произошла ошибка."
            sent_message = await bot.send_message(admin, confirmation_text, parse_mode="Markdown", disable_notification=True)
            asyncio.create_task(delete_message_after_delay(admin, sent_message.message_id, delay=15))
            await callback_query.answer()
            return
        sent_confirmation = await bot.send_message(
            chat_id=admin,
            text=confirmation_text,
            parse_mode="Markdown",
            disable_notification=True
        )
        asyncio.create_task(delete_message_after_delay(admin, sent_confirmation.message_id, delay=15))
    else:
        confirmation_text = "Не удалось добавить пользователя."
        sent_confirmation = await bot.send_message(
            chat_id=admin,
            text=confirmation_text,
            parse_mode="Markdown",
            disable_notification=True
        )
        asyncio.create_task(delete_message_after_delay(admin, sent_confirmation.message_id, delay=15))
    main_chat_id = user_main_messages.get(admin, {}).get('chat_id')
    main_message_id = user_main_messages.get(admin, {}).get('message_id')
    if main_chat_id and main_message_id:
        await bot.edit_message_text(
            chat_id=main_chat_id,
            message_id=main_message_id,
            text="Выберите действие:",
            reply_markup=main_menu_markup
        )
    else:
        await callback_query.answer("Выберите действие:", show_alert=True)
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('client_'))
async def client_selected_callback(callback_query: types.CallbackQuery):
    if callback_query.from_user.id != admin:
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
        
    if not current_server:
        await callback_query.answer("Сначала выберите сервер в разделе 'Управление серверами'", show_alert=True)
        return
        
    _, username = callback_query.data.split('client_', 1)
    username = username.strip()
    clients = db.get_client_list(server_id=current_server)
    client_info = next((c for c in clients if c[0] == username), None)
    if not client_info:
        await callback_query.answer("Ошибка: пользователь не найден.", show_alert=True)
        return

    expiration_time = db.get_user_expiration(username, server_id=current_server)
    traffic_limit = db.get_user_traffic_limit(username, server_id=current_server)
    status = "🔴 Offline"
    incoming_traffic = "↓—"
    outgoing_traffic = "↑—"
    ipv4_address = "—"
    total_bytes = 0
    formatted_total = "0.00B"

    active_clients = db.get_active_list(server_id=current_server)
    active_info = None
    for ac in active_clients:
        if isinstance(ac, dict) and ac.get('name') == username:
            active_info = ac
            break
        elif isinstance(ac, (list, tuple)) and ac[0] == username:
            active_info = {'name': ac[0], 'last_handshake': ac.get(1, 'never'), 'transfer': ac.get(2, '0/0')}
            break

    if active_info:
        last_handshake_str = active_info.get('last_handshake', 'never')
        if last_handshake_str.lower() not in ['never', 'нет данных', '-']:
            try:
                last_handshake_dt = parse_relative_time(last_handshake_str)
                if last_handshake_dt:
                    delta = datetime.now(pytz.UTC) - last_handshake_dt
                    if delta <= timedelta(minutes=1):
                        status = "🟢 Online"
                    else:
                        status = "🔴 Offline"

                transfer = active_info.get('transfer', '0/0')
                incoming_bytes, outgoing_bytes = parse_transfer(transfer)
                incoming_traffic = f"↓{humanize_bytes(incoming_bytes)}"
                outgoing_traffic = f"↑{humanize_bytes(outgoing_bytes)}"
                traffic_data = await update_traffic(username, incoming_bytes, outgoing_bytes)
                total_bytes = traffic_data.get('total_incoming', 0) + traffic_data.get('total_outgoing', 0)
                formatted_total = humanize_bytes(total_bytes)

                if traffic_limit != "Неограниченно":
                    limit_bytes = parse_traffic_limit(traffic_limit)
                    if total_bytes >= limit_bytes:
                        await deactivate_user(username)
                        await callback_query.answer(
                            f"Пользователь {username} превысил лимит трафика и был удален.",
                            show_alert=True
                        )
                        return
            except ValueError:
                logger.error(f"Некорректный формат даты для пользователя {username}: {last_handshake_str}")
                status = "🔴 Offline"
    else:
        traffic_data = await read_traffic(username)
        total_bytes = traffic_data.get('total_incoming', 0) + traffic_data.get('total_outgoing', 0)
        formatted_total = humanize_bytes(total_bytes)

    allowed_ips = client_info[2]
    ipv4_match = re.search(r'(\d{1,3}\.){3}\d{1,3}/\d+', allowed_ips)
    ipv4_address = ipv4_match.group(0) if ipv4_match else "—"

    if expiration_time:
        now = datetime.now(pytz.UTC)
        try:
            expiration_dt = expiration_time
            if expiration_dt.tzinfo is None:
                expiration_dt = expiration_dt.replace(tzinfo=pytz.UTC)
            remaining = expiration_dt - now
            if remaining.total_seconds() > 0:
                days, seconds = remaining.days, remaining.seconds
                hours = seconds // 3600
                minutes = (seconds % 3600) // 60
                date_end = f"📅 {days}д {hours}ч {minutes}м"
            else:
                date_end = "📅 ♾️ Неограниченно"
        except Exception as e:
            logger.error(f"Ошибка при обработке даты окончания: {e}")
            date_end = "📅 ♾️ Неограниченно"
    else:
        date_end = "📅 ♾️ Неограниченно"

    traffic_limit_display = "♾️ Неограниченно" if traffic_limit == "Неограниченно" else traffic_limit

    text = (
        f"📧 _Имя:_ {username}\n"
        f"🌐 _IPv4:_ {ipv4_address}\n"
        f"🌐 _Статус соединения:_ {status}\n"
        f"{date_end}\n"
        f"🔼 _Исходящий трафик:_ {incoming_traffic}\n"
        f"🔽 _Входящий трафик:_ {outgoing_traffic}\n"
        f"📊 _Всего:_ ↑↓{formatted_total} из **{traffic_limit_display}**\n"
    )

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("IP info", callback_data=f"ip_info_{username}"),
        InlineKeyboardButton("Подключения", callback_data=f"connections_{username}"),
        InlineKeyboardButton("Получить конфигурацию", callback_data=f"send_config_{username}")
    )
    keyboard.add(
        InlineKeyboardButton("Удалить", callback_data=f"delete_user_{username}")
    )
    keyboard.add(
        InlineKeyboardButton("Назад", callback_data="list_users"),
        InlineKeyboardButton("Домой", callback_data="home")
    )

    main_chat_id = user_main_messages.get(admin, {}).get('chat_id')
    main_message_id = user_main_messages.get(admin, {}).get('message_id')

    if main_chat_id and main_message_id:
        try:
            await bot.edit_message_text(
                chat_id=main_chat_id,
                message_id=main_message_id,
                text=text,
                parse_mode="Markdown",
                reply_markup=keyboard
            )
        except Exception as e:
            logger.error(f"Ошибка при редактировании сообщения: {e}")
            await callback_query.answer("Ошибка при обновлении сообщения.", show_alert=True)
    else:
        await callback_query.answer("Ошибка: главное сообщение не найдено.", show_alert=True)
        return

    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('list_users'))
async def list_users_callback(callback_query: types.CallbackQuery):
    if callback_query.from_user.id != admin:
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
        
    if not current_server:
        await callback_query.answer("Сначала выберите сервер в разделе 'Управление серверами'", show_alert=True)
        return

    clients = db.get_client_list(server_id=current_server)
    if not clients:
        await callback_query.answer("Список пользователей пуст.", show_alert=True)
        return

    active_clients = db.get_active_list(server_id=current_server)
    active_clients_dict = {}
    for client in active_clients:
        if isinstance(client, dict):
            username = client.get('name')
            last_handshake = client.get('last_handshake', 'never')
        else:
            username = client[0] if isinstance(client, (list, tuple)) else str(client)
            last_handshake = 'never'
        if username:
            active_clients_dict[username] = last_handshake

    keyboard = InlineKeyboardMarkup(row_width=2)
    now = datetime.now(pytz.UTC)

    for client in clients:
        username = client[0]  
        last_handshake_str = active_clients_dict.get(username)
        if last_handshake_str and last_handshake_str.lower() not in ['never', 'нет данных', '-']:
            try:
                last_handshake_dt = parse_relative_time(last_handshake_str)
                if last_handshake_dt:
                    delta = now - last_handshake_dt
                    delta_days = delta.days
                    if delta_days <= 5:
                        status_display = f"🟢({delta_days}d) {username}"
                    else:
                        status_display = f"❌({delta_days}d) {username}"
                else:
                    status_display = f"❌(?d) {username}"
            except ValueError:
                logger.error(f"Некорректный формат даты для пользователя {username}: {last_handshake_str}")
                status_display = f"❌(?d) {username}"
        else:
            status_display = f"❌(?d) {username}"

        keyboard.insert(InlineKeyboardButton(
            status_display,
            callback_data=f"client_{username}"
        ))

    keyboard.add(InlineKeyboardButton("Домой", callback_data="home"))

    main_chat_id = user_main_messages.get(admin, {}).get('chat_id')
    main_message_id = user_main_messages.get(admin, {}).get('message_id')

    if main_chat_id and main_message_id:
        try:
            await bot.edit_message_text(
                chat_id=main_chat_id,
                message_id=main_message_id,
                text="Выберите пользователя:",
                reply_markup=keyboard
            )
        except Exception as e:
            logger.error(f"Ошибка при редактировании сообщения: {e}")
            await callback_query.answer("Ошибка при обновлении сообщения.", show_alert=True)
    else:
        sent_message = await callback_query.message.reply(
            "Выберите пользователя:",
            reply_markup=keyboard
        )
        user_main_messages[admin] = {
            'chat_id': sent_message.chat.id,
            'message_id': sent_message.message_id
        }
        try:
            await bot.pin_chat_message(
                chat_id=sent_message.chat.id,
                message_id=sent_message.message_id,
                disable_notification=True
            )
        except:
            pass

    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('connections_'))
async def client_connections_callback(callback_query: types.CallbackQuery):
    if callback_query.from_user.id != admin:
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
        
    if not current_server:
        await callback_query.answer("Сначала выберите сервер в разделе 'Управление серверами'", show_alert=True)
        return
        
    _, username = callback_query.data.split('connections_', 1)
    username = username.strip()
    file_path = os.path.join('files', 'connections', f'{username}_ip.json')
    
    os.makedirs(os.path.join('files', 'connections'), exist_ok=True)
    
    try:
        active_clients = db.get_active_list(server_id=current_server)
        active_info = next((client for client in active_clients if isinstance(client, dict) and client.get('name') == username), None)
        
        if active_info and active_info.get('endpoint'):
            last_handshake_str = active_info.get('last_handshake', 'never')
            if last_handshake_str.lower() not in ['never', 'нет данных', '-']:
                try:
                    last_handshake_dt = parse_relative_time(last_handshake_str)
                    if last_handshake_dt:
                        delta = datetime.now(pytz.UTC) - last_handshake_dt
                        if delta <= timedelta(minutes=1):
                            endpoint = active_info['endpoint'].split(':')[0]
                            current_time = datetime.now().strftime('%d.%m.%Y %H:%M')
                            
                            if os.path.exists(file_path):
                                async with aiofiles.open(file_path, 'r') as f:
                                    data = json.loads(await f.read())
                            else:
                                data = {}

                            if endpoint not in data:
                                data[endpoint] = current_time
                            
                            async with aiofiles.open(file_path, 'w') as f:
                                await f.write(json.dumps(data))
                except ValueError:
                    logger.error(f"Некорректный формат времени последнего подключения: {last_handshake_str}")

        if os.path.exists(file_path):
            async with aiofiles.open(file_path, 'r') as f:
                data = json.loads(await f.read())
            
            sorted_connections = sorted(data.items(), key=lambda x: datetime.strptime(x[1], '%d.%m.%Y %H:%M'), reverse=True)
            
            text = f"Подключения пользователя {username} за последние 24 часа:\n\n"
            for i, (ip, time) in enumerate(sorted_connections, 1):
                connection_time = datetime.strptime(time, '%d.%m.%Y %H:%M')
                isp_info = await get_isp_info(ip)
                if datetime.now() - connection_time <= timedelta(days=1):
                    text += f"{i}. {ip} ({isp_info}) - {connection_time}\n"
        else:
            text = f"История подключений пользователя {username} отсутствует."
                
        keyboard = InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            InlineKeyboardButton("Назад", callback_data=f"client_{username}"),
            InlineKeyboardButton("Домой", callback_data="home"))

        await callback_query.message.edit_text(text, reply_markup=keyboard)
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Ошибка при обработке подключений: {e}")
        await callback_query.answer("Произошла ошибка при получении данных о подключениях.", show_alert=True) 
        
@dp.callback_query_handler(lambda c: c.data.startswith('ip_info_'))
async def ip_info_callback(callback_query: types.CallbackQuery):
    if callback_query.from_user.id != admin:
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
        
    if not current_server:
        await callback_query.answer("Сначала выберите сервер в разделе 'Управление серверами'", show_alert=True)
        return
        
    _, username = callback_query.data.split('ip_info_', 1)
    username = username.strip()
    active_clients = db.get_active_list(server_id=current_server)
    active_info = next((ac for ac in active_clients if ac.get('name') == username), None)
    if active_info:
        endpoint = active_info.get('endpoint', '')
        ip_address = endpoint.split(':')[0] if endpoint else None
    else:
        await callback_query.answer("Нет информации о подключении пользователя.", show_alert=True)
        return
    url = f"http://ip-api.com/json/{ip_address}?fields=message,country,countryCode,region,regionName,city,zip,lat,lon,timezone,isp,org,as,hosting"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if 'message' in data:
                        await callback_query.answer(f"Ошибка при получении данных: {data['message']}", show_alert=True)
                        return
                else:
                    await callback_query.answer(f"Ошибка при запросе к API: {resp.status}", show_alert=True)
                    return
    except Exception as e:
        logger.error(f"Ошибка при запросе к API: {e}")
        await callback_query.answer("Ошибка при запросе к API.", show_alert=True)
        return
    info_text = f"*IP информация для {username}:*\n"
    for key, value in data.items():
        info_text += f"{key.capitalize()}: {value}\n"
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("Назад", callback_data=f"client_{username}"),
        InlineKeyboardButton("Домой", callback_data="home")
    )
    main_chat_id = user_main_messages.get(admin, {}).get('chat_id')
    main_message_id = user_main_messages.get(admin, {}).get('message_id')
    if main_chat_id and main_message_id:
        try:
            await bot.edit_message_text(
                chat_id=main_chat_id,
                message_id=main_message_id,
                text=info_text,
                parse_mode="Markdown",
                reply_markup=keyboard
            )
        except Exception as e:
            logger.error(f"Ошибка при изменении сообщения: {e}")
            await callback_query.answer("Ошибка при обновлении сообщения.", show_alert=True)
            return
    else:
        await callback_query.answer("Ошибка: главное сообщение не найдено.", show_alert=True)
        return
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('delete_user_'))
async def client_delete_callback(callback_query: types.CallbackQuery):
    if callback_query.from_user.id != admin:
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
        
    if not current_server:
        await callback_query.answer("Сначала выберите сервер в разделе 'Управление серверами'", show_alert=True)
        return
        
    username = callback_query.data.split('delete_user_')[1]
    success = db.deactive_user_db(username, server_id=current_server)
    if success:
        db.remove_user_expiration(username, server_id=current_server)
        try:
            scheduler.remove_job(job_id=username)
        except:
            pass
        user_dir = os.path.join('users', username)
        try:
            if os.path.exists(user_dir):
                shutil.rmtree(user_dir)
        except Exception as e:
            logger.error(f"Ошибка при удалении директории для пользователя {username}: {e}")
            
        connections_file = os.path.join('files', 'connections', f'{username}_ip.json')
        try:
            if os.path.exists(connections_file):
                os.remove(connections_file)
        except Exception as e:
            logger.error(f"Ошибка при удалении файла подключений для пользователя {username}: {e}")
        confirmation_text = f"Пользователь **{username}** успешно удален."
    else:
        confirmation_text = f"Не удалось удалить пользователя **{username}**."
    main_chat_id = user_main_messages.get(admin, {}).get('chat_id')
    main_message_id = user_main_messages.get(admin, {}).get('message_id')
    if main_chat_id and main_message_id:
        await bot.edit_message_text(
            chat_id=main_chat_id,
            message_id=main_message_id,
            text=confirmation_text,
            parse_mode="Markdown",
            reply_markup=main_menu_markup
        )
    else:
        await callback_query.answer("Ошибка: главное сообщение не найдено.", show_alert=True)
        return
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data == 'manage_servers')
async def manage_servers_callback(callback_query: types.CallbackQuery):
    if callback_query.from_user.id != admin:
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
    
    servers = db.get_server_list()
    keyboard = InlineKeyboardMarkup(row_width=2)
    
    for server in servers:
        keyboard.insert(InlineKeyboardButton(
            f"{'✅ ' if server == current_server else ''}{server}",
            callback_data=f"select_server_{server}"
        ))
    
    keyboard.add(InlineKeyboardButton("Добавить сервер", callback_data="add_server"))
    if servers:
        keyboard.add(InlineKeyboardButton("Удалить сервер", callback_data="delete_server"))
    keyboard.add(InlineKeyboardButton("Домой", callback_data="home"))
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="Управление серверами:",
        reply_markup=keyboard
    )
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('select_server_'))
async def select_server_callback(callback_query: types.CallbackQuery):
    if callback_query.from_user.id != admin:
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
    
    server_id = callback_query.data.split('select_server_')[1]
    
    if update_server_settings(server_id):
        await callback_query.answer(f"Выбран сервер: {server_id}")
        await manage_servers_callback(callback_query)
    else:
        await callback_query.answer("Ошибка при выборе сервера", show_alert=True)

@dp.callback_query_handler(lambda c: c.data in ['auth_password', 'auth_key'])
async def auth_type_callback(callback_query: types.CallbackQuery):
    if callback_query.from_user.id != admin:
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
    
    auth_type = callback_query.data.split('_')[1]
    user_main_messages[admin]['auth_type'] = auth_type
    
    if auth_type == 'password':
        user_main_messages[admin]['state'] = 'waiting_for_password'
        user_main_messages[admin]['auth_message_id'] = callback_query.message.message_id
        await bot.edit_message_text(
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            text="Введите пароль SSH:",
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("Отмена", callback_data="manage_servers")
            )
        )
    else:
        user_main_messages[admin]['state'] = 'waiting_for_key_path'
        user_main_messages[admin]['auth_message_id'] = callback_query.message.message_id
        await bot.edit_message_text(
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            text="Введите путь до приватного SSH-ключа (например /home/user/.ssh/id_rsa):",
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("Отмена", callback_data="manage_servers")
            )
        )
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data == 'delete_server')
async def delete_server_callback(callback_query: types.CallbackQuery):
    if callback_query.from_user.id != admin:
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
    
    servers = db.get_server_list()
    keyboard = InlineKeyboardMarkup(row_width=2)
    
    for server in servers:
        keyboard.insert(InlineKeyboardButton(
            f"🗑 {server}",
            callback_data=f"confirm_delete_server_{server}"
        ))
    
    keyboard.add(InlineKeyboardButton("Отмена", callback_data="manage_servers"))
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="Выберите сервер для удаления.\n\n*ВНИМАНИЕ*: При удалении сервера будут удалены все его пользователи и конфигурации!",
        parse_mode="Markdown",
        reply_markup=keyboard
    )
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('confirm_delete_server_'))
async def confirm_delete_server_callback(callback_query: types.CallbackQuery):
    if callback_query.from_user.id != admin:
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
    
    server_id = callback_query.data.split('confirm_delete_server_')[1]
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Да, удалить", callback_data=f"delete_server_confirmed_{server_id}"),
        InlineKeyboardButton("❌ Отмена", callback_data="manage_servers")
    )
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=f"⚠️ Вы уверены, что хотите удалить сервер *{server_id}*?\n\nЭто действие нельзя отменить!",
        parse_mode="Markdown",
        reply_markup=keyboard
    )
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('delete_server_confirmed_'))
async def delete_server_confirmed_callback(callback_query: types.CallbackQuery):
    if callback_query.from_user.id != admin:
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
    
    server_id = callback_query.data.split('delete_server_confirmed_')[1]
    
    if server_id == current_server:
        update_server_settings(None)
    
    success = db.remove_server(server_id)
    
    if success:
        await callback_query.answer("Сервер успешно удален", show_alert=True)
    else:
        await callback_query.answer("Ошибка при удалении сервера", show_alert=True)
    
    await manage_servers_callback(callback_query)

@dp.callback_query_handler(lambda c: c.data == 'add_server')
async def add_server_callback(callback_query: types.CallbackQuery):
    if callback_query.from_user.id != admin:
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
    
    main_chat_id = user_main_messages.get(admin, {}).get('chat_id')
    main_message_id = user_main_messages.get(admin, {}).get('message_id')
    
    if main_chat_id and main_message_id:
        user_main_messages[admin]['state'] = 'waiting_for_server_id'
        await bot.edit_message_text(
            chat_id=main_chat_id,
            message_id=main_message_id,
            text="Введите идентификатор нового сервера:",
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("Отмена", callback_data="manage_servers")
            )
        )
    else:
        await callback_query.answer("Ошибка: главное сообщение не найдено.", show_alert=True)
        return
    
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('home'))
async def return_home(callback_query: types.CallbackQuery):
    if callback_query.from_user.id != admin:
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
    main_chat_id = user_main_messages.get(admin, {}).get('chat_id')
    main_message_id = user_main_messages.get(admin, {}).get('message_id')
    if main_chat_id and main_message_id:
        user_main_messages[admin].pop('state', None)
        user_main_messages[admin].pop('client_name', None)
        user_main_messages[admin].pop('duration_choice', None)
        user_main_messages[admin].pop('traffic_limit', None)
        try:
            await bot.edit_message_text(
                chat_id=main_chat_id,
                message_id=main_message_id,
                text="Выберите действие:",
                reply_markup=main_menu_markup
            )
        except:
            sent_message = await callback_query.message.reply("Выберите действие:", reply_markup=main_menu_markup)
            user_main_messages[admin] = {'chat_id': sent_message.chat.id, 'message_id': sent_message.message_id}
            try:
                await bot.pin_chat_message(chat_id=sent_message.chat.id, message_id=sent_message.message_id, disable_notification=True)
            except:
                pass
    else:
        sent_message = await callback_query.message.reply("Выберите действие:", reply_markup=main_menu_markup)
        user_main_messages[admin] = {'chat_id': sent_message.chat.id, 'message_id': sent_message.message_id}
        try:
            await bot.pin_chat_message(chat_id=sent_message.chat.id, message_id=sent_message.message_id, disable_notification=True)
        except:
            pass
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('send_config_'))
async def send_user_config(callback_query: types.CallbackQuery):
    if callback_query.from_user.id != admin:
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
        
    if not current_server:
        await callback_query.answer("Сначала выберите сервер в разделе 'Управление серверами'", show_alert=True)
        return
    _, username = callback_query.data.split('send_config_', 1)
    username = username.strip()
    sent_messages = []
    try:
        user_dir = os.path.join('users', username)
        conf_path = os.path.join(user_dir, f'{username}.conf')
        if not os.path.exists(conf_path):
            await callback_query.answer("Конфигурационный файл пользователя отсутствует. Возможно, пользователь был создан вручную, и его конфигурация недоступна.", show_alert=True)
            return
        if os.path.exists(conf_path):
            vpn_key = await generate_vpn_key(conf_path)
            if vpn_key:
                instruction_text = (
                    "\nAmneziaVPN [Google Play](https://play.google.com/store/apps/details?id=org.amnezia.vpn&hl=ru), "
                    "[GitHub](https://github.com/amnezia-vpn/amnezia-client)"
                )
                formatted_key = format_vpn_key(vpn_key)
                key_message = f"```\n{formatted_key}\n```"
                caption = f"{instruction_text}\n{key_message}"
            else:
                caption = "VPN ключ не был сгенерирован."
            with open(conf_path, 'rb') as config:
                sent_doc = await bot.send_document(
                    admin,
                    config,
                    caption=caption,
                    parse_mode="Markdown",
                    disable_notification=True
                )
                sent_messages.append(sent_doc.message_id)
        else:
            confirmation_text = f"Не удалось создать конфигурацию для пользователя **{username}**."
            sent_message = await bot.send_message(admin, confirmation_text, parse_mode="Markdown", disable_notification=True)
            asyncio.create_task(delete_message_after_delay(admin, sent_message.message_id, delay=15))
            await callback_query.answer()
            return
    except Exception as e:
        confirmation_text = f"Произошла ошибка: {e}"
        sent_message = await bot.send_message(admin, confirmation_text, parse_mode="Markdown", disable_notification=True)
        asyncio.create_task(delete_message_after_delay(admin, sent_message.message_id, delay=15))
        await callback_query.answer()
        return
    if not sent_messages:
        confirmation_text = f"Не удалось найти файлы конфигурации для пользователя **{username}**."
        sent_message = await bot.send_message(admin, confirmation_text, parse_mode="Markdown", disable_notification=True)
        asyncio.create_task(delete_message_after_delay(admin, sent_message.message_id, delay=15))
        await callback_query.answer()
        return
    else:
        confirmation_text = f"Конфигурация для **{username}** отправлена."
        sent_confirmation = await bot.send_message(
            chat_id=admin,
            text=confirmation_text,
            parse_mode="Markdown",
            disable_notification=True
        )
        asyncio.create_task(delete_message_after_delay(admin, sent_confirmation.message_id, delay=15))
    for message_id in sent_messages:
        asyncio.create_task(delete_message_after_delay(admin, message_id, delay=15))
        
    clients = db.get_client_list(server_id=current_server)
    client_info = next((c for c in clients if c[0] == username), None)
    
    if client_info:
        expiration_time = db.get_user_expiration(username, server_id=current_server)
        traffic_limit = db.get_user_traffic_limit(username, server_id=current_server)
        status = "🔴 Offline"
        incoming_traffic = "↓—"
        outgoing_traffic = "↑—"
        ipv4_address = "—"
        total_bytes = 0
        formatted_total = "0.00B"

        active_clients = db.get_active_list(server_id=current_server)
        active_info = None
        for ac in active_clients:
            if isinstance(ac, dict) and ac.get('name') == username:
                active_info = ac
                break
            elif isinstance(ac, (list, tuple)) and ac[0] == username:
                active_info = {'name': ac[0], 'last_handshake': ac.get(1, 'never'), 'transfer': ac.get(2, '0/0')}
                break

        if active_info:
            last_handshake_str = active_info.get('last_handshake', 'never')
            if last_handshake_str.lower() not in ['never', 'нет данных', '-']:
                try:
                    last_handshake_dt = parse_relative_time(last_handshake_str)
                    if last_handshake_dt:
                        delta = datetime.now(pytz.UTC) - last_handshake_dt
                        if delta <= timedelta(minutes=1):
                            status = "🟢 Online"
                        else:
                            status = "🔴 Offline"

                    transfer = active_info.get('transfer', '0/0')
                    incoming_bytes, outgoing_bytes = parse_transfer(transfer)
                    incoming_traffic = f"↓{humanize_bytes(incoming_bytes)}"
                    outgoing_traffic = f"↑{humanize_bytes(outgoing_bytes)}"
                    traffic_data = await update_traffic(username, incoming_bytes, outgoing_bytes)
                    total_bytes = traffic_data.get('total_incoming', 0) + traffic_data.get('total_outgoing', 0)
                    formatted_total = humanize_bytes(total_bytes)
                except ValueError:
                    logger.error(f"Некорректный формат даты для пользователя {username}: {last_handshake_str}")

        allowed_ips = client_info[2]
        ipv4_match = re.search(r'(\d{1,3}\.){3}\d{1,3}/\d+', allowed_ips)
        ipv4_address = ipv4_match.group(0) if ipv4_match else "—"

        if expiration_time:
            now = datetime.now(pytz.UTC)
            try:
                expiration_dt = expiration_time
                if expiration_dt.tzinfo is None:
                    expiration_dt = expiration_dt.replace(tzinfo=pytz.UTC)
                remaining = expiration_dt - now
                if remaining.total_seconds() > 0:
                    days, seconds = remaining.days, remaining.seconds
                    hours = seconds // 3600
                    minutes = (seconds % 3600) // 60
                    date_end = f"📅 {days}д {hours}ч {minutes}м"
                else:
                    date_end = "📅 ♾️ Неограниченно"
            except Exception as e:
                logger.error(f"Ошибка при обработке даты окончания: {e}")
                date_end = "📅 ♾️ Неограниченно"
        else:
            date_end = "📅 ♾️ Неограниченно"

        traffic_limit_display = "♾️ Неограниченно" if traffic_limit == "Неограниченно" else traffic_limit

        text = (
            f"📧 _Имя:_ {username}\n"
            f"🌐 _IPv4:_ {ipv4_address}\n"
            f"🌐 _Статус соединения:_ {status}\n"
            f"{date_end}\n"
            f"🔼 _Исходящий трафик:_ {incoming_traffic}\n"
            f"🔽 _Входящий трафик:_ {outgoing_traffic}\n"
            f"📊 _Всего:_ ↑↓{formatted_total} из **{traffic_limit_display}**\n"
        )

    if client_info:
        keyboard = InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            InlineKeyboardButton("IP info", callback_data=f"ip_info_{username}"),
            InlineKeyboardButton("Подключения", callback_data=f"connections_{username}"),
            InlineKeyboardButton("Получить конфигурацию", callback_data=f"send_config_{username}")
        )
        keyboard.add(
            InlineKeyboardButton("Удалить", callback_data=f"delete_user_{username}")
        )
        keyboard.add(
            InlineKeyboardButton("Назад", callback_data="list_users"),
            InlineKeyboardButton("Домой", callback_data="home")
        )
        
        try:
            await bot.edit_message_text(
                chat_id=callback_query.message.chat.id,
                message_id=callback_query.message.message_id,
                text=text,
                parse_mode="Markdown",
                reply_markup=keyboard
            )
        except aiogram_exceptions.MessageNotModified:
            pass
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('create_backup'))
async def create_backup_callback(callback_query: types.CallbackQuery):
    if callback_query.from_user.id != admin:
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
        
    if not current_server:
        await callback_query.answer("Сначала выберите сервер в разделе 'Управление серверами'", show_alert=True)
        return
    date_str = datetime.now().strftime('%Y-%m-%d')
    backup_filename = f"backup_{date_str}.zip"
    backup_filepath = os.path.join(os.getcwd(), backup_filename)
    try:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, create_zip, backup_filepath)
        if os.path.exists(backup_filepath):
            with open(backup_filepath, 'rb') as f:
                await bot.send_document(admin, f, caption=backup_filename, disable_notification=True)
            os.remove(backup_filepath)
        else:
            logger.error(f"Бекап файл не создан: {backup_filepath}")
            await bot.send_message(admin, "Не удалось создать бекап.", disable_notification=True)
    except Exception as e:
        logger.error(f"Ошибка при создании бекапа: {e}")
        await bot.send_message(admin, "Не удалось создать бекап.", disable_notification=True)
    await callback_query.answer()

def parse_transfer(transfer_str):
    try:
        if '/' in transfer_str:
            incoming, outgoing = transfer_str.split('/')
            incoming = incoming.strip()
            outgoing = outgoing.strip()
            incoming_match = re.match(r'([\d.]+)\s*(\w+)', incoming)
            outgoing_match = re.match(r'([\d.]+)\s*(\w+)', outgoing)
            def convert_to_bytes(value, unit):
                size_map = {
                    'B': 1,
                    'KB': 10**3,
                    'KiB': 1024,
                    'MB': 10**6,
                    'MiB': 1024**2,
                    'GB': 10**9,
                    'GiB': 1024**3,
                }
                return float(value) * size_map.get(unit, 1)
            incoming_bytes = convert_to_bytes(*incoming_match.groups()) if incoming_match else 0
            outgoing_bytes = convert_to_bytes(*outgoing_match.groups()) if outgoing_match else 0
            return incoming_bytes, outgoing_bytes
        else:
            parts = re.split(r'[/,]', transfer_str)
            if len(parts) >= 2:
                incoming = parts[0].strip()
                outgoing = parts[1].strip()
                incoming_match = re.match(r'([\d.]+)\s*(\w+)', incoming)
                outgoing_match = re.match(r'([\d.]+)\s*(\w+)', outgoing)
                def convert_to_bytes(value, unit):
                    size_map = {
                        'B': 1,
                        'KB': 10**3,
                        'KiB': 1024,
                        'MB': 10**6,
                        'MiB': 1024**2,
                        'GB': 10**9,
                        'GiB': 1024**3,
                    }
                    return float(value) * size_map.get(unit, 1)
                incoming_bytes = convert_to_bytes(*incoming_match.groups()) if incoming_match else 0
                outgoing_bytes = convert_to_bytes(*outgoing_match.groups()) if outgoing_match else 0
                return incoming_bytes, outgoing_bytes
            else:
                return 0, 0
    except Exception as e:
        logger.error(f"Ошибка при парсинге трафика: {e}")
        return 0, 0

def humanize_bytes(bytes_value):
    return humanize.naturalsize(bytes_value, binary=False)

async def read_traffic(username, server_id='default'):
    traffic_file = os.path.join('users', username, f'traffic_{server_id}.json')
    os.makedirs(os.path.dirname(traffic_file), exist_ok=True)
    if not os.path.exists(traffic_file):
        traffic_data = {
            "total_incoming": 0,
            "total_outgoing": 0,
            "last_incoming": 0,
            "last_outgoing": 0
        }
        async with aiofiles.open(traffic_file, 'w') as f:
            await f.write(json.dumps(traffic_data))
        return traffic_data
    else:
        async with aiofiles.open(traffic_file, 'r') as f:
            content = await f.read()
            try:
                traffic_data = json.loads(content)
                return traffic_data
            except json.JSONDecodeError:
                logger.error(f"Ошибка при чтении traffic.json для пользователя {username}. Инициализация заново.")
                traffic_data = {
                    "total_incoming": 0,
                    "total_outgoing": 0,
                    "last_incoming": 0,
                    "last_outgoing": 0
                }
                async with aiofiles.open(traffic_file, 'w') as f_write:
                    await f_write.write(json.dumps(traffic_data))
                return traffic_data

async def update_traffic(username, incoming_bytes, outgoing_bytes, server_id='default'):
    traffic_data = await read_traffic(username, server_id)
    delta_incoming = incoming_bytes - traffic_data.get('last_incoming', 0)
    delta_outgoing = outgoing_bytes - traffic_data.get('last_outgoing', 0)
    if delta_incoming < 0:
        delta_incoming = 0
    if delta_outgoing < 0:
        delta_outgoing = 0
    traffic_data['total_incoming'] += delta_incoming
    traffic_data['total_outgoing'] += delta_outgoing
    traffic_data['last_incoming'] = incoming_bytes
    traffic_data['last_outgoing'] = outgoing_bytes
    traffic_file = os.path.join('users', username, f'traffic_{server_id}.json')
    async with aiofiles.open(traffic_file, 'w') as f:
        await f.write(json.dumps(traffic_data))
    return traffic_data

async def update_all_clients_traffic():
    if not current_server:
        logger.info("Сервер не выбран, пропуск обновления трафика")
        return
        
    logger.info(f"Начало обновления трафика для всех клиентов на сервере {current_server}")
    active_clients = db.get_active_list(server_id=current_server)
    for client in active_clients:
        username = client.get('name')
        transfer = client.get('transfer', '0/0')
        incoming_bytes, outgoing_bytes = parse_transfer(transfer)
        traffic_data = await update_traffic(username, incoming_bytes, outgoing_bytes, current_server)
        logger.info(f"Обновлён трафик для пользователя {username}: Входящий {traffic_data['total_incoming']} B, Исходящий {traffic_data['total_outgoing']} B")
        traffic_limit = db.get_user_traffic_limit(username, server_id=current_server)
        if traffic_limit != "Неограниченно":
            limit_bytes = parse_traffic_limit(traffic_limit)
            total_bytes = traffic_data.get('total_incoming', 0) + traffic_data.get('total_outgoing', 0)
            if total_bytes >= limit_bytes:
                await deactivate_user(username)
    logger.info("Завершено обновление трафика для всех клиентов.")

async def generate_vpn_key(conf_path: str) -> str:
    try:
        process = await asyncio.create_subprocess_exec(
            'python3.11',
            'awg-decode.py',
            '--encode',
            conf_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            logger.error(f"awg-decode.py ошибка: {stderr.decode().strip()}")
            return ""
        vpn_key = stdout.decode().strip()
        if vpn_key.startswith('vpn://'):
            return vpn_key
        else:
            logger.error(f"awg-decode.py вернул некорректный формат: {vpn_key}")
            return ""
    except Exception as e:
        logger.error(f"Ошибка при вызове awg-decode.py: {e}")
        return ""

async def deactivate_user(client_name: str):
    success = db.deactive_user_db(client_name, server_id=current_server)
    if success:
        db.remove_user_expiration(client_name)
        try:
            scheduler.remove_job(job_id=client_name)
        except:
            pass
        user_dir = os.path.join('users', client_name)
        try:
            if os.path.exists(user_dir):
                shutil.rmtree(user_dir)
        except Exception as e:
            logger.error(f"Ошибка при удалении директории для пользователя {client_name}: {e}")
            
        connections_file = os.path.join('files', 'connections', f'{client_name}_ip.json')
        try:
            if os.path.exists(connections_file):
                os.remove(connections_file)
        except Exception as e:
            logger.error(f"Ошибка при удалении файла подключений для пользователя {client_name}: {e}")
        confirmation_text = f"Конфигурация пользователя **{client_name}** была деактивирована из-за превышения лимита трафика."
        sent_message = await bot.send_message(admin, confirmation_text, parse_mode="Markdown", disable_notification=True)
        asyncio.create_task(delete_message_after_delay(admin, sent_message.message_id, delay=15))
    else:
        sent_message = await bot.send_message(admin, f"Не удалось деактивировать пользователя **{client_name}**.", parse_mode="Markdown", disable_notification=True)
        asyncio.create_task(delete_message_after_delay(admin, sent_message.message_id, delay=15))

async def check_environment():
    if not current_server:
        logger.error("Сервер не выбран")
        return False
        
    servers = db.load_servers()
    if current_server not in servers:
        logger.error(f"Сервер {current_server} не найден в конфигурации")
        return False
        
    server_config = servers[current_server]
    try:
        if server_config.get('is_remote') == 'true':
            ssh = db.SSHManager(current_server)
            if not ssh.connect():
                logger.error("Не удалось установить SSH соединение")
                return False
                
            cmd = f"docker ps --filter 'name={DOCKER_CONTAINER}' --format '{{{{.Names}}}}'"
            output, error = ssh.execute_command(cmd)
            if not output or DOCKER_CONTAINER not in output:
                logger.error(f"Контейнер Docker '{DOCKER_CONTAINER}' не найден. Необходима инициализация AmneziaVPN.")
                return False

            cmd = f"docker exec {DOCKER_CONTAINER} test -f {WG_CONFIG_FILE}"
            output, error = ssh.execute_command(cmd)
            if error and 'No such file' in error:
                logger.error(f"Конфигурационный файл WireGuard '{WG_CONFIG_FILE}' не найден в контейнере '{DOCKER_CONTAINER}'.")
                return False
        else:
            cmd = f"docker ps --filter 'name={DOCKER_CONTAINER}' --format '{{{{.Names}}}}'"
            container_names = subprocess.check_output(cmd, shell=True).decode().strip().split('\n')
            if DOCKER_CONTAINER not in container_names:
                logger.error(f"Контейнер Docker '{DOCKER_CONTAINER}' не найден. Необходима инициализация AmneziaVPN.")
                return False

            cmd = f"docker exec {DOCKER_CONTAINER} test -f {WG_CONFIG_FILE}"
            try:
                subprocess.check_call(cmd, shell=True)
            except subprocess.CalledProcessError:
                logger.error(f"Конфигурационный файл WireGuard '{WG_CONFIG_FILE}' не найден в контейнере '{DOCKER_CONTAINER}'.")
                return False

        return True
    except Exception as e:
        logger.error(f"Ошибка при проверке окружения: {e}")
        return False

async def periodic_ensure_peer_names():
    db.ensure_peer_names(server_id=current_server)

async def on_startup(dp):
    os.makedirs('files/connections', exist_ok=True)
    os.makedirs('users', exist_ok=True)
    await load_isp_cache_task()
    
    global current_server
    if not current_server:
        servers = db.get_server_list()
        if servers:
            current_server = servers[0]
            if update_server_settings(current_server):
                logger.info(f"Выбран сервер по умолчанию: {current_server}")
            else:
                logger.error(f"Ошибка при инициализации сервера по умолчанию: {current_server}")
                await bot.send_message(admin, "Ошибка при инициализации сервера. Проверьте настройки в разделе 'Управление серверами'")
                return
        else:
            logger.error("Не найдено ни одного сервера")
            await bot.send_message(admin, "Не найдено ни одного сервера. Добавьте сервер через меню 'Управление серверами'")
            return
    
    environment_ok = await check_environment()
    if not environment_ok:
        logger.error("Необходимо инициализировать AmneziaVPN перед запуском бота.")
        await bot.send_message(admin, "Необходимо инициализировать AmneziaVPN перед запуском бота.")
        await bot.close()
        sys.exit(1)
    if not scheduler.running:
        scheduler.add_job(update_all_clients_traffic, IntervalTrigger(minutes=1))
        scheduler.add_job(periodic_ensure_peer_names, IntervalTrigger(minutes=1))
        scheduler.start()
        logger.info("Планировщик запущен для обновления трафика каждые 5 минут.")
    users = db.get_users_with_expiration(server_id=current_server)
    for user in users:
        client_name, expiration_time, traffic_limit = user
        if expiration_time:
            try:
                expiration_datetime = datetime.fromisoformat(expiration_time)
            except ValueError:
                logger.error(f"Некорректный формат даты для пользователя {client_name}: {expiration_time}")
                continue
            if expiration_datetime.tzinfo is None:
                expiration_datetime = expiration_datetime.replace(tzinfo=pytz.UTC)
            if expiration_datetime > datetime.now(pytz.UTC):
                scheduler.add_job(
                    deactivate_user,
                    trigger=DateTrigger(run_date=expiration_datetime),
                    args=[client_name],
                    id=client_name
                )
                logger.info(f"Запланирована деактивация пользователя {client_name} на {expiration_datetime}")
            else:
                await deactivate_user(client_name)

async def on_shutdown(dp):
    scheduler.shutdown()
    logger.info("Планировщик остановлен.")

executor.start_polling(dp, on_startup=on_startup, on_shutdown=on_shutdown)