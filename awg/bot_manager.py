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
from aiogram import Bot, types
from aiogram.dispatcher import Dispatcher
from aiogram.utils import executor
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

setting = db.get_config()
bot_token = setting.get('bot_token')
admin_id = setting.get('admin_id')
wg_config_file = setting.get('wg_config_file')
docker_container = setting.get('docker_container')
endpoint = setting.get('endpoint')

if not all([bot_token, admin_id, wg_config_file, docker_container, endpoint]):
    logger.error("Некоторые обязательные настройки отсутствуют в конфигурационном файле.")
    sys.exit(1)

bot = Bot(bot_token)
admin = int(admin_id)
WG_CONFIG_FILE = wg_config_file
DOCKER_CONTAINER = docker_container
ENDPOINT = endpoint

dp = Dispatcher(bot)
scheduler = AsyncIOScheduler(timezone=pytz.UTC)
scheduler.start()

main_menu_markup = InlineKeyboardMarkup(row_width=1).add(
    InlineKeyboardButton("Добавить пользователя", callback_data="add_user"),
    InlineKeyboardButton("Получить конфигурацию пользователя", callback_data="get_config"),
    InlineKeyboardButton("Список клиентов", callback_data="list_users"),
    InlineKeyboardButton("Создать бекап", callback_data="create_backup")
)

user_main_messages = {}
isp_cache = {}
ISP_CACHE_FILE = 'files/isp_cache.json'
CACHE_TTL = timedelta(hours=24)

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

@dp.message_handler(commands=['start', 'help'])
async def help_command_handler(message: types.Message):
    if message.chat.id == admin:
        sent_message = await message.answer("Выберите действие:", reply_markup=main_menu_markup)
        user_main_messages[admin] = (sent_message.chat.id, sent_message.message_id)
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
    if user_main_messages.get('waiting_for_user_name'):
        user_name = message.text.strip()
        if not all(c.isalnum() or c in "-_" for c in user_name):
            await message.reply("Имя пользователя может содержать только буквы, цифры, дефисы и подчёркивания.")
            asyncio.create_task(delete_message_after_delay(message.chat.id, message.message_id, delay=5))
            return
        user_main_messages['client_name'] = user_name
        user_main_messages['waiting_for_user_name'] = False
        duration_buttons = [
            InlineKeyboardButton("1 час", callback_data=f"duration_1h_{user_name}_noipv6"),
            InlineKeyboardButton("1 день", callback_data=f"duration_1d_{user_name}_noipv6"),
            InlineKeyboardButton("1 неделя", callback_data=f"duration_1w_{user_name}_noipv6"),
            InlineKeyboardButton("1 месяц", callback_data=f"duration_1m_{user_name}_noipv6"),
            InlineKeyboardButton("Без ограничений", callback_data=f"duration_unlimited_{user_name}_noipv6"),
            InlineKeyboardButton("Домой", callback_data="home")
        ]
        duration_markup = InlineKeyboardMarkup(row_width=1).add(*duration_buttons)
        main_chat_id, main_message_id = user_main_messages.get(admin, (None, None))
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

@dp.callback_query_handler(lambda c: c.data == "add_user")
async def prompt_for_user_name(callback_query: types.CallbackQuery):
    if callback_query.from_user.id != admin:
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
    main_chat_id, main_message_id = user_main_messages.get(admin, (None, None))
    if main_chat_id and main_message_id:
        await bot.edit_message_text(
            chat_id=main_chat_id,
            message_id=main_message_id,
            text="Введите имя пользователя для добавления:",
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("Домой", callback_data="home")
            )
        )
        user_main_messages['waiting_for_user_name'] = True
    else:
        await callback_query.answer("Ошибка: главное сообщение не найдено.", show_alert=True)
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('duration_'))
async def set_config_duration(callback: types.CallbackQuery):
    if callback.from_user.id != admin:
        await callback.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
    parts = callback.data.split('_')
    duration_choice = parts[1]
    client_name = parts[2]
    ipv6_flag = parts[3] if len(parts) > 3 else 'noipv6'
    main_chat_id, main_message_id = user_main_messages.get(admin, (None, None))
    if not main_chat_id or not main_message_id:
        await callback.answer("Ошибка: главное сообщение не найдено.", show_alert=True)
        return
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
        await bot.send_message(admin, "Неверный выбор времени.", reply_markup=main_menu_markup, disable_notification=True)
        asyncio.create_task(delete_message_after_delay(admin, main_message_id, delay=2))
        return
    success = db.root_add(client_name, ipv6=False)
    if success:
        try:
            conf_path = os.path.join('users', client_name, f'{client_name}.conf')
            png_path = os.path.join('users', client_name, f'{client_name}.png')
            
            if os.path.exists(png_path):
                with open(png_path, 'rb') as photo:
                    sent_photo = await bot.send_photo(admin, photo, disable_notification=True)
                    asyncio.create_task(delete_message_after_delay(admin, sent_photo.message_id, delay=15))
            
            vpn_key = ""
            if os.path.exists(conf_path):
                vpn_key = await generate_vpn_key(conf_path)
            
            if vpn_key:
                instruction_text = (
                    "\nAmneziaWG [Google play](https://play.google.com/store/apps/details?id=org.amnezia.awg&hl=ru), "
                    "[GitHub](https://github.com/amnezia-vpn/amneziawg-android)\n"
                    "AmneziaVPN [Google play](https://play.google.com/store/apps/details?id=org.amnezia.vpn&hl=ru), "
                    "[GitHub](https://github.com/amnezia-vpn/amnezia-client)\n"
                )
                caption = f"\n{instruction_text}\n```{vpn_key}```"
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
            confirmation_text = "Произошла ошибка."
            sent_message = await bot.send_message(admin, confirmation_text, parse_mode="Markdown", disable_notification=True)
            asyncio.create_task(delete_message_after_delay(admin, sent_message.message_id, delay=15))
            await callback_query.answer()
            return
        if duration:
            expiration_time = datetime.now(pytz.UTC) + duration
            scheduler.add_job(
                deactivate_user,
                trigger=DateTrigger(run_date=expiration_time),
                args=[client_name],
                id=client_name
            )
            db.set_user_expiration(client_name, expiration_time)
            confirmation_text = f"Пользователь **{client_name}** добавлен. Конфигурация истечет через **{duration_choice}**."
        else:
            db.set_user_expiration(client_name, None)
            confirmation_text = f"Пользователь **{client_name}** добавлен с неограниченным временем действия."
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
    await bot.edit_message_text(
        chat_id=main_chat_id,
        message_id=main_message_id,
        text="Выберите действие:",
        reply_markup=main_menu_markup
    )
    await callback.answer()

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

@dp.callback_query_handler(lambda c: c.data == "list_users")
async def list_users_callback(callback_query: types.CallbackQuery):
    if callback_query.from_user.id != admin:
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
    clients = db.get_client_list()
    if not clients:
        await callback_query.answer("Список пользователей пуст.", show_alert=True)
        return
    active_clients = db.get_active_list()
    active_clients_dict = {}
    for client in active_clients:
        username = client[0]
        last_handshake = client[1]
        active_clients_dict[username] = last_handshake
    keyboard = InlineKeyboardMarkup(row_width=2)
    now = datetime.now(pytz.UTC)
    for client in clients:
        username = client[0]
        last_handshake_str = active_clients_dict.get(username)
        if last_handshake_str and last_handshake_str != 'Нет данных':
            last_handshake = parse_relative_time(last_handshake_str)
            delta = now - last_handshake
            if delta <= timedelta(days=5):
                status_symbol = '✅'
            else:
                status_symbol = '❌'
        else:
            status_symbol = '❌'
        button_text = f"{status_symbol} {username}"
        keyboard.insert(InlineKeyboardButton(button_text, callback_data=f"client_{username}"))
    keyboard.add(InlineKeyboardButton("Домой", callback_data="home"))
    main_chat_id, main_message_id = user_main_messages.get(admin, (None, None))
    if main_chat_id and main_message_id:
        await bot.edit_message_text(
            chat_id=main_chat_id,
            message_id=main_message_id,
            text="Выберите пользователя:",
            reply_markup=keyboard
        )
    else:
        sent_message = await callback_query.message.reply("Выберите пользователя:", reply_markup=keyboard)
        user_main_messages[admin] = (sent_message.chat.id, sent_message.message_id)
        try:
            await bot.pin_chat_message(chat_id=sent_message.chat.id, message_id=sent_message.message_id, disable_notification=True)
        except:
            pass
    await callback_query.answer()

def format_transfer(transfer_str):
    try:
        if '/' in transfer_str:
            incoming, outgoing = transfer_str.split('/')
            incoming = incoming.strip()
            outgoing = outgoing.strip()

            incoming_match = re.match(r'([\d.]+)\s*(\w+)', incoming)
            outgoing_match = re.match(r'([\d.]+)\s*(\w+)', outgoing)

            if incoming_match:
                incoming_value, incoming_unit = incoming_match.groups()
                incoming = humanize.naturalsize(float(incoming_value), binary=False)
            else:
                incoming = "—"

            if outgoing_match:
                outgoing_value, outgoing_unit = outgoing_match.groups()
                outgoing = humanize.naturalsize(float(outgoing_value), binary=False)
            else:
                outgoing = "—"

            return incoming, outgoing
        else:
            parts = re.split(r'[/,]', transfer_str)
            if len(parts) >= 2:
                incoming = parts[0].strip()
                outgoing = parts[1].strip()

                incoming_match = re.match(r'([\d.]+)\s*(\w+)', incoming)
                outgoing_match = re.match(r'([\d.]+)\s*(\w+)', outgoing)

                if incoming_match:
                    incoming_value, incoming_unit = incoming_match.groups()
                    incoming = humanize.naturalsize(float(incoming_value), binary=False)
                else:
                    incoming = "—"

                if outgoing_match:
                    outgoing_value, outgoing_unit = outgoing_match.groups()
                    outgoing = humanize.naturalsize(float(outgoing_value), binary=False)
                else:
                    outgoing = "—"

                return incoming, outgoing
            else:
                return "—", "—"
    except Exception as e:
        logger.error(f"Ошибка при форматировании трафика: {e}")
        return "—", "—"

@dp.callback_query_handler(lambda c: c.data.startswith('client_'))
async def client_selected_callback(callback_query: types.CallbackQuery):
    _, username = callback_query.data.split('client_', 1)
    username = username.strip()
    clients = db.get_client_list()
    client_info = next((c for c in clients if c[0] == username), None)
    if not client_info:
        await callback_query.answer("Ошибка: пользователь не найден.", show_alert=True)
        return

    expiration_time = db.get_user_expiration(username)
    status = "🔴 Офлайн"
    incoming_traffic = "↓—"
    outgoing_traffic = "↑—"
    ipv4_address = "—"

    active_clients = db.get_active_list()
    active_info = next((ac for ac in active_clients if ac[0] == username), None)
    if active_info:
        last_handshake_str = active_info[1].lower()
        if last_handshake_str not in ['never', 'нет данных', '-']:
            status = "🟢 Онлайн"
            transfer = active_info[2]
            incoming, outgoing = format_transfer(transfer)
            incoming_traffic = f"↓{incoming}"
            outgoing_traffic = f"↑{outgoing}"

    allowed_ips = client_info[2]
    ipv4_match = re.search(r'(\d{1,3}\.){3}\d{1,3}/\d+', allowed_ips)
    if ipv4_match:
        ipv4_address = ipv4_match.group(0)
    else:
        ipv4_address = "—"

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
                date_end = f"{days}д {hours}ч {minutes}м"
            else:
                date_end = "♾️ Неограниченно"
        except Exception as e:
            logger.error(f"Ошибка при обработке даты окончания: {e}")
            date_end = "♾️ Неограниченно"
    else:
        date_end = "♾️ Неограниченно"

    text = (
        f"🌐 *IPv4:* {ipv4_address}\n"
        f"🌐 *Статус соединения:* {status}\n"
        f"📅 *Дата окончания:* {date_end}\n"
        f"🔼 *Исходящий трафик:* {outgoing_traffic}\n"
        f"🔽 *Входящий трафик:* {incoming_traffic}\n"
    )

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("IP info", callback_data=f"ip_info_{username}"),
        InlineKeyboardButton("Подключения", callback_data=f"connections_{username}")
    )
    keyboard.add(
        InlineKeyboardButton("Удалить", callback_data=f"delete_user_{username}")
    )
    keyboard.add(
        InlineKeyboardButton("Назад", callback_data="list_users"),
        InlineKeyboardButton("Домой", callback_data="home")
    )

    main_chat_id, main_message_id = user_main_messages.get(admin, (None, None))
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

@dp.callback_query_handler(lambda c: c.data.startswith('connections_'))
async def client_connections_callback(callback_query: types.CallbackQuery):
    _, username = callback_query.data.split('connections_', 1)
    username = username.strip()
    file_path = os.path.join('files', 'connections', f'{username}_ip.json')
    if not os.path.exists(file_path):
        await callback_query.answer("Нет данных о подключениях пользователя.", show_alert=True)
        return
    try:
        async with aiofiles.open(file_path, 'r') as f:
            data = json.loads(await f.read())
        sorted_ips = sorted(data.items(), key=lambda x: datetime.strptime(x[1], '%d.%m.%Y %H:%M'), reverse=True)
        last_connections = sorted_ips[:5]
        isp_tasks = [get_isp_info(ip) for ip, _ in last_connections]
        isp_results = await asyncio.gather(*isp_tasks)
        connections_text = f"*Последние подключения пользователя {username}:*\n"
        for (ip, timestamp), isp in zip(last_connections, isp_results):
            connections_text += f"{ip} ({isp}) - {timestamp}\n"
        keyboard = InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            InlineKeyboardButton("Назад", callback_data=f"client_{username}"),
            InlineKeyboardButton("Домой", callback_data="home")
        )
        await bot.edit_message_text(
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            text=connections_text,
            parse_mode="Markdown",
            reply_markup=keyboard
        )
    except:
        await callback_query.answer("Ошибка при получении данных о подключениях.", show_alert=True)
        return
    await cleanup_connection_data(username)
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('ip_info_'))
async def ip_info_callback(callback_query: types.CallbackQuery):
    _, username = callback_query.data.split('ip_info_', 1)
    username = username.strip()

    active_clients = db.get_active_list()
    active_info = next((ac for ac in active_clients if ac[0] == username), None)
    if active_info:
        endpoint = active_info[3]
        ip_address = endpoint.split(':')[0]
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

    main_chat_id, main_message_id = user_main_messages.get(admin, (None, None))
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
    username = callback_query.data.split('delete_user_')[1]
    success = db.deactive_user_db(username)
    if success:
        db.remove_user_expiration(username)
        try:
            scheduler.remove_job(job_id=username)
        except:
            pass
        conf_path = os.path.join('users', username, f'{username}.conf')
        png_path = os.path.join('users', username, f'{username}.png')
        try:
            if os.path.exists(conf_path):
                os.remove(conf_path)
            if os.path.exists(png_path):
                os.remove(png_path)
        except Exception as e:
            logger.error(f"Ошибка при удалении файлов для пользователя {username}: {e}")
        confirmation_text = f"Пользователь **{username}** успешно удален."
    else:
        confirmation_text = f"Не удалось удалить пользователя **{username}**."
    main_chat_id, main_message_id = user_main_messages.get(admin, (None, None))
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

@dp.callback_query_handler(lambda c: c.data == "home")
async def return_home(callback_query: types.CallbackQuery):
    if callback_query.from_user.id != admin:
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
    main_chat_id, main_message_id = user_main_messages.get(admin, (None, None))
    if main_chat_id and main_message_id:
        user_main_messages.pop('waiting_for_user_name', None)
        user_main_messages.pop('client_name', None)
        try:
            await bot.edit_message_text(
                chat_id=main_chat_id,
                message_id=main_message_id,
                text="Выберите действие:",
                reply_markup=main_menu_markup
            )
        except:
            sent_message = await callback_query.message.reply("Выберите действие:", reply_markup=main_menu_markup)
            user_main_messages[admin] = (sent_message.chat.id, sent_message.message_id)
            try:
                await bot.pin_chat_message(chat_id=sent_message.chat.id, message_id=sent_message.message_id, disable_notification=True)
            except:
                pass
    else:
        sent_message = await callback_query.message.reply("Выберите действие:", reply_markup=main_menu_markup)
        user_main_messages[admin] = (sent_message.chat.id, sent_message.message_id)
        try:
            await bot.pin_chat_message(chat_id=sent_message.chat.id, message_id=sent_message.message_id, disable_notification=True)
        except:
            pass
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data == "get_config")
async def list_users_for_config(callback_query: types.CallbackQuery):
    if callback_query.from_user.id != admin:
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
        return
    clients = db.get_client_list()
    if not clients:
        await callback_query.answer("Список пользователей пуст.", show_alert=True)
        return
    keyboard = InlineKeyboardMarkup(row_width=2)
    for client in clients:
        username = client[0]
        keyboard.insert(InlineKeyboardButton(username, callback_data=f"send_config_{username}"))
    keyboard.add(InlineKeyboardButton("Домой", callback_data="home"))
    main_chat_id, main_message_id = user_main_messages.get(admin, (None, None))
    if main_chat_id and main_message_id:
        await bot.edit_message_text(
            chat_id=main_chat_id,
            message_id=main_message_id,
            text="Выберите пользователя для получения конфигурации:",
            reply_markup=keyboard
        )
    else:
        sent_message = await callback_query.message.reply("Выберите пользователя для получения конфигурации:", reply_markup=keyboard)
        user_main_messages[admin] = (sent_message.chat.id, sent_message.message_id)
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
    _, username = callback_query.data.split('send_config_', 1)
    username = username.strip()
    sent_messages = []
    try:
        user_dir = os.path.join('users', username)
        conf_path = os.path.join(user_dir, f'{username}.conf')
        png_path = os.path.join(user_dir, f'{username}.png')

        if not os.path.exists(conf_path):
            success = db.root_add(username, ipv6=False)
            if not success:
                await callback_query.answer("Не удалось создать конфигурацию пользователя.", show_alert=True)
                return

        if os.path.exists(png_path):
            with open(png_path, 'rb') as photo:
                sent_photo = await bot.send_photo(admin, photo, disable_notification=True)
                sent_messages.append(sent_photo.message_id)

        if os.path.exists(conf_path):
            vpn_key = await generate_vpn_key(conf_path)
            if vpn_key:
                instruction_text = (
                    "\nAmneziaWG [Google play](https://play.google.com/store/apps/details?id=org.amnezia.awg&hl=ru), "
                    "[GitHub](https://github.com/amnezia-vpn/amneziawg-android)\n"
                    "AmneziaVPN [Google play](https://play.google.com/store/apps/details?id=org.amnezia.vpn&hl=ru), "
                    "[GitHub](https://github.com/amnezia-vpn/amnezia-client)\n"
                )
                caption = f"\n{instruction_text}\n```{vpn_key}```"
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
    await callback_query.answer()

@dp.callback_query_handler(lambda c: c.data == "create_backup")
async def create_backup_callback(callback_query: types.CallbackQuery):
    if callback_query.from_user.id != admin:
        await callback_query.answer("У вас нет прав для выполнения этого действия.", show_alert=True)
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


def parse_relative_time(time_str):
    now = datetime.now(pytz.UTC)
    delta = timedelta()
    parts = time_str.strip().split(',')
    for part in parts:
        part = part.strip()
        match = re.match(r'(\d+)\s+(day|hour|minute|second)s?', part)
        if match:
            value = int(match.group(1))
            unit = match.group(2)
            if unit == 'day':
                delta += timedelta(days=value)
            elif unit == 'hour':
                delta += timedelta(hours=value)
            elif unit == 'minute':
                delta += timedelta(minutes=value)
            elif unit == 'second':
                delta += timedelta(seconds=value)
    last_handshake_time = now - delta
    return last_handshake_time

@dp.callback_query_handler(lambda c: True)
async def process_unknown_callback(callback_query: types.CallbackQuery):
    await callback_query.answer("Неизвестная команда.", show_alert=True)

async def deactivate_user(client_name: str):
    success = db.deactive_user_db(client_name)
    if success:
        conf_path = os.path.join('users', client_name, f'{client_name}.conf')
        png_path = os.path.join('users', client_name, f'{client_name}.png')
        try:
            if os.path.exists(conf_path):
                os.remove(conf_path)
            if os.path.exists(png_path):
                os.remove(png_path)
        except Exception as e:
            logger.error(f"Ошибка при удалении файлов для пользователя {client_name}: {e}")
        sent_message = await bot.send_message(admin, f"Конфигурация пользователя **{client_name}** истекла и была деактивирована.", parse_mode="Markdown", disable_notification=True)
        asyncio.create_task(delete_message_after_delay(admin, sent_message.message_id, delay=15))
        db.remove_user_expiration(client_name)
    else:
        sent_message = await bot.send_message(admin, f"Не удалось деактивировать пользователя **{client_name}** по истечении времени.", parse_mode="Markdown", disable_notification=True)
        asyncio.create_task(delete_message_after_delay(admin, sent_message.message_id, delay=15))

async def check_environment():
    try:
        cmd = "docker ps --filter 'name={}' --format '{{{{.Names}}}}'".format(DOCKER_CONTAINER)
        container_names = subprocess.check_output(cmd, shell=True).decode().strip().split('\n')
        if DOCKER_CONTAINER not in container_names:
            logger.error(f"Контейнер Docker '{DOCKER_CONTAINER}' не найден. Необходима инициализация AmneziaVPN.")
            return False
    except subprocess.CalledProcessError as e:
        logger.error(f"Ошибка при проверке Docker-контейнера: {e}")
        return False

    try:
        cmd = f"docker exec {DOCKER_CONTAINER} test -f {WG_CONFIG_FILE}"
        subprocess.check_call(cmd, shell=True)
    except subprocess.CalledProcessError:
        logger.error(f"Конфигурационный файл WireGuard '{WG_CONFIG_FILE}' не найден в контейнере '{DOCKER_CONTAINER}'. Необходима инициализация AmneziaVPN.")
        return False

    return True

async def on_startup(dp):
    os.makedirs('files/connections', exist_ok=True)
    os.makedirs('users', exist_ok=True)
    await load_isp_cache_task()

    environment_ok = await check_environment()
    if not environment_ok:
        logger.error("Необходимо инициализировать AmneziaVPN перед запуском бота.")
        await bot.send_message(admin, "Необходимо инициализировать AmneziaVPN перед запуском бота.")
        await bot.close()
        sys.exit(1)

    users = db.get_users_with_expiration()
    for user in users:
        client_name, expiration_time = user
        if expiration_time:
            try:
                expiration_datetime = datetime.fromisoformat(expiration_time)
            except ValueError:
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
            else:
                await deactivate_user(client_name)

executor.start_polling(dp, on_startup=on_startup)
