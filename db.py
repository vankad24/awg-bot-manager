import os
import subprocess
import configparser
import json
from datetime import datetime
import pytz

EXPIRATIONS_FILE = 'files/expirations.json'
UTC = pytz.UTC

def create_config(path='files/setting.ini'):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    config = configparser.ConfigParser()
    config.add_section("setting")

    bot_token = input('Введите токен Telegram бота: ').strip()
    admin_id = input('Введите Telegram ID администратора: ').strip()
    docker_container = input('Введите имя Docker-контейнера с WireGuard (например, amnezia-awg): ').strip()

    cmd = f"docker exec {docker_container} find / -name wg0.conf"
    try:
        wg_config_file = subprocess.check_output(cmd, shell=True).decode().strip()
    except subprocess.CalledProcessError:
        print("Ошибка при определении пути к файлу конфигурации WireGuard.")
        wg_config_file = '/opt/amnezia/awg/wg0.conf'

    try:
        ip_output = subprocess.check_output("ip -4 addr show scope global", shell=True).decode()
        ip_address = None
        for line in ip_output.split('\n'):
            if 'inet' in line:
                parts = line.strip().split()
                ip = parts[1].split('/')[0]
                ip_address = ip
                break
        if ip_address:
            endpoint = ip_address
        else:
            endpoint = input('Не удалось автоматически определить внешний IP-адрес. Пожалуйста, введите его вручную: ').strip()
    except subprocess.CalledProcessError:
        print("Ошибка при определении внешнего IP-адреса сервера.")
        endpoint = input('Не удалось автоматически определить внешний IP-адрес. Пожалуйста, введите его вручную: ').strip()

    config.set("setting", "bot_token", bot_token)
    config.set("setting", "admin_id", admin_id)
    config.set("setting", "docker_container", docker_container)
    config.set("setting", "wg_config_file", wg_config_file)
    config.set("setting", "endpoint", endpoint)

    with open(path, "w") as config_file:
        config.write(config_file)

def get_config(path='files/setting.ini'):
    if not os.path.exists(path):
        create_config(path)

    config = configparser.ConfigParser()
    config.read(path)
    out = {}
    for key in config['setting']:
        out[key] = config['setting'][key]

    return out

def save_client_endpoint(username, endpoint):
    os.makedirs('files/connections', exist_ok=True)
    file_path = os.path.join('files', 'connections', f'{username}_ip.json')
    timestamp = datetime.now().strftime('%d.%m.%Y %H:%M')
    ip_address = endpoint.split(':')[0]

    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                data = {}
    else:
        data = {}

    data[ip_address] = timestamp

    with open(file_path, 'w') as f:
        json.dump(data, f)

def root_add(id_user, ipv6=False):
    setting = get_config()
    endpoint = setting['endpoint']
    wg_config_file = setting['wg_config_file']
    docker_container = setting['docker_container']

    if ipv6:
        cmd = ["./newclient.sh", id_user, endpoint, wg_config_file, docker_container, 'ipv6']
    else:
        cmd = ["./newclient.sh", id_user, endpoint, wg_config_file, docker_container]

    if subprocess.call(cmd) == 0:
        return True
    return False

def get_client_list():
    setting = get_config()
    wg_config_file = setting['wg_config_file']
    docker_container = setting['docker_container']

    try:
        cmd = f"docker exec -i {docker_container} cat {wg_config_file}"
        call = subprocess.check_output(cmd, shell=True)
        config_content = call.decode('utf-8')

        clients = []
        lines = config_content.splitlines()
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if line.startswith('[Peer]'):
                client_name = 'Unknown'
                public_key = ''
                allowed_ips = ''
                i += 1
                while i < len(lines):
                    peer_line = lines[i].strip()
                    if peer_line == '':
                        break
                    if peer_line.startswith('#'):
                        client_name = peer_line[1:].strip()
                    elif peer_line.startswith('PublicKey ='):
                        public_key = peer_line.split('=', 1)[1].strip()
                    elif peer_line.startswith('AllowedIPs ='):
                        allowed_ips = peer_line.split('=', 1)[1].strip()
                    i += 1
                clients.append([client_name, public_key, allowed_ips])
            else:
                i += 1
        return clients
    except subprocess.CalledProcessError as e:
        print(f"Ошибка при получении списка клиентов: {e}")
        return []

def get_active_list():
    setting = get_config()
    docker_container = setting['docker_container']

    try:
        clients = get_client_list()
        client_key = {client[1]: client[0] for client in clients}

        cmd = f"docker exec -i {docker_container} wg"
        call = subprocess.check_output(cmd, shell=True)
        wg_output = call.decode('utf-8')

        active_clients = []
        current_peer = None
        for line in wg_output.splitlines():
            line = line.strip()
            if line.startswith('peer:'):
                peer_public_key = line.split('peer: ')[1].strip()
                current_peer = {'public_key': peer_public_key}
            elif line.startswith('endpoint:') and current_peer is not None:
                current_peer['endpoint'] = line.split('endpoint: ')[1].strip()
            elif line.startswith('latest handshake:') and current_peer is not None:
                current_peer['latest_handshake'] = line.split('latest handshake: ')[1].strip()
            elif line.startswith('transfer:') and current_peer is not None:
                current_peer['transfer'] = line.split('transfer: ')[1].strip()
            elif line == '' and current_peer is not None:
                peer_public_key = current_peer.get('public_key')
                if peer_public_key in client_key:
                    username = client_key[peer_public_key]
                    last_time = current_peer.get('latest_handshake', 'Нет данных')
                    transfer = current_peer.get('transfer', 'Нет данных')
                    endpoint = current_peer.get('endpoint', 'Нет данных')
                    save_client_endpoint(username, endpoint)
                    active_clients.append([username, last_time, transfer, endpoint])
                current_peer = None

        if current_peer is not None:
            peer_public_key = current_peer.get('public_key')
            if peer_public_key in client_key:
                username = client_key[peer_public_key]
                last_time = current_peer.get('latest_handshake', 'Нет данных')
                transfer = current_peer.get('transfer', 'Нет данных')
                endpoint = current_peer.get('endpoint', 'Нет данных')
                save_client_endpoint(username, endpoint)
                active_clients.append([username, last_time, transfer, endpoint])

        return active_clients

    except subprocess.CalledProcessError as e:
        print(f"Ошибка при получении активных клиентов: {e}")
        return []

def deactive_user_db(client_name):
    setting = get_config()
    wg_config_file = setting['wg_config_file']
    docker_container = setting['docker_container']

    clients = get_client_list()
    client_entry = next((c for c in clients if c[0] == client_name), None)
    if client_entry:
        client_public_key = client_entry[1]
        if subprocess.call(["./removeclient.sh", client_name, client_public_key, wg_config_file, docker_container]) == 0:
            return True
    return False

def load_expirations():
    if not os.path.exists(EXPIRATIONS_FILE):
        return {}
    with open(EXPIRATIONS_FILE, 'r') as f:
        try:
            data = json.load(f)
            for user, timestamp in data.items():
                if timestamp:
                    data[user] = datetime.fromisoformat(timestamp).replace(tzinfo=UTC)
                else:
                    data[user] = None
            return data
        except json.JSONDecodeError:
            return {}

def save_expirations(expirations):
    os.makedirs(os.path.dirname(EXPIRATIONS_FILE), exist_ok=True)
    data = {user: (ts.isoformat() if ts else None) for user, ts in expirations.items()}
    with open(EXPIRATIONS_FILE, 'w') as f:
        json.dump(data, f)

def set_user_expiration(username: str, expiration: datetime):
    expirations = load_expirations()
    if expiration:
        if expiration.tzinfo is None:
            expiration = expiration.replace(tzinfo=UTC)
        expirations[username] = expiration
    else:
        expirations[username] = None
    save_expirations(expirations)

def remove_user_expiration(username: str):
    expirations = load_expirations()
    if username in expirations:
        del expirations[username]
        save_expirations(expirations)

def get_users_with_expiration():
    expirations = load_expirations()
    return [(user, ts.isoformat() if ts else None) for user, ts in expirations.items()]

def get_user_expiration(username: str):
    expirations = load_expirations()
    return expirations.get(username, None)
