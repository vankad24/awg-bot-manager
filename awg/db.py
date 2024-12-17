import os
import subprocess
import configparser
import json
import pytz
import socket
import logging
import tempfile
import paramiko
import getpass
import threading
import time
from datetime import datetime, timedelta

EXPIRATIONS_FILE = 'files/expirations.json'
UTC = pytz.UTC

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SSHManager:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SSHManager, cls).__new__(cls)
            cls._instance.client = None
            cls._instance.initialized = False
        return cls._instance

    def __init__(self, host=None, port=None, username=None, auth_type=None, password=None, key_path=None):
        if not getattr(self, 'initialized', False):
            self.client = None
            self.host = host
            self.port = port
            self.username = username
            self.auth_type = auth_type
            self.password = password
            self.key_path = key_path
            self.initialized = True

    def load_settings_from_config(self):
        try:
            config = configparser.ConfigParser()
            config.read('files/setting.ini')
            if config.has_section('setting'):
                if config.get('setting', 'is_remote', fallback='false').lower() == 'true':
                    self.host = config.get('setting', 'remote_host')
                    self.port = int(config.get('setting', 'remote_port'))
                    self.username = config.get('setting', 'remote_user')
                    self.auth_type = config.get('setting', 'remote_auth_type')
                    if self.auth_type == 'password':
                        self.password = config.get('setting', 'remote_password')
                    else:
                        self.key_path = config.get('setting', 'remote_key_path')
                    return True
            return False
        except Exception as e:
            logger.error(f"Ошибка загрузки настроек SSH: {e}")
            return False

    def ensure_connection(self):
        if not self.client or not self.client.get_transport() or not self.client.get_transport().is_active():
            if not all([self.host, self.port, self.username, self.auth_type]):
                if not self.load_settings_from_config():
                    logger.error("Не удалось загрузить настройки SSH из конфигурации")
                    return False

            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            try:
                if self.auth_type == "password":
                    self.client.connect(
                        self.host,
                        self.port,
                        self.username,
                        self.password,
                        timeout=10
                    )
                else:
                    private_key = paramiko.RSAKey.from_private_key_file(self.key_path)
                    self.client.connect(
                        self.host,
                        self.port,
                        self.username,
                        pkey=private_key,
                        timeout=10
                    )
                return True
            except Exception as e:
                logger.error(f"Ошибка подключения SSH: {e}")
                return False
        return True

    def connect(self):
        if not all([self.host, self.port, self.username, self.auth_type]):
            if not self.load_settings_from_config():
                logger.error("Не все параметры подключения установлены")
                return False
        return self.ensure_connection()

    def execute_command(self, command):
        try:
            if not self.ensure_connection():
                return None, "Failed to establish SSH connection"
            
            stdin, stdout, stderr = self.client.exec_command(command, timeout=30)
            output = stdout.read().decode()
            error = stderr.read().decode()
            return output, error
        except Exception as e:
            logger.error(f"Ошибка выполнения команды: {e}")
            self.client = None
            return None, str(e)

    def close(self):
        if self.client:
            try:
                self.client.close()
            except:
                pass
            self.client = None

ssh_manager = SSHManager()

def execute_docker_command(command):
    setting = get_config()
    if setting.get("is_remote") == "true":
        try:
            if not ssh_manager.ensure_connection():
                raise Exception("Не удалось установить SSH подключение")

            output, error = ssh_manager.execute_command(command)
            if error and ('error' in error.lower() or 'command not found' in error.lower()):
                raise Exception(error)
            if output is None:
                raise Exception("Failed to execute command")
            return output
        except Exception as e:
            raise Exception(f"SSH command failed: {e}")
    else:
        return subprocess.check_output(command, shell=True).decode()
    
def get_amnezia_container():
    try:
        cmd = "docker ps --filter 'name=amnezia-awg' --format '{{.Names}}'"
        if subprocess.call(['docker', 'ps'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) != 0:
            logger.error("Docker не установлен или не запущен на локальной машине.")
            exit(1)
            
        output = subprocess.check_output(cmd, shell=True).decode().strip()
        if output:
            return output
        else:
            logger.error("Docker-контейнер 'amnezia-awg' не найден или не запущен.")
            exit(1)
    except subprocess.CalledProcessError as e:
        logger.error(f"Ошибка при поиске контейнера: {e}")
        exit(1)

def create_config(path='files/setting.ini'):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    config = configparser.ConfigParser()
    config.add_section("setting")

    bot_token = input('Введите токен Telegram бота: ').strip()
    admin_id = input('Введите Telegram ID администратора: ').strip()
    
    is_remote = input("\nИспользовать удаленное подключение? (y/n): \n").lower() == 'y'
    
    if is_remote:
        host = input("Введите IP-адрес удаленного сервера: ").strip()
        port = input("Введите SSH порт (по умолчанию 22): ").strip() or "22"
        username = input("Введите имя пользователя: ").strip()
        key_path = input("Введите путь до приватного SSH-ключа, например /home/user/.ssh/id_rsa (или нажмите Enter для ввода пароля): ").strip()
        
        if key_path:
            password = ""
            auth_type = "key"
        else:
            password = getpass.getpass("Введите пароль: ")
            key_path = ""
            auth_type = "password"

        ssh_manager.host = host
        ssh_manager.port = int(port)
        ssh_manager.username = username
        ssh_manager.auth_type = auth_type
        ssh_manager.password = password
        ssh_manager.key_path = key_path

        if not ssh_manager.connect():
            logger.error("Не удалось установить SSH соединение")
            exit(1)

        output, error = ssh_manager.execute_command("docker ps --filter 'name=amnezia-awg' --format '{{.Names}}'")
        if output:
            docker_container = output.strip()
            print(f"Найден контейнер: {docker_container}")
        else:
            logger.error("Docker контейнер не найден на удаленном сервере")
            exit(1)

        wg_config_file = '/opt/amnezia/awg/wg0.conf'
        try:
            output, error = ssh_manager.execute_command("curl -s https://api.ipify.org")
            endpoint = output.strip() if output else None
            if not endpoint or error:
                endpoint = input('Не удалось автоматически определить внешний IP-адрес. Пожалуйста, введите его вручную: ').strip()
        except Exception:
            endpoint = input('Введите внешний IP-адрес сервера: ').strip()

        config.set("setting", "is_remote", "true")
        config.set("setting", "remote_host", host)
        config.set("setting", "remote_port", port)
        config.set("setting", "remote_user", username)
        config.set("setting", "remote_auth_type", auth_type)
        if auth_type == "password":
            config.set("setting", "remote_password", password)
        if key_path:
            config.set("setting", "remote_key_path", key_path)

    else:
        if subprocess.call(['docker', 'ps'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) != 0:
            logger.error("Docker не установлен или не запущен на локальной машине.")
            exit(1)

        cmd = "docker ps --filter 'name=amnezia-awg' --format '{{.Names}}'"
        try:
            docker_container = subprocess.check_output(cmd, shell=True).decode().strip()
            if not docker_container:
                logger.error("Docker-контейнер 'amnezia-awg' не найден или не запущен.")
                exit(1)
        except subprocess.CalledProcessError as e:
            logger.error(f"Ошибка при поиске контейнера: {e}")
            exit(1)

        wg_config_file = '/opt/amnezia/awg/wg0.conf'
        try:
            endpoint = subprocess.check_output("curl -s https://api.ipify.org", shell=True).decode().strip()
            socket.inet_aton(endpoint)
        except (subprocess.CalledProcessError, socket.error):
            logger.error("Ошибка при определении внешнего IP-адреса сервера.")
            endpoint = input('Не удалось определить IP-адрес. Введите его вручную: ').strip()
        
        config.set("setting", "is_remote", "false")

    config.set("setting", "bot_token", bot_token)
    config.set("setting", "admin_id", admin_id)
    config.set("setting", "docker_container", docker_container)
    config.set("setting", "wg_config_file", wg_config_file)
    config.set("setting", "endpoint", endpoint)

    with open(path, "w") as config_file:
        config.write(config_file)
    logger.info(f"Конфигурация сохранена в {path}")
    return True

def get_config(path='files/setting.ini'):
    if not os.path.exists(path):
        create_config(path)

    config = configparser.ConfigParser()
    config.read(path)
    out = {}
    for key in config['setting']:
        out[key] = config['setting'][key]

    return out

def get_clients_from_clients_table():
    setting = get_config()
    docker_container = setting['docker_container']
    clients_table_path = '/opt/amnezia/awg/clientsTable'
    try:
        cmd = f"docker exec -i {docker_container} cat {clients_table_path}"
        call = execute_docker_command(cmd)
        clients_table = json.loads(call)
        client_map = {client['clientId']: client['userData']['clientName'] for client in clients_table}
        return client_map
    except Exception as e:
        logger.error(f"Ошибка при получении clientsTable: {e}")
        return {}

def parse_client_name(full_name):
    return full_name.split('[')[0].strip()

def get_client_list():
    setting = get_config()
    wg_config_file = setting['wg_config_file']
    docker_container = setting['docker_container']

    client_map = get_clients_from_clients_table()

    try:
        cmd = f"docker exec -i {docker_container} cat {wg_config_file}"
        config_content = execute_docker_command(cmd)

        clients = []
        lines = config_content.splitlines()
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if line.startswith('[Peer]'):
                client_public_key = ''
                allowed_ips = ''
                client_name = 'Unknown'
                i += 1
                while i < len(lines):
                    peer_line = lines[i].strip()
                    if peer_line == '':
                        break
                    if peer_line.startswith('#'):
                        full_client_name = peer_line[1:].strip()
                        client_name = parse_client_name(full_client_name)
                    elif peer_line.startswith('PublicKey ='):
                        client_public_key = peer_line.split('=', 1)[1].strip()
                    elif peer_line.startswith('AllowedIPs ='):
                        allowed_ips = peer_line.split('=', 1)[1].strip()
                    i += 1
                client_name = client_map.get(client_public_key, client_name if 'client_name' in locals() else 'Unknown')
                clients.append([client_name, client_public_key, allowed_ips])
            else:
                i += 1
        return clients
    except Exception as e:
        logger.error(f"Ошибка при получении списка клиентов: {e}")
        return []

def get_active_list():
    setting = get_config()
    docker_container = setting['docker_container']
    
    try:
        clients = get_client_list()
        client_key_map = {client[1]: client[0] for client in clients}
        
        cmd = f"docker exec -i {docker_container} wg show"
        wg_output = execute_docker_command(cmd)
        
        active_clients = []
        current_peer = {}
        
        for line in wg_output.splitlines():
            line = line.strip()
            if line.startswith('peer:'):
                if current_peer and 'public_key' in current_peer and current_peer['public_key'] in client_key_map:
                    current_peer['name'] = client_key_map[current_peer['public_key']]
                    active_clients.append(current_peer)
                peer_public_key = line.split('peer: ')[1].strip()
                current_peer = {'public_key': peer_public_key}
            elif line.startswith('endpoint:'):
                current_peer['endpoint'] = line.split('endpoint: ')[1].strip()
            elif line.startswith('latest handshake:'):
                current_peer['last_handshake'] = line.split('latest handshake: ')[1].strip()
            elif line.startswith('transfer:'):
                current_peer['transfer'] = line.split('transfer: ')[1].strip()
        
        if current_peer and 'public_key' in current_peer and current_peer['public_key'] in client_key_map:
            current_peer['name'] = client_key_map[current_peer['public_key']]
            active_clients.append(current_peer)
            
        return active_clients
    except Exception as e:
        logger.error(f"Error getting active list: {e}")
        return []

def root_add(id_user, ipv6=False):
    setting = get_config()
    endpoint = setting['endpoint']
    wg_config_file = setting['wg_config_file']
    docker_container = setting['docker_container']
    is_remote = setting.get('is_remote') == 'true'

    clients = get_client_list()
    client_entry = next((c for c in clients if c[0] == id_user), None)
    if client_entry:
        logger.info(f"Пользователь {id_user} уже существует.")
        return False

    pwd = os.getcwd()
    os.makedirs(f"{pwd}/users/{id_user}", exist_ok=True)
    os.makedirs(f"{pwd}/files", exist_ok=True)

    if is_remote:
        try:
            if not ssh_manager.connect():
                logger.error("Не удалось установить SSH соединение")
                return False

            output, error = ssh_manager.execute_command(f"docker exec -i {docker_container} wg genkey")
            if error:
                logger.error(f"Ошибка генерации приватного ключа: {error}")
                return False
            private_key = output.strip()

            cmd = f"echo '{private_key}' | docker exec -i {docker_container} wg pubkey"
            output, error = ssh_manager.execute_command(cmd)
            if error:
                logger.error(f"Ошибка генерации публичного ключа: {error}")
                return False
            client_public_key = output.strip()

            output, error = ssh_manager.execute_command(f"docker exec -i {docker_container} wg genpsk")
            if error:
                logger.error(f"Ошибка генерации PSK: {error}")
                return False
            psk = output.strip()

            server_conf_path = f"{pwd}/files/server.conf"
            output, error = ssh_manager.execute_command(f"docker exec -i {docker_container} cat {wg_config_file}")
            if error:
                logger.error(f"Ошибка получения конфигурации сервера: {error}")
                return False
            with open(server_conf_path, 'w') as f:
                f.write(output)

            cmd = f"docker exec -i {docker_container} sh -c 'grep PrivateKey {wg_config_file} | cut -d\" \" -f 3'"
            output, error = ssh_manager.execute_command(cmd)
            if error:
                logger.error(f"Ошибка получения приватного ключа сервера: {error}")
                return False
            server_private_key = output.strip()

            cmd = f"echo '{server_private_key}' | docker exec -i {docker_container} wg pubkey"
            output, error = ssh_manager.execute_command(cmd)
            if error:
                logger.error(f"Ошибка генерации публичного ключа сервера: {error}")
                return False
            server_public_key = output.strip()

            if not all([private_key, client_public_key, server_public_key, psk]):
                logger.error("Не все ключи были успешно сгенерированы")
                return False

            listen_port = None
            additional_params = []
            with open(server_conf_path, 'r') as f:
                for line in f:
                    if line.startswith('ListenPort'):
                        listen_port = line.split('=')[1].strip()
                    elif any(line.startswith(p) for p in ['Jc', 'Jmin', 'Jmax', 'S1', 'S2', 'H1', 'H2', 'H3', 'H4']):
                        additional_params.append(line.strip())

            if not listen_port:
                logger.error("Не удалось получить порт сервера")
                return False

            octet = 2
            client_ip = None
            while octet <= 254:
                ip = f"10.8.1.{octet}/32"
                with open(server_conf_path, 'r') as f:
                    if ip not in f.read():
                        client_ip = ip
                        break
                octet += 1

            if not client_ip:
                logger.error("Нет свободных IP-адресов")
                return False

            client_config = f"""[Interface]
Address = {client_ip}
DNS = 1.1.1.1, 1.0.0.1
PrivateKey = {private_key}
{os.linesep.join(additional_params)}
[Peer]
PublicKey = {server_public_key}
PresharedKey = {psk}
AllowedIPs = 0.0.0.0/0
Endpoint = {endpoint}:{listen_port}
PersistentKeepalive = 25"""

            client_config_path = f"{pwd}/users/{id_user}/{id_user}.conf"
            with open(client_config_path, 'w') as f:
                f.write(client_config)

            if not os.path.exists(client_config_path):
                logger.error("Не удалось создать конфигурационный файл клиента")
                return False

            peer_config = f"""
[Peer]
# {id_user}
PublicKey = {client_public_key}
PresharedKey = {psk}
AllowedIPs = {client_ip}
"""
            with open(server_conf_path, 'a') as f:
                f.write(peer_config)

            sftp = ssh_manager.client.open_sftp()
            sftp.put(server_conf_path, "/tmp/server.conf")
            ssh_manager.execute_command(f"docker cp /tmp/server.conf {docker_container}:{wg_config_file}")
            ssh_manager.execute_command(f"docker exec -i {docker_container} sh -c 'wg-quick down {wg_config_file} && wg-quick up {wg_config_file}'")
            ssh_manager.execute_command("rm /tmp/server.conf")

            output, error = ssh_manager.execute_command(f"docker exec -i {docker_container} cat /opt/amnezia/awg/clientsTable")
            try:
                clients_table = json.loads(output or "[]")
            except json.JSONDecodeError:
                clients_table = []

            creation_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            clients_table.append({
                "clientId": client_public_key,
                "userData": {
                    "clientName": id_user,
                    "creationDate": creation_date
                }
            })

            clients_table_path = f"{pwd}/files/clientsTable"
            with open(clients_table_path, 'w') as f:
                json.dump(clients_table, f)

            sftp.put(clients_table_path, "/tmp/clientsTable")
            ssh_manager.execute_command(f"docker cp /tmp/clientsTable {docker_container}:/opt/amnezia/awg/clientsTable")
            ssh_manager.execute_command("rm /tmp/clientsTable")
            sftp.close()

            traffic_file = f"{pwd}/users/{id_user}/traffic.json"
            with open(traffic_file, 'w') as f:
                json.dump({
                    "total_incoming": 0,
                    "total_outgoing": 0,
                    "last_incoming": 0,
                    "last_outgoing": 0
                }, f)

            return True

        except Exception as e:
            logger.error(f"Ошибка при добавлении пользователя через SSH: {e}")
            return False
    else:
        cmd = ["./newclient.sh", id_user, endpoint, wg_config_file, docker_container]
        if subprocess.call(cmd) == 0:
            return True
        return False

def deactive_user_db(client_name):
    setting = get_config()
    wg_config_file = setting['wg_config_file']
    docker_container = setting['docker_container']
    is_remote = setting.get('is_remote') == 'true'

    clients = get_client_list()
    client_entry = next((c for c in clients if c[0] == client_name), None)
    if not client_entry:
        logger.error(f"Пользователь {client_name} не найден в списке клиентов.")
        return False

    client_public_key = client_entry[1]
    pwd = os.getcwd()

    if is_remote:
        try:
            if not ssh_manager.connect():
                logger.error("Не удалось установить SSH соединение")
                return False

            awk_script = f"""
            BEGIN {{in_peer=0; skip=0}}
            /^\\[Peer\\]/ {{
                in_peer=1
                peer_block = $0 "\\n"
                next
            }}
            in_peer == 1 {{
                peer_block = peer_block $0 "\\n"
                if ($0 ~ /^PublicKey =/) {{
                    split($0, a, " = ")
                    if (a[2] == "{client_public_key}") {{
                        skip=1
                    }}
                }}
                if ($0 ~ /^\\[Peer\\]/ || $0 ~ /^\\[Interface\\]/) {{
                    if (skip == 1) {{
                        skip=0
                        in_peer=0
                        next
                    }} else {{
                        printf "%s", peer_block
                        in_peer=0
                    }}
                }}
                if ($0 == "") {{
                    if (skip == 1) {{
                        skip=0
                        in_peer=0
                        next
                    }} else {{
                        printf "%s", peer_block
                        in_peer=0
                    }}
                }}
                next
            }}
            {{
                print
            }}
            END {{
                if (in_peer == 1 && skip != 1) {{
                    printf "%s", peer_block
                }}
            }}
            """

            ssh_manager.execute_command(f'echo \'{awk_script}\' > /tmp/remove_peer.awk')

            commands = [
                f'docker exec -i {docker_container} cat {wg_config_file} > /tmp/wg0.conf',
                f'awk -f /tmp/remove_peer.awk /tmp/wg0.conf > /tmp/wg0.conf.new',
                f'mv /tmp/wg0.conf.new /tmp/wg0.conf',
                f'docker cp /tmp/wg0.conf {docker_container}:{wg_config_file}',
                f'rm -f /tmp/remove_peer.awk /tmp/wg0.conf',
                f'docker exec -i {docker_container} sh -c "wg-quick down {wg_config_file} && wg-quick up {wg_config_file}"'
            ]

            for cmd in commands:
                output, error = ssh_manager.execute_command(cmd)
                if error and not ('Warning' in error or 'wireguard-go' in error):
                    logger.error(f"Ошибка выполнения команды {cmd}: {error}")
                    return False
                
            output, _ = ssh_manager.execute_command(f"docker exec -i {docker_container} cat /opt/amnezia/awg/clientsTable")
            try:
                clients_table = json.loads(output or "[]")
                clients_table = [client for client in clients_table if client['clientId'] != client_public_key]
                clients_table_json = json.dumps(clients_table)
                ssh_manager.execute_command(f'echo \'{clients_table_json}\' > /tmp/clientsTable')
                ssh_manager.execute_command(f'docker cp /tmp/clientsTable {docker_container}:/opt/amnezia/awg/clientsTable')
                ssh_manager.execute_command('rm -f /tmp/clientsTable')
            except Exception as e:
                logger.error(f"Ошибка обновления clientsTable: {e}")

            try:
                user_dir = f"{pwd}/users/{client_name}"
                user_conf = f"{user_dir}/{client_name}.conf"
                traffic_file = f"{user_dir}/traffic.json"
                
                for file in [user_conf, traffic_file]:
                    if os.path.exists(file):
                        os.remove(file)
                
                if os.path.exists(user_dir):
                    os.rmdir(user_dir)
            except Exception as e:
                logger.error(f"Ошибка удаления локальных файлов: {e}")

            return True

        except Exception as e:
            logger.error(f"Ошибка при удалении пользователя через SSH: {e}")
            return False
    else:
        if subprocess.call(["./removeclient.sh", client_name, client_public_key, wg_config_file, docker_container]) == 0:
            return True
        return False

def load_expirations():
    if not os.path.exists(EXPIRATIONS_FILE):
        return {}
    with open(EXPIRATIONS_FILE, 'r') as f:
        try:
            data = json.load(f)
            for user, info in data.items():
                if info.get('expiration_time'):
                    data[user]['expiration_time'] = datetime.fromisoformat(info['expiration_time']).replace(tzinfo=UTC)
                else:
                    data[user]['expiration_time'] = None
            return data
        except json.JSONDecodeError:
            logger.error("Ошибка при загрузке expirations.json.")
            return {}

def save_expirations(expirations):
    os.makedirs(os.path.dirname(EXPIRATIONS_FILE), exist_ok=True)
    data = {}
    for user, info in expirations.items():
        data[user] = {
            'expiration_time': info['expiration_time'].isoformat() if info['expiration_time'] else None,
            'traffic_limit': info.get('traffic_limit', "Неограниченно")
        }
    with open(EXPIRATIONS_FILE, 'w') as f:
        json.dump(data, f)

def set_user_expiration(username: str, expiration: datetime, traffic_limit: str):
    expirations = load_expirations()
    if username not in expirations:
        expirations[username] = {}
    if expiration:
        if expiration.tzinfo is None:
            expiration = expiration.replace(tzinfo=UTC)
        expirations[username]['expiration_time'] = expiration
    else:
        expirations[username]['expiration_time'] = None
    expirations[username]['traffic_limit'] = traffic_limit
    save_expirations(expirations)

def remove_user_expiration(username: str):
    expirations = load_expirations()
    if username in expirations:
        del expirations[username]
        save_expirations(expirations)

def get_users_with_expiration():
    expirations = load_expirations()
    return [(user, info['expiration_time'].isoformat() if info['expiration_time'] else None, 
             info.get('traffic_limit', "Неограниченно")) for user, info in expirations.items()]

def get_user_expiration(username: str):
    expirations = load_expirations()
    return expirations.get(username, {}).get('expiration_time', None)

def get_user_traffic_limit(username: str):
    expirations = load_expirations()
    return expirations.get(username, {}).get('traffic_limit', "Неограниченно")
