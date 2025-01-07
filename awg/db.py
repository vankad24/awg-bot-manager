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
import bcrypt
from datetime import datetime, timedelta

EXPIRATIONS_FILE = 'files/expirations.json'
SERVERS_FILE = 'files/servers.json'
UTC = pytz.UTC

def load_servers():
    if not os.path.exists(SERVERS_FILE):
        return {}
    with open(SERVERS_FILE, 'r') as f:
        return json.load(f)

def save_servers(servers):
    os.makedirs(os.path.dirname(SERVERS_FILE), exist_ok=True)
    with open(SERVERS_FILE, 'w') as f:
        json.dump(servers, f)

def hash_password(password):
    if not password:
        return None
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def verify_password(password, hashed):
    if not password or not hashed:
        return False
    return bcrypt.checkpw(password.encode(), hashed.encode())

def add_server(server_id, host, port, username, auth_type, password=None, key_path=None):
    servers = load_servers()
    server_config = {
        'host': host,
        'port': port,
        'username': username,
        'auth_type': auth_type,
        'password': hash_password(password) if auth_type == 'password' else None,
        '_original_password': password if auth_type == 'password' else None,
        'key_path': key_path if auth_type == 'key' else None,
        'docker_container': 'amnezia-awg',
        'wg_config_file': '/opt/amnezia/awg/wg0.conf',
        'endpoint': None,
        'is_remote': 'true'
    }
    servers[server_id] = server_config
    save_servers(servers)
    
    try:
        ssh = SSHManager(
            server_id=server_id,
            host=host,
            port=int(port),
            username=username,
            auth_type=auth_type,
            password=password,
            key_path=key_path
        )
        if ssh.connect():
            output, error = ssh.execute_command("curl -s https://api.ipify.org")
            if output and not error:
                server_config['endpoint'] = output.strip()
                servers[server_id] = server_config
                save_servers(servers)
    except Exception as e:
        logger.error(f"Не удалось получить endpoint для сервера {server_id}: {e}")
    
    return server_config

def remove_server(server_id):
    try:
        servers = load_servers()
        if server_id not in servers:
            logger.error(f"Сервер {server_id} не найден")
            return False

        server_config = servers[server_id]
        
        expirations = load_expirations()
        for username in list(expirations.keys()):
            if server_id in expirations[username]:
                del expirations[username][server_id]
                if not expirations[username]:
                    del expirations[username]
        save_expirations(expirations)

        if server_id in SSHManager._instances:
            SSHManager._instances[server_id].close()
            del SSHManager._instances[server_id]

        del servers[server_id]
        save_servers(servers)

        pwd = os.getcwd()
        users_dir = f"{pwd}/users"
        if os.path.exists(users_dir):
            for user_dir in os.listdir(users_dir):
                user_path = os.path.join(users_dir, user_dir)
                if os.path.isdir(user_path):
                    try:
                        for file in os.listdir(user_path):
                            file_path = os.path.join(user_path, file)
                            if os.path.isfile(file_path):
                                os.remove(file_path)
                        os.rmdir(user_path)
                    except Exception as e:
                        logger.error(f"Ошибка при удалении файлов пользователя {user_dir}: {e}")

        return True
    except Exception as e:
        logger.error(f"Ошибка при удалении сервера {server_id}: {e}")
        return False

def get_server_list():
    return list(load_servers().keys())

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SSHManager:
    _instances = {}

    def __new__(cls, server_id=None, *args, **kwargs):
        if server_id not in cls._instances:
            cls._instances[server_id] = super(SSHManager, cls).__new__(cls)
            cls._instances[server_id].client = None
            cls._instances[server_id].initialized = False
        return cls._instances[server_id]

    def __init__(self, server_id=None, host=None, port=None, username=None, auth_type=None, password=None, key_path=None):
        if not getattr(self, 'initialized', False):
            self.client = None
            self.server_id = server_id
            self.host = host
            self.port = port
            self.username = username
            self.auth_type = auth_type
            self.key_path = key_path
            self.password = password
            self.initialized = True
        if password is not None:
            self.password = password

    def load_settings_from_config(self):
        try:
            servers = load_servers()
            if self.server_id in servers:
                server = servers[self.server_id]
                self.host = server['host']
                self.port = int(server['port'])
                self.username = server['username']
                self.auth_type = server['auth_type']
                if self.auth_type == 'password':
                    if not self.password:
                        self.password = server.get('_original_password')
                        if not self.password:
                            logger.error("Пароль не установлен")
                            return False
                else:
                    self.key_path = server['key_path']
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
                        timeout=10,
                        look_for_keys=False,
                        allow_agent=False
                    )
                    
                    if not hasattr(self, '_original_password'):
                        self._original_password = self.password
                    else:
                        self.password = self._original_password
                else:
                    private_key = paramiko.RSAKey.from_private_key_file(self.key_path)
                    self.client.connect(
                        self.host,
                        self.port,
                        self.username,
                        pkey=private_key,
                        timeout=10,
                        look_for_keys=False,
                        allow_agent=False
                    )
                return True
            except Exception as e:
                logger.error(f"Ошибка подключения SSH: {e}")
                return False
        return True

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

    def connect(self):
        if not all([self.host, self.port, self.username, self.auth_type]):
            if not self.load_settings_from_config():
                logger.error("Не все параметры подключения установлены")
                return False
        return self.ensure_connection()

    def close(self):
        if self.client:
            try:
                self.client.close()
            except:
                pass
            self.client = None

ssh_manager = SSHManager()

def execute_docker_command(command, server_id=None):
    if server_id is None:
        raise Exception("Server ID is required")
    setting = get_config(server_id=server_id)
    if setting.get("is_remote") == "true":
        try:
            servers = load_servers()
            server_config = servers.get(server_id, {})
            
            if server_id in SSHManager._instances and hasattr(SSHManager._instances[server_id], '_original_password'):
                ssh = SSHManager._instances[server_id]
            else:
                ssh = SSHManager(
                    server_id=server_id,
                    host=server_config.get('host'),
                    port=int(server_config.get('port', 22)),
                    username=server_config.get('username'),
                    auth_type=server_config.get('auth_type'),
                    key_path=server_config.get('key_path'),
                    password=server_config.get('_original_password')
                )
            if not ssh.ensure_connection():
                raise Exception("Не удалось установить SSH подключение")

            output, error = ssh.execute_command(command)
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

def create_config(path='files/setting.ini', servers_list=None):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    config = configparser.ConfigParser()
    config.add_section("setting")

    bot_token = input('Введите токен Telegram бота: ').strip()
    admin_id = input('Введите Telegram ID администратора: ').strip()
    
    if servers_list:
        for server in servers_list:
            is_remote = server.get('is_remote', False)
            if is_remote:
                host = server.get('host') or input(f"Введите IP-адрес для сервера {server['name']}: ").strip()
                port = server.get('port') or input(f"Введите SSH порт для сервера {server['name']} (по умолчанию 22): ").strip() or "22"
                username = server.get('username') or input(f"Введите имя пользователя для сервера {server['name']}: ").strip()
                key_path = server.get('key_path') or input(f"Введите путь до приватного SSH-ключа для сервера {server['name']}, например /home/user/.ssh/id_rsa (или нажмите Enter для ввода пароля): ").strip()
                
                if key_path:
                    password = ""
                    auth_type = "key"
                else:
                    password = server.get('password') or getpass.getpass(f"Введите пароль для сервера {server['name']}: ")
                    key_path = ""
                    auth_type = "password"

                ssh_manager.host = host
                ssh_manager.port = int(port)
                ssh_manager.username = username
                ssh_manager.auth_type = auth_type
                ssh_manager.password = password
                ssh_manager.key_path = key_path

                if not ssh_manager.connect():
                    logger.error(f"Не удалось установить SSH соединение для сервера {server['name']}")
                    continue

                output, error = ssh_manager.execute_command("docker ps --filter 'name=amnezia-awg' --format '{{.Names}}'")
                if output:
                    docker_container = output.strip()
                    print(f"Найден контейнер для сервера {server['name']}: {docker_container}")
                else:
                    logger.error(f"Docker контейнер не найден на сервере {server['name']}")
                    continue

                wg_config_file = '/opt/amnezia/awg/wg0.conf'
                try:
                    output, error = ssh_manager.execute_command("curl -s https://api.ipify.org")
                    endpoint = output.strip() if output else None
                    if not endpoint or error:
                        endpoint = input(f'Не удалось автоматически определить внешний IP-адрес для сервера {server["name"]}. Пожалуйста, введите его вручную: ').strip()
                except Exception:
                    endpoint = input(f'Введите внешний IP-адрес для сервера {server["name"]}: ').strip()

                server_config = add_server(
                    server['name'],
                    host,
                    port,
                    username,
                    auth_type,
                    password=password,
                    key_path=key_path
                )
                
                if not server_config:
                    logger.error(f"Не удалось добавить сервер {server['name']}")
                    continue

                if server.get('is_default', False):
                    config.set("setting", "is_remote", "true")
                    config.set("setting", "bot_token", bot_token)
                    config.set("setting", "admin_id", admin_id)
                    config.set("setting", "docker_container", server_config['docker_container'])
                    config.set("setting", "wg_config_file", server_config['wg_config_file'])
                    config.set("setting", "endpoint", server_config['endpoint'])
            else:
                if subprocess.call(['docker', 'ps'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) != 0:
                    logger.error(f"Docker не установлен или не запущен для сервера {server['name']}")
                    continue

                cmd = "docker ps --filter 'name=amnezia-awg' --format '{{.Names}}'"
                try:
                    docker_container = subprocess.check_output(cmd, shell=True).decode().strip()
                    if not docker_container:
                        logger.error(f"Docker-контейнер 'amnezia-awg' не найден для сервера {server['name']}")
                        continue
                except subprocess.CalledProcessError as e:
                    logger.error(f"Ошибка при поиске контейнера для сервера {server['name']}: {e}")
                    continue

                wg_config_file = '/opt/amnezia/awg/wg0.conf'
                try:
                    endpoint = subprocess.check_output("curl -s https://api.ipify.org", shell=True).decode().strip()
                    socket.inet_aton(endpoint)
                except (subprocess.CalledProcessError, socket.error):
                    logger.error(f"Ошибка при определении внешнего IP-адреса для сервера {server['name']}")
                    endpoint = input(f'Введите IP-адрес для сервера {server["name"]}: ').strip()

                if server.get('is_default', False):
                    config.set("setting", "is_remote", "false")
                    config.set("setting", "bot_token", bot_token)
                    config.set("setting", "admin_id", admin_id)
                    config.set("setting", "docker_container", docker_container)
                    config.set("setting", "wg_config_file", wg_config_file)
                    config.set("setting", "endpoint", endpoint)
    else:
        servers_list = []
        while True:
            server_name = input("\nВведите имя сервера (или нажмите Enter для завершения): ").strip()
            if not server_name:
                if not servers_list:
                    print("Необходимо добавить хотя бы один сервер!")
                    continue
                break
                
            is_remote = input("Использовать удаленное подключение для этого сервера? (y/n): ").lower() == 'y'
            
            server_config = {
                'name': server_name,
                'is_remote': is_remote,
                'is_default': len(servers_list) == 0
            }
            
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
                    
                server_config.update({
                    'host': host,
                    'port': port,
                    'username': username,
                    'auth_type': auth_type,
                    'password': password,
                    'key_path': key_path
                })
                
                ssh_manager.host = host
                ssh_manager.port = int(port)
                ssh_manager.username = username
                ssh_manager.auth_type = auth_type
                ssh_manager.password = password
                ssh_manager.key_path = key_path

                if not ssh_manager.connect():
                    print("Не удалось установить SSH соединение. Попробуйте снова.")
                    continue

                output, error = ssh_manager.execute_command("docker ps --filter 'name=amnezia-awg' --format '{{.Names}}'")
                if not output:
                    print("Docker контейнер не найден на удаленном сервере. Попробуйте снова.")
                    continue
                    
                docker_container = output.strip()
                print(f"Найден контейнер: {docker_container}")
                
                try:
                    output, error = ssh_manager.execute_command("curl -s https://api.ipify.org")
                    endpoint = output.strip() if output and not error else None
                    if not endpoint:
                        endpoint = input('Не удалось автоматически определить внешний IP-адрес. Пожалуйста, введите его вручную: ').strip()
                except Exception:
                    endpoint = input('Введите внешний IP-адрес сервера: ').strip()
                    
                server_config['endpoint'] = endpoint
                server_config['docker_container'] = docker_container
                server_config['wg_config_file'] = '/opt/amnezia/awg/wg0.conf'
                
            else:
                if subprocess.call(['docker', 'ps'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) != 0:
                    print("Docker не установлен или не запущен на локальной машине. Попробуйте снова.")
                    continue

                cmd = "docker ps --filter 'name=amnezia-awg' --format '{{.Names}}'"
                try:
                    docker_container = subprocess.check_output(cmd, shell=True).decode().strip()
                    if not docker_container:
                        print("Docker-контейнер 'amnezia-awg' не найден или не запущен. Попробуйте снова.")
                        continue
                except subprocess.CalledProcessError as e:
                    print(f"Ошибка при поиске контейнера: {e}")
                    continue

                try:
                    endpoint = subprocess.check_output("curl -s https://api.ipify.org", shell=True).decode().strip()
                    socket.inet_aton(endpoint)
                except (subprocess.CalledProcessError, socket.error):
                    endpoint = input('Не удалось определить IP-адрес. Введите его вручную: ').strip()
                    
                server_config['endpoint'] = endpoint
                server_config['docker_container'] = docker_container
                server_config['wg_config_file'] = '/opt/amnezia/awg/wg0.conf'
                
            servers_list.append(server_config)
            print(f"Сервер {server_name} успешно добавлен!")
            
            add_another = input("\nДобавить еще один сервер? (y/n): ").lower() == 'y'
            if not add_another:
                break
                
        default_server = next(s for s in servers_list if s['is_default'])
        
        if default_server['is_remote']:
            config.set("setting", "is_remote", "true")
            config.set("setting", "bot_token", bot_token)
            config.set("setting", "admin_id", admin_id)
            config.set("setting", "docker_container", "amnezia-awg")
            config.set("setting", "wg_config_file", "/opt/amnezia/awg/wg0.conf")
            config.set("setting", "endpoint", default_server['endpoint'])
        else:
            config.set("setting", "is_remote", "false")
            config.set("setting", "bot_token", bot_token)
            config.set("setting", "admin_id", admin_id)
            config.set("setting", "docker_container", docker_container)
            config.set("setting", "wg_config_file", "/opt/amnezia/awg/wg0.conf")
            config.set("setting", "endpoint", default_server['endpoint'])
            
        for server in servers_list:
            if server['is_remote']:
                add_server(
                    server['name'],
                    server['host'],
                    server['port'],
                    server['username'],
                    server['auth_type'],
                    password=server.get('password'),
                    key_path=server.get('key_path')
                )
            else:
                servers = load_servers()
                servers[server['name']] = {
                    'docker_container': server['docker_container'],
                    'wg_config_file': server['wg_config_file'],
                    'endpoint': server['endpoint'],
                    'is_remote': 'false'
                }
                save_servers(servers)

    with open(path, "w") as config_file:
        config.write(config_file)
    logger.info(f"Конфигурация сохранена в {path}")
    return True

def get_config(path='files/setting.ini', server_id=None):
    if server_id:
        servers = load_servers()
        if server_id in servers:
            return servers[server_id]
        else:
            logger.error(f"Сервер {server_id} не найден")
            return {}
    else:
        if not os.path.exists(path):
            create_config(path)

        config = configparser.ConfigParser()
        config.read(path)
        out = {}
        for key in config['setting']:
            out[key] = config['setting'][key]

        return out

def get_clients_from_clients_table(server_id=None):
    if server_id is None:
        return {}
    setting = get_config(server_id=server_id)
    docker_container = setting['docker_container']
    clients_table_path = '/opt/amnezia/awg/clientsTable'
    is_remote = setting.get('is_remote') == 'true'
    
    try:
        if is_remote:
            servers = load_servers()
            server_config = servers.get(server_id, {})
            
            if server_id in SSHManager._instances and hasattr(SSHManager._instances[server_id], '_original_password'):
                ssh = SSHManager._instances[server_id]
            else:
                ssh = SSHManager(
                    server_id=server_id,
                    host=server_config.get('host'),
                    port=int(server_config.get('port', 22)),
                    username=server_config.get('username'),
                    auth_type=server_config.get('auth_type'),
                    key_path=server_config.get('key_path'),
                    password=server_config.get('_original_password')
                )
            if not ssh.connect():
                logger.error("Не удалось установить SSH соединение")
                return {}
                
        cmd = f"docker exec -i {docker_container} cat {clients_table_path}"
        if is_remote:
            output, error = ssh.execute_command(cmd)
            if error:
                logger.error(f"Ошибка выполнения команды: {error}")
                return {}
            clients_table = json.loads(output or "[]")
        else:
            output = subprocess.check_output(cmd, shell=True).decode()
            clients_table = json.loads(output)
            
        client_map = {client['clientId']: client['userData']['clientName'] for client in clients_table}
        return client_map
    except Exception as e:
        logger.error(f"Ошибка при получении clientsTable: {e}")
        return {}

def parse_client_name(full_name):
    return full_name.split('[')[0].strip()

def get_client_list(server_id=None):
    if server_id is None:
        return []
    setting = get_config(server_id=server_id)
    wg_config_file = setting['wg_config_file']
    docker_container = setting['docker_container']
    is_remote = setting.get('is_remote') == 'true'

    client_map = get_clients_from_clients_table(server_id=server_id)

    try:
        if is_remote:
            servers = load_servers()
            server_config = servers.get(server_id, {})
            
            if server_id in SSHManager._instances and hasattr(SSHManager._instances[server_id], '_original_password'):
                ssh = SSHManager._instances[server_id]
            else:
                ssh = SSHManager(
                    server_id=server_id,
                    host=server_config.get('host'),
                    port=int(server_config.get('port', 22)),
                    username=server_config.get('username'),
                    auth_type=server_config.get('auth_type'),
                    key_path=server_config.get('key_path'),
                    password=server_config.get('_original_password')
                )
            if not ssh.connect():
                logger.error("Не удалось установить SSH соединение")
                return []
        cmd = f"docker exec -i {docker_container} cat {wg_config_file}"
        config_content = execute_docker_command(cmd, server_id=server_id)

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

def get_active_list(server_id=None):
    if server_id is None:
        return []
    setting = get_config(server_id=server_id)
    docker_container = setting['docker_container']
    is_remote = setting.get('is_remote') == 'true'
    
    try:
        clients = get_client_list(server_id=server_id)
        client_key_map = {client[1]: client[0] for client in clients}
        
        if is_remote:
            servers = load_servers()
            server_config = servers.get(server_id, {})
            
            if server_id in SSHManager._instances and hasattr(SSHManager._instances[server_id], '_original_password'):
                ssh = SSHManager._instances[server_id]
            else:
                ssh = SSHManager(
                    server_id=server_id,
                    host=server_config.get('host'),
                    port=int(server_config.get('port', 22)),
                    username=server_config.get('username'),
                    auth_type=server_config.get('auth_type'),
                    key_path=server_config.get('key_path'),
                    password=server_config.get('_original_password')
                )
            if not ssh.connect():
                logger.error("Не удалось установить SSH соединение")
                return []
                
        cmd = f"docker exec -i {docker_container} wg show"
        if is_remote:
            output, error = ssh.execute_command(cmd)
            if error:
                logger.error(f"Ошибка выполнения команды: {error}")
                return []
            wg_output = output
        else:
            wg_output = subprocess.check_output(cmd, shell=True).decode()
        
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

def root_add(id_user, server_id=None, ipv6=False):
    if server_id is None:
        return False
    setting = get_config(server_id=server_id)
    endpoint = setting['endpoint']
    wg_config_file = setting['wg_config_file']
    docker_container = setting['docker_container']
    is_remote = setting.get('is_remote') == 'true'

    clients = get_client_list(server_id=server_id)
    client_entry = next((c for c in clients if c[0] == id_user), None)
    if client_entry:
        logger.info(f"Пользователь {id_user} уже существует.")
        return False

    pwd = os.getcwd()
    os.makedirs(f"{pwd}/users/{id_user}", exist_ok=True)
    os.makedirs(f"{pwd}/files", exist_ok=True)

    if is_remote:
        try:
            servers = load_servers()
            server_config = servers.get(server_id, {})
            
            if server_id in SSHManager._instances and hasattr(SSHManager._instances[server_id], '_original_password'):
                ssh = SSHManager._instances[server_id]
            else:
                ssh = SSHManager(
                    server_id=server_id,
                    host=server_config.get('host'),
                    port=int(server_config.get('port', 22)),
                    username=server_config.get('username'),
                    auth_type=server_config.get('auth_type'),
                    key_path=server_config.get('key_path'),
                    password=server_config.get('_original_password')
                )
            if not ssh.connect():
                logger.error("Не удалось установить SSH соединение")
                return False

            output, error = ssh.execute_command(f"docker exec -i {docker_container} wg genkey")
            if error:
                logger.error(f"Ошибка генерации приватного ключа: {error}")
                return False
            private_key = output.strip()

            cmd = f"echo '{private_key}' | docker exec -i {docker_container} wg pubkey"
            output, error = ssh.execute_command(cmd)
            if error:
                logger.error(f"Ошибка генерации публичного ключа: {error}")
                return False
            client_public_key = output.strip()

            output, error = ssh.execute_command(f"docker exec -i {docker_container} wg genpsk")
            if error:
                logger.error(f"Ошибка генерации PSK: {error}")
                return False
            psk = output.strip()

            server_conf_path = f"{pwd}/files/server.conf"
            output, error = ssh.execute_command(f"docker exec -i {docker_container} cat {wg_config_file}")
            if error:
                logger.error(f"Ошибка получения конфигурации сервера: {error}")
                return False
            with open(server_conf_path, 'w') as f:
                f.write(output)

            cmd = f"docker exec -i {docker_container} sh -c 'grep PrivateKey {wg_config_file} | cut -d\" \" -f 3'"
            output, error = ssh.execute_command(cmd)
            if error:
                logger.error(f"Ошибка получения приватного ключа сервера: {error}")
                return False
            server_private_key = output.strip()

            cmd = f"echo '{server_private_key}' | docker exec -i {docker_container} wg pubkey"
            output, error = ssh.execute_command(cmd)
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

            sftp = ssh.client.open_sftp()
            sftp.put(server_conf_path, "/tmp/server.conf")
            ssh.execute_command(f"docker cp /tmp/server.conf {docker_container}:{wg_config_file}")
            ssh.execute_command(f"docker exec -i {docker_container} sh -c 'wg-quick down {wg_config_file} && wg-quick up {wg_config_file}'")
            ssh.execute_command("rm /tmp/server.conf")

            output, error = ssh.execute_command(f"docker exec -i {docker_container} cat /opt/amnezia/awg/clientsTable")
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
            ssh.execute_command(f"docker cp /tmp/clientsTable {docker_container}:/opt/amnezia/awg/clientsTable")
            ssh.execute_command("rm /tmp/clientsTable")
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

def deactive_user_db(client_name, server_id=None):
    if server_id is None:
        return False
    setting = get_config(server_id=server_id)
    wg_config_file = setting['wg_config_file']
    docker_container = setting['docker_container']
    is_remote = setting.get('is_remote') == 'true'

    clients = get_client_list(server_id=server_id)
    client_entry = next((c for c in clients if c[0] == client_name), None)
    if not client_entry:
        logger.error(f"Пользователь {client_name} не найден в списке клиентов.")
        return False

    client_public_key = client_entry[1]
    pwd = os.getcwd()

    if is_remote:
        try:
            servers = load_servers()
            server_config = servers.get(server_id, {})
            
            if server_id in SSHManager._instances and hasattr(SSHManager._instances[server_id], '_original_password'):
                ssh = SSHManager._instances[server_id]
            else:
                ssh = SSHManager(
                    server_id=server_id,
                    host=server_config.get('host'),
                    port=int(server_config.get('port', 22)),
                    username=server_config.get('username'),
                    auth_type=server_config.get('auth_type'),
                    key_path=server_config.get('key_path'),
                    password=server_config.get('_original_password')
                )
            if not ssh.connect():
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

            ssh.execute_command(f'echo \'{awk_script}\' > /tmp/remove_peer.awk')

            commands = [
                f'docker exec -i {docker_container} cat {wg_config_file} > /tmp/wg0.conf',
                f'awk -f /tmp/remove_peer.awk /tmp/wg0.conf > /tmp/wg0.conf.new',
                f'mv /tmp/wg0.conf.new /tmp/wg0.conf',
                f'docker cp /tmp/wg0.conf {docker_container}:{wg_config_file}',
                f'rm -f /tmp/remove_peer.awk /tmp/wg0.conf',
                f'docker exec -i {docker_container} sh -c "wg-quick down {wg_config_file} && wg-quick up {wg_config_file}"'
            ]

            for cmd in commands:
                output, error = ssh.execute_command(cmd)
                if error and not ('Warning' in error or 'wireguard-go' in error):
                    logger.error(f"Ошибка выполнения команды {cmd}: {error}")
                    return False
                
            output, _ = ssh.execute_command(f"docker exec -i {docker_container} cat /opt/amnezia/awg/clientsTable")
            try:
                clients_table = json.loads(output or "[]")
                clients_table = [client for client in clients_table if client['clientId'] != client_public_key]
                clients_table_json = json.dumps(clients_table)
                ssh.execute_command(f'echo \'{clients_table_json}\' > /tmp/clientsTable')
                ssh.execute_command(f'docker cp /tmp/clientsTable {docker_container}:/opt/amnezia/awg/clientsTable')
                ssh.execute_command('rm -f /tmp/clientsTable')
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
            if data and not isinstance(next(iter(data.values())), dict):
                new_data = {}
                for user, info in data.items():
                    if isinstance(info, dict):
                        new_data[user] = {'default': info}
                    else:
                        new_data[user] = {'default': {
                            'expiration_time': info.get('expiration_time'),
                            'traffic_limit': info.get('traffic_limit', "Неограниченно")
                        }}
                data = new_data
            
            for user, servers in data.items():
                for server_id, info in servers.items():
                    if info.get('expiration_time'):
                        data[user][server_id]['expiration_time'] = datetime.fromisoformat(info['expiration_time']).replace(tzinfo=UTC)
                    else:
                        data[user][server_id]['expiration_time'] = None
            return data
        except json.JSONDecodeError:
            logger.error("Ошибка при загрузке expirations.json.")
            return {}

def save_expirations(expirations):
    os.makedirs(os.path.dirname(EXPIRATIONS_FILE), exist_ok=True)
    data = {}
    for user, servers in expirations.items():
        data[user] = {}
        for server_id, info in servers.items():
            data[user][server_id] = {
                'expiration_time': info['expiration_time'].isoformat() if info['expiration_time'] else None,
                'traffic_limit': info.get('traffic_limit', "Неограниченно")
            }
    with open(EXPIRATIONS_FILE, 'w') as f:
        json.dump(data, f)

def set_user_expiration(username: str, expiration: datetime, traffic_limit: str, server_id: str = None):
    if server_id is None:
        return
    expirations = load_expirations()
    if username not in expirations:
        expirations[username] = {}
    if server_id not in expirations[username]:
        expirations[username][server_id] = {}
    if expiration:
        if expiration.tzinfo is None:
            expiration = expiration.replace(tzinfo=UTC)
        expirations[username][server_id]['expiration_time'] = expiration
    else:
        expirations[username][server_id]['expiration_time'] = None
    expirations[username][server_id]['traffic_limit'] = traffic_limit
    save_expirations(expirations)

def remove_user_expiration(username: str, server_id: str = None):
    if server_id is None:
        return
    expirations = load_expirations()
    if username in expirations and server_id in expirations[username]:
        del expirations[username][server_id]
        if not expirations[username]:
            del expirations[username]
        save_expirations(expirations)

def get_users_with_expiration(server_id: str = None):
    if server_id is None:
        return []
    expirations = load_expirations()
    result = []
    for user, servers in expirations.items():
        if server_id in servers:
            info = servers[server_id]
            result.append((
                user,
                info['expiration_time'].isoformat() if info['expiration_time'] else None,
                info.get('traffic_limit', "Неограниченно")
            ))
    return result

def get_user_expiration(username: str, server_id: str = None):
    if server_id is None:
        return None
    expirations = load_expirations()
    return expirations.get(username, {}).get(server_id, {}).get('expiration_time', None)

def get_user_traffic_limit(username: str, server_id: str = None):
    if server_id is None:
        return "Неограниченно"
    expirations = load_expirations()
    return expirations.get(username, {}).get(server_id, {}).get('traffic_limit', "Неограниченно")

def ensure_peer_names(server_id=None):
    if server_id is None:
        return False
    try:
        clients = get_client_list(server_id=server_id)
        client_map = {client[1]: client[0] for client in clients}
        
        setting = get_config(server_id=server_id)
        wg_config_file = setting['wg_config_file']
        docker_container = setting['docker_container']
        
        cmd = f"docker exec -i {docker_container} cat {wg_config_file}"
        config_content = execute_docker_command(cmd, server_id=server_id)
        
        lines = config_content.splitlines()
        new_config = []
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if line.startswith('[Peer]'):
                new_config.append(line)
                peer_lines = []
                i += 1
                public_key = None
                while i < len(lines):
                    peer_line = lines[i].strip()
                    if peer_line == '':
                        break
                    if peer_line.startswith('PublicKey ='):
                        public_key = peer_line.split('=', 1)[1].strip()
                    if not peer_line.startswith('#'):
                        peer_lines.append(peer_line)
                    i += 1
                
                if public_key and public_key in client_map:
                    new_config.append(f"# {client_map[public_key]}")
                new_config.extend(peer_lines)
                new_config.append('')
            else:
                new_config.append(line)
                i += 1
        
        new_config_content = '\n'.join(new_config)
        
        if setting.get('is_remote') == 'true':
            servers = load_servers()
            server_config = servers.get(server_id, {})
            
            if server_id in SSHManager._instances and hasattr(SSHManager._instances[server_id], '_original_password'):
                ssh = SSHManager._instances[server_id]
            else:
                ssh = SSHManager(
                    server_id=server_id,
                    host=server_config.get('host'),
                    port=int(server_config.get('port', 22)),
                    username=server_config.get('username'),
                    auth_type=server_config.get('auth_type'),
                    key_path=server_config.get('key_path'),
                    password=server_config.get('_original_password')
                )
            
            ssh.execute_command(f'echo \'{new_config_content}\' > /tmp/wg0.conf')
            ssh.execute_command(f'docker cp /tmp/wg0.conf {docker_container}:{wg_config_file}')
            ssh.execute_command('rm -f /tmp/wg0.conf')
        else:
            with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
                temp_file.write(new_config_content)
                temp_path = temp_file.name
            
            subprocess.run(['docker', 'cp', temp_path, f"{docker_container}:{wg_config_file}"])
            os.unlink(temp_path)
        
        return True
    except Exception as e:
        logger.error(f"Ошибка при обновлении имен пиров: {e}")
        return False
