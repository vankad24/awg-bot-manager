#!/bin/bash

SERVICE_NAME="awg_bot"

GREEN=$'\033[0;32m'
YELLOW=$'\033[1;33m'
RED=$'\033[0;31m'
BLUE=$'\033[0;34m'
NC=$'\033[0m'

# Определение абсолютного пути к скрипту
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCRIPT_NAME="$(basename "$0")"
SCRIPT_PATH="$SCRIPT_DIR/$SCRIPT_NAME"

run_with_spinner() {
    local description="$1"
    shift
    local cmd="$@"

    local stdout_temp=$(mktemp)
    local stderr_temp=$(mktemp)

    eval "$cmd" >"$stdout_temp" 2>"$stderr_temp" &
    local pid=$!

    local spinner='|/-\'
    local i=0

    echo -ne "\n${BLUE}${description}... ${spinner:i++%${#spinner}:1}${NC}"

    while kill -0 "$pid" 2>/dev/null; do
        printf "\r${BLUE}${description}... ${spinner:i++%${#spinner}:1}${NC}"
        sleep 0.1
    done

    wait "$pid"
    local status=$?
    if [ $status -eq 0 ]; then
        printf "\r${GREEN}${description}... Done!${NC}\n"
        rm -f "$stdout_temp" "$stderr_temp"
    else
        printf "\r${RED}${description}... Failed!${NC}\n"
        echo -e "${RED}Ошибка при выполнении команды: $cmd${NC}"
        echo -e "${RED}Вывод ошибки:${NC}"
        cat "$stderr_temp"
        rm -f "$stdout_temp" "$stderr_temp"
        exit 1
    fi
}

update_and_clean_system() {
    run_with_spinner "Обновление системы" "sudo apt-get update -qq && sudo apt-get upgrade -y -qq"
    run_with_spinner "Очистка системы от ненужных пакетов" "sudo apt-get autoclean -qq && sudo apt-get autoremove --purge -y -qq"
}

check_python() {
    if command -v python3.11 &>/dev/null; then
        echo -e "\n${GREEN}Python 3.11 установлен.${NC}"
    else
        echo -e "\n${RED}Python 3.11 не установлен или версия не подходит.${NC}"
        read -p "Установить Python 3.11? (y/n): " install_python
        if [[ "$install_python" == "y" || "$install_python" == "Y" ]]; then
            run_with_spinner "Установка Python 3.11" "sudo apt-get install software-properties-common -y && sudo add-apt-repository ppa:deadsnakes/ppa -y && sudo apt-get update -qq && sudo apt-get install python3.11 python3.11-venv python3.11-dev -y -qq"
            if ! command -v python3.11 &>/dev/null; then
                echo -e "\n${RED}Не удалось установить Python 3.11. Завершение работы.${NC}"
                exit 1
            fi
            echo -e "\n${GREEN}Python 3.11 успешно установлен.${NC}"
        else
            echo -e "\n${RED}Установка Python 3.11 обязательна. Завершение работы.${NC}"
            exit 1
        fi
    fi
}

install_dependencies() {
    run_with_spinner "Установка системных зависимостей" "sudo apt-get install jq net-tools iptables resolvconf git -y -qq"
}

install_and_configure_needrestart() {
    run_with_spinner "Установка needrestart" "sudo apt-get install needrestart -y -qq"

    sudo sed -i 's/^#\?\(nrconf{restart} = "\).*$/\1a";/' /etc/needrestart/needrestart.conf
    grep -q 'nrconf{restart} = "a";' /etc/needrestart/needrestart.conf || echo 'nrconf{restart} = "a";' | sudo tee /etc/needrestart/needrestart.conf >/dev/null 2>&1
}

clone_repository() {
    if [ ! -d "awg_bot" ]; then
        run_with_spinner "Клонирование репозитория" "git clone https://github.com/JB-SelfCompany/awg-docker-bot.git >/dev/null 2>&1"
        if [ $? -ne 0 ]; then
            echo -e "\n${RED}Ошибка при клонировании репозитория. Завершение работы.${NC}"
            exit 1
        fi
        echo -e "\n${GREEN}Репозиторий успешно клонирован.${NC}"
    else
        echo -e "\n${YELLOW}Репозиторий уже существует. Пропуск клонирования.${NC}"
    fi
    cd awg-docker-bot || { echo -e "\n${RED}Не удалось перейти в директорию awg-docker-bot. Завершение работы.${NC}"; exit 1; }
}

setup_venv() {
    if [ -d "myenv" ]; then
        echo -e "\n${YELLOW}Виртуальное окружение уже существует. Пропуск создания и установки зависимостей.${NC}"
    else
        run_with_spinner "Настройка виртуального окружения" "python3.11 -m venv myenv && source myenv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt && deactivate"
        echo -e "\n${GREEN}Виртуальное окружение настроено и зависимости установлены.${NC}"
    fi
}

set_permissions() {
    echo -e "\n${BLUE}Установка прав на скрипты...${NC}"

    echo "Текущая директория: $(pwd)"
    echo "Найденные .sh файлы:"
    find . -type f -name "*.sh" -print

    find . -type f -name "*.sh" -exec chmod +x {} \; 2>chmod_error.log

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}Права на скрипты установлены.${NC}"
    else
        echo -e "${RED}Ошибка при установке прав на скрипты. Проверьте файл chmod_error.log для деталей.${NC}"
        cat chmod_error.log
        rm -f chmod_error.log
        exit 1
    fi

    rm -f chmod_error.log
}

initialize_bot() {
    echo -e "\n${BLUE}Запуск бота для инициализации...${NC}"

    cd awg || { echo -e "\n${RED}Не удалось перейти в директорию awg. Завершение работы.${NC}"; exit 1; }

    ../myenv/bin/python3.11 bot_manager.py < /dev/tty &
    BOT_PID=$!

    echo -e "${YELLOW}Бот запущен с PID $BOT_PID. Пожалуйста, завершите инициализацию.${NC}"

    while [ ! -f "files/setting.ini" ]; do
        sleep 2
        if ! kill -0 "$BOT_PID" 2>/dev/null; then
            echo -e "\n${RED}Бот завершил работу до завершения инициализации. Завершение установки.${NC}"
            exit 1
        fi
    done

    echo -e "\n${GREEN}Инициализация завершена. Остановка бота...${NC}"
    kill "$BOT_PID"
    wait "$BOT_PID" 2>/dev/null

    echo -e "${GREEN}Бот остановлен.${NC}"

    cd ..
}

create_service() {
    run_with_spinner "Создание системной службы" "echo '[Unit]
Description=AmneziaVPN Docker Telegram Bot
After=network.target

[Service]
User=$USER
WorkingDirectory=$(pwd)/awg
ExecStart=$(pwd)/myenv/bin/python3.11 bot_manager.py
Restart=always

[Install]
WantedBy=multi-user.target' | sudo tee /etc/systemd/system/$SERVICE_NAME.service >/dev/null 2>&1"

    run_with_spinner "Перезагрузка демонов systemd" "sudo systemctl daemon-reload -qq"

    run_with_spinner "Запуск службы $SERVICE_NAME" "sudo systemctl start $SERVICE_NAME -qq"

    run_with_spinner "Включение службы $SERVICE_NAME при загрузке системы" "sudo systemctl enable $SERVICE_NAME -qq"

    if systemctl is-active --quiet "$SERVICE_NAME"; then
        echo -e "\n${GREEN}Служба $SERVICE_NAME успешно запущена.${NC}"
    else
        echo -e "\n${RED}Не удалось запустить службу $SERVICE_NAME. Проверьте логи с помощью команды:${NC}"
        echo "sudo systemctl status $SERVICE_NAME"
    fi
}

service_menu() {
    while true; do
        echo -e "\n=== Управление службой $SERVICE_NAME ==="
        sudo systemctl status "$SERVICE_NAME" | grep -E "Active:|Loaded:"

        echo -e "\n1. Остановить службу"
        echo "2. Перезапустить службу"
        echo "3. Удалить службу"
        echo "4. Выйти"
        read -p "Выберите действие: " action

        case $action in
            1)
                run_with_spinner "Остановка службы" "sudo systemctl stop $SERVICE_NAME -qq"
                echo -e "\n${GREEN}Служба остановлена.${NC}"
                ;;
            2)
                run_with_spinner "Перезапуск службы" "sudo systemctl restart $SERVICE_NAME -qq"
                echo -e "\n${GREEN}Служба перезапущена.${NC}"
                ;;
            3)
                run_with_spinner "Удаление службы" "sudo systemctl stop $SERVICE_NAME -qq && sudo systemctl disable $SERVICE_NAME -qq && sudo rm /etc/systemd/system/$SERVICE_NAME.service && sudo systemctl daemon-reload -qq"
                echo -e "\n${GREEN}Служба удалена.${NC}"
                ;;
            4)
                echo -e "\n${BLUE}Выход из меню управления.${NC}"
                break
                ;;
            *)
                echo -e "\n${RED}Некорректный ввод. Пожалуйста, выберите действительный вариант.${NC}"
                ;;
        esac
    done
}

install_bot() {
    update_and_clean_system
    check_python
    install_dependencies
    install_and_configure_needrestart
    clone_repository
    setup_venv
    set_permissions
    initialize_bot
    create_service
}

main() {
    echo -e "=== AWG Docker Telegram Bot ==="
    echo -e "Начало установки..."

    if systemctl list-units --type=service --all | grep -q "$SERVICE_NAME.service"; then
        echo -e "\n${YELLOW}Бот установлен в системе.${NC}"
        service_menu
    else
        echo -e "\n${RED}Бот не установлен.${NC}"
        install_bot
        echo -e "\n${GREEN}Установка завершена.${NC}"

        ( sleep 1; rm -- "$SCRIPT_PATH" ) &
        exit 0
    fi
}

main
