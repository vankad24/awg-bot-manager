# AmneziaVPN Telegram Bot

Телеграм-бот на Python для управления [AmneziaVPN](https://github.com/amnezia-vpn/amnezia-client). Этот бот позволяет легко управлять клиентами. Подразумевается, что у вас уже установлен Python 3.11.x. Используется библиотека `aiogram` версии 2.25.2.

## Оглавление

- [Возможности](#возможности)
- [Установка](#установка)
- [Запуск](#запуск)
- [Заметки](#заметки)
- [Поддержка](#поддержка)

## Возможности

- Добавление клиентов
- Удаление клиентов
- Создание временных конфигураций (1 час, 1 день, 1 неделя, 1 месяц, неограниченно)
- Получение информации об IP-адресе клиента (берется из Endpoint, используется API ресурса [ip-api.com](http://ip-api.com))
- Создание ключа в формате `vpn://` при генерации нового клиента (так же, при получении конфигурации клиента), для использования в [AmneziaVPN](https://github.com/amnezia-vpn/amnezia-client)
- Создание резервной копии

## Установка

1. Установите [AmneziaVPN](https://github.com/amnezia-vpn/amnezia-client) (без данного шага бот РАБОТАТЬ НЕ БУДЕТ).
2. Пройдите первоначальную [инициализацию](https://docs.amnezia.org/ru/documentation/instructions/install-vpn-on-server/), выбрав протокол AmneziaWG, в клиенте [AmneziaVPN](https://github.com/amnezia-vpn/amnezia-client).

3. Клонируйте репозиторий:

    ```bash
    git clone https://github.com/JB-SelfCompany/awg-docker-bot.git
    ```

    Перейдите в репозиторий:

    ```bash
    cd awg-docker-bot
    ```

  #### Опционально (рекомендуется устанавливать библиотеки в виртуальное окружение)
  
   Установка Python 3.11 для Linux:

    sudo apt-get install software-properties-common -y && sudo add-apt-repository ppa:deadsnakes/ppa && sudo apt update && sudo apt install python3.11 python3.11-dev python3.11-venv -y

   Установка Python 3.11 для Windows производится с официального [сайта](https://www.python.org/downloads/release/python-31110/). 
   
   Создайте и активируйте виртуальное окружение для Python:

    python3.11 -m venv myenv
        
   Активация виртуального окружения для Linux:
    
    source myenv/bin/activate

   Для Windows:
  
    python -m myenv\Scripts\activate

4. Установите зависимости:

    ```bash
    pip install -r requirements.txt
    sudo apt update && sudo apt install jq qrencode -y
    ```

5. Создайте бота в Telegram:

- Откройте Telegram и найдите бота [BotFather](https://t.me/BotFather).
- Начните диалог, отправив команду `/start`.
- Введите команду `/newbot`, чтобы создать нового бота.
- Следуйте инструкциям BotFather, чтобы:
    - Придумать имя для вашего бота (например, `WireGuardManagerBot`).
    - Придумать уникальное имя пользователя для бота (например, `WireGuardManagerBot_bot`). Оно должно оканчиваться на `_bot`.
- После создания бота BotFather отправит вам токен для доступа к API. Его запросит бот во время первоначальной инициализации.

## Запуск

1. Запустите бота:

    ```bash                           
    python3.11 bot_manager.py              
    ```
    
2. Добавьте бота в Telegram и отправьте команду `/start` или `/help` для начала работы.

## Заметки

При создании резервной копии, в архив добавляется директория connections (создается и содержит в себе логи подключений клиентов), conf, png, и сам конфигурационный файл. 

При первом запуске бота, по умолчанию будет существовать клиент с названием Unknown. Это пользователь, созданный самим [AmneziaVPN](https://github.com/amnezia-vpn/amnezia-client) в процессе [инициализации](https://docs.amnezia.org/ru/documentation/instructions/install-vpn-on-server/). По желанию, можете удалить его. 

Так же, вы можете запускать бота как службу на вашем сервере. Для этого:
1. Скопируйте файл `awg-docker-bot.service` в директорию `/etc/systemd/system/`:

    ```bash
    sudo cp awg-docker-bot.service /etc/systemd/system/
    ```

2. Отредактируйте параметры внутри файла с помощью `nano` (или любого удобного текстового редактора):

    ```bash
    sudo nano /etc/systemd/system/awg-docker-bot.service
    ```
    
3. Перезагрузите системный демон и запустите службу:

    ```bash
    sudo systemctl daemon-reload
    sudo systemctl start awg-docker-bot.service
    sudo systemctl enable awg-docker-bot.service
    ```

## Поддержка

Если у вас возникли вопросы или проблемы с установкой и использованием бота, создайте [issue](https://github.com/JB-SelfCompany/awg-docker-bot/issues) в этом репозитории или обратитесь к разработчику.

- [Matrix](https://matrix.to/#/@jack_benq:shd.company)
