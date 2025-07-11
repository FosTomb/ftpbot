import nest_asyncio
nest_asyncio.apply()

import asyncio
import threading
import posixpath
import stat
import re
from io import BytesIO

import paramiko
import pytz
from ftplib import FTP, error_perm
from apscheduler.schedulers.asyncio import AsyncIOScheduler



# --- Функции мониторинга ---
# Глобальный набор известных элементов (имен) для мониторинга
known_items = set()

def extract_code(name: str) -> str:
    """
    Пытается извлечь либо 12-значный (UPC), либо 13-значный (EAN) код из имени.
    Если найден код, возвращает его; иначе – исходное имя.
    """
    match = re.search(r'\b(\d{12}|\d{13})\b', name)
    if match:
        return match.group(0)
    return name

async def check_new_items(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.data  # получаем chat_id, переданный через data
    global known_items
    try:
        transport = paramiko.Transport((source_sftp_config['host'], 22))
        transport.connect(username=source_sftp_config['username'], password=source_sftp_config['password'])
        sftp = paramiko.SFTPClient.from_transport(transport)
        # Получаем список элементов в указанной директории
        items = sftp.listdir(source_sftp_config['directory'])
        sftp.close()
        transport.close()
        # Проверяем, появились ли новые элементы
        for item in items:
            if item not in known_items:
                known_items.add(item)
                code = extract_code(item)
                await context.bot.send_message(chat_id=chat_id, text=f"Новый релиз: {code}")
    except Exception as e:
        await context.bot.send_message(chat_id=chat_id, text=f"Ошибка при проверке элементов: {e}")

# --- Функции переноса файлов ---
def ftp_mkdir_p(ftp, remote_directory):
    """
    Рекурсивно создаёт директории на FTP-сервере.
    Формирует абсолютный путь.
    """
    dirs = remote_directory.strip("/").split("/")
    current_path = ""
    for d in dirs:
        current_path += "/" + d
        try:
            ftp.mkd(current_path)
        except error_perm:
            pass

def transfer_dir(sftp, ftp, sftp_path, ftp_base, send_update, relative_path=""):
    """
    Рекурсивно переносит файлы и папки с SFTP на FTP.
    После успешного копирования файла он удаляется с SFTP.
    """
    current_sftp_path = posixpath.join(sftp_path, relative_path)
    current_ftp_path = posixpath.join(ftp_base, relative_path)

    ftp.cwd('/')
    ftp_mkdir_p(ftp, current_ftp_path)

    for item in sftp.listdir_attr(current_sftp_path):
        sftp_item_path = posixpath.join(current_sftp_path, item.filename)
        relative_item_path = posixpath.join(relative_path, item.filename)
        if stat.S_ISDIR(item.st_mode):
            send_update(f"📁 Обработка папки: {relative_item_path}")
            transfer_dir(sftp, ftp, sftp_path, ftp_base, send_update, relative_item_path)
        elif stat.S_ISREG(item.st_mode):
            send_update(f"📄 Перенос файла: {relative_item_path}")
            ftp.cwd('/')
            ftp_mkdir_p(ftp, posixpath.dirname(relative_item_path))
            with BytesIO() as buffer:
                sftp.getfo(sftp_item_path, buffer)
                buffer.seek(0)
                ftp.storbinary(f"STOR {item.filename}", buffer)
            sftp.remove(sftp_item_path)
            send_update(f"🗑️ Файл удалён с источника: {relative_item_path}")
    if relative_path:
        try:
            sftp.rmdir(current_sftp_path)
            send_update(f"🗑️ Папка удалена с источника: {relative_path}")
        except Exception as e:
            send_update(f"Ошибка при удалении папки {relative_path}: {e}")

def sftp_to_ftp_with_structure(source_cfg, dest_cfg, send_update):
    """
    Устанавливает соединения с SFTP и FTP, запускает перенос файлов и закрывает соединения.
    """
    try:
        transport = paramiko.Transport((source_cfg['host'], 22))
        transport.connect(username=source_cfg['username'], password=source_cfg['password'])
        sftp = paramiko.SFTPClient.from_transport(transport)
    except Exception as e:
        send_update(f"Ошибка подключения к SFTP: {e}")
        return

    try:
        ftp = FTP(dest_cfg['host'])
        ftp.login(dest_cfg['user'], dest_cfg['passwd'])
    except Exception as e:
        send_update(f"Ошибка подключения к FTP: {e}")
        sftp.close()
        transport.close()
        return

    try:
        transfer_dir(sftp, ftp, source_cfg['directory'], dest_cfg['directory'], send_update)
        send_update("✅ Перенос файлов завершён успешно!")
    except Exception as e:
        send_update(f"Ошибка при переносе файлов: {e}")
    finally:
        try:
            sftp.close()
            transport.close()
            ftp.quit()
        except Exception:
            pass

# --- Команды бота ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Привет! Доступны команды:\n"
                                    "/monitor – начать мониторинг новых релизов\n"
                                    "/stopmonitor – остановить мониторинг\n"
                                    "/transfer – перенести файлы с SFTP на FTP")

async def monitor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    # Запускаем повторяющуюся задачу (каждые 30 секунд) с передачей chat_id через data
    context.job_queue.run_repeating(check_new_items, interval=30, first=0, data=chat_id, name=str(chat_id))
    await update.message.reply_text("Мониторинг новых релизов запущен.")

async def stopmonitor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    current_jobs = context.job_queue.get_jobs_by_name(str(chat_id))
    for job in current_jobs:
        job.schedule_removal()
    await update.message.reply_text("Мониторинг новых релизов остановлен.")

async def transfer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await update.message.reply_text("Запускается перенос файлов. Пожалуйста, подождите...")

    # Функция для отправки сообщений из отдельного потока:
    def send_update(message):
        # Планируем выполнение send_message в основном event loop
        asyncio.run_coroutine_threadsafe(
            context.bot.send_message(chat_id=chat_id, text=message),
            context.application.loop
        )

    def run_transfer():
        try:
            sftp_to_ftp_with_structure(source_sftp_config, dest_ftp_config, send_update)
        except Exception as e:
            send_update(f"Ошибка при запуске переноса: {e}")

    thread = threading.Thread(target=run_transfer)
    thread.start()


# --- Запуск приложения ---
async def main():
    app = ApplicationBuilder() \
        .token("7972509725:AAEcvy6pv0V6cFyFBtOURZAGk__Mc8bDS_E") \
        .build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("monitor", monitor))
    app.add_handler(CommandHandler("stopmonitor", stopmonitor))
    app.add_handler(CommandHandler("transfer", transfer))
    
    await app.run_polling(close_loop=False)

# Запуск через уже существующий event loop
loop = asyncio.get_event_loop()
loop.run_until_complete(main())
