import os
from ftplib import FTP

from core import settings


def download(hourly=False):
    if hourly:
        local_file_path = os.path.join(
            settings.FTP_serv_cred.current_directory,
            settings.FTP_serv_cred.local_file_hourly_path,
        )
    else:
        local_file_path = os.path.join(
            settings.FTP_serv_cred.current_directory,
            settings.FTP_serv_cred.local_file_path,
        )  # Полный путь для сохранения файла

    try:
        # Подключение к FTP-серверу
        ftp = FTP(settings.FTP_serv_cred.ftp_server)
        ftp.login(
            user=settings.FTP_serv_cred.ftp_user,
            passwd=settings.FTP_serv_cred.ftp_password,
        )

        # Загрузка файла
        with open(local_file_path, "wb") as local_file:
            if hourly:
                ftp.retrbinary(
                    f"RETR {settings.FTP_serv_cred.remote_file_hourly_path}",
                    local_file.write,
                )
            else:
                ftp.retrbinary(
                    f"RETR {settings.FTP_serv_cred.remote_file_path}", local_file.write
                )

        # Закрытие соединения
        ftp.quit()
    except Exception as e:
        print(f"Ошибка: {e}")


# скрипт для загрузки на ФТП сервер
# remote_directory = "/httpdocs/shops/orders/"
# local_file_path = os.path.join(current_directory, "test.txt")  # Полный путь для сохранения файла

# try:
#     # Подключение к FTP-серверу
#     ftp = FTP(ftp_server)
#     ftp.login(user=ftp_user, passwd=ftp_password)
#     print("Успешное подключение к серверу")

#     # Переход в нужную директорию на сервере
#     ftp.cwd(remote_directory)

#     # Загрузка файла
#     with open(local_file_path, "rb") as local_file:
#         ftp.storbinary(f"STOR {os.path.basename(local_file_path)}", local_file)
#         print(f"Файл успешно загружен в {remote_directory}")

#     # Закрытие соединения
#     ftp.quit()
# except Exception as e:
#     print(f"Ошибка: {e}")
