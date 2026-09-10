import logging
import os
import re
from datetime import datetime

FOLDER_DATE_RE = re.compile(r'^(\d{4})\.(\d{2})\.(\d{2})')


def _list_dir(client, remote_path: str) -> list:
    items = client.list(remote_path, get_info=True)
    # webdav3 иногда включает в список саму запрошенную папку - отсекаем её
    return [i for i in items if i.get('name') and i.get('path', '').rstrip('/') != remote_path.rstrip('/')]


def find_latest_date_folder(client, base_path: str) -> dict:
    """Ищет среди подпапок base_path самую свежую по дате в начале имени
    (формат "YYYY.MM.DD..."). Бросает ValueError, если таких подпапок нет."""
    entries = _list_dir(client, base_path)

    dated_folders = []
    for entry in entries:
        if not entry.get('isdir'):
            continue
        name = entry['name'].rstrip('/')
        match = FOLDER_DATE_RE.match(name)
        if not match:
            continue
        try:
            folder_date = datetime.strptime(match.group(0), '%Y.%m.%d')
        except ValueError:
            continue
        dated_folders.append((folder_date, name))

    if not dated_folders:
        raise ValueError(f"В папке '{base_path}' не найдено ни одной подпапки с датой в имени.")

    dated_folders.sort(key=lambda x: x[0])
    _, latest_name = dated_folders[-1]

    return {
        'folder_name': latest_name,
        'folder_path': os.path.join(base_path, latest_name),
    }


def find_local_file(client, folder_path: str) -> str:
    """Ищет в folder_path файл, в названии которого есть "LOCAL". Бросает
    ValueError, если такого файла нет."""
    entries = _list_dir(client, folder_path)

    local_files = [
        entry['name'] for entry in entries
        if not entry.get('isdir') and 'local' in entry['name'].lower()
    ]

    if not local_files:
        raise ValueError(f"В папке '{folder_path}' не найден файл с 'LOCAL' в названии.")
    if len(local_files) > 1:
        logging.warning(
            "В папке '%s' найдено несколько файлов с 'LOCAL': %s. Берём первый.",
            folder_path, local_files,
        )

    return local_files[0]
