"""
Кастомный трансфер с поддержкой:
- Multipart upload для S3 (>5MB)
- Resume после обрыва
- Pause/Stop с приоритетами
- Параллельная загрузка последовательностей
"""

import os
import sys
import json
import logging
import threading
import hashlib
import re
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List, TYPE_CHECKING
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.config import Config
from boto3.s3.transfer import TransferConfig
import fileseq
import ftrack_api

# Импортируем типы только для type checking, чтобы не ломать загрузку на этапе обнаружения плагинов
if TYPE_CHECKING:
    from ftrack_api.entity import Component, Location  # type: ignore

logger = logging.getLogger(__name__)

# Multipart threshold (5MB)
MULTIPART_THRESHOLD = 5 * 1024 * 1024  # 5MB
MULTIPART_CHUNK_SIZE = 8 * 1024 * 1024  # 8MB chunks


def copy_to_s3_multipart(
    source_path: str,
    bucket: str,
    key: str,
    endpoint_url: Optional[str] = None,
    s3_client: Optional[boto3.client] = None,
    job_data: Optional[Dict[str, Any]] = None,
    progress_callback: Optional[callable] = None
) -> Tuple[bool, int]:
    """Копировать файл в S3 с поддержкой multipart upload и resume.
    
    Args:
        source_path: Путь к исходному файлу
        bucket: Имя S3 bucket
        key: S3 key для файла
        endpoint_url: URL endpoint для S3
        s3_client: Предсозданный S3 клиент
        job_data: Job.data для сохранения прогресса и resume
        progress_callback: Функция для обновления прогресса (bytes_transferred, total_size)
    
    Returns:
        (success, bytes_copied)
    """
    try:
        if s3_client is None:
            s3_client = boto3.client(
                's3',
                endpoint_url=endpoint_url or os.getenv('S3_MINIO_ENDPOINT_URL'),
                config=Config(signature_version='s3v4'),
                use_ssl=True,
                verify=True,
            )
        
        import time
        total_size = os.path.getsize(source_path)
        bytes_copied = [0]
        last_job_update = [time.time()]  # Время последнего обновления job_data
        
        def callback(bytes_amount):
            bytes_copied[0] += bytes_amount
            if progress_callback:
                progress_callback(bytes_copied[0], total_size)
            
            # Обновляем job_data не на каждом callback, а раз в 5 секунд или каждые 10%
            # Это значительно ускоряет загрузку, так как datetime.now().isoformat() очень медленный
            if job_data is not None:
                current_time = time.time()
                progress = (bytes_copied[0] / total_size * 100) if total_size > 0 else 0
                should_update = (
                    current_time - last_job_update[0] >= 5.0 or  # Раз в 5 секунд
                    int(progress) % 10 == 0  # Или каждые 10%
                )
                
                if should_update:
                    job_data['bytes_transferred'] = bytes_copied[0]
                    job_data['last_updated'] = datetime.now(timezone.utc).isoformat()
                    last_job_update[0] = current_time
        
        # Используем стандартный boto3 upload_fileobj с TransferConfig
        # Это автоматически использует multipart upload с параллельной загрузкой частей для больших файлов
        
        # Для больших файлов увеличиваем размер chunk для лучшей производительности
        # Чем больше файл, тем больше chunk size (как в test_full_transfer.py)
        if total_size > 100 * 1024 * 1024 * 1024:  # > 100 GB
            chunk_size = 128 * 1024 * 1024  # 128 MB
            max_concurrency = 10
        elif total_size > 10 * 1024 * 1024 * 1024:  # > 10 GB
            chunk_size = 64 * 1024 * 1024  # 64 MB
            max_concurrency = 15
        elif total_size > 1024 * 1024 * 1024:  # > 1 GB
            chunk_size = 32 * 1024 * 1024  # 32 MB
            max_concurrency = 15
        elif total_size > 100 * 1024 * 1024:  # > 100MB
            chunk_size = 16 * 1024 * 1024  # 16MB для больших файлов
            max_concurrency = 20
        elif total_size > 10 * 1024 * 1024:  # > 10MB
            chunk_size = 12 * 1024 * 1024  # 12MB для средних файлов
            max_concurrency = 20
        else:
            chunk_size = 8 * 1024 * 1024  # 8MB для маленьких файлов
            max_concurrency = 20
        
        # Переопределяем max_concurrency из job_data, если указан (но не для очень больших файлов)
        if job_data and total_size <= 100 * 1024 * 1024 * 1024:
            user_max_workers = job_data.get('max_workers')
            if user_max_workers:
                max_concurrency = max(user_max_workers, max_concurrency)
        
        transfer_config = TransferConfig(
            multipart_threshold=5 * 1024 * 1024,  # 5MB - порог для multipart
            multipart_chunksize=chunk_size,  # Динамический размер части
            max_concurrency=max_concurrency,  # Количество параллельных потоков для загрузки частей
            use_threads=True
        )
        
        logger.debug(f"  TransferConfig: max_concurrency={max_concurrency}, chunk_size={chunk_size / (1024*1024):.1f}MB")
        
        with open(source_path, 'rb') as f:
            s3_client.upload_fileobj(
                f, bucket, key,
                Config=transfer_config,
                Callback=callback
            )
        
        # Финальное обновление job_data
        if job_data is not None:
            job_data['bytes_transferred'] = bytes_copied[0]
            job_data['last_updated'] = datetime.now(timezone.utc).isoformat()
        
        logger.info(f"✓ Файл загружен в S3: {bytes_copied[0]}/{total_size} bytes")
        return True, bytes_copied[0]
        
    except Exception as e:
        logger.error(f"✗ Ошибка загрузки в S3: {e}", exc_info=True)
        return False, 0


def _upload_single_file(
    s3_client: boto3.client,
    source_path: str,
    bucket: str,
    key: str,
    progress_callback: Optional[callable],
    job_data: Optional[Dict[str, Any]]
) -> Tuple[bool, int]:
    """Обычная загрузка файла (для файлов <5MB)."""
    total_size = os.path.getsize(source_path)
    bytes_copied = [0]
    
    def callback(bytes_amount):
        bytes_copied[0] += bytes_amount
        if progress_callback:
            progress_callback(bytes_copied[0], total_size)
        if job_data is not None:
                        job_data['bytes_transferred'] = bytes_copied[0]
                        job_data['last_updated'] = datetime.now(timezone.utc).isoformat()
    
    # Используем TransferConfig для параллельной загрузки больших файлов
    max_concurrency = 20  # Увеличено для лучшей производительности
    if job_data:
        max_concurrency = max(job_data.get('max_workers', 20), 20)  # Минимум 20
    
    # Для больших файлов увеличиваем размер chunk
    if total_size > 100 * 1024 * 1024:  # > 100MB
        chunk_size = 16 * 1024 * 1024  # 16MB
    elif total_size > 10 * 1024 * 1024:  # > 10MB
        chunk_size = 12 * 1024 * 1024  # 12MB
    else:
        chunk_size = 8 * 1024 * 1024  # 8MB
    
    transfer_config = TransferConfig(
        multipart_threshold=5 * 1024 * 1024,  # 5MB
        multipart_chunksize=chunk_size,
        max_concurrency=max_concurrency,
        use_threads=True
    )
    
    with open(source_path, 'rb') as f:
        s3_client.upload_fileobj(f, bucket, key, Config=transfer_config, Callback=callback)
    
    logger.info(f"✓ Файл загружен в S3: {bytes_copied[0]}/{total_size} bytes")
    return True, bytes_copied[0]


def _upload_multipart(
    s3_client: boto3.client,
    source_path: str,
    bucket: str,
    key: str,
    total_size: int,
    progress_callback: Optional[callable],
    job_data: Optional[Dict[str, Any]]
) -> Tuple[bool, int]:
    """Multipart upload с поддержкой resume."""
    # Проверяем, есть ли незавершённый upload
    upload_id = None
    completed_parts = []
    bytes_transferred = 0
    
    if job_data:
        upload_id = job_data.get('upload_id')
        completed_parts = job_data.get('completed_parts', [])
        bytes_transferred = job_data.get('bytes_transferred', 0)
    
    # Если нет upload_id, начинаем новый multipart upload
    if not upload_id:
        try:
            response = s3_client.create_multipart_upload(
                Bucket=bucket,
                Key=key
            )
            upload_id = response['UploadId']
            if job_data:
                job_data['upload_id'] = upload_id
                job_data['completed_parts'] = []
                job_data['bytes_transferred'] = 0
            logger.info(f"  Начат multipart upload: {upload_id}")
        except Exception as e:
            logger.error(f"✗ Ошибка создания multipart upload: {e}")
            return False, 0
    else:
        logger.info(f"  Resume multipart upload: {upload_id} (уже загружено: {bytes_transferred}/{total_size})")
    
    # Вычисляем части для загрузки
    num_parts = (total_size + MULTIPART_CHUNK_SIZE - 1) // MULTIPART_CHUNK_SIZE
    parts_to_upload = []
    
    for part_num in range(1, num_parts + 1):
        # Проверяем, загружена ли эта часть
        part_uploaded = any(
            p.get('PartNumber') == part_num 
            for p in completed_parts
        )
        
        if not part_uploaded:
            parts_to_upload.append(part_num)
    
    if not parts_to_upload:
        # Все части загружены, завершаем upload
        return _complete_multipart_upload(
            s3_client, bucket, key, upload_id, completed_parts, total_size
        )
    
    # Загружаем оставшиеся части
    with open(source_path, 'rb') as f:
        for part_num in parts_to_upload:
            # Проверяем статус задачи (pause/stop)
            if job_data:
                status = job_data.get('status', 'running')
                if status == 'paused':
                    logger.info(f"  Трансфер приостановлен на части {part_num}")
                    return False, bytes_transferred
                elif status == 'stopped':
                    logger.info(f"  Трансфер остановлен на части {part_num}")
                    return False, bytes_transferred
            
            # Вычисляем диапазон байтов для этой части
            start_byte = (part_num - 1) * MULTIPART_CHUNK_SIZE
            end_byte = min(start_byte + MULTIPART_CHUNK_SIZE - 1, total_size - 1)
            part_size = end_byte - start_byte + 1
            
            f.seek(start_byte)
            part_data = f.read(part_size)
            
            try:
                response = s3_client.upload_part(
                    Bucket=bucket,
                    Key=key,
                    PartNumber=part_num,
                    UploadId=upload_id,
                    Body=part_data
                )
                
                etag = response['ETag']
                completed_parts.append({
                    'PartNumber': part_num,
                    'ETag': etag
                })
                
                bytes_transferred += part_size
                
                if progress_callback:
                    progress_callback(bytes_transferred, total_size)
                
                if job_data:
                    job_data['completed_parts'] = completed_parts
                    job_data['bytes_transferred'] = bytes_transferred
                    job_data['last_updated'] = datetime.now(timezone.utc).isoformat()
                
                logger.debug(f"  Часть {part_num}/{num_parts} загружена: {part_size} bytes")
                
            except Exception as e:
                logger.error(f"✗ Ошибка загрузки части {part_num}: {e}")
                return False, bytes_transferred
    
    # Все части загружены, завершаем upload
    return _complete_multipart_upload(
        s3_client, bucket, key, upload_id, completed_parts, total_size
    )


def _complete_multipart_upload(
    s3_client: boto3.client,
    bucket: str,
    key: str,
    upload_id: str,
    completed_parts: List[Dict],
    total_size: int
) -> Tuple[bool, int]:
    """Завершить multipart upload."""
    try:
        # Сортируем части по номеру
        completed_parts.sort(key=lambda x: x['PartNumber'])
        
        s3_client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={'Parts': completed_parts}
        )
        
        logger.info(f"✓ Multipart upload завершён: {total_size} bytes")
        return True, total_size
        
    except Exception as e:
        logger.error(f"✗ Ошибка завершения multipart upload: {e}")
        return False, 0


def copy_from_s3_to_disk(
    bucket: str,
    key: str,
    target_path: str,
    endpoint_url: Optional[str] = None,
    s3_client: Optional[boto3.client] = None,
    job_data: Optional[Dict[str, Any]] = None,
    progress_callback: Optional[callable] = None,
    session: Optional[ftrack_api.Session] = None
) -> Tuple[bool, int]:
    """Скачать файл из S3 на Disk с поддержкой resume.
    
    Args:
        bucket: Имя S3 bucket
        key: S3 key для файла
        target_path: Путь к целевому файлу на диске
        endpoint_url: URL endpoint для S3
        s3_client: Предсозданный S3 клиент
        job_data: Job.data для сохранения прогресса и resume
        progress_callback: Функция для обновления прогресса (bytes_transferred, total_size)
        session: ftrack Session для проверки статуса задачи
    
    Returns:
        (success, bytes_copied)
    """
    try:
        if s3_client is None:
            s3_client = boto3.client(
                's3',
                endpoint_url=endpoint_url or os.getenv('S3_MINIO_ENDPOINT_URL'),
                config=Config(signature_version='s3v4'),
                use_ssl=True,
                verify=True,
            )
        
        # Получаем размер файла из S3
        try:
            response = s3_client.head_object(Bucket=bucket, Key=key)
            total_size = response['ContentLength']
        except Exception as e:
            logger.error(f"✗ Ошибка получения размера файла из S3: {e}")
            return False, 0
        
        bytes_transferred = 0
        
        # Проверяем, есть ли частично загруженный файл для resume
        if job_data and os.path.exists(target_path):
            existing_size = os.path.getsize(target_path)
            if 0 < existing_size < total_size:
                bytes_transferred = existing_size
                logger.info(f"  Resume: файл уже существует ({existing_size}/{total_size} bytes)")
            elif existing_size == total_size:
                logger.info(f"  Файл уже полностью скачан ({total_size} bytes)")
                return True, total_size
        
        # Создаём директорию, если нужно
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        
        mode = 'ab' if bytes_transferred > 0 else 'wb'
        
        # Используем download_fileobj для скачивания
        # Настраиваем max_concurrency из job_data, если указан
        max_concurrency = 10  # По умолчанию
        if job_data:
            max_concurrency = job_data.get('max_workers', 10)
        
        transfer_config = TransferConfig(
            multipart_threshold=5 * 1024 * 1024,  # 5MB - порог для multipart
            multipart_chunksize=8 * 1024 * 1024,  # 8MB - размер части
            max_concurrency=max_concurrency,  # Количество параллельных потоков
            use_threads=True
        )
        
        def callback(bytes_amount):
            nonlocal bytes_transferred
            bytes_transferred += bytes_amount
            if progress_callback:
                progress_callback(bytes_transferred, total_size)
            if job_data is not None:
                job_data['bytes_transferred'] = bytes_transferred
                job_data['last_updated'] = datetime.now(timezone.utc).isoformat()
            
            # Проверяем статус задачи (pause/stop)
            try:
                if session and job_data and 'job_id' in job_data:
                    job_check = session.get('Job', job_data['job_id'])
                    if job_check:
                        status = job_check.get('status', 'running')
                        if status == 'paused':
                            raise InterruptedError("Transfer paused")
                        elif status == 'killed':
                            raise InterruptedError("Transfer stopped")
            except InterruptedError:
                raise
            except Exception:
                pass  # Игнорируем ошибки проверки статуса
        
        with open(target_path, mode) as f:
            if bytes_transferred > 0:
                # Resume: используем Range запрос для скачивания с нужной позиции
                try:
                    response = s3_client.get_object(
                        Bucket=bucket,
                        Key=key,
                        Range=f'bytes={bytes_transferred}-'
                    )
                    # Читаем данные и записываем в файл
                    chunk_size = 8 * 1024 * 1024  # 8MB chunks
                    while True:
                        chunk = response['Body'].read(chunk_size)
                        if not chunk:
                            break
                        f.write(chunk)
                        callback(len(chunk))
                except Exception as e:
                    logger.error(f"✗ Ошибка resume скачивания: {e}")
                    return False, bytes_transferred
            else:
                # Полное скачивание
                s3_client.download_fileobj(
                    bucket, key, f,
                    Config=transfer_config,
                    Callback=callback
                )
        
        logger.info(f"✓ Файл скачан из S3: {bytes_transferred}/{total_size} bytes")
        return True, bytes_transferred
        
    except InterruptedError:
        logger.info(f"  Трансфер прерван ({bytes_transferred}/{total_size} bytes)")
        return False, bytes_transferred
    except Exception as e:
        logger.error(f"✗ Ошибка скачивания из S3: {e}", exc_info=True)
        return False, bytes_transferred if 'bytes_transferred' in locals() else 0


def copy_file_to_disk(
    source_path: str,
    target_path: str,
    job_data: Optional[Dict[str, Any]] = None,
    progress_callback: Optional[callable] = None,
    chunk_size: int = 1024 * 1024,  # 1MB
    session: Optional[ftrack_api.Session] = None
) -> Tuple[bool, int]:
    """Копировать файл на Disk с поддержкой resume.
    
    Args:
        source_path: Путь к исходному файлу
        target_path: Путь к целевому файлу
        job_data: Job.data для сохранения прогресса и resume
        progress_callback: Функция для обновления прогресса (bytes_transferred, total_size)
        chunk_size: Размер блока для копирования
    
    Returns:
        (success, bytes_copied)
    """
    try:
        total_size = os.path.getsize(source_path)
        bytes_transferred = 0
        
        # Проверяем, есть ли частично загруженный файл для resume
        if job_data and os.path.exists(target_path):
            existing_size = os.path.getsize(target_path)
            if 0 < existing_size < total_size:
                bytes_transferred = existing_size
                logger.info(f"  Resume: файл уже существует ({existing_size}/{total_size} bytes)")
            elif existing_size == total_size:
                logger.info(f"  Файл уже полностью скопирован ({total_size} bytes)")
                return True, total_size
        
        # Создаём директорию, если нужно
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        
        mode = 'ab' if bytes_transferred > 0 else 'wb'
        
        with open(source_path, 'rb') as src:
            with open(target_path, mode) as dst:
                if bytes_transferred > 0:
                    src.seek(bytes_transferred)
                
                while True:
                    # Проверяем статус задачи (pause/stop) через job в session
                    # job_data может быть устаревшим, поэтому проверяем напрямую через session
                    try:
                        if session and job_data and 'job_id' in job_data:
                            job_check = session.get('Job', job_data['job_id'])
                            if job_check:
                                status = job_check.get('status', 'running')
                                if status == 'paused':
                                    logger.info(f"  Трансфер приостановлен ({bytes_transferred}/{total_size} bytes)")
                                    return False, bytes_transferred
                                elif status == 'killed':
                                    logger.info(f"  Трансфер остановлен ({bytes_transferred}/{total_size} bytes)")
                                    return False, bytes_transferred
                    except Exception:
                        pass  # Игнорируем ошибки проверки статуса
                    
                    chunk = src.read(chunk_size)
                    if not chunk:
                        break
                    
                    dst.write(chunk)
                    bytes_transferred += len(chunk)
                    
                    if progress_callback:
                        progress_callback(bytes_transferred, total_size)
                    
                    if job_data:
                        job_data['bytes_transferred'] = bytes_transferred
                        job_data['last_updated'] = datetime.now(timezone.utc).isoformat()
        
        logger.info(f"✓ Файл скопирован: {bytes_transferred}/{total_size} bytes")
        return True, bytes_transferred
        
    except Exception as e:
        logger.error(f"✗ Ошибка копирования файла: {e}", exc_info=True)
        return False, bytes_transferred if 'bytes_transferred' in locals() else 0


def transfer_component_custom(
    session: ftrack_api.Session,
    component: "Component",  # type: ignore
    source_location: "Location",  # type: ignore
    target_location: "Location",  # type: ignore
    job_data: Optional[Dict[str, Any]] = None,
    progress_callback: Optional[callable] = None
) -> bool:
    """Трансфер одного компонента используя кастомный механизм копирования.
    
    Args:
        session: ftrack Session
        component: Component entity для трансфера
        source_location: Source Location
        target_location: Target Location
        job_data: Job.data для сохранения прогресса (будет обновляться)
        progress_callback: Функция для обновления прогресса (bytes_transferred, total_size)
    
    Returns:
        True если успешно, False если ошибка
    """
    try:
        # 1. Получаем source resource_identifier и путь
        source_resource_id = source_location.get_resource_identifier(component)
        logger.info(f"  Source resource_identifier: {source_resource_id}")
        
        # Определяем тип source location
        source_path = None
        is_s3_source = False
        is_disk_source = False
        
        if source_location.accessor:
            if 's3' in str(type(source_location.accessor)).lower():
                is_s3_source = True
            elif isinstance(source_location.accessor, ftrack_api.accessor.disk.DiskAccessor):
                is_disk_source = True
                source_path = source_location.get_filesystem_path(component)
        
        if not is_s3_source and not is_disk_source:
            logger.error("✗ Source location должна быть Disk или S3")
            return False
        
        # 2. Генерируем target resource_identifier
        context = {'source_resource_identifier': source_resource_id}
        target_resource_id = target_location.structure.get_resource_identifier(component, context)
        logger.info(f"  Target resource_identifier: {target_resource_id}")
        
        # 3. Определяем тип target location
        is_s3_target = False
        is_disk_target = False
        
        if target_location.accessor:
            if 's3' in str(type(target_location.accessor)).lower():
                is_s3_target = True
            elif isinstance(target_location.accessor, ftrack_api.accessor.disk.DiskAccessor):
                is_disk_target = True
        
        if not is_s3_target and not is_disk_target:
            logger.error("✗ Target location должна быть Disk или S3")
            return False
        
        # 4. Проверяем, является ли это последовательностью и получаем список файлов
        is_sequence = False
        sequence_files = []  # Для Disk: пути к файлам, для S3: список (key, frame_num)
        
        if is_disk_source and source_path:
            # Проверяем, является ли это последовательностью по типу компонента или паттерну в пути
            if component.entity_type == 'SequenceComponent' or '%' in str(source_path) or '@' in str(source_path):
                is_sequence = True
                logger.info(f"  Обнаружена последовательность: {source_path}")
                
                try:
                    seq = fileseq.findSequenceOnDisk(str(source_path))
                    if seq:
                        sequence_files = [str(f) for f in seq]
                        logger.info(f"  Найдено файлов в последовательности: {len(sequence_files)}")
                        if not sequence_files:
                            logger.error(f"✗ Последовательность не найдена на диске: {source_path}")
                            return False
                    else:
                        logger.error(f"✗ Последовательность не найдена на диске: {source_path}")
                        return False
                except Exception as e:
                    logger.error(f"✗ Ошибка поиска последовательности: {e}", exc_info=True)
                    return False
            else:
                if not os.path.exists(source_path):
                    logger.error(f"✗ Файл не существует: {source_path}")
                    return False
                sequence_files = [source_path]
        
        elif is_s3_source:
            # Для S3 определяем последовательность по resource_identifier
            if component.entity_type == 'SequenceComponent' or '%' in str(source_resource_id) or '@' in str(source_resource_id):
                is_sequence = True
                logger.info(f"  Обнаружена последовательность в S3: {source_resource_id}")
                
                # Для S3 нужно получить реальный список файлов из bucket
                # Используем S3 list_objects для получения всех файлов с нужным префиксом
                try:
                    s3_bucket = os.getenv('S3_BUCKET', 'proj')
                    s3_endpoint = os.getenv('S3_MINIO_ENDPOINT_URL')
                    
                    # Создаём временный S3 клиент для получения списка файлов
                    temp_s3_client = boto3.client(
                        's3',
                        endpoint_url=s3_endpoint,
                        config=Config(signature_version='s3v4'),
                        use_ssl=True,
                        verify=True,
                    )
                    
                    # Получаем префикс (директорию) из resource_identifier
                    # Например: lunapark/whores/sh0010/heavy_seq/v018/geo_seq.%04d.sc
                    # Префикс: lunapark/whores/sh0010/heavy_seq/v018/geo_seq.
                    prefix = source_resource_id
                    # Удаляем паттерн, оставляя только префикс
                    if '%04d' in prefix:
                        # Заменяем %04d на пустую строку, но оставляем точку перед расширением
                        prefix = prefix.replace('%04d', '')
                    elif '%d' in prefix:
                        prefix = prefix.replace('%d', '')
                    elif '@' in prefix:
                        # Заменяем @@@@ на пустую строку для получения префикса
                        prefix = re.sub(r'@+', '', prefix)
                    
                    # Убеждаемся, что префикс заканчивается точкой (для поиска файлов типа geo_seq.1021.sc)
                    # Если после удаления паттерна осталась двойная точка (geo_seq..sc), убираем одну
                    prefix = re.sub(r'\.\.', '.', prefix)
                    
                    # Если префикс не заканчивается точкой, но должен (для последовательностей), добавляем
                    # Проверяем, есть ли расширение в конце
                    if '.' in prefix:
                        # Разделяем на путь и расширение
                        base, ext = os.path.splitext(prefix)
                        # Если base не заканчивается точкой, добавляем её для поиска файлов с номерами
                        if not base.endswith('.'):
                            prefix = base + '.'
                        else:
                            prefix = base
                    else:
                        # Если нет расширения, просто добавляем точку в конец
                        prefix = prefix + '.'
                    
                    logger.info(f"  Поиск файлов в S3 с префиксом: {prefix}")
                    
                    # Получаем список объектов из S3
                    sequence_files = []
                    paginator = temp_s3_client.get_paginator('list_objects_v2')
                    pages = paginator.paginate(Bucket=s3_bucket, Prefix=prefix)
                    
                    for page in pages:
                        if 'Contents' in page:
                            for obj in page['Contents']:
                                key = obj['Key']
                                # Проверяем, что файл соответствует паттерну последовательности
                                # Извлекаем номер кадра из имени файла
                                frame_match = re.search(r'\.(\d+)\.', key)
                                if frame_match:
                                    frame_num = int(frame_match.group(1))
                                    sequence_files.append((key, frame_num))
                    
                    # Сортируем по номеру кадра
                    sequence_files.sort(key=lambda x: x[1])
                    
                    logger.info(f"  Найдено файлов в последовательности S3: {len(sequence_files)}")
                    if not sequence_files:
                        logger.error(f"✗ Последовательность не найдена в S3: {source_resource_id} (префикс: {prefix})")
                        return False
                except Exception as e:
                    logger.error(f"✗ Ошибка получения списка последовательности из S3: {e}", exc_info=True)
                    # Fallback: используем один файл
                    sequence_files = [(source_resource_id, 0)]
            else:
                sequence_files = [(source_resource_id, 0)]
        
        # 5. Копируем файлы
        success = False
        bytes_copied = 0
        
        if is_s3_target:
            # Копирование в S3
            s3_bucket = os.getenv('S3_BUCKET', 'proj')
            s3_endpoint = os.getenv('S3_MINIO_ENDPOINT_URL')
            
            # Thread-local storage для S3 клиентов
            thread_local = threading.local()
            
            def get_s3_client():
                if not hasattr(thread_local, 's3_client'):
                    thread_local.s3_client = boto3.client(
                        's3',
                        endpoint_url=s3_endpoint,
                        config=Config(signature_version='s3v4'),
                        use_ssl=True,
                        verify=True,
                    )
                return thread_local.s3_client
            
            if len(sequence_files) > 1:
                # Последовательность файлов - параллельная загрузка
                # Используем max_workers из job_data, если указан, иначе 10
                max_workers = job_data.get('max_workers', 10) if job_data else 10
                logger.info(f"  Копирование последовательности из {len(sequence_files)} файлов в {max_workers} потоков...")
                
                # Вычисляем общий размер всех файлов для прогресса
                total_sequence_size = sum(os.path.getsize(f) for f in sequence_files if os.path.exists(f))
                logger.info(f"  Общий размер последовательности: {total_sequence_size} bytes")
                
                tasks = []
                for idx, source_file in enumerate(sequence_files):
                    if not os.path.exists(source_file):
                        continue
                    
                    frame_match = re.search(r'\.(\d+)\.', os.path.basename(source_file))
                    if frame_match:
                        frame_num = int(frame_match.group(1))
                    else:
                        frame_num = idx
                    
                    target_key = target_resource_id.replace('%04d', f'{frame_num:04d}')
                    tasks.append((idx, source_file, target_key, frame_num))
                
                total_bytes = 0
                files_copied = 0
                files_failed = 0
                bytes_lock = threading.Lock()
                
                def copy_single_file(task_data):
                    idx, source_file, target_key, frame_num = task_data
                    try:
                        s3_client = get_s3_client()
                        file_success, file_bytes = copy_to_s3_multipart(
                            source_file,
                            s3_bucket,
                            target_key,
                            s3_endpoint,
                            s3_client=s3_client,
                            job_data=None,  # Не передаём job_data в отдельные файлы, обновляем общий прогресс
                            progress_callback=None  # Обновляем прогресс централизованно
                        )
                        
                        # Обновляем общий прогресс
                        if file_success and progress_callback and total_sequence_size > 0:
                            with bytes_lock:
                                nonlocal total_bytes
                                total_bytes += file_bytes
                                progress_callback(total_bytes, total_sequence_size)
                        
                        return (file_success, file_bytes, idx)
                    except Exception as e:
                        logger.error(f"✗ Исключение: {os.path.basename(source_file)} - {e}")
                        return (False, 0, idx)
                
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_task = {executor.submit(copy_single_file, task): task for task in tasks}
                    
                    for future in as_completed(future_to_task):
                        try:
                            file_success, file_bytes, idx = future.result()
                            if file_success:
                                files_copied += 1
                            else:
                                files_failed += 1
                        except Exception as e:
                            logger.error(f"✗ Исключение при обработке результата: {e}", exc_info=True)
                            files_failed += 1
                
                if files_failed == 0:
                    logger.info(f"✓ Последовательность скопирована: {files_copied}/{len(tasks)} файлов, {total_bytes} bytes")
                    success = True
                    bytes_copied = total_bytes
                else:
                    logger.error(f"✗ Ошибки при копировании: {files_failed} файлов не скопированы")
                    success = False
            else:
                # Одиночный файл
                s3_client = get_s3_client()
                success, bytes_copied = copy_to_s3_multipart(
                    sequence_files[0],
                    s3_bucket,
                    target_resource_id,
                    s3_endpoint,
                    s3_client=s3_client,
                    job_data=job_data,
                    progress_callback=progress_callback
                )
        
        elif is_disk_target:
            # Копирование на Disk
            target_path = target_location.accessor.get_filesystem_path(target_resource_id)
            
            if is_s3_source:
                # S3 -> Disk
                s3_bucket = os.getenv('S3_BUCKET', 'proj')
                s3_endpoint = os.getenv('S3_MINIO_ENDPOINT_URL')
                
                # Thread-local storage для S3 клиентов
                thread_local = threading.local()
                
                def get_s3_client():
                    if not hasattr(thread_local, 's3_client'):
                        thread_local.s3_client = boto3.client(
                            's3',
                            endpoint_url=s3_endpoint,
                            config=Config(signature_version='s3v4'),
                            use_ssl=True,
                            verify=True,
                        )
                    return thread_local.s3_client
                
                if len(sequence_files) > 1:
                    # Последовательность файлов из S3
                    max_workers = job_data.get('max_workers', 10) if job_data else 10
                    logger.info(f"  Скачивание последовательности из {len(sequence_files)} файлов из S3 в {max_workers} потоков...")
                    
                    # TODO: Получить реальный список файлов из S3 bucket
                    # Пока используем простой подход с генерацией ключей
                    tasks = []
                    for idx, (source_key, frame_num) in enumerate(sequence_files):
                        # source_key уже содержит правильный S3 key с заменённым номером кадра
                        # frame_num уже извлечён из fileseq
                        
                        # Генерируем target resource_identifier для этого кадра
                        file_resource_id = target_resource_id
                        if '%04d' in file_resource_id:
                            file_resource_id = file_resource_id.replace('%04d', f'{frame_num:04d}')
                        elif '%d' in file_resource_id:
                            file_resource_id = file_resource_id.replace('%d', str(frame_num))
                        elif '@' in file_resource_id:
                            file_resource_id = re.sub(r'@+', lambda m: f'{frame_num:0{len(m.group())}d}', file_resource_id)
                        
                        file_target_path = target_location.accessor.get_filesystem_path(file_resource_id)
                        tasks.append((idx, source_key, file_target_path, frame_num))
                    
                    # Получаем общий размер всех файлов из S3
                    total_size = 0
                    s3_client = get_s3_client()
                    logger.info(f"  Получение размеров файлов из S3...")
                    for source_key, frame_num in sequence_files:
                        try:
                            response = s3_client.head_object(Bucket=s3_bucket, Key=source_key)
                            file_size = response.get('ContentLength', 0)
                            total_size += file_size
                        except Exception as e:
                            logger.warning(f"  Не удалось получить размер файла {source_key}: {e}")
                    
                    logger.info(f"  Общий размер последовательности: {total_size} bytes ({total_size / (1024*1024):.2f} MB)")
                    
                    total_bytes = 0
                    files_copied = 0
                    files_failed = 0
                    bytes_lock = threading.Lock()
                    
                    def download_single_file(task_data):
                        idx, source_key, target_file, frame_num = task_data
                        try:
                            s3_client = get_s3_client()
                            file_success, file_bytes = copy_from_s3_to_disk(
                                s3_bucket,
                                source_key,
                                target_file,
                                s3_endpoint,
                                s3_client=s3_client,
                                job_data=None,  # Не передаём job_data в отдельные файлы
                                progress_callback=None,  # Обновляем прогресс централизованно
                                session=session
                            )
                            
                            # Обновляем общий прогресс
                            if file_success and progress_callback:
                                with bytes_lock:
                                    nonlocal total_bytes
                                    total_bytes += file_bytes
                                    # Обновляем прогресс с правильным общим размером
                                    if total_size > 0:
                                        progress_callback(total_bytes, total_size)
                            
                            return (file_success, file_bytes, idx)
                        except Exception as e:
                            logger.error(f"✗ Исключение: {source_key} - {e}")
                            return (False, 0, idx)
                    
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        future_to_task = {executor.submit(download_single_file, task): task for task in tasks}
                        
                        for future in as_completed(future_to_task):
                            try:
                                file_success, file_bytes, idx = future.result()
                                if file_success:
                                    files_copied += 1
                                else:
                                    files_failed += 1
                            except Exception as e:
                                logger.error(f"✗ Исключение при обработке результата: {e}", exc_info=True)
                                files_failed += 1
                    
                    if files_failed == 0:
                        logger.info(f"✓ Последовательность скачана: {files_copied}/{len(tasks)} файлов, {total_bytes} bytes")
                        success = True
                        bytes_copied = total_bytes
                    else:
                        logger.error(f"✗ Ошибки при скачивании: {files_failed} файлов не скачаны")
                        success = False
                else:
                    # Одиночный файл из S3
                    s3_client = get_s3_client()
                    source_key = sequence_files[0][0] if isinstance(sequence_files[0], tuple) else sequence_files[0]
                    os.makedirs(os.path.dirname(target_path), exist_ok=True)
                    success, bytes_copied = copy_from_s3_to_disk(
                        s3_bucket,
                        source_key,
                        target_path,
                        s3_endpoint,
                        s3_client=s3_client,
                        job_data=job_data,
                        progress_callback=progress_callback,
                        session=session
                    )
            
            elif is_disk_source:
                # Disk -> Disk
                if len(sequence_files) > 1:
                    # Последовательность файлов
                    total_bytes = 0
                    files_copied = 0
                    
                    for idx, source_file in enumerate(sequence_files):
                        if not os.path.exists(source_file):
                            continue
                        
                        frame_match = re.search(r'\.(\d+)\.', os.path.basename(source_file))
                        if frame_match:
                            frame_num = int(frame_match.group(1))
                        else:
                            frame_num = idx
                        
                        file_resource_id = target_resource_id.replace('%04d', f'{frame_num:04d}')
                        file_target_path = target_location.accessor.get_filesystem_path(file_resource_id)
                        
                        os.makedirs(os.path.dirname(file_target_path), exist_ok=True)
                        
                        file_success, file_bytes = copy_file_to_disk(
                            source_file,
                            file_target_path,
                            job_data=job_data,
                            session=session
                        )
                        
                        if file_success:
                            total_bytes += file_bytes
                            files_copied += 1
                    
                    if files_copied == len(sequence_files):
                        logger.info(f"✓ Последовательность скопирована: {files_copied}/{len(sequence_files)} файлов, {total_bytes} bytes")
                        success = True
                        bytes_copied = total_bytes
                    else:
                        logger.error(f"✗ Ошибки при копировании: {len(sequence_files) - files_copied} файлов не скопированы")
                        success = False
                else:
                    # Одиночный файл
                    os.makedirs(os.path.dirname(target_path), exist_ok=True)
                    success, bytes_copied = copy_file_to_disk(
                        sequence_files[0],
                        target_path,
                        job_data=job_data,
                        progress_callback=progress_callback,
                        session=session
                    )
        
        if not success:
            logger.error("✗ Копирование не удалось")
            return False
        
        # 6. Регистрируем компонент в target location
        try:
            component_location = session.create(
                'ComponentLocation',
                data={
                    'component': component,
                    'location': target_location,
                    'resource_identifier': target_resource_id
                }
            )
            session.commit()
            logger.info(f"✓ Компонент зарегистрирован в target location")
            return True
            
        except Exception as e:
            # Если компонент уже зарегистрирован, это нормально
            if 'DuplicateEntryError' in str(type(e).__name__):
                logger.info(f"✓ Компонент уже зарегистрирован в target location")
                return True
            logger.error(f"✗ Ошибка регистрации: {e}", exc_info=True)
            return False
            
    except Exception as e:
        logger.error(f"✗ Ошибка трансфера компонента: {e}", exc_info=True)
        return False
