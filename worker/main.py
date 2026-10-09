import json
import time
import random
import io
import os
import pika
import psycopg2
from minio import Minio
from PIL import Image

# Config
DB_CONFIG = {
    "dbname": "cv_pipeline",
    "user": "dev_user",
    "password": "dev_password",
    "host": os.getenv("DB_HOST", "localhost"),
    "port": 5432
}

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
QUEUE_NAME = "tasks"

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def process_image_task(task_id, bucket_name, object_name, minio_client, db_conn):
    with db_conn.cursor() as cursor:
        cursor.execute(
            "UPDATE tasks SET status = 'PROCESSING', updated_at = NOW() WHERE id = %s",
            (task_id,)
        )
        db_conn.commit()

    # Lấy ảnh từ Minio
    img_width, img_height = 0, 0
    try:
        response = minio_client.get_object(bucket_name, object_name)
        img_data = response.read()
        response.close()
        response.release_conn()

        with Image.open(io.BytesIO(img_data)) as img:
            img_width, img_height = img.size
    except Exception as e:
        print(f"[Worker] Không đọc được ảnh từ MinIO: {e}")

    # !!! Chưa tích hợp mô hình thật, giả lập kết quả đầu ra
    start_time = time.time()
    mock_delay = random.uniform(1.0, 2.0)
    time.sleep(mock_delay)
    inference_time_ms = int((time.time() - start_time) * 1000)

    # Mô phỏng kết quả đầu ra
    mock_result = {
        "image_dimensions": {"width": img_width, "height": img_height},
        "inference_time_ms": inference_time_ms,
    }

    # Cập nhật kết quả vào psql
    with db_conn.cursor() as cursor:
        cursor.execute(
            "UPDATE tasks SET status = 'COMPLETED', result = %s, updated_at = NOW() WHERE id = %s",
            (json.dumps(mock_result), task_id)
        )
        db_conn.commit()

def mark_task_failed(task_id, error_message, db_conn):
    # Cập nhật trạng thái khi xử lý lỗi
    try:
        with db_conn.cursor() as cursor:
            err_payload = json.dumps({"error": str(error_message)})
            cursor.execute(
                "UPDATE tasks SET status = 'FAILED', result = %s, updated_at = NOW() WHERE id = %s",
                (err_payload, task_id)
            )
            db_conn.commit()
    except Exception as db_err:
        print(f"[Worker] Không thể ghi nhận trạng thái FAILED vào DB: {db_err}")
        db_conn.rollback()
    

def main():
    # Khởi tạo kết nối MinIO
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    print("[Worker] Đã kết nối MinIO!")

    # Khởi tạo kết nối PostgreSQL
    db_conn = get_db_connection()
    print("[Worker] Đã kết nối PostgreSQL!")

    # Khởi tạo kết nối RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    channel.basic_qos(prefetch_count=1)

    def on_message(ch, method, properties, body):
        try:
            task = json.loads(body.decode("utf-8"))
            task_id = task.get("task_id")
            object_name = task.get("object_name")
            bucket_name = task.get("bucket", "raw-images")

            print(f"[Worker] Nhận task: {task_id} (File: {object_name})")

            # Xử lý
            process_image_task(task_id, bucket_name, object_name, minio_client, db_conn)

            # Gửi ACK xác nhận hoàn thành
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f"[Worker] Đã xử lý xong & ACK task: {task_id}")

        except Exception as err:
            print(f"[Worker] Lỗi xử lý task: {err}")

            # Cập nhật status là failed trong db
            if task_id:
                mark_task_failed(task_id, str(err), db_conn)

            # Từ chối message và không requeue nếu gặp lỗi format/fatal
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message)

    print(f"[Worker] Đang lắng nghe queue '{QUEUE_NAME}'")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("[Worker] Dừng worker.")
        channel.stop_consuming()
        connection.close()
        db_conn.close()

if __name__ == "__main__":
    main()