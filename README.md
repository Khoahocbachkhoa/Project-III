# Asynchronous Image Processing Pipeline

Hệ thống xử lý ảnh bất đồng bộ (asynchronous) với khả năng tự động mở rộng (autoscaling) theo tải.

## 1. Ý tưởng

Khi client gửi nhiều ảnh cùng lúc, server không xử lý trực tiếp trong request mà:

1. Nhận ảnh, lưu trữ và trả về ngay một `task_id`.
2. Đẩy tác vụ vào hàng đợi (message queue).
3. Nhiều worker cùng lấy tác vụ từ hàng đợi để xử lý song song.
4. Số lượng worker tự động tăng/giảm theo độ dài hàng đợi. 
5. Client dùng `task_id` để tra cứu trạng thái và kết quả.

## 2. Kiến trúc

```
                       ┌──────────────┐
  Client ── upload ──▶ │     API      │ ──▶ MinIO       (lưu ảnh gốc)
                       │              │ ──▶ PostgreSQL  (lưu trạng thái task)
                       └──────┬───────┘
                              │ publish task
                              ▼
                       ┌──────────────┐
                       │  RabbitMQ    │  Message queue
                       └──────┬───────┘
                              │ consume
          ┌───────────────────┼───────────────────┐
          ▼                   ▼                   ▼
     Worker pod 1        Worker pod 2   ...   Worker pod N   (k8s cluster)
          │  đọc ảnh từ MinIO, xử lý, ghi kết quả vào PostgreSQL
          │
          └── KEDA theo dõi độ dài queue → tự scale số pod theo tải.
```

### Thành phần

| Thành phần | Công nghệ | Vai trò |
|---|---|---|
| Ingestion API | Go, Gin | Nhận ảnh, lưu MinIO + PostgreSQL, publish task vào RabbitMQ, cung cấp API tra cứu |
| Message queue | RabbitMQ | Hàng đợi `tasks` (durable), phân phối tác vụ cho các worker |
| Object storage | MinIO | Lưu ảnh gốc để cho worker truy cập và xử lý |
| Database | PostgreSQL | Lưu trạng thái và kết quả của từng task |
| Worker | Python (pika, Pillow) | Lấy task, đọc ảnh, xử lý (mock), cập nhật kết quả |
| Autoscaling | KEDA | Scale worker theo số message trong queue |

## 3. Luồng xử lý một ảnh

1. Client gọi `POST /api/v1/images` kèm file ảnh.
2. API sinh `task_id` (UUID), lưu ảnh vào MinIO.
3. API ghi bản ghi vào PostgreSQL với trạng thái `PENDING`.
4. API publish message `{task_id, object_name, bucket}` vào queue `tasks` và trả về `202 Accepted`.
5. Một worker nhận message, đổi trạng thái sang `PROCESSING`.
6. Worker tải ảnh từ MinIO, thực hiện xử lý ảnh (mock)
7. Worker ghi kết quả, đổi trạng thái sang `COMPLETED` rồi `ACK` message.
8. Nếu có lỗi, trạng thái chuyển sang `FAILED` và message bị `NACK` (không requeue).

Trạng thái task: `PENDING` → `PROCESSING` → `COMPLETED` / `FAILED`.

## 4. Cơ chế cân bằng tải

- Nhiều worker cùng đọc một queue, mỗi message chỉ được giao cho một worker.
- Mỗi worker chỉ giữ một message tại một thời điểm, nên worker xong trước sẽ nhận việc tiếp theo (phân chia theo tốc độ xử lý).
- **KEDA ScaledObject:** theo dõi độ dài queue `tasks`.
  - Mục tiêu khoảng 5 message trên mỗi worker (`QueueLength`, `value: 5`).
  - Số worker tối thiểu 1, tối đa 8.

## 5. API

### Upload ảnh

```
POST /api/v1/images
Content-Type: multipart/form-data
Field: image (file)
```

Phản hồi `202 Accepted`:

```json
{
  "task_id": "…",
  "object_name": "….jpg",
  "status": "PENDING"
}
```

### Tra cứu task

```
GET /api/v1/tasks/:id
```

Phản hồi `200 OK`:

```json
{
  "task_id": "…",
  "object_name": "….jpg",
  "status": "COMPLETED",
  "result": {
    "image_dimensions": { "width": 1920, "height": 1080 },
    "inference_time_ms": 1530
  },
  "created_at": "…",
  "updated_at": "…"
}
```

## 6. Cấu trúc thư mục

```
.
├── deploy
│   └── k8s
│       └── workder-keda.yaml   # Secret, TriggerAuthentication, Deployment worker, ScaledObject
├── docker-compose.yml          # Psql, Minio, RabbitMQ
├── ingestion-service           # API
│   ├── Dockerfile
│   ├── go.mod
│   ├── go.sum
│   └── main.go
├── README.md
├── scripts                     # Testing script
│   └── load_test.sh
└── worker                      # Worker
    ├── Dockerfile
    ├── main.py
    └── requirements.txt
```

## 7. Hướng dẫn chạy

### Yêu cầu

- Docker và Docker Compose
- Go
- `kubectl`, `kind`, `helm`

### Bước 1: Chạy hạ tầng (PostgreSQL, RabbitMQ, MinIO)

```bash
docker compose up -d
```

- RabbitMQ Management: http://localhost:15672 (guest / guest)
- MinIO Console: http://localhost:9001 (minioadmin / minioadmin)

### Bước 2: Tạo cluster và cài KEDA

```bash
kind create cluster --name p3

helm repo add kedacore https://kedacore.github.io/charts
helm repo update
helm install keda kedacore/keda -n keda --create-namespace
kubectl get pods -n keda
```

### Bước 3: Build image worker và nạp vào kind

```bash
docker build -t worker:v1 ./worker
kind load docker-image worker:v1 --name p3
```

### Bước 5: Triển khai worker và KEDA

```bash
kubectl apply -f ./deploy/k8s/worker-keda.yaml
kubectl get pods
kubectl get scaledobject
kubectl get hpa
```

### Bước 6: Chạy API

```bash
go run ingestion-service/.
```

API lắng nghe tại `http://localhost:8080`.

## 8. Kiểm thử

### Gửi một ảnh

```bash
curl -F "image=@test.jpg" http://localhost:8080/api/v1/images
curl http://localhost:8080/api/v1/tasks/<task_id>
```

### Kiểm thử tải (gửi đồng thời nhiều ảnh)

Chạy script tạo file ảnh giả lập và gửi 100 request song song, trong khi theo dõi số pod:

```bash
# Terminal 1
kubectl get pods -w

# Terminal 2
bash ./scripts/load_test.sh
```

### Kết quả quan sát được

- Queue `tasks` tăng lên khi nhiều ảnh được gửi cùng lúc.
- KEDA/HPA tăng số worker từ 1 lên tối đa 8 pod theo từng đợt.
- Các worker xử lý song song, queue giảm dần về 0.
- Khi hết tải, số pod giảm dần về 1 (mất khoảng vài phút do cửa sổ ổn định mặc định của HPA).

## 9. Những hạn chế hiện tại

- Phần xử lý ảnh là **giả lập** (chờ ngẫu nhiên 1–2 giây, đọc kích thước ảnh), chưa tích hợp mô hình thật.
- API mới chạy một instance ngoài cluster, **chưa được cân bằng tải** ở tầng nhận request.
- Chưa có cơ chế retry hoặc dead-letter queue; task lỗi bị bỏ (`NACK`, không requeue).
- Có thể xảy ra trạng thái không nhất quán nếu API lỗi giữa chừng (ví dụ đã lưu ảnh/DB nhưng publish thất bại thì task kẹt ở `PENDING`).
- Chưa giới hạn kích thước/loại file upload.
- Chưa có monitoring (Prometheus/Grafana) và chưa có test tự động.
- Phần kết quả chủ yếu là giả lập và cần phát triển thêm nhiều để có thể chạy thực tế.

## 10. Hướng phát triển

- Thay phần mock bằng mô hình xử lý ảnh thật.
- Đóng gói API thành Deployment, đặt sau Service/Ingress để scale thêm tầng request.
- Bổ sung retry.
- Thêm monitoring và logging.