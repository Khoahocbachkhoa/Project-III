# Project-III
## Asynchronous Image Processing Pipeline

### Temp

project-iii/
├── ingestion-service/    # Code Go API nhận ảnh
├── worker/               # Code Worker mô phỏng xử lý CV
├── deploy/               # Chứa file Dockerfile, docker-compose, K8s manifests
│   └── k8s/
├── scripts/              # Chứa file test tải / mock client
└── docker-compose.yml    # Khởi chạy hạ tầng nền cho local test

database:

CREATE TABLE IF NOT EXISTS tasks (
    id VARCHAR(36) PRIMARY KEY,
    object_name VARCHAR(255) NOT NULL,
    status VARCHAR(20) DEFAULT 'PENDING',
    result JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

## Task

- Xem lại DockerFile
- Xem lại kiểm tra các image
- Kiểm tra và viết chi tiết các câu lệnh cấu hình để chạy Demo được

- Cố lên nào, 

Hạn chốt bài này: Cuối tuần 10/10