package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	amqp "github.com/rabbitmq/amqp091-go"
)

type App struct {
	db          *pgxpool.Pool
	minioClient *minio.Client
	rabbitCh    *amqp.Channel
	bucketName  string
	queueName   string
}

type TaskPayload struct {
	TaskID     string `json:"task_id"`
	ObjectName string `json:"object_name"`
	Bucket     string `json:"bucket"`
}

func main() {
	ctx := context.Background()

	// Kết nối psql
	dbURL := "postgres://dev_user:dev_password@localhost:5432/cv_pipeline"
	dbPool, err := pgxpool.New(ctx, dbURL)

	if err != nil {
		log.Fatalf("Không thể kết nối psql: %v", err)
	}
	defer dbPool.Close()

	if err := dbPool.Ping(ctx); err != nil {
		log.Fatalf("Ping psql thất bại : %v", err)
	}

	query := `CREATE TABLE IF NOT EXISTS tasks (
			id VARCHAR(36) PRIMARY KEY,
			object_name VARCHAR(255) NOT NULL,
			status VARCHAR(20) DEFAULT 'PENDING',
			result JSONB,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);`

	_, err = dbPool.Exec(ctx, query)

	if err != nil {
		log.Fatalf("Khởi tạo bảng thất bại: %v", err)
	}

	log.Println("Đã kết nối PostgreSQL!")

	// Kết nối MinIO
	minioEndpoint := "localhost:9000"
	minioUser := "minioadmin"
	minioPass := "minioadmin"
	bucketName := "raw-images"

	minioClient, err := minio.New(minioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(minioUser, minioPass, ""),
		Secure: false,
	})
	if err != nil {
		log.Fatalf("Không thể khởi tạo MinIO: %v", err)
	}

	// Khởi tạo bucket
	exists, err := minioClient.BucketExists(ctx, bucketName)
	if err != nil || !exists {
		err = minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
		if err != nil {
			log.Fatalf("Không thể tạo bucket MinIO: %v", err)
		}
	}

	log.Println("Đã kết nối MinIO!")

	// Kết nối RabbitMQ
	rabbitURL := "amqp://guest:guest@localhost:5672/"
	rabbitConn, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Fatalf("Không thể kết nối RabbitMQ: %v", err)
	}

	defer rabbitConn.Close()

	// Tạo một channel cho kết nối
	rabbitCh, err := rabbitConn.Channel()
	if err != nil {
		log.Fatalf("Không thể mở channel RabbitMQ: %v", err)
	}
	defer rabbitCh.Close()

	// Khai báo một queue
	queueName := "tasks"

	_, err = rabbitCh.QueueDeclare(
		queueName,
		true,  // durable
		false, // autoDelete
		false, // exclusive
		false, // noWait
		nil,
	)

	if err != nil {
		log.Fatalf("Không thể khai báo queue: %v", err)
	}

	app := &App{
		db:          dbPool,
		minioClient: minioClient,
		rabbitCh:    rabbitCh,
		bucketName:  bucketName,
		queueName:   queueName,
	}

	log.Println("Khởi tạo RabbitMQ thành công")

	// Khởi tạo web server
	r := gin.Default()
	v1 := r.Group("/api/v1")
	{
		v1.POST("/images", app.handleUpload)
		v1.GET("/tasks/:id", app.handleGetTask)
	}

	log.Println("Lắng nghe tại port 8080...")
	if err := r.Run(":8080"); err != nil {
		log.Fatalf("Lỗi khởi chạy HTTP server: %v", err)
	}
}

// Xử lý upload ảnh bất đồng bộ
func (a *App) handleUpload(c *gin.Context) {
	file, header, err := c.Request.FormFile("image")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Thiếu file ảnh (form-data key: 'image')"})
		return
	}
	defer file.Close()

	taskID := uuid.New().String()
	ext := filepath.Ext(header.Filename)
	objectName := fmt.Sprintf("%s%s", taskID, ext)

	// Lưu ảnh vào minio
	contentType := header.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	_, err = a.minioClient.PutObject(c.Request.Context(), a.bucketName, objectName, file, header.Size, minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Lỗi lưu ảnh vào MinIO"})
		return
	}

	// Lưu trạng thái PENDING vào psql
	query := `INSERT INTO tasks (id, object_name, status, created_at, updated_at) VALUES ($1, $2, 'PENDING', NOW(), NOW())`
	_, err = a.db.Exec(c.Request.Context(), query, taskID, objectName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Lỗi database"})
		return
	}

	// Tạo task vào rabbitMQ
	payloadData := TaskPayload{
		TaskID:     taskID,
		ObjectName: objectName,
		Bucket:     a.bucketName,
	}
	payloadBytes, _ := json.Marshal(payloadData)

	err = a.rabbitCh.PublishWithContext(
		c.Request.Context(),
		"",          // exchange mặc định
		a.queueName, // routing key = tên queue
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         payloadBytes,
		},
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Lỗi gửi task vào Message Queue"})
		return
	}

	// Trả về JSON cùng với ID
	c.JSON(http.StatusAccepted, gin.H{
		"task_id":     taskID,
		"object_name": objectName,
		"status":      "PENDING",
	})
}

// Tra cứu task
func (a *App) handleGetTask(c *gin.Context) {
	taskID := c.Param("id")

	var id, objectName, status string
	var rawResult []byte
	var createdAt, updatedAt time.Time

	query := `SELECT id, object_name, status, result, created_at, updated_at FROM tasks WHERE id = $1`
	row := a.db.QueryRow(c.Request.Context(), query, taskID)

	err := row.Scan(&id, &objectName, &status, &rawResult, &createdAt, &updatedAt)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Không tìm thấy task"})
		return
	}

	var parsedResult any
	if len(rawResult) > 0 {
		_ = json.Unmarshal(rawResult, &parsedResult)
	}

	c.JSON(http.StatusOK, gin.H{
		"task_id":     id,
		"object_name": objectName,
		"status":      status,
		"result":      parsedResult,
		"created_at":  createdAt,
		"updated_at":  updatedAt,
	})
}
