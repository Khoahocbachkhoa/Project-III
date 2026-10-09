#!/bin/bash

image="test.jpg"

# Ghi dữ liệu ngẫu nhiên giả lập ảnh
head -c 10240 /dev/urandom > "$image"

# Cùng lúc gửi 100 ảnh lên server
for _ in $(seq 1 100); do
  curl -sF "image=@$image" http://localhost:8080/api/v1/images &
done
wait