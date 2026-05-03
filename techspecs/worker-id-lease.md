# Worker ID Lease — Technical Specification

## Vấn đề cần giải quyết

Snowflake ID yêu cầu mỗi node sinh ID phải mang một **Worker ID duy nhất** trong khoảng 0–1023. Trong môi trường container (Docker Swarm, Kubernetes), số lượng container thay đổi liên tục do deploy, crash, scale. Nếu dùng `INCR % 1024` thuần túy:

```
deploy lần 1  → INCR = 1   → worker 1
deploy lần 2  → INCR = 2   → worker 2
...
deploy lần 1025 → INCR = 1025 % 1024 = 1 → worker 1 (trùng với container cũ chưa chết!)
```

Container cũ (worker 1) vẫn còn sống, container mới cũng nhận worker 1 → **hai node cùng worker ID → collision**.

---

## Giải pháp: TTL-bound Lease với Heartbeat

Thay vì đếm tổng số lần khởi động, mỗi container **giữ một slot** trong Redis. Slot tự động hết hạn nếu container không còn sống.

### Cấu trúc Redis key

```
snowflake:worker:<id>  →  <random-token>   (TTL = 30s)
```

Mỗi ID (0–1023) tương ứng một key riêng. Giá trị là token ngẫu nhiên 32 hex chars — đóng vai trò định danh owner, ngăn container khác xóa nhầm key.

---

## Luồng hoạt động

### 1. Khởi động — Acquire

```
for id := 0; id <= 1023; id++ {
    SET snowflake:worker:<id> <token> NX PX 30000
    nếu OK → dùng id này, thoát vòng lặp
}
nếu không có id nào → trả về ErrNoWorkerIDAvailable
```

`SET NX` (Set if Not eXists) là atomic: đảm bảo không có hai container nào cùng claim một ID, ngay cả khi khởi động đồng thời.

**Code:** [`workerid/redis_lease.go:66-88`](../workerid/redis_lease.go#L66)

### 2. Heartbeat — refreshLoop

Background goroutine chạy mỗi **10 giây**, dùng Lua script để gia hạn TTL:

```lua
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
  return 0
end
```

Điều kiện `GET == token` đảm bảo chỉ owner thật sự mới được gia hạn. Nếu Redis đã trao key cho container khác (do TTL hết), refresh sẽ là no-op.

**Code:** [`workerid/redis_lease.go:90-103`](../workerid/redis_lease.go#L90)

### 3. Tắt graceful — Release

Khi nhận `SIGINT`/`SIGTERM`:

1. Dừng goroutine heartbeat.
2. Chạy Lua script xóa key ngay lập tức (nếu token vẫn khớp).

```lua
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end
```

Nhờ đó, ID được giải phóng ngay, không cần chờ 30s TTL — giúp deploy nhanh hơn.

**Code:** [`workerid/redis_lease.go:106-114`](../workerid/redis_lease.go#L106)

### 4. Container bị kill đột ngột (crash / OOM / Swarm kill)

Heartbeat dừng → Redis không nhận ping mới → key hết TTL sau tối đa **30 giây** → ID tự động available cho container tiếp theo.

---

## Tham số cấu hình

| Tham số        | Default | Ý nghĩa                                              |
|----------------|---------|------------------------------------------------------|
| `LeaseTTL`     | 30s     | Thời gian key tồn tại nếu không có heartbeat         |
| `RefreshEvery` | 10s     | Chu kỳ gia hạn TTL (phải < LeaseTTL / 2)            |
| `KeyPrefix`    | `snowflake:worker:` | Prefix của Redis key                   |
| `MaxWorkerID`  | 1023    | Giới hạn trên của worker ID (10-bit = 1023)          |

**Bất biến cần giữ:** `RefreshEvery < LeaseTTL / 2`  
Với mặc định 10s và 30s, TTL không bao giờ hết trong lúc container còn sống — trừ khi Redis mất kết nối > 30s.

---

## So sánh với INCR % 1024

| Tiêu chí                              | INCR % 1024   | TTL Lease (cách này)  |
|---------------------------------------|---------------|-----------------------|
| Collision khi deploy nhiều lần        | Có (sau 1024 lần) | Không              |
| Collision khi container chết chậm     | Có            | Không (TTL bảo vệ)    |
| Tốc độ khởi động                      | O(1)          | O(n) scan, tối đa 1024 SETNX |
| Giải phóng ID khi shutdown graceful   | Không         | Có (DEL ngay lập tức) |
| Phụ thuộc Redis liên tục              | Không         | Có (mất Redis > TTL → ID không được gia hạn) |

---

## Rủi ro còn lại

### Redis không khả dụng > LeaseTTL (30s)

Nếu Redis bị down hơn 30 giây trong lúc hệ thống đang chạy:

- Container hiện tại vẫn sinh ID bình thường (Snowflake không cần Redis để gen ID, chỉ cần khi acquire).
- Sau khi Redis khôi phục, heartbeat refresh lại thất bại trong khoảng downtime → key đã hết TTL → ID bị container khác claim.
- Nếu **container mới** được khởi động trong lúc Redis down và dùng static worker ID: có nguy cơ trùng.

**Mitigation:** Monitor Redis uptime; không dùng static `--worker-id` trong production trừ khi đã có cơ chế điều phối riêng.

### Slow-stop container

Docker Swarm / Kubernetes gửi `SIGTERM` rồi đợi `gracePeriod` (default 10s) trước khi `SIGKILL`. Code đã xử lý `SIGTERM` → Release chạy đúng. Nếu `SIGKILL` đến trước khi Release hoàn thành → key sẽ hết TTL tự nhiên sau tối đa 30s. Không có collision vì container cũ không còn gen ID sau khi bị kill.

---

## Sơ đồ vòng đời

```
Container khởi động
        │
        ▼
  SETNX worker:<id> token PX 30000
        │ ok
        ▼
  [gen ID bình thường]
        │
        ├──── mỗi 10s ──► PEXPIRE worker:<id> 30000  (nếu token khớp)
        │
  SIGTERM nhận được
        │
        ▼
  DEL worker:<id>  (nếu token khớp)
        │
        ▼
  container tắt

--- hoặc ---

  Container bị SIGKILL / crash
        │
        ▼
  [không ping nữa]
        │  30s sau
        ▼
  Redis tự xóa key
        │
        ▼
  Container mới claim được ID này
```

---

## File liên quan

| File | Vai trò |
|------|---------|
| [`workerid/redis_lease.go`](../workerid/redis_lease.go) | Toàn bộ logic acquire / heartbeat / release |
| [`snowflake/snowflake.go`](../snowflake/snowflake.go) | Sinh Snowflake ID, không biết gì về Redis |
| [`cmd/snowflake-server/main.go`](../cmd/snowflake-server/main.go) | Kết nối lease với HTTP server, xử lý graceful shutdown |
