# Decode / Debug Endpoint — Technical Specification

## Mục đích

Khi một record trong database được tạo, primary key của nó là một Snowflake ID (int64). Từ ID đó, ta có thể truy ngược lại:

- **Server nào** đã tạo record (Worker ID → hostname / instance)
- **Thời điểm chính xác** record được tạo (millisecond precision)
- **Thứ tự tương đối** trong cùng một millisecond (Sequence)

Không cần column `created_at` riêng — thông tin đã được nhúng trong ID.

---

## Cấu trúc bit của Snowflake ID

```
 63        22 21      12 11       0
 ┌──────────┬──────────┬──────────┐
 │ timestamp│ worker_id│ sequence │
 │ 42 bits  │ 10 bits  │ 12 bits  │
 └──────────┴──────────┴──────────┘
```

| Field       | Bits | Range         | Ý nghĩa                                        |
|-------------|------|---------------|------------------------------------------------|
| `timestamp` | 42   | ~139 năm      | Milliseconds kể từ epoch `2024-01-01T00:00:00Z` |
| `worker_id` | 10   | 0–1023        | Node sinh ID, map sang hostname/instance        |
| `sequence`  | 12   | 0–4095        | Counter trong cùng 1ms, reset về 0 ở ms mới    |

**Epoch mặc định:** `1704067200000` ms = `2024-01-01 00:00:00 UTC`  
**Code:** [`snowflake/snowflake.go:21`](../snowflake/snowflake.go#L21)

---

## Endpoint hiện tại: `GET /decode?id=<snowflake_id>`

**Code:** [`cmd/snowflake-server/main.go:87-95`](../cmd/snowflake-server/main.go#L87)

### Request

```
GET /decode?id=7283910234567892992
```

### Response hiện tại

```json
{
  "TimestampMs": 1704067201234,
  "WorkerID": 5,
  "Sequence": 0
}
```

`TimestampMs` là Unix timestamp dạng số nguyên — phải tự chuyển đổi để đọc được.

---

## Đề xuất mở rộng

Thêm các field human-readable vào response mà không thay đổi field cũ (backward compatible):

### Response mới

```json
{
  "TimestampMs": 1704067201234,
  "WorkerID":    5,
  "Sequence":    0,
  "time":        "2024-01-01T00:00:01.234Z",
  "worker_id":   5,
  "sequence":    0
}
```

| Field mới   | Kiểu    | Ý nghĩa                                         |
|-------------|---------|--------------------------------------------------|
| `time`      | string  | RFC3339Nano UTC — thời điểm tạo record           |
| `worker_id` | int     | Alias snake_case của `WorkerID`                  |
| `sequence`  | int     | Alias snake_case của `Sequence`                  |

`time` cho phép đọc trực tiếp mà không cần convert, đồng thời vẫn giữ `TimestampMs` để các client cũ không bị ảnh hưởng.

---

## Cách map Worker ID → Server

Worker ID **không tự biết** hostname của node đã tạo. Có hai cách thực tế:

### Cách 1: Log khi acquire (đơn giản, đủ dùng)

Khi server khởi động, log dòng:

```
acquired worker_id=5 host=backend-3.internal addr=:8080
```

Tra log theo `worker_id=<X>` → biết container nào đã giữ ID đó vào thời điểm nào.

### Cách 2: Lưu metadata vào Redis (tra cứu runtime)

Khi acquire thành công, ghi thêm key phụ:

```
snowflake:worker:meta:<id>  →  {"host":"backend-3","addr":":8080","since":"2024-01-01T00:00:00Z"}
```

TTL bằng `LeaseTTL`, refresh cùng với lease chính. Thêm endpoint:

```
GET /debug/worker?id=5
```

```json
{
  "worker_id": 5,
  "host":      "backend-3.internal",
  "addr":      ":8080",
  "since":     "2024-01-01T00:00:00.000Z",
  "status":    "active"
}
```

`status: "active"` khi key còn TTL, `"expired"` khi key đã biến mất (container đã chết).

---

## Luồng debug thực tế

```
Tìm record lạ trong DB
        │
        ▼
GET /decode?id=<snowflake_id>
        │
        ├── "time": "2024-03-15T14:23:01.456Z"   → biết lúc nào
        └── "worker_id": 5                        → biết node nào
                │
                ▼
        grep log "worker_id=5"
        hoặc GET /debug/worker?id=5
                │
                ▼
        biết hostname / container / pod đã sinh record đó
```

---

## File liên quan

| File | Liên quan |
|------|-----------|
| [`snowflake/snowflake.go:113-130`](../snowflake/snowflake.go#L113) | `Decoded` struct, hàm `Decode()` |
| [`cmd/snowflake-server/main.go:87-95`](../cmd/snowflake-server/main.go#L87) | Handler `/decode` hiện tại |
| [`workerid/redis_lease.go:66-88`](../workerid/redis_lease.go#L66) | Acquire — nơi log worker_id + host |
| [`techspecs/worker-id-lease.md`](./worker-id-lease.md) | Cơ chế TTL lease, map worker_id → container |
