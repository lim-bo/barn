# Barn – Distributed Object Storage (S3-like)

Barn is a distributed object storage system inspired by the Amazon S3 specification.  
It provides **buckets**, **object management**, and **authentication** services via **gRPC** and an **HTTP Gateway**, making it compatible with REST-style clients while preserving the efficiency of gRPC for internal communication.

---

## 🚀 Features

- **Bucket management** (`Create`, `Delete`, `List`, `CheckExist`)
- **Authentication service**
  - Register with keys
  - Register with username/password
  - Login with username/password
- **Object service**
  - Upload objects (`PUT /{bucket}/{key}`)
  - Download objects (`GET /{bucket}/{key}`)
  - Get object metadata (`HEAD /{bucket}/{key}`)
  - Delete objects
  - List objects in bucket
- **Auth interceptor** with HMAC-based request signing
- **PostgreSQL storage** for users, buckets, and object metadata
- **HTTP-gateway** for compatibility with REST clients (curl, browsers)

---

## 🏗 Architecture

- **Clients** (CLI, SDKs, curl, browser) interact with the system via HTTP/REST or gRPC.  
- The **HTTP Gateway** translates REST requests into gRPC calls.  
- **AuthService**, **BucketService**, and **ObjectService** implement business logic.  
- **PostgreSQL** stores metadata (users, buckets, objects).  
- **ObjectService** stores raw file data in a local volume (or external storage backend in the future).  

---

## 📦 Services

Defined in [`services.proto`](./proto/services.proto).  
Example:

### BucketService
- `PUT /{name}` → Create bucket
- `GET /` → List buckets
- `DELETE /{name}` → Delete bucket
- `HEAD /{name}` → Check existence

### AuthService
- `POST /auth/register` → Register with keys
- `POST /auth/register-pass` → Register with credentials
- `POST /auth/login` → Login with credentials

### ObjectService
- `PUT /{bucket}/{key}` → Upload object (`application/octet-stream`)
- `GET /{bucket}/{key}` → Download object
- `HEAD /{bucket}/{key}` → Get object metadata (`etag`, `last-modified`, `content-length`)
- `DELETE /{bucket}/{key}` → Delete object
- `GET /{bucket}?limit=&offset=` → List objects

---

## ⚙️ Setup & Run

### Requirements
- Docker + Docker Compose

### Start all services

```bash
make prepare
cd deploy
docker compose up --build
```
## Roadmap
- Multipart upload
- CLI-tool for sending signed requests
- GO-package for sending signed requests
