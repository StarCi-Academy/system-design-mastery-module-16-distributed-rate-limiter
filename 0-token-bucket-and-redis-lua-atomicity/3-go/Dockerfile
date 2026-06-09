# ---- build stage ----
FROM golang:1.22-alpine AS build
WORKDIR /app
COPY go.mod ./
COPY main.go ./
# Resolve and pin dependencies (populates go.sum), then build a static binary.
RUN go mod tidy && CGO_ENABLED=0 go build -o token-bucket-service .

# ---- runtime stage ----
FROM alpine:3.20 AS runtime
WORKDIR /app
COPY --from=build /app/token-bucket-service ./
EXPOSE 3000
CMD ["./token-bucket-service"]
