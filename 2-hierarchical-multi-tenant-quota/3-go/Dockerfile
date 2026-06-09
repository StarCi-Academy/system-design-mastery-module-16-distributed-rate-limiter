# ---- build stage ----
FROM golang:1.22-alpine AS build
WORKDIR /app
COPY go.mod ./
RUN go mod download || true
COPY . .
RUN go mod tidy && CGO_ENABLED=0 go build -o quota-service .

# ---- runtime stage ----
FROM alpine:3.20 AS runtime
WORKDIR /app
COPY --from=build /app/quota-service ./quota-service
EXPOSE 3000
CMD ["./quota-service"]
