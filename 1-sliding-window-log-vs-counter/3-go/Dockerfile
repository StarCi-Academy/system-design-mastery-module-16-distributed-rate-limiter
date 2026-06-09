# ---- build stage ----
FROM golang:1.22-alpine AS build
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . ./
RUN go mod tidy && CGO_ENABLED=0 go build -o /sliding-window-service .

# ---- runtime stage ----
FROM alpine:3.20 AS runtime
WORKDIR /app
COPY --from=build /sliding-window-service ./sliding-window-service
EXPOSE 3000
CMD ["./sliding-window-service"]
