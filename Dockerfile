# Build stage
FROM golang:1.23 as builder
WORKDIR /app
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /bin/server ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /bin/worker ./cmd/worker

# Final stage
FROM gcr.io/distroless/base-debian12
COPY --from=builder /bin/server /server
COPY --from=builder /bin/worker /worker
EXPOSE 8080
ENTRYPOINT ["/server"]
