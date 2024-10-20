# Use an official Go image as a base
FROM golang:1.22 as builder

# Set the working directory inside the container
WORKDIR /app

# Copy the Go modules manifest and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code
COPY . .

# Build the Go application
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o asb-client .

# Create a minimal runtime image
FROM alpine:latest

# Set the working directory inside the container
WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/asb-client .

# Make the binary executable
RUN chmod +x asb-client

# Set the entrypoint for the container
CMD ["./asb-client"]