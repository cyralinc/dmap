FROM golang:1.22 as build

ARG VERSION="latest"

# Set destination for COPY.
WORKDIR /app

# Download dependencies.
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code.
COPY . .

# Build.
RUN CGO_ENABLED=0 go build -ldflags="-s -w -X main.version=$VERSION" -o dmap cmd/dmap/*.go

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /app/dmap /dmap

ENTRYPOINT ["/dmap"]
