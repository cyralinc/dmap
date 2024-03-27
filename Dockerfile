FROM golang:1.22 as build

# Set destination for COPY.
WORKDIR /app

# Download dependencies.
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code.
COPY . .

# Build.
RUN CGO_ENABLED=0 go build -o dmap cmd/*.go

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /app/dmap /dmap

ENTRYPOINT ["/dmap"]
