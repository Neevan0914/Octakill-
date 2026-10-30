# Builds the OCTAKILL game + multiplayer server (main.go) into a tiny image.
FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go octa-kill_1.html ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /octakill-server .

FROM scratch
COPY --from=build /octakill-server /octakill-server
EXPOSE 10000
ENV PORT=10000
CMD ["/octakill-server"]
