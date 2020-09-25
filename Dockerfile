FROM golang:alpine AS build

WORKDIR /workspace
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /bin/saver


FROM scratch

COPY --from=build /bin/saver /bin/

ENTRYPOINT [ "/bin/saver" ]
