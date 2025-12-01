FROM alpine:latest

# Install C++ compiler and build tools
RUN apk add --no-cache g++ make cmake

WORKDIR /app

COPY src/ ./src/

# Compile the crawler
RUN g++ -o crawler src/main.cpp

CMD ["./crawler"]
