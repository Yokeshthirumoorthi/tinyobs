# TinyObs justfile

# Build and run tinyobs-lite Docker container
run:
  docker build -f crates/tinyobs-lite/Dockerfile -t tinyobs .
  docker run -d --rm \
    -p 4318:4318 \
    -v tinyobs-data:/app/data \
    --name tinyobs \
    tinyobs

# Reset: remove container, volume, and run again
reset:
  docker rm -f tinyobs 2>/dev/null || true
  docker volume rm tinyobs-data 2>/dev/null || true
  just run

# Build and push tinyobs-lite to registry
push:
  docker build -f crates/tinyobs-lite/Dockerfile -t tinyobs .
  docker tag tinyobs tracing.paradise-grue.ts.net:5000/tinyobs
  docker push tracing.paradise-grue.ts.net:5000/tinyobs

# Build and run tinyobs-pro Docker container
run-pro:
  docker build -f crates/tinyobs-pro/docker/Dockerfile -t tinyobs-pro .
  docker run -d --rm \
    -p 4318:4318 \
    -v tinyobs-pro-data:/var/lib/clickhouse \
    --name tinyobs-pro \
    tinyobs-pro

# Reset tinyobs-pro
reset-pro:
  docker rm -f tinyobs-pro 2>/dev/null || true
  docker volume rm tinyobs-pro-data 2>/dev/null || true
  just run-pro

# Build and push tinyobs-pro to registry
push-pro:
  docker build -f crates/tinyobs-pro/docker/Dockerfile -t tinyobs-pro .
  docker tag tinyobs-pro tracing.paradise-grue.ts.net:5000/tinyobs-pro
  docker push tracing.paradise-grue.ts.net:5000/tinyobs-pro
