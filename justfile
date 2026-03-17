# TinyObs justfile

# Build and run the Docker container
run:
    docker build -t tinyobs .
    docker run -it --rm \
        -p 4319:4319 \
        -v tinyobs-data:/app/data \
        --name tinyobs \
        tinyobs

# Reset: remove container, volume, and run again
reset:
    docker rm -f tinyobs 2>/dev/null || true
    docker volume rm tinyobs-data 2>/dev/null || true
    just run
