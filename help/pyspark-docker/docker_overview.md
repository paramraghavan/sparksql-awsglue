# How Docker Works

Docker is a platform designed to make it easier to create, deploy, and run applications by using containers. Containers allow a developer to package up an application with all parts it needs, such as libraries and other dependencies, and ship it all out as one package.

## 1. Docker Images
- **Definition:** A Docker image is a lightweight, standalone, executable package that includes everything needed to run a piece of software, including the code, a runtime, libraries, environment variables, and configuration files.
- **Creation:** Docker images are created using a file called a `Dockerfile`, which contains a set of instructions on how to build the image.
- **Storage:** Images are stored in Docker registries, such as Docker Hub, which can be public or private.

## 2. Docker Containers
- **Definition:** A container is a runnable instance of an image. You can create, start, stop, move, and delete a container using Docker commands.
- **Isolation:** Containers run in isolation from one another and the host system, ensuring that applications run consistently regardless of where they are deployed.
- **Lifecycle:** Containers can be started, stopped, and removed as needed, making them very flexible.

## 3. Docker Engine
- **Components:** The Docker Engine is the core component of Docker, comprising:
  - **Docker Daemon:** A background service responsible for managing images, containers, networks, and storage volumes.
  - **Docker CLI:** A command-line interface that users interact with to communicate with the Docker Daemon.
  - **REST API:** An API that programs can use to communicate with the Docker Daemon.

## 4. Docker Architecture
- **Client-Server Model:** Docker uses a client-server architecture where the Docker client communicates with the Docker Daemon (server), which does the heavy lifting of building, running, and distributing Docker containers.
- **Layers:** Docker images are made up of a series of layers. Each layer represents an instruction in the image’s Dockerfile. When an image is updated, only the layers that have changed are rebuilt.

## 5. Docker Registry
- **Public Registries:** Docker Hub is the default public registry where Docker images are stored and shared.
- **Private Registries:** Organizations can host their own private registries to store and manage Docker images internally.

## 6. Networking
- **Bridge Network:** The default network driver. Containers can communicate with each other and the host system.
- **Host Network:** A container shares the host's network stack.
- **Overlay Network:** Used for swarm services to communicate across multiple Docker Daemons.

## 7. Storage
- **Volumes:** Persist data beyond the life of a container. Volumes are the preferred mechanism for persisting data in Docker.
- **Bind Mounts:** A way to mount a file or directory from the host system into a container.

## Typical Workflow:
1. **Build:** Create a Dockerfile and build an image using `docker build`.
2. **Ship:** Push the image to a registry using `docker push`.
3. **Run:** Pull the image from the registry using `docker pull` and run it using `docker run`.

## Example Commands:
- **Build an image:** `docker build -t myapp .`
- **List images:** `docker images`
- **Run a container:** `docker run -d -p 8080:80 myapp`
- **List running containers:** `docker ps`
- **Stop a container:** `docker stop <container_id>`
- **Remove a container:** `docker rm <container_id>`
- **Start services:** docker-compose up
- **Stop services:** docker-compose down

Docker simplifies the deployment process, making it easier to manage and scale applications consistently across different environments.


## Example using Docker Compose to define and run multiple containers as part of a single application stack.

Docker Compose allows you to define a multi-container application with a docker-compose.yml file. This file can specify
multiple services, each with its own container configuration.

Example docker-compose.yaml
```yaml
version: '3.8'
services:
  web:
    image: my-web-app:latest
    ports:
      - "8080:80"
    depends_on:
      - db

  db:
    image: postgres:latest
    environment:
      POSTGRES_USER: example
      POSTGRES_PASSWORD: example

```

### Whats going on

**services**: Define each container as a service.
**web and db**: Names of the services. Each service can have its own configuration like image, ports, environment variables,
etc.
**depends_on:** Defines dependencies between services, ensuring that the db service starts before the web service.

Using Docker Compose, you can easily manage multi-container applications, ensuring that all components of your
application start, stop, and communicate with each other as needed.
