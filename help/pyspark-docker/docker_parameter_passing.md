# Parameter passing

Passing parameters to code running inside a Docker container can be done in several ways, depending on what you need to
achieve. Here are some common methods:

## Using Environment Variables
You can pass environment variables to your Docker container using the -e flag with the docker run command or by
specifying them in a docker-compose.yml file.

**Example using docker run:**
```shell
docker run -e MY_ENV_VAR=value myimage

```
**Corresponding docker-compose.yaml:**
```yaml
version: '3.8'
services:
  myservice:
    image: myimage
    environment:
      - MY_ENV_VAR=value

```

**Inside your code**, you can access these environment variables. For example, in Python:
```python
import os

my_var = os.getenv('MY_ENV_VAR')
print(my_var)

```

## Using Command-Line Arguments
You can pass command-line arguments to your entrypoint or command defined in the Dockerfile.
**Example Dockerfile:**
```dockerfile
FROM python:3.8

COPY script.py /script.py

ENTRYPOINT ["python", "/script.py"]

```

**You can pass arguments when you run the container:**
```shell
docker run myimage arg1 arg2
```

**And access them in your Python script:**
```python
import sys

print(sys.argv[1])  # prints arg1
print(sys.argv[2])  # prints arg2

```

## Using Volumes for Configuration Files
If you have a configuration file, you can mount it as a volume and read from it inside the container.

**Example using docker run:**
```shell
docker run -v /path/to/config:/config myimage
```

**Example using docker-compose.yml:**
```yaml
version: '3.8'
services:
  myservice:
    image: myimage
    volumes:
      - /path/to/config:/config
```
**Your code can read the configuration file:**
```python
with open('/config/config.yaml', 'r') as f:
    config = f.read()
    print(config)

```

## Using Dockerfile ARG
You can also use ARG in a Dockerfile for build-time parameters/variables:

**Example Dockerfile:**
```dockerfile
FROM alpine

ARG my_arg
RUN echo "The value of my_arg is $my_arg"

```
**Build the image with the --build-arg flag:**
```shell
docker build --build-arg my_arg=value -t myimage .

```