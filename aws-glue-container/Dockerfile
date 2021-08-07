# Dockerfile
# docker build -t docker-aws-glue-libs .
# docker run -itd -p 8888:8888 -p 4040:4040 -v %UserProfile%\.aws:/root/.aws:ro -v %UserProfile%\aws_fargate\docker-apache-sparksql:/home/jobs --name glue  docker-aws-glue-libs

FROM amazon/aws-glue-libs:glue_libs_1.0.0_image_01

ENV PYSPARK_PYTHON=python3.6
ENV PYSPARK_DRIVER_PYTHON=python3.6

RUN python3.6 -m pip install awswrangler