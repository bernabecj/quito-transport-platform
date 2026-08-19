# Lambda container image for the network and weather ingestion loaders.
# One image serves both quito-network-ingestion and quito-weather-ingestion —
# they only differ by the handler set in each Lambda's image_config.command.
#
# Build from the repo root (context must include ingestion/constants.py etc.
# under the `ingestion` package path used by the loaders' imports):
#   docker build -f ingestion/Dockerfile -t <image> .
FROM public.ecr.aws/lambda/python:3.11

COPY ingestion/requirements.txt ./ingestion/requirements.txt
RUN pip install --no-cache-dir -r ingestion/requirements.txt

COPY ingestion/ ./ingestion/

CMD ["ingestion.network_loader.handler"]