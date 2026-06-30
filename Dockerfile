# Use Google’s Python39 Flex-Template launcher base
FROM gcr.io/dataflow-templates-base/python39-template-launcher-base:latest

WORKDIR /templates

# 1) Install your Python dependencies at build time
COPY requirements.txt .
RUN pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# 2) Copy your streaming pipeline code
COPY transformation_beam.py .

# 3) Tell the launcher which script to run
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/templates/transformation_beam.py"

# 4) Tell Dataflow to use your image as the SDK container
ENV FLEX_TEMPLATE_PYTHON_CONTAINER_IMAGE="gcr.io/p101-473210/gaming-pipeline-flex:latest"