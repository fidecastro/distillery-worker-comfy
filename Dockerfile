# Using the latest Distillery SD filesystem as the base image
FROM felipeinfante/distillery-worker:base-comfy-20230930

# Setting the working directory in the container
WORKDIR /workspace

# Copy the Python script and SD folder into the container
COPY ComfyUI ./ComfyUI
COPY distillery_aws.py .
COPY distillery_comfy.py .
COPY distillery_worker.py .
COPY set_env_variables.sh .
COPY docker_run.sh .
COPY test_payload.json .

RUN git config --global --add safe.directory '*'

# Specifying the command to run the script
CMD ["./docker_run.sh"]
