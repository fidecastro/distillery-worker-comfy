#!/bin/bash

export API_URL='127.0.0.1'
export INITIAL_PORT='8188'
export API_COMMAND_LINE='python3 ComfyUI/main.py --dont-upcast-attention'
export APP_NAME='WORKER'
export AWS_REGION_NAME='us-east-1'
export AWS_LOG_GROUP='XXXXXXX'
export AWS_LOG_STREAM_NAME='XXXXXXX'
export AWS_S3_BUCKET_NAME='XXXXXXX'
export AWS_S3_ACCESS_KEY='XXXXXXX'
export AWS_S3_SECRET_KEY='XXXXXXX'
export AWS_ACCESS_KEY_ID='XXXXXXX'
export AWS_SECRET_ACCESS_KEY='XXXXXXX'
export NETWORK_STORAGE='/runpod-volume'
export MODELS_FOLDER='ComfyUI/models'
export WORKER_TIMEOUT=360
export TEST_PAYLOAD='test_payload.json'
