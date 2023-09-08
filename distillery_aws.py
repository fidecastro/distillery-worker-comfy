#### Distillery AWS Connector Lite Sync - v2.4 - Aug 30 2023 - AWS Manager and Database Connector for backup server

import boto3
from watchtower import CloudWatchLogHandler
from typing import List, Tuple
import os
import logging
import inspect
import time
from io import BytesIO
import json
import time
import socket
from botocore.exceptions import ClientError

APP_NAME = os.getenv('APP_NAME')
AWS_REGION_NAME = os.getenv('AWS_REGION_NAME')
AWS_LOG_GROUP = os.getenv('AWS_LOG_GROUP')
AWS_LOG_STREAM_NAME = os.getenv('AWS_LOG_STREAM_NAME')
AWS_S3_BUCKET_NAME = os.getenv('AWS_S3_BUCKET_NAME')
AWS_S3_ACCESS_KEY = os.getenv('AWS_S3_ACCESS_KEY')
AWS_S3_SECRET_KEY = os.getenv('AWS_S3_SECRET_KEY')

class AWSConnector:
    _instance = None

    def __new__(cls):
        if not cls._instance:
            cls._instance = super().__new__(cls)
            cls._instance.region_name = AWS_REGION_NAME
            cls._instance.log_group = AWS_LOG_GROUP
            cls._instance.log_stream_name = AWS_LOG_STREAM_NAME
            cls._instance.setup_logging()
        return cls._instance
    
    def setup_logging(self, level=logging.DEBUG): 
        root_logger = logging.getLogger()
        root_logger.setLevel(level)
        session = boto3.Session(region_name=self.region_name)
        cloudwatch_client = session.client('logs')
        cw_handler = CloudWatchLogHandler(boto3_client=cloudwatch_client, log_group=self.log_group, stream_name=self.log_stream_name, create_log_group=False, create_log_stream=False)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        cw_handler.setFormatter(formatter)
        root_logger.addHandler(cw_handler)

    def print_log(self, request_id, context, message, level='INFO'): 
        caller_frame = inspect.currentframe().f_back
        script_name = os.path.basename(caller_frame.f_globals["__file__"])
        line_number = caller_frame.f_lineno
        function_name = caller_frame.f_code.co_name
        hostname = socket.gethostname() 
        log_data = {
            "context": context,
            "timestamp": f"{time.time():.3f}",
            "request_id": request_id,
            "message": message,
            "script_name": script_name,
            "function_name": function_name,
            "line_number": line_number,
            "hostname": hostname
        }
        message_to_print = json.dumps(log_data)
        if level == 'INFO':
            logging.info(message_to_print)
        elif level == 'ERROR':
            logging.error(message_to_print)
        elif level == 'WARNING':
            logging.warning(message_to_print)

    def upload_fileobj(self, files: List[Tuple[BytesIO, str]]):
        s3 = boto3.client('s3', aws_access_key_id=AWS_S3_ACCESS_KEY, aws_secret_access_key=AWS_S3_SECRET_KEY, region_name=AWS_REGION_NAME)
        try:
            for file_obj, key in files:
                file_obj.seek(0)  # Ensure we're at the start of the file
                s3.upload_fileobj(file_obj, AWS_S3_BUCKET_NAME, key)
        except Exception as e:
            self.print_log('N/A', APP_NAME, f"Error uploading file objects to AWS S3 bucket {AWS_S3_BUCKET_NAME}: {e}", level='ERROR')

    def download_fileobj(self, keys: List[str]) -> List[BytesIO]:
        file_objs = []
        try:
            s3 = boto3.client('s3', aws_access_key_id=AWS_S3_ACCESS_KEY, aws_secret_access_key=AWS_S3_SECRET_KEY, region_name=AWS_REGION_NAME)
            for key in keys:
                try:
                    file_obj = BytesIO()
                    s3.download_fileobj(AWS_S3_BUCKET_NAME, key, file_obj)
                    file_obj.seek(0)  # Ensure we're at the start of the file
                    file_objs.append(file_obj)
                except ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        self.print_log('N/A', APP_NAME, f"File with key {key} was not found in AWS S3 bucket {AWS_S3_BUCKET_NAME}", level='ERROR')
                    else:
                        self.print_log('N/A', APP_NAME, f"Error downloading file with key {key} from AWS S3 bucket {AWS_S3_BUCKET_NAME}: {e}", level='ERROR')
            return file_objs
        except Exception as e:
            self.print_log('N/A', APP_NAME, f"Error downloading file objects from AWS S3 bucket {AWS_S3_BUCKET_NAME}: {e}", level='ERROR')
            return file_objs

    def upload_files(self, files: List[Tuple[str, str]]):
        s3 = boto3.client('s3', aws_access_key_id=AWS_S3_ACCESS_KEY, aws_secret_access_key=AWS_S3_SECRET_KEY, region_name=AWS_REGION_NAME)
        try:
            for file_name, key in files:
                s3.upload_file(file_name, AWS_S3_BUCKET_NAME, key)
        except Exception as e:
            self.print_log('N/A', APP_NAME, f"Error uploading files to AWS S3 bucket {AWS_S3_BUCKET_NAME}: {e}", level='ERROR')

    def download_files(self, files: List[Tuple[str, str]]):
        s3 = boto3.client('s3', aws_access_key_id=AWS_S3_ACCESS_KEY, aws_secret_access_key=AWS_S3_SECRET_KEY, region_name=AWS_REGION_NAME)
        try:
            for key, file_name in files:
                s3.download_file(AWS_S3_BUCKET_NAME, key, file_name)
        except Exception as e:
            self.print_log('N/A', APP_NAME, f"Error downloading files from AWS S3 bucket {AWS_S3_BUCKET_NAME}: {e}", level='ERROR')
