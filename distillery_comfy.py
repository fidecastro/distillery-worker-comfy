
import uuid
import json
import urllib.request
import urllib.parse
from PIL import Image
from websocket import WebSocket #NOTE: websocket-client (https://github.com/websocket-client/websocket-client)
import io
import requests
import time
import os
import subprocess
import tempfile
from typing import List

APP_NAME = os.getenv('APP_NAME') # Name of the application
API_COMMAND_LINE = os.getenv('API_COMMAND_LINE') # Command line to start the API server
API_URL = os.getenv('API_URL')  # URL of the API server
INSTANCE_IDENTIFIER = APP_NAME+'-'+str(uuid.uuid4()) # Unique identifier for this instance of the worker

class ComfyConnector:
    _instance = None
    _process = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(ComfyConnector, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.server_address = API_URL
            self.client_id = INSTANCE_IDENTIFIER
            self.ws = WebSocket()
            self.start_api()
            self.initialized = True
    
    def start_api(self): # This method is used to start the API server
        if not self.is_api_running(): # Block execution until the API server is running
            api_command_line = API_COMMAND_LINE
            if self._process is None or self._process.poll() is not None: # Check if the process is not running or has terminated for some reason
                self._process = subprocess.Popen(api_command_line.split())
                print("API process started with PID:", self._process.pid)
                while not self.is_api_running(): # Block execution until the API server is running
                    time.sleep(0.5)  # Wait for 0.5 seconds before checking again
                time.sleep(0.5)  # Wait for 0.5 seconds before returning

    def is_api_running(self): # This method is used to check if the API server is running
        try:
            response = requests.get(f"http://{self.server_address}")
            if response.status_code == 200: # Check if the API server tells us it's running by returning a 200 status code
                self.ws.connect(f"ws://{self.server_address}/ws?clientId={self.client_id}")
                return True
            else:
                return False
        except Exception as e:
            print("API not running:", e)
            return False

    def kill_api(self): # This method is used to kill the API server
        if self._process is not None and self._process.poll() is None:
            self._process.kill()
            self._process = None
            print("API process killed")

    def get_history(self, prompt_id): # This method is used to retrieve the history of a prompt from the API server
        with urllib.request.urlopen(f"http://{self.server_address}/history/{prompt_id}") as response:
            return json.loads(response.read())

    def get_image(self, filename, subfolder, folder_type): # This method is used to retrieve an image from the API server
        data = {"filename": filename, "subfolder": subfolder, "type": folder_type}
        url_values = urllib.parse.urlencode(data)
        with urllib.request.urlopen(f"http://{self.server_address}/view?{url_values}") as response:
            return response.read()

    def queue_prompt(self, prompt): # This method is used to queue a prompt for execution
        p = {"prompt": prompt, "client_id": self.client_id}
        data = json.dumps(p).encode('utf-8')
        headers = {'Content-Type': 'application/json'}  # Set Content-Type header
        req = urllib.request.Request(f"http://{self.server_address}/prompt", data=data, headers=headers)
        return json.loads(urllib.request.urlopen(req).read())

    def generate_images(self, payload): # This method is used to generate images from a prompt and is the main method of this class
        prompt_id = self.queue_prompt(payload)['prompt_id']
        while True:
            out = self.ws.recv() # Wait for a message from the API server
            if isinstance(out, str): # Check if the message is a string
                message = json.loads(out) # Parse the message as JSON
                if message['type'] == 'executing': # Check if the message is an 'executing' message
                    data = message['data'] # Extract the data from the message
                    if data['node'] is None and data['prompt_id'] == prompt_id:
                        break
        address = self.find_output_node(payload)
        history = self.get_history(prompt_id)[prompt_id]
        filenames = eval(f"history['outputs']{address}")['images']  # Extract all images
        images = []
        for img_info in filenames:
            filename = img_info['filename']
            subfolder = img_info['subfolder']
            folder_type = img_info['type']
            image_data = self.get_image(filename, subfolder, folder_type)
            image_file = io.BytesIO(image_data)
            image = Image.open(image_file)
            images.append(image)
        return images

    def upload_image(self, filepath, subfolder=None, folder_type=None, overwrite=False): # This method is used to upload an image to the API server for use in img2img or controlnet
        url = f"http://{self.server_address}/upload/image"
        files = {'image': open(filepath, 'rb')}
        data = {
            'overwrite': str(overwrite).lower()
        }
        if subfolder:
            data['subfolder'] = subfolder
        if folder_type:
            data['type'] = folder_type
        response = requests.post(url, files=files, data=data)
        return response.json()

    @staticmethod
    def find_output_node(json_object): # This method is used to find the node containing the SaveImage class in a prompt
        for key, value in json_object.items():
            if isinstance(value, dict):
                if value.get("class_type") == "SaveImage":
                    return f"['{key}']"  # Return the key containing the SaveImage class
                result = ComfyConnector.find_output_node(value)
                if result:
                    return result
        return None
    
    @staticmethod
    def load_payload(path):
        with open(path, 'r') as file:
            return json.load(file)

    def upload_from_s3_to_input(self, aws_connector, s3_keys: List[str]):
        try:
            file_objs = aws_connector.download_fileobj(s3_keys) # Download file objects from AWS S3
            for s3_key, file_obj in zip(s3_keys, file_objs): # Iterate through the downloaded file objects and corresponding S3 keys
                temp_file_path = os.path.join(tempfile.gettempdir(), os.path.basename(s3_key)) # Create a temporary file with the same name as the S3 key
                with open(temp_file_path, 'wb') as temp_file:
                    temp_file.write(file_obj.read())
                response = self.upload_image(filepath=temp_file_path, folder_type='input') # Upload the temporary file to the Comfy API in the 'input' folder
                os.unlink(temp_file_path) # Delete the temporary file
        except Exception as e:
            raise RuntimeError(f"An error occurred while uploading from S3 to input: {str(e)}")
