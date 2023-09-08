##### Distillery Worker for serverless Runpod - Comfy - Version 2.4 Angostura - Aug 24 2023

import time
import runpod
import uuid
from distillery_aws import AWSConnector
from distillery_comfy import ComfyConnector
import os
import io
from urllib.parse import urlparse
from PIL import PngImagePlugin
import json
from concurrent.futures import ThreadPoolExecutor
import copy
import sys

START_TIME = time.time() # Time at which the worker was initialized
APP_NAME = os.getenv('APP_NAME') # Name of the application
INSTANCE_IDENTIFIER = APP_NAME+'-'+str(uuid.uuid4()) # Unique identifier for this instance of the worker
NETWORK_STORAGE = os.getenv("NETWORK_STORAGE") # Path to network storage mount
MODELS_FOLDER = os.getenv("MODELS_FOLDER") # Path to models folder in ComfyUI
WORKER_TIMEOUT = int(os.getenv("WORKER_TIMEOUT")) # Timeout for the worker in seconds

def fetch_images(comfy_api, template_inputs):
    try:
        aws_connector = AWSConnector() 
        comfy_connector = ComfyConnector()
        image_files = []
        images = comfy_connector.generate_images(comfy_api)
        for image in images: 
            # Create a unique filename
            filename = f'distillery_{str(uuid.uuid4())}.png'

            # Modify the metadata
            combined_metadata = {}
            existing_metadata_str= image.info.get('prompt')
            existing_metadata = json.loads(existing_metadata_str)
            combined_metadata['comfy_api'] = existing_metadata
            combined_metadata['template_inputs'] = template_inputs
            combined_metadata_str = json.dumps(combined_metadata)
            pnginfo = PngImagePlugin.PngInfo()
            pnginfo.add_text('prompt', combined_metadata_str)

            # Save the image to an in-memory file object
            image_file = io.BytesIO()
            image.save(image_file, format='PNG', pnginfo=pnginfo)
            image_file.seek(0)

            # Upload the in-memory file to S3
            aws_connector.upload_fileobj([(image_file, filename)]) # Upload the in-memory file to S3
            image_files.append(filename)

        return image_files # Return the list of keys of the images in S3
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        line_no = exc_traceback.tb_lineno
        error_message = f'Unhandled error at line {line_no}: {str(e)}'
        print(INSTANCE_IDENTIFIER + " - fetch_images - " + error_message)
        aws_connector.print_log('N/A', INSTANCE_IDENTIFIER, error_message, level='ERROR')
        raise RuntimeError(f"An error occurred while fetching images in line {line_no}: {str(e)}")

class InputPreprocessor:
    @staticmethod
    def update_paths(json_obj, paths, input_value):
        try:
            aws_connector = AWSConnector()
            updated_json_obj = copy.deepcopy(json_obj)  # Create a deep copy of the original JSON object
            for path in paths:
                target = updated_json_obj
                for key in path[:-1]:  # Traverse all but the last key in the path
                    target = target.get(key, {})  # Use get to avoid KeyError
                if path[-1] in target:  # Check if the key exists
                    target[path[-1]] = input_value  # Update the value at the last key in the path
            return updated_json_obj
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            line_no = exc_traceback.tb_lineno
            error_message = f'Unhandled error at line {line_no}: {str(e)}'
            print(INSTANCE_IDENTIFIER + " - update_paths - " + error_message)
            aws_connector.print_log('N/A', INSTANCE_IDENTIFIER, error_message, level='ERROR')
            raise RuntimeError(f"An error occurred while updating the paths in line {line_no}: {str(e)}")

    @staticmethod
    def tally_models_to_fetch(template_inputs):
        try:
            aws_connector = AWSConnector()
            models_to_fetch = []
            for key in template_inputs:
                if key in ['SD15_CHECKPOINT', 'SDXL_BASE_CHECKPOINT', 'SDXL_REFINER_CHECKPOINT']:
                    model_to_add = template_inputs[key]
                    models_to_fetch.append({"model_type": "sd_model", "model_name": model_to_add})
                elif key in ['SD15_LORA_1','SD15_LORA_2','SD15_LORA_3','SD15_LORA_4','SD15_LORA_5','SDXL_LORA_1','SDXL_LORA_2','SDXL_LORA_3','SDXL_LORA_4','SDXL_LORA_5']:
                    model_to_add = template_inputs[key]
                    if model_to_add not in models_to_fetch: models_to_fetch.append({"model_type": "lora_model", "model_name": model_to_add})
                elif key in ['SD15_CONTROLNET_MODEL_NAME','SDXL_CONTROLNET_MODEL_NAME']:
                    model_to_add = template_inputs[key]
                    models_to_fetch.append({"model_type": "controlnet_model", "model_name": model_to_add})
            return models_to_fetch
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            line_no = exc_traceback.tb_lineno
            error_message = f'Unhandled error at line {line_no}: {str(e)}'
            print(INSTANCE_IDENTIFIER + " - tally_models_to_fetch - " + error_message)
            aws_connector.print_log('N/A', INSTANCE_IDENTIFIER, error_message, level='ERROR')
            raise RuntimeError(f"An error occurred while tallying models to fetch in line {line_no}: {str(e)}")

    @staticmethod
    def get_models_from_storage(models_list):
        try:
            aws_connector = AWSConnector()
            start_time = time.time()
            copied_models = []
            copied_models_times = []
            total_models_processed = 0
            for model in models_list:  # will iterate across all model types in the list
                try:
                    copy_start_time = time.time()
                    model_path = None
                    if model["model_type"] == "sd_model":
                        model_type_path ="checkpoints"
                        model_path = f"{MODELS_FOLDER}/checkpoints/"
                    elif model["model_type"] == "lora_model":
                        model_type_path ="loras"
                        model_path = f"{MODELS_FOLDER}/loras/"
                    elif model["model_type"] == "controlnet_model":
                        model_type_path ="controlnet"
                        model_path = f"{MODELS_FOLDER}/controlnet/"
                    if not os.path.exists(f"{model_path}/{model['model_name']}"):
                        print(f"Model {model['model_name']} not found in {model_path}. Copying from storage to {model_path}")
                        os.system(f"cp {NETWORK_STORAGE}/{model_type_path}/{model['model_name']} {model_path}")
                        copy_end_time = time.time()
                        copied_models.append(model['model_name'])
                        copied_models_times.append(copy_end_time - copy_start_time)
                    else:
                        print(f"Model {model['model_name']} found in {model_path}. Skipping copy.")
                    total_models_processed += 1
                except Exception as e:
                    print(f"Error copying model {model['model_name']}: {e}")
            end_time = time.time()
            total_time_consumed = end_time - start_time
            aws_connector.print_log('N/A', INSTANCE_IDENTIFIER, f"All models processed. Total time consumed: {total_time_consumed} seconds. Total number of models processed: {total_models_processed}, List: {models_list}. Total number of models copied: {len(copied_models)}, List with times: {list(zip(copied_models, copied_models_times))}", level='INFO')
            print(f"All models processed. Total time consumed: {total_time_consumed} seconds. Total number of models processed: {total_models_processed}, List: {models_list}. Total number of models copied: {len(copied_models)}, List with times: {list(zip(copied_models, copied_models_times))}")
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            line_no = exc_traceback.tb_lineno
            error_message = f'Unhandled error at line {line_no}: {str(e)}'
            print(INSTANCE_IDENTIFIER + " - get_models_from_storage - " + error_message)
            aws_connector.print_log('N/A', INSTANCE_IDENTIFIER, error_message, level='ERROR')
            raise RuntimeError(f"An error occurred while getting models from storage in line {line_no}: {str(e)}. Variables were: Models List: {models_list}, model: {model}, model_path: {model_path}, model_type_path: {model_type_path}")

def flatten_list(nested_list):
    flat_list = []
    for item in nested_list:
        if isinstance(item, list):
            flat_list.extend(flatten_list(item))
        else:
            flat_list.append(item)
    return flat_list

def worker_routine(event):
    try:
        aws_connector = AWSConnector()
        comfy_connector = ComfyConnector()
        aws_connector.print_log('N/A', INSTANCE_IDENTIFIER, f"Worker initialized!", level='INFO')
        payload = event['input']
        if payload is None:
            aws_connector.print_log('N/A', INSTANCE_IDENTIFIER, f"Worker was passed a None payload from event.", level='ERROR')        
            return "ERROR: No payload provided in event"
        comfy_api = payload['comfy_api']
        template_inputs = payload['template_inputs']
        images_per_batch = payload['images_per_batch']    
        if template_inputs['INPUT_IMAGE'] != "": comfy_connector.upload_from_s3_to_input(aws_connector, [template_inputs['INPUT_IMAGE']])
        if template_inputs['MASK_IMAGE'] != "": comfy_connector.upload_from_s3_to_input(aws_connector, [template_inputs['MASK_IMAGE']])
        if template_inputs['CONTROLNET_IMAGE'] != "": comfy_connector.upload_from_s3_to_input(aws_connector, [template_inputs['CONTROLNET_IMAGE']])
        models_to_fetch = InputPreprocessor.tally_models_to_fetch(template_inputs)
        InputPreprocessor.get_models_from_storage(models_to_fetch) # Copy models from network storage to ComfyUI
        files = []
        for i in range(images_per_batch):
            file = fetch_images(comfy_api, template_inputs)
            files.append(file)
            template_inputs['NOISE_SEED']=template_inputs['NOISE_SEED']+1
            comfy_api = InputPreprocessor.update_paths(comfy_api, template_inputs['NOISE_SEED_TEMPLATE_PATHS'], template_inputs['NOISE_SEED'])
            print(f"Image {i+1} - New Seed: {template_inputs['NOISE_SEED']}")
            aws_connector.print_log('N/A', INSTANCE_IDENTIFIER, f"Image {i+1} - New Seed: {template_inputs['NOISE_SEED']}", level='INFO')    
        #comfy_connector.kill_api()
        corrected_files = flatten_list(files)
        return corrected_files
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        line_no = exc_traceback.tb_lineno
        error_message = f'Unhandled error after {(time.time()-START_TIME):.2f} seconds at line {line_no}: {str(e)}'
        print(INSTANCE_IDENTIFIER + " - worker_routine - " + error_message)        
        aws_connector.print_log('N/A', INSTANCE_IDENTIFIER, error_message, level='ERROR')
        comfy_connector.kill_api()

def handler(event):
    aws_connector = AWSConnector()
    aws_connector.print_log('N/A', INSTANCE_IDENTIFIER, f"Worker was called by Master. event = {event}.", level='INFO')        
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(worker_routine, event)
        try:
            # Waiting for the result within WORKER_TIMEOUT seconds
            result = future.result(timeout=WORKER_TIMEOUT)
        except TimeoutError:
            # If the timeout occurs, log an error and return a timeout response
            aws_connector = AWSConnector()
            aws_connector.print_log('N/A', INSTANCE_IDENTIFIER, f"Handler timed out after {WORKER_TIMEOUT} seconds.", level='ERROR')
            return f"ERROR: Handler timed out after {WORKER_TIMEOUT} seconds."
        # Cancel the future to explicitly kill the thread if it's still running
        future.cancel()
        aws_connector.print_log('N/A', INSTANCE_IDENTIFIER, f"Worker finished! Throughput time: {(time.time()-START_TIME):.2f} seconds.", level='INFO')
        return result

runpod.serverless.start({"handler": handler})