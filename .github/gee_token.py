# workflow taken from https://github.com/gee-community/geemap/blob/master/.github/ee_token.py
# https://github.com/gee-community/geemap/discussions/1341
import os

ee_token = os.environ["EARTHENGINE_TOKEN"]
credential_file_path = os.path.expanduser("~/.config/earthengine/")
os.makedirs(credential_file_path, exist_ok=True)
with open(credential_file_path + "credentials", "w") as file:
    file.write(ee_token)
