import json
import os

# Get the directory of the root of the project
dir_path = os.path.dirname(
    os.path.dirname(
        os.path.realpath(__file__)
    )
)

with open(os.path.join(dir_path, 'config.json'), encoding="utf-8") as f:
    config = json.load(f)

DATA_PATH = config['data_path']
