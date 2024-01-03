import json
import os

from imerit_ango.sdk import SDK
from imerit_ango.plugins import ModelPlugin, run

HOST = os.environ.get('HOST')
PLUGIN_ID = os.environ.get('PLUGIN_ID', )
PLUGIN_SECRET = os.environ.get('PLUGIN_SECRET')

def run_model(**data):
    # Extract input parameters
    project_id = data.get('projectId')
    asset_id = data.get('assetId')
    category_schema = data.get('categorySchema')
    # logger = data.get('logger')
    api_key = data.get('apiKey')
    config_str = data.get('configJSON')
    if config_str is not None:
        config = json.loads(config_str)

    # logger.info("Plugin session is started!")

    sdk = SDK(api_key=api_key, host=HOST)

    get_asset_response = sdk.get_assets(project_id=project_id, asset_id=asset_id)
    external_id = get_asset_response['data']['assets'][0]['externalId']

    if category_schema is None:
        bbox_obj = [{"bounding-box": {"x": 20, "y": 30, "width": 50, "height": 60}}]
        annotation_json = {"externalId": external_id,
                           "objects": bbox_obj, "classifications": [], "relations": []}
    else:
        schema_id = category_schema[0]['schemaId']
        bbox_obj = [{"schemaId": schema_id,
                     "bounding-box": {"x": 20, "y": 30, "width": 50, "height": 60}}]
        annotation_json = {"externalId": external_id,
                           "objects": bbox_obj, "classifications": [], "relations": []}

    # logger.info("Plugin session is ended!")
    return annotation_json


if __name__ == "__main__":
    plugin = ModelPlugin(id=PLUGIN_ID,
                         secret=PLUGIN_SECRET,
                         callback=run_model,
                         host=HOST)

    run(plugin, host=HOST)