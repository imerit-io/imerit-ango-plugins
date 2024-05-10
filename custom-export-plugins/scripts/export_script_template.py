import json
import os


def test(**data):
    # get project id from data response
    project_id = data.get("projectId")

    # get logger from data response
    logger = data.get("logger")

    # log message example
    logger.info(f"running test script {project_id}")

    # get config from data response if you need to get any config values
    config_str = data.get("configJSON")
    config = json.loads(config_str)

    # get json export from data response, this is a list of dictionaries, unfiltered
    json_export = data.get("jsonExport")

    # create output folder if it doesn't exist
    output_folder = os.getcwd() + f"/{project_id}"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    with open(f"{output_folder}/json_export.json", "w") as writer:
        json.dump(json_export, writer, indent=4)

    return output_folder

    # # example of how to iterate through json_export writing a new json for each asset
    # new_json_example = {}
    # for asset in json_export:
    #     # example of keys for each asset:
    #     # dict_keys(['asset', 'externalId', 'metadata', 'labeledAt', 'status', 'labelDuration', 'consensus', 'tasks', 'batches'])
    #     # annotations are under 'tasks' key at tasks[0]['objects']
    #     # 'batches' is an array of strings

    #     filename = asset["externalId"].split(".")[0]
    #     new_json_example["batch"] = asset["batches"]
    #     new_json_example["metadata"] = asset["metadata"]
    #     new_json_example["annotations"] = asset["tasks"][0]["objects"]

    #     # write the json file for the asset
    #     with open(f"{output_folder}/{filename}.json", "w") as writer:
    #         json.dump(new_json_example, writer, indent=4)

    # # the only thing we need to return is the output folder path
    # return output_folder


# print("test script loaded")
