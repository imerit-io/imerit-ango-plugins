import json
import os
import uuid
import hashlib


def selex_convert(**data):
    # get project id from data response
    project_id = data.get("projectId")

    # get logger from data response
    logger = data.get("logger")

    # log message example
    logger.info(f"running selex script {project_id}")

    # get config from data response if you need to get any config values
    config_str = data.get("configJSON")
    config = json.loads(config_str)

    # get json export from data response, this is a list of dictionaries, unfiltered
    json_export = data.get("jsonExport")

    # create output folder if it doesn't exist
    output_folder = os.getcwd() + f"/{project_id}"

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for asset in json_export:
        image_name = asset["externalId"]
        batch = "batch000"
        if len(asset["batches"]) >= 1:
            batch = asset["batches"][0]

        if not os.path.exists(f"{output_folder}/{batch}"):
            os.makedirs(f"{output_folder}/{batch}")
        # new_filename = image_name.split(".")[0]
        new_filename = image_name.rsplit(".", 1)[0]
        task = asset["task"]
        labeler = task["updatedBy"]
        labeler_id = hashlib.md5(labeler.encode()).hexdigest()
        is_valid = True
        # skip to next asset if there are no classifications
        if task["classifications"] == []:
            is_valid = False
            continue

        output_dict = {
            "annotation": {
                "filename": image_name,
                "labeler_id": labeler_id,
                "object": [],
            }
        }
        output_object_dict = {}
        for _class in task["classifications"]:
            title = _class["title"]

            # get out of loop if there is an invalid image
            if title == "invalid_image":
                output_object_dict["correct_crop"] = "False"
                # is_valid = False
                # break

            if title == "plate_stacked":
                # output_object_dict["plate_stacked"] = True
                output_object_dict["plate_stacked"] = "True"

            if title == "plate":
                # output_object_dict["correct_crop"] = True
                output_object_dict["correct_crop"] = "True"
                answer = _class["answer"]
                for i in range(10):  # This will loop from 0 to 9 inclusive
                    if i < len(answer):
                        output_object_dict[f"plate_char_{i}"] = "Don't_know" if answer[i] == "_" else answer[i]

                        # output_object_dict[f"plate_char_{i}"] = answer[i]
                    else:
                        output_object_dict[f"plate_char_{i}"] = ""

        output_dict["annotation"]["object"].append(output_object_dict)
        # write the json file if it is valid, otherwise skip to next asset

        if is_valid:
            if "plate_stacked" in output_dict["annotation"]["object"][0]:
                temp_dict = output_dict["annotation"]["object"][0]
                ordered_dict = {
                    "plate_stacked": temp_dict["plate_stacked"],
                }
                del temp_dict["plate_stacked"]
                ordered_dict.update(temp_dict)
                output_dict["annotation"]["object"][0] = ordered_dict

            with open(f"{output_folder}/{batch}/{new_filename}.json", "w") as output:
                json.dump(output_dict, output, indent=4)
        else:
            continue

    return output_folder
