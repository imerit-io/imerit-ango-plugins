import json
import os
import uuid
import copy


def alcon_new_wf(**data):
    # project_id = data.get("projectId")
    project_id = "1234"

    # Get json export from data response
    # json_export = data.get("jsonExport")
    json_path = "/Users/home/Downloads/New_WF-task-export-2024-09-17-12_29_56_GMT.json"
    with open(json_path, 'r') as file:
        json_export = json.load(file)

    # get logger from data response
    # logger = data.get("logger")

    # # log message example
    # logger.info(f"running alcon_conversions_second script on Project: {project_id}")

    # create output folder if it doesn't exist
    output_folder = os.getcwd() + f"/{project_id}"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for asset in json_export:        
        external_id = asset["externalId"].rsplit(".", 1)[0]
        print(external_id)
            
        new_ann = {}

        for ann in asset["task"]["tools"]:
            # adjust polygon anns if necessary
            if "segmentation" in ann:
                regions = []
                for zone in ann["segmentation"]["zones"]:
                    polygon = zone["region"]
                    if polygon[0] != polygon[-1]:
                        polygon.append(polygon[0])
                    # new_ann[ann["title"]] = polygon
                    regions.append(polygon)
                if ann["title"] in new_ann:
                    new_ann[ann["title"]]["annotations"].append(regions)
                elif ann["title"] not in new_ann:
                    # new_ann[ann["title"]]["annotations"] = [regions]
                    new_ann[ann["title"]] = {"annotations": [regions]}
                # if ann["classifications"] != []:
                #     title = ann["classifications"][0]["title"]
                #     answer = ann["classifications"][0]["answer"]
                #     new_ann[ann["title"]].update({title: answer})
                
            if "point" in ann:
                if ann["title"] in new_ann:
                    new_ann[ann["title"]]["annotations"].append(ann["point"])
                else:
                    new_ann[ann["title"]] = {"annotations": [ann["point"]]}
                
        # Process classifications recursively
        if "classifications" in asset["task"]:
            process_classifications(asset["task"]["classifications"], new_ann)
                    
        with open(
            f"{output_folder}/{external_id}.json",
            "w",
            encoding="utf-8",
        ) as f:
            json.dump(new_ann, f, indent=4)
            
    # logger.info("script completed. zipping output")
    return output_folder

def process_classifications(classifications, new_ann, key_value=None, key=None, value=None):
    for classification in classifications:
        # Check for "Key-value pairs" to initialize the key-value pair dictionary
        if classification.get("title") == "Key-value pairs":
            key_value = classification.get("answer")
            if key_value not in new_ann:
                new_ann[key_value] = {}

        # If "key" is found, create an entry under the current key_value
        if classification.get("title") == "key":
            key = classification.get("answer")
            if key_value and key not in new_ann[key_value]:
                new_ann[key_value][key] = ""

        # If "value" is found, assign the value to the corresponding key under key_value
        if classification.get("title") == "value":
            value = classification.get("answer")
            if key_value and key:
                new_ann[key_value][key] = value

        # Recursively process child classifications if they exist
        if classification.get("classifications"):
            process_classifications(classification["classifications"], new_ann, key_value, key, value)

if __name__ == "__main__":
    alcon_new_wf()
