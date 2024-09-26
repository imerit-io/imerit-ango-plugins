import json
import os
import uuid
import hashlib

def torc_output_multipage(**data):
    # get project id from data response
    project_id = data.get("projectId")

    # get logger from data response
    logger = data.get("logger")

    # log message example
    logger.info(f"running basic_multipg_to_single script {project_id}")

    # get config from data response if you need to get any config values
    config_str = data.get("configJSON")
    config = json.loads(config_str)

    # get json export from data response, this is a list of dictionaries, unfiltered
    json_export = data.get("jsonExport")
    
    
    # json_export_path = "/Users/home/Downloads/Production-task-export-2024-09-10-12_44_40_GMT.json"
    # json_export = json.load(open(json_export_path))
    # project_id = "1234TEST"



    # create output folder if it doesn't exist
    output_folder = os.getcwd() + f"/{project_id}"

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    output_file = []

    for asset in json_export:
        collector = {}
        task_id = asset["task"]["taskId"]
        stage = asset["task"]["stage"]
        stage_id = asset["task"]["stageId"]
        updated_at = asset["task"]["updatedAt"]
        updated_by = asset["task"]["updatedBy"]
        total_duration = asset["task"]["totalDuration"]
        classifications = asset["task"]["classifications"]
        for annotation in asset["task"]["tools"]:
            dataset = asset["dataset"]
            dataset_reference = dataset[annotation["page"]]
            filepath = dataset_reference.split("?")[0].split("amazonaws.com/")[1]
            filename = filepath.split("/")[-1]
            if annotation["page"] not in collector:
                collector[annotation["page"]] = {
                    "asset": dataset_reference,
                    "externalId": filename,
                    "metadata": asset["metadata"],
                    "batches": asset["batches"],
                    "task": {
                        "taskId": task_id,
                        "stage": stage,
                        "stageId": stage_id,
                        "updatedAt": updated_at,
                        "updatedBy": updated_by,
                        "totalDuration": total_duration,
                        "tools": [],
                        "classifications": classifications,
                        "relations": []
                    }
               }
            new_annotation = annotation.copy()
            del new_annotation["page"]
            collector[annotation["page"]]["task"]["tools"].append(new_annotation)
            
        # for classif in asset["task"]["classifications"]:
        #     if classif["page"] not in collector:
        #         collector[classif["page"]] = {
        #             "asset": dataset_reference,
        #             "externalId": filename,
        #             "metadata": asset["metadata"],
        #             "batches": asset["batches"],
        #             "task": {
        #                 "taskId": task_id,
        #                 "stage": stage,
        #                 "stageId": stage_id,
        #                 "updatedAt": updated_at,
        #                 "updatedBy": updated_by,
        #                 "totalDuration": total_duration,
        #                 "tools": [],
        #                 "classifications": [],
        #                 "relations": []
        #             }
        #         }
        #     new_classif = classif.copy()
        #     del new_classif["page"]
        #     collector[classif["page"]]["task"]["classifications"].append(new_classif)
        
        for images in collector.values():
            output_file.append(images)
            
    with open(f"{output_folder}/{project_id}.json", "w") as f:
        json.dump(output_file, f, indent=2)
                
    return output_folder