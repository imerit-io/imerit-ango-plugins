import os
import numpy as np
import json
import pandas as pd
from imerit_ango.sdk import SDK
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload 
from dotenv import load_dotenv
load_dotenv()


def jj_wf1_output(**data):
        logger = data.get("logger")
        project_id = data.get("projectId")
        json_export = data.get("jsonExport")
        config_str = data.get("configJSON")
        config = json.loads(config_str)
        try:
            batches = config.get("batches")
            experts = config.get("experts")
            for expert in experts:
                ango_json_path = Functions.download_ango_json(expert, batches)
                nrrd_dictionary_path = f"{os.getcwd()}/scripts/jj_support/step_4_1_output_nrrd_metadata_dict.json"
                df = Functions.init_pandas_df()
                nrrd_dictionary = Functions.process_assets(ango_json_path, nrrd_dictionary_path)
                Functions.make_csv(df, nrrd_dictionary, batches, expert)
                expert_drive = os.getenv(f'{expert}_drive')
                Functions.upload_output(expert_drive, expert)
        except Exception as e:
            logger.error(f"Error: {e}")
            return

class Functions:
    
    # Validate if the coordinate space and directions are as expected
    @staticmethod
    def check_coordinates(input_space, input_space_directions):
        if input_space != "left-posterior-superior":
            return False
        index_1 = np.argmax(input_space_directions, axis=0)
        index_2 = np.argmax(input_space_directions, axis=1)
        expected_index = [0, 1, 2]
        if (np.array_equal(index_1, expected_index)) & (
            np.array_equal(index_2, expected_index)
        ):
            return True
        return False

    # Convert a point from the image coordinate system to the world coordinate system
    @staticmethod
    def convert_coordinates(point, space_directions, space_origins):
        world_coordinates_multiplication = np.matmul(point, space_directions)
        world_coordinates = world_coordinates_multiplication + space_origins
        adjusted_world_coordinates = world_coordinates * [1, -1, 1]
        return adjusted_world_coordinates

    # Calculate length between two points in the world coordinate system
    @staticmethod
    def calculate_length(point1, point2, space_directions, space_origins):
        world_point1 = Functions.convert_coordinates(point1, space_directions, space_origins)
        world_point2 = Functions.convert_coordinates(point2, space_directions, space_origins)
        distance = np.linalg.norm(world_point2 - world_point1)
        return world_point1, world_point2, distance

    # Convert polyline and bounding box to world coordinates and lengths in millimeters
    @staticmethod
    def process_tool(tool, space_directions, space_origins):
        if "polyline" in tool:
            polyline = tool["polyline"]
            point1 = np.array([min(polyline["x1"], polyline["x2"]), min(polyline["y1"], polyline["y2"]), min(polyline["z1"], polyline["z2"])])
            point2 = np.array([max(polyline["x1"], polyline["x2"]), max(polyline["y1"], polyline["y2"]), max(polyline["z1"], polyline["z2"])])
            # point1 = np.array([polyline["x1"], polyline["y1"], polyline["z1"]])
            # point2 = np.array([polyline["x2"], polyline["y2"], polyline["z2"]])
            world_point1, world_point2, length = Functions.calculate_length(
                point1, point2, space_directions, space_origins
            )
            return {
                "ann_type": "Polyline",
                "description": tool["metadata"]["description"],
                "voxel_coordinates": [point1.tolist(), point2.tolist()],
                "world_coordinates": [world_point1.tolist(), world_point2.tolist()],
                "lengths_mm": length,
            }
        elif "bounding-box" in tool:
            bbox = tool["bounding-box"]
            point1 = np.array([min(bbox["x1"], bbox["x2"]), min(bbox["y1"], bbox["y2"]), min(bbox["z1"], bbox["z2"])])
            point2 = np.array([max(bbox["x1"], bbox["x2"]), max(bbox["y1"], bbox["y2"]), max(bbox["z1"], bbox["z2"])])
            # point1 = np.array([bbox["x1"], bbox["y1"], bbox["z1"]])
            # point2 = np.array([bbox["x2"], bbox["y2"], bbox["z2"]])
            world_point1 = Functions.convert_coordinates(point1, space_directions, space_origins)
            world_point2 = Functions.convert_coordinates(point2, space_directions, space_origins)
            lengths_mm = np.abs(world_point2 - world_point1)
            return {
                "ann_type": "3dbbox",
                "description": tool["metadata"]["description"],
                "voxel_coordinates": [point1.tolist(), point2.tolist()],
                "world_coordinates": [world_point1.tolist(), world_point2.tolist()],
                "lengths_mm": lengths_mm.tolist(),
            }
        return None

    @staticmethod
    def init_pandas_df():
        # Define the column headers
        headers = [
            "Ango_External_id",
            "Case_id",  # (original patient_id or name of parent folder)
            "Study_id",  #  (name of subfolder)
            "Series_id",  # (name of child folder)
            "Study_id_date",
            "RT_start_date",
            "RT_end_date",
            "RP/RF",
            "RP/RF_specify",
            "RP/RF_impact",
            "RP/RF_Is tumor response evaluable based on RECIST 1.1",
            "RP/RF_Kouloulias radiological grading",
            "RP/RF_Dahele radiographic pattern",
            "RP/RF_Chen radiological grading/number of involved lobes",
            "RP/RF_Chen radiological grading/extent of changes",
            "RP/RF_Chen radiological grading/distribution of changes",
            "RP/RF_Chen radiological grading/CT findings",
            "RP/RF_Chen radiological grading/radiographic pattern",
            "RP/RF_Chen radiological grading/sharp border",
            "Polyline/L1_min_coord",
            "Polyline/L2_min_coord",
            "Polyline/L3_min_coord",
            "Polyline/L1_max_coord",
            "Polyline/L2_max_coord",
            "Polyline/L3_max_coord",
            "Polyline/L1_best_coord",
            "Polyline/L2_best_coord",
            "Polyline/L3_best_coord",
            "Polyline/L1 (mm)",
            "Polyline/L2 (mm)",
            "Polyline/L3 (mm)",
            "Polyline/L1_min (mm)",
            "Polyline/L2_min (mm)",
            "Polyline/L3_min (mm)",
            "Polyline/L1_max (mm)",
            "Polyline/L2_max (mm)",
            "Polyline/L3_max (mm)",
            "Polyline/L1_best (mm)",
            "Polyline/L2_best (mm)",
            "Polyline/L3_best (mm)",
            "3dbbox_RP/Voxel coordinates",
            "3dbbox_RP/World coordinates",
            "3dbbox_RP/length X (mm)",
            "3dbbox_RP/length Y (mm)",
            "3dbbox_RP/length Z (mm)",
            "3dbbox_RF/Voxel coordinates",
            "3dbbox_RF/World coordinates",
            "3dbbox_RF/length X (mm)",
            "3dbbox_RF/length Y (mm)",
            "3dbbox_RF/length Z (mm)",
        ]
        # Create an empty DataFrame with the defined column headers
        df = pd.DataFrame(columns=headers)

        return df

    @staticmethod
    def process_assets(json_data, nrrd_dictionary_path):
        # with open(json_path) as output_file:
        #     json_data = json.load(output_file)
        

        with open(nrrd_dictionary_path) as nrrd_file:
            nrrd_dictionary = json.load(nrrd_file)

        for asset in json_data:
            externalId = asset["externalId"]
            # Handle the case when the asset has content in the tools array
            if asset["task"]["tools"] != []:
                space = nrrd_dictionary[externalId]["space"]
                space_directions = nrrd_dictionary[externalId]["space_directions"]
                space_origins = nrrd_dictionary[externalId]["space_origins"]
                coordinate_flag = Functions.check_coordinates(space, space_directions)
                if coordinate_flag is True:
                    for tool in asset["task"]["tools"]:
                        try:
                            result = Functions.process_tool(tool, space_directions, space_origins)
                        except KeyError:
                            print(f"Asset: {externalId}")
                            print(tool["objectId"] + " has no description")
                            print("--------------------")
                            result = None
                        if result:
                            description = tool["metadata"].get("description", None)
                            if description is None:
                                print(f"Asset: {externalId}")
                                print(tool["objectId"] + " has no description")
                                print("--------------------")

                            ann_type = result.get("ann_type")
                            description = result.get("description")
                            voxel_coordinates = result.get("voxel_coordinates")
                            world_coordinates = result.get("world_coordinates")
                            lengths_mm = result.get("lengths_mm")
                            if ann_type == "Polyline":
                                nrrd_dictionary[externalId][
                                    f"Polyline/{description}"
                                ] = lengths_mm
                                nrrd_dictionary[externalId][f"Polyline/{description}_coord"] = voxel_coordinates
                            if ann_type == "3dbbox":
                                x, y, z = lengths_mm
                                nrrd_dictionary[externalId][
                                    f"3dbbox_{description}/Voxel coordinates"
                                ] = voxel_coordinates
                                nrrd_dictionary[externalId][
                                    f"3dbbox_{description}/World coordinates"
                                ] = world_coordinates
                                nrrd_dictionary[externalId][
                                    f"3dbbox_{description}/length X (mm)"
                                ] = x
                                nrrd_dictionary[externalId][
                                    f"3dbbox_{description}/length Y (mm)"
                                ] = y
                                nrrd_dictionary[externalId][
                                    f"3dbbox_{description}/length Z (mm)"
                                ] = z

                    for main_class in asset["task"]["classifications"]:
                        rp_rf = main_class.get("answer", None)
                        if rp_rf == "yes":
                            nrrd_dictionary[externalId]["RP/RF"] = rp_rf
                            for sub_class in main_class["classifications"]:
                                if sub_class["title"] == "specify":
                                    nrrd_dictionary[externalId]["RP/RF_specify"] = (
                                        sub_class.get("answer", "")[0]
                                    )
                                if sub_class["title"] == "impact":
                                    nrrd_dictionary[externalId]["RP/RF_impact"] = (
                                        sub_class.get("answer", "")
                                    )
                                if (
                                    sub_class["title"]
                                    == "Is tumor response evaluable based on RECIST 1.1"
                                ):
                                    nrrd_dictionary[externalId][
                                        "RP/RF_Is tumor response evaluable based on RECIST 1.1"
                                    ] = sub_class.get("answer", "")
                                if sub_class["title"] == "Kouloulias radiological grading":
                                    nrrd_dictionary[externalId][
                                        "RP/RF_Kouloulias radiological grading"
                                    ] = sub_class.get("answer", "")
                                if sub_class["title"] == "Dahele radiographic pattern":
                                    nrrd_dictionary[externalId][
                                        "RP/RF_Dahele radiographic pattern"
                                    ] = sub_class.get("answer", "")[0]
                                if sub_class["title"] == "Chen radiological grading":
                                    answer_dict = {
                                        "number of involved lobes": "",
                                        "extent of changes": "",
                                        "distribution of changes": "",
                                        "CT findings": "",
                                        "radiographic pattern": "",
                                        "sharp border": "",
                                    }
                                    for answer in sub_class["answer"]:
                                        answer_split = answer.split(" / ")
                                        if len(answer_split) == 2:
                                            answer_dict[answer_split[0]] = answer_split[1]
                                    for key, value in answer_dict.items():
                                        nrrd_dictionary[externalId][
                                            f"RP/RF_Chen radiological grading/{key}"
                                        ] = value

                        elif rp_rf == "no":
                            print("Tools != []", f"Asset: {externalId}", rp_rf)
                        else:
                            print("Tools != []", f"Asset: {externalId}", rp_rf)
                            nrrd_dictionary[externalId]["RP/RF"] = rp_rf

            # Handle the case when the asset has no content in the tools array
            else:
                for main_class in asset["task"]["classifications"]:
                    rp_rf = main_class.get("answer", None)
                    if rp_rf == "yes":
                        nrrd_dictionary[externalId]["RP/RF"] = rp_rf
                        for sub_class in main_class["classifications"]:
                            if sub_class["title"] == "specify":
                                nrrd_dictionary[externalId]["RP/RF_specify"] = (
                                    sub_class.get("answer", "")[0]
                                )
                            if sub_class["title"] == "impact":
                                nrrd_dictionary[externalId]["RP/RF_impact"] = sub_class.get(
                                    "answer", ""
                                )
                            if (
                                sub_class["title"]
                                == "Is tumor response evaluable based on RECIST 1.1"
                            ):
                                nrrd_dictionary[externalId][
                                    "RP/RF_Is tumor response evaluable based on RECIST 1.1"
                                ] = sub_class.get("answer", "")
                            if sub_class["title"] == "Kouloulias radiological grading":
                                nrrd_dictionary[externalId][
                                    "RP/RF_Kouloulias radiological grading"
                                ] = sub_class.get("answer", "")
                            if sub_class["title"] == "Dahele radiographic pattern":
                                nrrd_dictionary[externalId][
                                    "RP/RF_Dahele radiographic pattern"
                                ] = sub_class.get("answer", "")[0]
                            if sub_class["title"] == "Chen radiological grading":
                                answer_dict = {
                                    "number of involved lobes": "",
                                    "extent of changes": "",
                                    "distribution of changes": "",
                                    "CT findings": "",
                                    "radiographic pattern": "",
                                    "sharp border": "",
                                }
                                for answer in sub_class["answer"]:
                                    answer_split = answer.split(" / ")
                                    if len(answer_split) == 2:
                                        answer_dict[answer_split[0]] = answer_split[1]
                                for key, value in answer_dict.items():
                                    nrrd_dictionary[externalId][
                                        f"RP/RF_Chen radiological grading/{key}"
                                    ] = value
                    elif rp_rf == "no":
                        print("Tools == []", f"Asset: {externalId}", rp_rf)
                        nrrd_dictionary[externalId]["RP/RF"] = rp_rf
                    else:
                        nrrd_dictionary[externalId][
                            "Treatment volume type (only for R series)"
                        ] = main_class.get("answer", "")

        with open("steps_output/TEST.json", "w") as file:
            json.dump(nrrd_dictionary, file, indent=4)

        return nrrd_dictionary

    @staticmethod
    def make_csv(df, nrrd_dictionary, batches, expert):
        # data = []
        data = {}
        for batch in batches:
            data[batch] = []
        for key, value in nrrd_dictionary.items():
            for batch in batches:
                if f"{batch}_" in key:
                    if value.get("RP/RF") == "yes" or value.get("RP/RF") == "no":
                        data[batch].append(
                            {
                                "Ango_External_id": key,
                                "Case_id": value.get("case_id", ""),
                                "Study_id": f'"{value.get("study_id", "")}"',
                                "Series_id": f'"{value.get("series_id", "")}"',
                                "Study_id_date": value.get("study_id_date", ""),
                                "RT_start_date": value.get("rt_start_date", ""),
                                "RT_end_date": value.get("rt_end_date", ""),
                                "RP/RF": value.get("RP/RF"),
                                "RP/RF_specify": value.get("RP/RF_specify", ""),
                                "RP/RF_impact": value.get("RP/RF_impact", ""),
                                "RP/RF_Is tumor response evaluable based on RECIST 1.1": value.get(
                                    "RP/RF_Is tumor response evaluable based on RECIST 1.1", ""
                                ),
                                "RP/RF_Kouloulias radiological grading": value.get(
                                    "RP/RF_Kouloulias radiological grading", ""
                                ),
                                "RP/RF_Dahele radiographic pattern": value.get(
                                    "RP/RF_Dahele radiographic pattern", ""
                                ),
                                "RP/RF_Chen radiological grading/number of involved lobes": value.get(
                                    "RP/RF_Chen radiological grading/number of involved lobes", ""
                                ),
                                "RP/RF_Chen radiological grading/extent of changes": value.get(
                                    "RP/RF_Chen radiological grading/extent of changes", ""
                                ),
                                "RP/RF_Chen radiological grading/distribution of changes": value.get(
                                    "RP/RF_Chen radiological grading/distribution of changes", ""
                                ),
                                "RP/RF_Chen radiological grading/CT findings": value.get(
                                    "RP/RF_Chen radiological grading/CT findings", ""
                                ),
                                "RP/RF_Chen radiological grading/radiographic pattern": value.get(
                                    "RP/RF_Chen radiological grading/radiographic pattern", ""
                                ),
                                "RP/RF_Chen radiological grading/sharp border": value.get(
                                    "RP/RF_Chen radiological grading/sharp border", ""
                                ),
                                "Polyline/L1_min_coord": value.get("Polyline/L1_min_coord", ""),
                                "Polyline/L2_min_coord": value.get("Polyline/L2_min_coord", ""),
                                "Polyline/L3_min_coord": value.get("Polyline/L3_min_coord", ""),
                                "Polyline/L1_max_coord": value.get("Polyline/L1_max_coord", ""),
                                "Polyline/L2_max_coord": value.get("Polyline/L2_max_coord", ""),
                                "Polyline/L3_max_coord": value.get("Polyline/L3_max_coord", ""),
                                "Polyline/L1_best_coord": value.get("Polyline/L1_best_coord", ""),
                                "Polyline/L2_best_coord": value.get("Polyline/L2_best_coord", ""),
                                "Polyline/L3_best_coord": value.get("Polyline/L3_best_coord", ""),
                                "Polyline/L1 (mm)": value.get("Polyline/L1", ""),
                                "Polyline/L2 (mm)": value.get("Polyline/L2", ""),
                                "Polyline/L3 (mm)": value.get("Polyline/L3", ""),
                                "Polyline/L1_min (mm)": value.get("Polyline/L1_min", ""),
                                "Polyline/L2_min (mm)": value.get("Polyline/L2_min", ""),
                                "Polyline/L3_min (mm)": value.get("Polyline/L3_min", ""),
                                "Polyline/L1_max (mm)": value.get("Polyline/L1_max", ""),
                                "Polyline/L2_max (mm)": value.get("Polyline/L2_max", ""),
                                "Polyline/L3_max (mm)": value.get("Polyline/L3_max", ""),
                                "Polyline/L1_best (mm)": value.get("Polyline/L1_best", ""),
                                "Polyline/L2_best (mm)": value.get("Polyline/L2_best", ""),
                                "Polyline/L3_best (mm)": value.get("Polyline/L3_best", ""),
                                "3dbbox_RP/Voxel coordinates": value.get(
                                    "3dbbox_RP/Voxel coordinates", ""
                                ),
                                "3dbbox_RP/World coordinates": value.get(
                                    "3dbbox_RP/World coordinates", ""
                                ),
                                "3dbbox_RP/length X (mm)": value.get("3dbbox_RP/length X (mm)", ""),
                                "3dbbox_RP/length Y (mm)": value.get("3dbbox_RP/length Y (mm)", ""),
                                "3dbbox_RP/length Z (mm)": value.get("3dbbox_RP/length Z (mm)", ""),
                                "3dbbox_RF/Voxel coordinates": value.get(
                                    "3dbbox_RF/Voxel coordinates", ""
                                ),
                                "3dbbox_RF/World coordinates": value.get(
                                    "3dbbox_RF/World coordinates", ""
                                ),
                                "3dbbox_RF/length X (mm)": value.get("3dbbox_RF/length X (mm)", ""),
                                "3dbbox_RF/length Y (mm)": value.get("3dbbox_RF/length Y (mm)", ""),
                                "3dbbox_RF/length Z (mm)": value.get("3dbbox_RF/length Z (mm)", ""),
                                "Treatment volume type (only for R series)": value.get(
                                    "Treatment volume type (only for R series)", ""
                                ),
                            }
                        )

        # Make a csv for each batch
        for key, value in data.items():
            new_df = pd.DataFrame(value)
            new_df_sorted = new_df.sort_values(by="Study_id_date", ascending=True)
            if not os.path.exists(f"steps_output/{expert}/{key}"):
                os.makedirs(f"steps_output/{expert}/{key}")
            new_df_sorted.to_csv(f"steps_output/{expert}/{key}/{key}_output.csv", index=False)
    
    @staticmethod  
    def download_ango_json(expert, batches):
        ango_sdk = SDK(os.getenv('api_key'))

        if expert == "expert_1":
            project_id = "667db48a1c2ced00160539c7"
        elif expert == "expert_2":
            project_id = "669533ec63390e00163a3b73"
        elif expert == "expert_3":
            project_id = "6695340711d2e10015b4e169"
        
        batch_ids = []
        batch_input_equals_output = {}
        for batch in batches:
            batch_input_equals_output[batch] = False  
        ango_batches_output = ango_sdk.get_batches(project_id=project_id)
        for batch in ango_batches_output:
            if batch["name"] in batches:
                batch_ids.append(batch["_id"])
                batch_input_equals_output[batch["name"]] = True
        for key, value in batch_input_equals_output.items():
            if value == False:
                print(f"Batch {key} not found in Ango")
            
                
        ango_json_path = ango_sdk.exportV3(project_id=project_id, batches=batch_ids)

        return ango_json_path    

    @staticmethod
    def create_drive_folder(service, name, parent_id=None):
        folder_metadata = {
            'name': name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        if parent_id:
            folder_metadata['parents'] = [parent_id]
        folder = service.files().create(body=folder_metadata, fields='id').execute()
        return folder.get('id')

    @staticmethod
    def upload_output(expert_drive, expert):
        # Path to the service account key file
        SERVICE_ACCOUNT_FILE = 'ango-plugins-34d6441a8f2b.json'

        # Define the required scopes
        SCOPES = ['https://www.googleapis.com/auth/drive.file']
        
        # Authenticate and create the service
        credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build('drive', 'v3', credentials=credentials)

        # Specify the parent folder ID where you want to create subfolders and upload the files
        parent_folder_id = expert_drive

        # Upload all files in the expert directory, creating subfolders as needed
        for root, dirs, files in os.walk(f'steps_output/{expert}'):
            for dir_name in dirs:
                folder_path = os.path.join(root, dir_name)
                relative_path = os.path.relpath(folder_path, f'steps_output/{expert}')
                folder_id = Functions.create_drive_folder(service, relative_path, parent_folder_id)
                
                for file_name in os.listdir(folder_path):
                    file_path = os.path.join(folder_path, file_name)
                    
                    if os.path.isfile(file_path):
                        file_metadata = {
                            'name': file_name,
                            'parents': [folder_id]
                        }
                        media = MediaFileUpload(file_path, resumable=True)
                        uploaded_file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
                        print(f'Uploaded file "{file_name}" to folder "{relative_path}" with ID: {uploaded_file.get("id")}')

if __name__ == "__main__":
    jj_wf1_output()
