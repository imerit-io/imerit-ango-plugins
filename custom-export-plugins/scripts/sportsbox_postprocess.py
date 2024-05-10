import json
import shelve
from typing import Dict, Any, Union, Optional
from pathlib import Path
import pandas as pd
from pydantic import BaseModel
import os
import boto3
import re


# DEVELOPED BY ANDREW
def sportsbox_postprocess(**data):
    ango_data = data.get("jsonExport")
    project_id = data.get("projectId")
    logger = data.get("logger")
    logger.info(f"running sportsbox_postprocess {project_id}")

    if len(data.get("stage")) == 0:  # Stage is a list, no stage selected returns []
        logger.error("stage is []")
        raise ValueError("You must choose a Stage Filter to run this plugin.")

    df = Post().process(ango_data)

    output_folder = os.getcwd() + f"/{project_id}"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    output_path = f"{output_folder}/sportsbox_postprocess_{project_id}.csv"
    df.to_csv(output_path, index=False)

    return output_folder


class Post:
    @staticmethod
    def check_metadata(key: str) -> Optional[int]:
        path = 'sportsbox_metadata'
        if Path(f"{path}.db").is_file():
            with shelve.open(path) as d:
                if key in d: return d[key]
                else: return None
        else: return None

    @staticmethod
    def update_metadata(key, file_size):
        path = 'sportsbox_metadata'
        with shelve.open(path) as d:
            d[key] = file_size
    
    def get_file_size(self, s3_key: Path) -> int:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=os.environ.get("sportsbox_aws_access_key"),
            aws_secret_access_key=os.environ.get("sportsbox_aws_secret_key"),
        )
        BUCKET = 'imerit-sportsboxai-data'
        match = re.search(r"(\.com\/)(.*?\.jpg)", s3_key)
        key = match.group(2)
        file_size = self.check_metadata(key)
        if file_size: return file_size
        else:
            # Get the object metadata from S3.
            try:
                object_metadata = s3.get_object(Bucket = BUCKET, Key = key)
            except:
                space_key = key.replace('%20', ' ')
                object_metadata = s3.get_object(Bucket = BUCKET, Key = space_key)
            # Get the size of the image file from the object metadata.
            file_size = object_metadata['ContentLength']
            self.update_metadata(key, file_size)
            return file_size

    @staticmethod
    def get_file_name(s3_key: Path) -> str:
        file_name = Path(re.findall(r".*\.jpg", s3_key)[0]).name
        space_name = file_name.replace('%20', ' ')
        return space_name

    @staticmethod
    def get_reg_sh_atts(tool: Dict[str, Any]) -> Dict[str, Any]:
        region_shape_attributes = {
            "cx": 0,
            "cy": 0,
            "name": "point"
            # conf: float = 0
        }
        region_shape_attributes["cx"] += int(tool["point"][0])
        region_shape_attributes["cy"] += int(tool["point"][1])
        return region_shape_attributes

    def process(self, ango_data: Dict[str, Any]) -> pd.DataFrame:
        data = []
        file_names_and_sizes = {}
        for asset in ango_data:
            for tool in asset["task"]["tools"]:
                sports_box = {
                    "filename": "",
                    "file_size": 0,
                    "region_shape_attributes": None,
                    "file_attributes": {},
                    "region_count": 0,
                    "region_id": 0,
                    "region_attributes": {},
                }
                tool_s3_key = asset["dataset"][tool["page"]]
                filename = self.get_file_name(tool_s3_key)
                sports_box["filename"] = filename
                if filename not in file_names_and_sizes.keys():
                    file_size = self.get_file_size(tool_s3_key)
                    sports_box["file_size"] += file_size
                    file_names_and_sizes[filename] = file_size
                else:
                    sports_box["file_size"] += file_names_and_sizes[filename]
                sports_box["region_shape_attributes"] = self.get_reg_sh_atts(tool)
                sports_box["region_attributes"] = json.dumps({"type": tool["title"]})
                data.append(sports_box)
        df = pd.DataFrame(data)
        df["region_shape_attributes"] = [
            json.dumps(item) for item in df["region_shape_attributes"].to_list()
        ]
        # Reorder columns:
        df = df[
            [
                "filename",
                "file_size",
                "file_attributes",
                "region_count",
                "region_id",
                "region_shape_attributes",
                "region_attributes",
            ]
        ]
        return df

    # def test(self, ango_json: Path) -> pd.DataFrame:
    #     ango_data = self.load_json(ango_json)
    #     df = self.process(ango_data)
    #     return df
