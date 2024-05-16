from pathlib import Path
from typing import Dict, List, Any
from imerit_ango.sdk import SDK
import json
import traceback
import boto3

# Plugin script developed by andrew.k@imerit.net
def tally_postprocess(**data):
    ango_data = data.get("jsonExport")
    project_id = data.get("projectId")
    api_key = data.get("apiKey")
    logger = data.get("logger")
    logger.info(f"Running Tally_postprocess {project_id}!")

    if len(data.get("stage")) == 0:  # Stage is a list, no stage selected returns []
        logger.error("stage is []")
        raise ValueError("You must choose a Stage Filter to run this plugin!")

    sdk = SDK(api_key = api_key)
    storage_response = sdk.get_storages()
    storages_list = storage_response['data']['storages']
    projects = sdk.list_projects(limit = 666)
    for project in projects['data']['projects']:
        if project['_id'] == project_id:
            project_name = project['name']
            break

    Post().process(ango_data, 
                   project_name, 
                   storages_list, 
                   logger
                   )
    return None

class Post:
    @staticmethod
    def build_roi_anno(asset: Dict[str, Any], 
                       tool: Dict[str, Any], 
                       page_no: int
                       ) -> Dict[str, Any]:
        if tool.get('pdf', None):
            total_width = tool['pdf']['position']['boundingRect']['width']
            total_height = tool['pdf']['position']['boundingRect']['height']
            x1 = tool['pdf']['position']['boundingRect']['x1']
            y1 = tool['pdf']['position']['boundingRect']['y1']
            x2 = tool['pdf']['position']['boundingRect']['x2']
            y2 = tool['pdf']['position']['boundingRect']['y2']
            width = (x2 - x1) / total_width
            height = (y2 - y1) / total_height
        if tool.get('bounding-box', None):
            total_width = asset['metadata']['width']
            total_height = asset['metadata']['height']
            x1 = tool['bounding-box']['x']
            y1 = tool['bounding-box']['y']
            box_width = tool['bounding-box']['width']
            box_height = tool['bounding-box']['height']
            width = box_width / total_width
            height = box_height / total_height
        left = x1 / total_width
        top = y1 / total_height
        annotation = {tool['title']: 
                        {"location": 
                            {"pageNo": page_no, 
                                "ltwh": [left,
                                         top,
                                         width,
                                         height
                                         ]
                            }
                        }
                    }
        return annotation

    def process_roi(self,
                    asset: Dict[str, Any], 
                    ) -> List[Dict[str, Any]]:
        """
        `ltwh` = left, top, width, height
        Calculated as decimal ratio; see above
        The height and width in the annotation 
        export refer to the PDF file's height 
        and width
        Ango docs: https://docs.imerit.net/data/importing-and-exporting-annotations/importing-annotations/ango-import-format#bounding-box
        """
        roi_data = []
        page_nums = []
        for tool in asset["task"]["tools"]:
            if tool.get('pdf', None): page_no = tool['pdf']['position']['pageNumber']
            else: page_no = 1
            annotation = self.build_roi_anno(asset, tool, page_no)
            roi_data.append(annotation)
            page_nums.append(page_no)
        # Go back and write to files
        pages = list(set(page_nums))
        output = []
        for page in pages:
            sorted_roi_data = {"ROI": []}
            for roi_item in roi_data:
                p_no = next(iter(roi_item.values()))['location']['pageNo']
                if str(p_no) == str(page):
                    sorted_roi_data['ROI'].append(roi_item)
            output.append(sorted_roi_data)
        output_json = json.dumps(output)
        return output_json
    
    @staticmethod
    def check_id_group(asset: Dict[str, Any], tool: Dict[str, Any]) -> str:
        group_id = None
        for relation in asset['task']['relations']:
            if tool['objectId'] in relation['group']:
                group_id = relation['objectId']
                break
        return group_id

    def build_individual_anno(self, 
                              asset: Dict[str, Any], 
                              tool: Dict[str, Any], 
                              page_no: int
                              ) -> Dict[str, Any]:
        if tool.get('pdf', None):
            text = tool['pdf']['content']['text']
            total_width = tool['pdf']['position']['boundingRect']['width']
            total_height = tool['pdf']['position']['boundingRect']['height']
            x1 = tool['pdf']['position']['boundingRect']['x1']
            y1 = tool['pdf']['position']['boundingRect']['y1']
            x2 = tool['pdf']['position']['boundingRect']['x2']
            y2 = tool['pdf']['position']['boundingRect']['y2']
            width = (x2 - x1) / total_width
            height = (y2 - y1) / total_height
        if tool.get('bounding-box', None):
            text = tool['ocr']['text']
            total_width = asset['metadata']['width']
            total_height = asset['metadata']['height']
            x1 = tool['bounding-box']['x']
            y1 = tool['bounding-box']['y']
            box_width = tool['bounding-box']['width']
            box_height = tool['bounding-box']['height']
            width = box_width / total_width
            height = box_height / total_height
        left = x1 / total_width
        top = y1 / total_height
        object_group_id = self.check_id_group(asset, tool)
        annotation = {"object_id": tool['objectId'],
                      "object_group_id": object_group_id,
                      "text": text,
                      "location": 
                            {"pageNo": page_no, 
                             "ltwh": [left,
                                      top,
                                      width,
                                      height
                                      ]
                            }
                    }
        return annotation

    def process_individual(self, asset) -> List[Dict[str, Any]]:
        with open('if_schema_v3.json', 'r') as j:
            schema = json.load(j)
        if asset["task"]["tools"][0].get('pdf', None):
            pages = sorted(list(set([tool['pdf']['position']['pageNumber'] for tool in asset["task"]["tools"]]))) # could have obtained from metadata in asset
        else: pages = [1]
        if_items = []
        for page in pages:
            if_data = {}
            try:
                for tool in asset["task"]["tools"]:
                    if tool.get('pdf', None): page_no = tool['pdf']['position']['pageNumber']
                    else: page_no = 1
                    if str(page) == str(page_no):
                        annotation = self.build_individual_anno(asset, tool, page_no)
                        
                        # for annotations at the top level of the schema's keys
                        if tool['title'] in schema.keys():
                            if 'ContactNo' in tool['title']: 
                                if tool['title'] in if_data.keys(): 
                                    if_data[tool['title']].append(annotation)
                                else: if_data[tool['title']] = [annotation]
                            else: if_data[tool['title']] = annotation 

                        # for annotations in the Table key of the schema:          
                        elif tool['title'] in schema['Table'][0].keys():
                            if not if_data.get('Table', None): 
                                if_data['Table'] = [{'object_group_id': annotation['object_group_id']}]

                            # Look in the tables. If the matching group is found, add on the annotation to the group
                            found_table = False
                            for table in if_data['Table']:
                                if table['object_group_id'] == annotation['object_group_id']:
                                    if tool['title'] not in schema['Table'][0]['TaxAmount'].keys():
                                        table[tool['title']] = annotation
                                    else:
                                        if not table.get('TaxAmount', None):
                                            table['TaxAmount'] = {tool['title']: annotation}
                                        else: table['TaxAmount'][tool['title']] = annotation
                                    found_table = True
                                    break

                            # We didn't find the matching group. Create the first item of the group
                            if not found_table:
                                if tool['title'] not in schema['Table'][0]['TaxAmount'].keys():
                                    if_data['Table'].append({'object_group_id': annotation['object_group_id'], tool['title']: annotation})
                                else:
                                    if_data['Table'].append({'object_group_id': annotation['object_group_id'], "TaxAmount": {tool['title']: annotation}}) 
                                
                        # for annotations in the BatchGodownDetails key of the schema
                        elif tool['title'] in schema['Table'][0]["BatchGodownDetails"][0].keys():
                            # find the table that the BatchGodownDetails item is attached to
                            for relation in asset['task']['relations']:
                                if annotation['object_group_id'] in relation['group']:
                                    table_group_id = relation['objectId']
                                    break
                            if not if_data.get('Table', None): 
                                if_data['Table'] = [{'object_group_id': table_group_id,
                                                     'BatchGodownDetails': [{'object_group_id': annotation['object_group_id']}]}]
                            for table in if_data['Table']:
                                if table['object_group_id'] == table_group_id:
                                    if not table.get('BatchGodownDetails', None):
                                        table['BatchGodownDetails'] = [{'object_group_id': annotation['object_group_id']}]
                                    for batch in table['BatchGodownDetails']:
                                        if batch['object_group_id'] == annotation['object_group_id']:
                                            batch[tool['title']] = annotation

                        # for annotations in the LedgerDetails key of the schema
                        elif tool['title'] in schema["LedgerDetails"][0].keys():
                            if not if_data.get('LedgerDetails', None):
                                if_data['LedgerDetails'] = [{'object_group_id': annotation['object_group_id']}]
                            for ledger in if_data['LedgerDetails']:
                                if ledger['object_group_id'] == annotation['object_group_id']:
                                    ledger[tool['title']] = annotation
            except Exception as e:
                print(f"Error found in asset with external ID {asset['externalId']}")
                print(traceback.format_exc())
                print(json.dumps(tool))
            if_items.append(if_data)
        output_json = json.dumps(if_items)
        return output_json

    @staticmethod
    def get_s3_key(storage_info: Dict[str, Any], external_id: str) -> str:
        if storage_info['name'] == 'tally-imerit-data-safehouse':
            # Set up your credentials and region
            access_key = storage_info['publicKey']
            secret_key = storage_info['privateKey']
            session_token = storage_info['sessionToken']
            region = storage_info['region']
            # Create a session using your credentials and region
            session = boto3.Session(aws_access_key_id = access_key,
                                    aws_secret_access_key = secret_key,
                                    aws_session_token = session_token,
                                    region_name = region
                                    )
            # Use the session to create an S3 client
            s3 = session.client('s3')
            # Now you can use the S3 client to interact with the S3 bucket
            bucket_name = storage_info['name']
            paginator = s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket = bucket_name)
            for page in pages:
                for obj in page['Contents']:
                    if Path(obj['Key']).name == external_id:
                        return obj['Key'].replace('.pdf', '.json')
        else: 
            print("You might want to check the input storage info on Ango!")
            return None

    @staticmethod
    def upload_to_s3(storage_info: Dict[str, Any], 
                     output_json: List[Dict[str, Any]], 
                     output_s3_key: str,
                     metadata: Dict[str, Any]
                     ):
        if storage_info['name'] == 'tally-imerit-jsons-safehouse':
            access_key = storage_info['publicKey']
            secret_key = storage_info['privateKey']
            session_token = storage_info['sessionToken']
            region = storage_info['region']
            session = boto3.Session(aws_access_key_id = access_key,
                                    aws_secret_access_key = secret_key,
                                    aws_session_token = session_token,
                                    region_name = region
                                    )
            s3 = session.client('s3')
            bucket_name = storage_info['name']
            s3.put_object(Body = output_json, 
                          Bucket = bucket_name, 
                          Key = output_s3_key, 
                          Metadata = metadata
                          )
        else: 
            print("You might want to check the output storage info on Ango!")
            return None

    def process(self, 
                ango_data: Dict[str, Any], 
                project_name: str, 
                storages_list: List[Dict[str, Any]],
                logger: Any
                ):
        logger.info("Processing data!")
        for asset in ango_data:
            if 'ROI' in project_name:
                output_json = self.process_roi(asset)
                output_s3_key = self.get_s3_key(storages_list[3], asset['externalId']).replace('.json', '_ROI.json')
            if 'Individual' in project_name: 
                output_json = self.process_individual(asset)
                output_s3_key = self.get_s3_key(storages_list[3], asset['externalId']).replace('.json', '_IndividualField.json')
            if not asset.get('contextData', None):
                logger.info(f"No context data found for asset with external ID {asset['externalId']}!")
                continue
            else:
                metadata = {"original_filename": asset['contextData']["original_filename"], 
                            "variations": asset['contextData']["variations"],
                            "uploader": asset['contextData']["uploader"]
                            }
                self.upload_to_s3(storages_list[2], output_json, output_s3_key, metadata)
        logger.info("Task complete!")