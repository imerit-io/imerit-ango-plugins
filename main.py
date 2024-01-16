import os
import re
import json
import zipfile
import traceback
import pandas as pd
from tqdm import tqdm
from io import BytesIO
from datetime import datetime
from imerit_ango.sdk import SDK
from imerit_ango.plugins import ExportPlugin, ExportResponse, run

HOST = os.environ['HOST']
PLUGIN_ID = os.environ['PLUGIN_ID']
PLUGIN_SECRET = os.environ['PLUGIN_SECRET']


def print_session_start(logger):
    time_str = str(datetime.utcnow())
    time_str = time_str[:-3].replace(' ', 'T') + 'Z'
    time_str = time_str.replace(':', '-')

    print("[" + time_str + "][INFO]" + " Plugin session is started!")
    logger.info("Plugin session is started!")


def print_session_info(data):
    try:
        project_id = data.get('projectId')
        run_by = data.get('runBy')
        organization_id = data.get('orgId')
        config = data.get('configJSON')
        stages = data.get('stage')
        batches = data.get('batches')
        num_assets = data.get('numTasks')
        api_key = data.get("apiKey")
        sdk = SDK(api_key=api_key, host=HOST)

        sdk_response = sdk.get_project(project_id)

        stage_names = ', '.join(stages)
        project_name = ""
        batch_names = ""
        if 'data' in sdk_response:
            project_name = sdk_response['data']['project']['name']
            project_batches = sdk_response['data']['project']['batches']
            batch_name_list = []
            if batches is not None:
                for batch_id in batches:
                    for project_batch in project_batches:
                        if project_batch['_id'] == batch_id:
                            batch_name = project_batch['name']
                            batch_name_list.append(batch_name)
                            continue
                batch_names = ', '.join(batch_name_list)

        print('-' * 60)
        print('Project Name: ' + project_name)
        print('Project ID: ' + project_id)
        print('Run By: ' + run_by)
        print('Organization ID: ' + organization_id)
        print('Stages: ' + stage_names)
        print('Batches: ' + batch_names)
        print('Num Assets: ' + str(num_assets))
        print('Config: ')
        print(config)
        print('-' * 60)
    except Exception as e:
        print(e)


def print_session_end(logger):
    time_str = str(datetime.utcnow())
    time_str = time_str[:-3].replace(' ', 'T') + 'Z'
    time_str = time_str.replace(':', '-')

    print("[" + time_str + "][INFO]" + " Plugin session is ended!")
    logger.info("Plugin session is ended!")


def print_info_message(logger, message):
    time_str = str(datetime.utcnow())
    time_str = time_str[:-3].replace(' ', 'T') + 'Z'
    time_str = time_str.replace(':', '-')

    print("[" + time_str + "][INFO]" + " " + message)
    logger.info(message)


def check_config(config):
    date_regex = re.compile(r"\d\d\d\d-\d\d-\d\d")

    start_date = None
    if "start_date" in config:
        if isinstance(config['start_date'], str):
            start_date = config['start_date']
            if date_regex.match(start_date) is None:
                start_date = None

    end_date = None
    if "end_date" in config:
        if isinstance(config['end_date'], str):
            end_date = config['end_date']
            if date_regex.match(end_date) is None:
                end_date = None

    duration_unit = "sec"
    if "duration_unit" in config:
        if isinstance(config['duration_unit'], str):
            duration_unit = config['duration_unit']

    count_nested_classifications = True
    if 'count_nested_classifications' in config:
        if isinstance(config['count_nested_classifications'], bool):
            count_nested_classifications = config['count_nested_classifications']

    verbose = True
    if 'verbose' in config:
        if isinstance(config['verbose'], bool):
            verbose = config['verbose']

    verbose_frequency = 1000
    if 'verbose_frequency' in config:
        if isinstance(config['verbose_frequency'], int):
            if config['verbose_frequency'] > 0:
                verbose_frequency = config['verbose_frequency']

    return start_date, end_date, duration_unit, count_nested_classifications, verbose, verbose_frequency


def scrape_answers(tool_list, tool_tree=[], classification_answer_dict_list=[]):
    for classification_tool in tool_list:
        answer = ''
        if 'answer' in classification_tool:
            answer = classification_tool['answer']
        # print(classification_tool['title'], '-', classification_tool['schemaId'], '-', answer, '->', tool_tree)
        classification_answer_dict_list.append({'title': classification_tool['title'],
                                                'schemaId': classification_tool['schemaId'],
                                                'answer': answer,
                                                'tool_tree': tool_tree.copy(),
                                                'tool_type': classification_tool['tool'],
                                                'level': len(tool_tree.copy())})

        if 'classifications' in classification_tool:
            tool_tree_org = tool_tree.copy()
            tool_tree.append(classification_tool['title'])
            scrape_answers(classification_tool['classifications'], tool_tree, classification_answer_dict_list)
            tool_tree = tool_tree_org


def check_date(stage_history, start_date, end_date):
    if (start_date is None) & (end_date is None):
        return True
    if start_date is None:
        start_date = "2020-12-31"
    if end_date is None:
        end_date = "2100-01-01"

    date_format = '%Y-%m-%d'
    start_datetime = datetime.strptime(start_date, date_format)
    end_datetime = datetime.strptime(end_date, date_format)

    for current_stage in stage_history:
        if 'completedAt' in current_stage:
            completed_at = current_stage['completedAt']
            time_split = completed_at.split('T')
            completed_date = time_split[0]
            year, month, day = completed_date.split('-')
            label_date = str(year) + '-' + str(month).zfill(2) + '-' + str(day).zfill(2)
            label_datetime = datetime.strptime(label_date, date_format)
            # Compare date
            if start_datetime <= label_datetime <= end_datetime:
                return True
    return False


def callback_function(**data):
    logger = data.get('logger')

    try:
        return tpt_export(data)
    except Exception as error_message:
        full_traceback = traceback.format_exc()
        traceback_lines = full_traceback.splitlines()
        processed_traceback_list = []
        for line in traceback_lines:
            traceback_line = re.sub(r'File ".*[\\/]([^\\/]+.py)"', r'File "\1"', line)
            processed_traceback_list.append(traceback_line)

        processed_traceback = '\n'.join(processed_traceback_list)
        print_info_message(logger, "Error!")
        print_info_message(logger, processed_traceback)
        raise Exception(error_message)


def tpt_export(data):
    # Extract input parameters
    project_id = data.get('projectId')
    json_export = data.get('jsonExport')
    num_assets = data.get('numTasks')
    api_key = data.get("apiKey")
    logger = data.get('logger')
    config_str = data.get('configJSON')
    try:
        config = json.loads(config_str)
    except Exception as error:
        print(error)
        message = 'The config JSON format is wrong!'
        print_info_message(logger, message)
        message = 'Default parameters are used.'
        print_info_message(logger, message)
        config = {}
    # -------------------------------------------------
    print_session_start(logger)
    print_session_info(data)
    print_info_message(logger, "The number of exported assets: " + str(num_assets))
    if num_assets == 0:
        raise Exception("Empty export exception!")

    start_date, end_date, duration_unit, count_nested_classifications, verbose, verbose_frequency = check_config(config)

    sdk = SDK(api_key=api_key, host=HOST)

    # Get project stages
    sdk_response = sdk.get_project(project_id)
    project_name = sdk_response['data']['project']['name']
    project_stages = sdk_response['data']['project']['stages']
    stage_id_list = []
    stage_type_list = []
    stage_name_list = []
    for project_stage in project_stages:
        stage_id = project_stage['id']
        stage_type = project_stage['type']
        stage_name = project_stage['name']

        stage_id_list.append(stage_id)
        stage_type_list.append(stage_type)
        stage_name_list.append(stage_name)

    stage_df = pd.DataFrame()
    stage_df['stage_id'] = stage_id_list
    stage_df['stage_type'] = stage_type_list
    stage_df['stage_name'] = stage_name_list

    batch_name_list = []
    stage_list = []
    asset_url_list = []
    external_id_list = []
    task_id_list = []
    labeler_name_list = []
    reviewer_name_list = []
    label_date_list = []
    review_date_list = []
    label_duration_list = []
    review_duration_list = []
    num_page_list = []

    num_classifications_list = []
    num_tools_list = []
    num_tool_classifications_list = []
    num_relations_list = []

    num_classifications = 0
    num_tools = 0
    num_tool_classifications = 0
    num_relations = 0

    for image_index, asset in enumerate(tqdm(json_export)):
        if verbose & (image_index % verbose_frequency == 0):
            logger.info("Assets Processed: [" + str(num_assets) + "/" + str(image_index) + "]")
        # --------------------------------------------------------------------------------------------------------------
        asset_url = asset['asset']
        external_id = asset['externalId']
        task = asset['task']

        batches = asset['batches']
        if None in batches:
            tmp_batches = []
            for batch in batches:
                if batch is not None:
                    tmp_batches.append(batch)
            batches = []
            batches = tmp_batches.copy()
        batches_str = '_'.join(batches)
        # --------------------------------------------------------------------------------------------------------------
        label_stage = task['stage']
        task_id = task['taskId']
        num_page = ""
        if 'dataset' in asset:
            num_page = str(len(asset['dataset']))
        if 'metadata' in asset:
            if 'frameTotal' in asset['metadata']:
                num_page = str(asset['metadata']['frameTotal'])

        labeler_name = ""
        reviewer_name = ""
        label_date = ""
        review_date = ""
        label_duration = 0
        review_duration = 0
        label_duration_str = "0"
        review_duration_str = "0"

        stage_history = []
        if 'stageHistory' in task:
            stage_history = task['stageHistory']
        else:
            continue

        # Check whether the date is between start_date and end_date
        if not check_date(stage_history, start_date, end_date):
            continue

        for current_stage in stage_history:
            stage_name = ""
            if 'stage' in current_stage:
                stage_name = current_stage['stage']

            filtered_stage_df = stage_df[stage_df['stage_name'] == stage_name]
            if len(filtered_stage_df) > 0:
                stage_type = filtered_stage_df.iloc[0]['stage_type']
            else:
                continue

            if (stage_type != 'Label') & (stage_type != 'Review'):
                continue

            if stage_type == 'Label':
                label_duration = label_duration + current_stage['duration']
                if 'completedBy' in current_stage:
                    labeler_name = current_stage['completedBy']
                if 'completedAt' in current_stage:
                    completed_at = current_stage['completedAt']
                    time_split = completed_at.split('T')
                    completed_date = time_split[0]
                    year, month, day = completed_date.split('-')
                    label_date = str(year) + '-' + str(month).zfill(2) + '-' + str(day).zfill(2)
            if stage_type == 'Review':
                review_duration = review_duration + current_stage['duration']
                if 'completedBy' in current_stage:
                    reviewer_name = current_stage['completedBy']
                if 'completedAt' in current_stage:
                    completed_at = current_stage['completedAt']
                    time_split = completed_at.split('T')
                    completed_date = time_split[0]
                    year, month, day = completed_date.split('-')
                    review_date = str(year) + '-' + str(month).zfill(2) + '-' + str(day).zfill(2)
            # ------------------------------
            # Count Annotations
            object_list = task['tools']
            classification_list = task['classifications']
            relation_list = task['relations']

            if count_nested_classifications:
                num_classifications = 0
                num_tools = 0
                for obj in object_list:
                    if 'interpolationStopped' in obj:
                        if obj['interpolationStopped']:
                            continue
                    num_tools += 1
                num_tool_classifications = 0
                num_relations = len(relation_list)

                # Get all nested classifications recursively
                if len(classification_list) > 0:
                    classification_answer_dict_list = []
                    scrape_answers(classification_list, [], classification_answer_dict_list)
                    num_classifications += len(classification_answer_dict_list)

                for current_obj in object_list:
                    if 'interpolationStopped' in current_obj:
                        if current_obj['interpolationStopped']:
                            continue
                    if 'classifications' in current_obj:
                        classification_list = []
                        classification_list = current_obj['classifications']
                        if len(classification_list) > 0:
                            classification_answer_dict_list = []
                            scrape_answers(classification_list, [], classification_answer_dict_list)
                            num_tool_classifications += len(classification_answer_dict_list)

            else:
                num_classifications = len(classification_list)
                num_tools = 0
                for obj in object_list:
                    if 'interpolationStopped' in obj:
                        if not obj['interpolationStopped']:
                            continue
                    num_tools += 1
                num_tool_classifications = 0
                num_relations = len(relation_list)

            if label_duration > 0:
                if duration_unit == 'msec':
                    label_duration_str = str(label_duration)
                elif duration_unit == 'sec':
                    label_duration_str = '{0:.2f}'.format(float(label_duration / 1000))
                elif duration_unit == 'min':
                    label_duration_str = '{0:.2f}'.format(float(label_duration / 60_000))
                elif duration_unit == 'hour':
                    label_duration_str = '{0:.2f}'.format(float(label_duration / 3_600_000))
                else:
                    duration_unit = 'msec'

            if review_duration > 0:
                if duration_unit == 'msec':
                    review_duration_str = str(review_duration)
                elif duration_unit == 'sec':
                    review_duration_str = '{0:.2f}'.format(float(review_duration / 1000))
                elif duration_unit == 'min':
                    review_duration_str = '{0:.2f}'.format(float(review_duration / 60_000))
                elif duration_unit == 'hour':
                    review_duration_str = '{0:.2f}'.format(float(review_duration / 3_600_000))
                else:
                    duration_unit = 'msec'

        batch_name_list.append(batches_str)
        stage_list.append(label_stage)
        asset_url_list.append(asset_url)
        external_id_list.append(external_id)
        task_id_list.append(task_id)
        labeler_name_list.append(labeler_name)
        reviewer_name_list.append(reviewer_name)
        label_date_list.append(label_date)
        review_date_list.append(review_date)
        label_duration_list.append(label_duration_str)
        review_duration_list.append(review_duration_str)
        num_page_list.append(num_page)
        num_classifications_list.append(num_classifications)
        num_tools_list.append(num_tools)
        num_tool_classifications_list.append(num_tool_classifications)
        num_relations_list.append(num_relations)

    logger.info("Assets Processed: [" + str(num_assets) + "/" + str(num_assets) + "]")
    data_df = pd.DataFrame()
    # data_df['Asset URL'] = asset_url_list
    data_df['Batch'] = batch_name_list
    data_df['Stage'] = stage_list
    data_df['External ID'] = external_id_list
    data_df['Task ID'] = task_id_list
    data_df['Labeler Name'] = labeler_name_list
    data_df['Reviewer Name'] = reviewer_name_list
    data_df['Label Date'] = label_date_list
    data_df['Review Date'] = review_date_list
    data_df['Label Duration (' + duration_unit + ')'] = label_duration_list
    data_df['Review Duration (' + duration_unit + ')'] = review_duration_list
    data_df['Number of Pages'] = num_page_list
    data_df['Number of Classifications'] = num_classifications_list
    data_df['Number of Tools'] = num_tools_list
    data_df['Number of Tool Classifications'] = num_tool_classifications_list
    data_df['Number of Relations'] = num_relations_list

    time_str = str(datetime.utcnow())
    time_str = time_str[:-3].replace(' ', 'T') + 'Z'
    time_str = time_str.replace(':', '-')

    output_filename = project_id + '-export-' + time_str + '_[TPT].csv'
    data_df.to_csv(output_filename, index=False)

    zip_filename = project_id + '-export-' + time_str + '.zip'
    mem_zip = BytesIO()
    with zipfile.ZipFile(mem_zip, mode="w") as zf:
        zf.write(output_filename)
    os.remove(output_filename)

    print_session_end(logger)
    export_response_obj = ExportResponse(file=mem_zip, file_name=zip_filename, storage_id="", bucket="")
    return export_response_obj


if __name__ == "__main__":
    plugin = ExportPlugin(id=PLUGIN_ID,
                          secret=PLUGIN_SECRET,
                          callback=callback_function,
                          host=HOST,
                          version='v3')

    run(plugin, host=HOST)