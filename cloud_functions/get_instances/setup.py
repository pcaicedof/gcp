
#Python
import pandas as pd
from datetime import datetime
#google
import google.cloud.compute_v1 as compute_v1
from googleapiclient.errors import HttpError
from google.api_core.exceptions import BadRequest, NotFound
from googleapiclient import discovery
from google.oauth2.service_account import Credentials
from google.cloud import bigquery as bq


auth = Credentials.from_service_account_file('C:\\Users\\BI\\sa_gcp\\pcf-portfolio-7b5d2bf607c2.json')
ZONES = ['us-central1-a','us-central1-b','us-central1-c','us-west1-a','us-west1-b','us-west1-c','us-east4-c','us-east4-b','us-east4-a']
PROJECT = 'pcf-portfolio'
DATASET = 'instances'
TABLE_NAME = 'instances_info'


def create_compute_service(auth, service_name):
    service = discovery.build(
    serviceName=service_name,
    version="v1",
    credentials=auth,
    cache_discovery=False)
    return service  

def get_projects(auth):
    service_cloudrm = create_compute_service(auth, 'cloudresourcemanager')
    total_projects = service_cloudrm.projects().list().execute()['projects']
    project_list = []
    total = len(total_projects) - 1
    for i in range(0,total):
        project = total_projects[i]['projectId']
        project_list.append(project)
    return project_list

def get_instances(auth, project, zone):
    SERVICE_COMPUTE = create_compute_service(auth, 'compute')
    instances = SERVICE_COMPUTE.instances().list(project=project, zone=zone).execute()['items']
    return instances

def get_disk_size(disk_list):
    total_disk = 0
    for i, disk in enumerate(disk_list):
        total_disk += int(disk_list[i]['diskSizeGb'])
    return total_disk
        
def get_instances_info(instances_list, project, zone):
    SERVICE_COMPUTE = create_compute_service(auth, 'compute')
    instance_info = []
    for i, instance in enumerate(instances_list):
        dict_instance = {}
        instance = instances_list[i]
        machine_type = instance['machineType'].rsplit('/', 1)[-1]
        machine = SERVICE_COMPUTE.machineTypes().get(project=project, zone=zone, machineType=machine_type).execute()
        dict_instance['id'] = instance['id']
        dict_instance['name'] = instance['name']
        dict_instance['ram'] = (machine['memoryMb']/1024)
        dict_instance['cpu'] = machine['guestCpus']
        dict_instance['machine_type'] = machine_type
        dict_instance['last_start_time'] = instance['lastStartTimestamp']
        disks_list = instance['disks']
        total_disk = get_disk_size(disks_list)
        dict_instance['disk_size'] = total_disk
        instance_info.append(dict_instance)
    return instance_info
    


def write_to_bq_from_df(PROJECT, DATASET, TABLE_NAME, df):
    bq_client = bq.Client(project=PROJECT)
    dataset_ref = bq_client.dataset(DATASET)
    table_ref = dataset_ref.table(TABLE_NAME)
    job_config = bq.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    try:
        bq_client.load_table_from_dataframe(df, table_ref,
                                            job_config=job_config).result()
        print(f'Escribe {TABLE_NAME}')
    except Exception as e:
        print(e)       
        
        
def get_instances_df(project_list):    
    instances_projects = []
    for project in project_list:
        print(project)
        for i, zone in enumerate(ZONES):
            dict_intances_projects = {}
            try:
                instances = get_instances(auth, project, zone)
                dict_intances_projects['Project'] = project
                dict_intances_projects['zone'] = zone        
                instance_info = get_instances_info(instances, project, zone)
                dict_intances_projects['instance_info'] = instance_info
                instances_projects.append(dict_intances_projects)
            except Exception as e:
                pass
    df_instances = pd.DataFrame(instances_projects)
    print('termina')
    return df_instances   
    
def main():
    
    project_list = get_projects(auth)
    start_time = datetime.now()
    print("start Time =", start_time)
    df = get_instances_df(project_list)
    write_to_bq_from_df(PROJECT, DATASET, TABLE_NAME, df)
    end_time = datetime.now()
    time_elapsed = end_time - start_time
    print("end Time =", time_elapsed)


if __name__ == '__main__':
    main()

