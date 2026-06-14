import uuid
import datetime
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

YC_DP_AZ = 'ru-central1-d'
YC_DP_SSH_PUBLIC_KEY = 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDc6txCJW0Vd8bexFFSWzXUjzeXhmn+5cBGAgt7vEZA4NSFADhzQvBSWP6df+zsnRL5u+mjLMdkiO5YQngotKqrIZa6I9J6ixFwMohlfjpLyScvvEd7Wuk24nRlzI/cGoUUoqO/6rt/Sg0cSeVotp+Leka/qBP/7yUtnu53pu3rYd85ryEJAeWlKlCCWMzOQ8jhgTmMRLoyyLeHWyTyAVuVCPgHdAmnGxBct2nDkAG8j+X3DpOfKfl65+mcs4SkOTnN9W31ZIMPXagT32n/qSDjwmz+s62mEBzTHcKcqi4cyJGhh6uD6izt3W0IFIHnLyN+6bSNUhAEzPhBGoennLZR valch@DESKTOP-19RL358'  # Ваш открытый SSH-ключ
YC_DP_SUBNET_ID = 'fl826crftksdjlsbimbc'
YC_DP_SA_ID = 'ajen1k20libeo514vk2v'
YC_DP_METASTORE_URI = '10.130.0.5'
YC_BUCKET = 'dz1'

with DAG(
        'Data_processing',
        schedule_interval=None,
        tags=['data-processing', 'loans', 'spark'],
        start_date=datetime.datetime.now(),
        max_active_runs=1,
        catchup=False,
        description='Обработка кредитных заявок из 30 CSV файлов'
) as loan_dag:

    create_spark_cluster = DataprocCreateClusterOperator(
        task_id='create-dataproc-cluster',
        cluster_name=f'loan-processing-{uuid.uuid4()}',
        cluster_description='Временный кластер для обработки кредитных заявок',
        ssh_public_keys=YC_DP_SSH_PUBLIC_KEY,
        service_account_id=YC_DP_SA_ID,
        subnet_id=YC_DP_SUBNET_ID,
        s3_bucket=YC_BUCKET,
        zone=YC_DP_AZ,
        cluster_image_version='2.1',
        masternode_resource_preset='s2.small',
        masternode_disk_type='network-ssd',
        masternode_disk_size=20,
        computenode_resource_preset='s2.small',
        computenode_disk_type='network-ssd',
        computenode_disk_size=20,
        computenode_count=2,
        computenode_max_hosts_count=5,
        services=['YARN', 'SPARK'],
        datanode_count=0,
        properties={'spark:spark.hive.metastore.uris': f'thrift://{YC_DP_METASTORE_URI}:9083',},
    )

    run_spark_job = DataprocCreatePysparkJobOperator(
        task_id='run-loan-processing-job',
        main_python_file_uri=f's3a://{YC_BUCKET}/Scripts/create_table.py',
        properties={
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        },
    )

    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id='delete-dataproc-cluster',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_spark_cluster >> run_spark_job >> delete_spark_cluster