from datetime import datetime
import ydb.iam
import csv
import time

endpoint = "grpcs://ydb.serverless.yandexcloud.net:2135"
database = "/ru-central1/b1gmruici8ab4700qd6c/etnqrjjh5eo8mmf1kj4u"

driver = ydb.Driver(
    endpoint=endpoint,
    database=database,
    credentials=ydb.iam.ServiceAccountCredentials.from_file("authorized_key.json")
)

driver.wait(fail_fast=True, timeout=10)
session = driver.table_client.session().create()


def execute_batch(session, batch):
    structs = []
    for b in batch:
        structs.append({
            'call_id': to_bytes(b['call_id']),
            'call_time': b['call_time'],
            'client_id': to_bytes(b['client_id']),
            'region_code': to_bytes(b['region_code']),
            'campaign_type': to_bytes(b['campaign_type']),
            'call_status': to_bytes(b['call_status']),
            'client_response': to_bytes(b['client_response']),
            'duration_sec': b['duration_sec'],
            'follow_up_required': b['follow_up_required']
        })

    query = """
        declare $rows as List<struct<
            call_id: string,
            call_time: timestamp,
            client_id: string,
            region_code: string,
            campaign_type: string,
            call_status: string,
            client_response: string,
            duration_sec: uint32,
            follow_up_required: bool
        >>;

    upsert into call_data
    select call_id, call_time, client_id, region_code, campaign_type, call_status, client_response, duration_sec, follow_up_required
    from as_table($rows);
    """

    prepared = session.prepare(query)
    session.transaction().execute(prepared, {'$rows': structs}, commit_tx=True)


def to_bytes(s):
    return s.encode('utf-8') if isinstance(s, str) else s


try:
    batch_size = 100
    batch = []
    total = 0
    batch_num = 0

    with open('transactions_v2.csv', 'r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)

        for row in csv_reader:
            dt = datetime.strptime(row['call_time'], '%Y-%m-%d %H:%M:%S')

            batch.append({
                'call_id': row['call_id'],
                'call_time': dt,
                'client_id': row['client_id'],
                'region_code': row['region_code'],
                'campaign_type': row['campaign_type'],
                'call_status': row['call_status'],
                'client_response': row['client_response'],
                'duration_sec': int(row['duration_sec']),
                'follow_up_required': row['follow_up_required'].lower() == 'true'
            })

            if len(batch) >= batch_size:
                batch_num += 1
                execute_batch(session, batch)
                total += len(batch)
                print(f"Батч {batch_num}: загружено {total} записей")
                batch = []
                time.sleep(0.5) # Побороть RU лимит не удалось, т.ч. пришлось поставить задержку в батчах.
                                # Причём лимит приследовал меня даже после его отключения в БД Managed Service for YDB
        if batch:
            batch_num += 1
            execute_batch(session, batch)
            total += len(batch)
            print(f"Батч {batch_num}: загружено {total} записей")

    print(f"Загружено {total} записей")

except Exception as e:
    print(f"Ошибка: {e}")

finally:
    session.delete()
    driver.stop()