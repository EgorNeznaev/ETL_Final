import ydb
import csv

ENDPOINT = "***"
DATABASE = "***"
IAM_TOKEN = "***" 

def get_driver():
    credentials = ydb.AccessTokenCredentials(IAM_TOKEN)  
    driver_config = ydb.DriverConfig(
        ENDPOINT,
        DATABASE,
        credentials=credentials,
        root_certificates=ydb.load_ydb_root_certificate(),
    )    
    print("Подключение к YDB")
    driver = ydb.Driver(driver_config)
    driver.wait(timeout=15)
    print("Подключено успешно")
    return driver

def create_table(session):
    try:
        session.execute_scheme("DROP TABLE fraud_transactions")
        print("Таблица удалена")
    except Exception:
        pass
    query = """
    CREATE TABLE fraud_transactions (
        transaction_id Utf8 NOT NULL,
        customer_id Utf8 NOT NULL,
        transaction_date Utf8 NOT NULL,
        transaction_time Utf8 NOT NULL,
        hour_of_day Int32 NOT NULL,
        is_weekend Int32 NOT NULL,
        is_night_transaction Int32 NOT NULL,
        country Utf8 NOT NULL,
        city Utf8 NOT NULL,
        merchant_category Utf8 NOT NULL,
        payment_method Utf8 NOT NULL,
        device_type Utf8 NOT NULL,
        customer_age Int32 NOT NULL,
        credit_score Int32 NOT NULL,
        account_age_years Double NOT NULL,
        account_balance Double NOT NULL,
        transaction_amount Double NOT NULL,
        num_prev_transactions Int32 NOT NULL,
        transaction_freq_monthly Int32 NOT NULL,
        distance_from_home_km Double NOT NULL,
        time_since_last_txn_hrs Double NOT NULL,
        is_international Int32 NOT NULL,
        failed_attempts Int32 NOT NULL,
        pin_changed_recently Int32 NOT NULL,
        is_fraud Int32 NOT NULL,
        fraud_type Utf8,
        PRIMARY KEY (transaction_id)
    );
    """
    session.execute_scheme(query)
    print("Таблица создана")

def insert_batch(session, batch):
    query = """
    DECLARE $rows AS List<Struct<
        transaction_id: Utf8,
        customer_id: Utf8,
        transaction_date: Utf8,
        transaction_time: Utf8,
        hour_of_day: Int32,
        is_weekend: Int32,
        is_night_transaction: Int32,
        country: Utf8,
        city: Utf8,
        merchant_category: Utf8,
        payment_method: Utf8,
        device_type: Utf8,
        customer_age: Int32,
        credit_score: Int32,
        account_age_years: Double,
        account_balance: Double,
        transaction_amount: Double,
        num_prev_transactions: Int32,
        transaction_freq_monthly: Int32,
        distance_from_home_km: Double,
        time_since_last_txn_hrs: Double,
        is_international: Int32,
        failed_attempts: Int32,
        pin_changed_recently: Int32,
        is_fraud: Int32,
        fraud_type: Utf8
    >>;
    
    INSERT INTO fraud_transactions
    SELECT * FROM AS_TABLE($rows);
    """
    
    prepared = session.prepare(query)
    session.transaction().execute(
        prepared,
        {"$rows": batch},
        commit_tx=True
    )

def load_csv_to_ydb(csv_filename):
    driver = get_driver()
    session = driver.table_client.session().create()
    create_table(session)
    print(f"Чтение файла {csv_filename}...")
    batch = []
    total_rows = 0
    with open(csv_filename, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)       
        for row in reader:
            fraud_type_val = row['fraud_type']
            if fraud_type_val == 'None' or fraud_type_val is None or fraud_type_val == '':
                fraud_type_val = ""
            
            record = {
                'transaction_id': str(row['transaction_id']),
                'customer_id': str(row['customer_id']),
                'transaction_date': str(row['transaction_date']),
                'transaction_time': str(row['transaction_time']),
                'hour_of_day': int(row['hour_of_day']),
                'is_weekend': int(row['is_weekend']),
                'is_night_transaction': int(row['is_night_transaction']),
                'country': str(row['country']),
                'city': str(row['city']),
                'merchant_category': str(row['merchant_category']),
                'payment_method': str(row['payment_method']),
                'device_type': str(row['device_type']),
                'customer_age': int(row['customer_age']),
                'credit_score': int(row['credit_score']),
                'account_age_years': float(row['account_age_years']),
                'account_balance': float(row['account_balance']),
                'transaction_amount': float(row['transaction_amount']),
                'num_prev_transactions': int(row['num_prev_transactions']),
                'transaction_freq_monthly': int(row['transaction_freq_monthly']),
                'distance_from_home_km': float(row['distance_from_home_km']),
                'time_since_last_txn_hrs': float(row['time_since_last_txn_hrs']),
                'is_international': int(row['is_international']),
                'failed_attempts': int(row['failed_attempts']),
                'pin_changed_recently': int(row['pin_changed_recently']),
                'is_fraud': int(row['is_fraud']),
                'fraud_type': fraud_type_val
            }
            batch.append(record)
            
            if len(batch) >= 100:
                insert_batch(session, batch)
                total_rows += len(batch)
                print(f"Вставлено {total_rows} строк")
                batch = []
    
    if batch:
        insert_batch(session, batch)
        total_rows += len(batch)
        print(f"Вставлено {total_rows} строк")
    
    print(f"Всего вставлено {total_rows} строк")
    driver.stop()

if __name__ == "__main__":
    load_csv_to_ydb('bank_fraud.csv')