import sys
import logging
import config
import pymysql
import json

# rds settings
rds_host = "asset-manager-db.c9zxukrtlwzh.us-east-2.rds.amazonaws.com"
name = config.db_username
password = config.db_password
db_name = config.db_name

print(rds_host)
# logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# connect using creds from rds_config.py
try:
    conn = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name, port=3306)
except Exception as e:
    print(e)
    logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
    sys.exit()

logger.info("SUCCESS: Connection to RDS mysql instance succeeded")


def check_duplicate(user):
    cursor = conn.cursor()
    sql = "SELECT * FROM `tbl_users` WHERE username = (%s)"
    cursor.execute(sql, user)

    rows = cursor.fetchall()
    if len(rows) > 0:
        return False
    return True


# executes upon API event
def lambda_handler(event, context):
    request_body = event["body"]
    print(request_body)
    data = json.loads(request_body)
    if check_duplicate(data['user']) is False:
        print("duplicate")
        return {
            'statusCode': 401,
            "body": json.dumps({
                "message": "User have been registered"
            })
        }
    with conn.cursor() as cur:
        sql = "INSERT INTO `tbl_users` (`username`,`user_password`, `fullname`, `department`, `role_id`) VALUES (%s, " \
              "%s, %s, %s, %s) "
        cur.execute(sql, (data['user'], data['pass'], data['fullname'], data['department'], 1))
        conn.commit()

    return {
        'statusCode': 200,
        "body": json.dumps({
            "message": "Register successfully"
        })
    }
