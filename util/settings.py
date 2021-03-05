# coding: utf-8
import os
from collections import namedtuple


class KafkaSettings:
    HOST = 'public-kafka-wsc-website-checker.aivencloud.com'
    PORT = 28916
    SERVICE_URI = f'{HOST}:{PORT}'

    CERT_DIR = os.path.expanduser("~/.kafka")
    SSL_CONFIG = dict(
        ssl_cafile=os.path.join(CERT_DIR, "ca.pem"),
        ssl_certfile=os.path.join(CERT_DIR, "service.cert"),
        ssl_keyfile=os.path.join(CERT_DIR, "service.key"),
    )
    SECURITY_CONFIG = dict(
        security_protocol="SSL",
    )
    SECURITY_CONFIG.update(SSL_CONFIG)

    WSC_TOPIC = 'wsc_topic'


class PostgresSettings:
    HOST = 'pg-wsc-website-checker.aivencloud.com'
    PORT = 28914
    USER = 'avnadmin'
    DB_NAME = 'defaultdb'
    SSL_MODE = 'require'
    CERT_DIR = os.path.expanduser("~/.postgres")
    PASSWORD_FNAME = os.path.join(CERT_DIR, "password.txt")

    # SERVICE_URI = f'postgres://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}?sslmode={SSL_MODE}'

    TableField = namedtuple('TableField', 'name type col_constraint')

    WEBSITES_TABLE = 'websites_table'
    WEBSITES_TABLE_SCHEMA = [
        TableField('url_md5', 'bigint', 'PRIMARY KEY'),
        TableField('response_time', 'int', ''),
        TableField('status_code', 'int', ''),
        TableField('pattern', 'text', ''),
        TableField('match', 'text', ''),
    ]


class ProducerSettings:
    BATCH_SIZE_BYTES = 4 * 1024


class ConsumerSettings:
    BATCH_SIZE = 4
    SLEEP_INTERVAL_SEC = 20
    SESSION_LENGTH_SEC = 60 * 3
