# coding: utf-8
import itertools
import logging
import pickle
import time

from psycopg2 import sql
from kafka import KafkaConsumer

from util.settings import KafkaSettings, PostgresSettings, ConsumerSettings
from util.db_worker import DbWorker
from util.utils import unique_by_nth_field


class Consumer:
    def __init__(self, db_worker):
        self.db_worker = db_worker
        self.consumer = KafkaConsumer(
            KafkaSettings.WSC_TOPIC,
            auto_offset_reset="earliest",
            bootstrap_servers=KafkaSettings.SERVICE_URI,
            client_id="demo-client-1",
            group_id="demo-group",
            **KafkaSettings.SECURITY_CONFIG,
        )

    @staticmethod
    def _message_to_table_values(msg):
        """
        msg from kafka to list of fields by order that matches postgres table_schema
        >>> consumer = Consumer(None)   # todo: use unittest with mock for such classes
        >>> message_dct = dict(
        ...     status_code=200,
        ...     response_time=20,
        ... )
        >>> try:
        ...     consumer._message_to_table_values(pickle.dumps(message_dct))
        ... except Exception as ex:
        ...     ex
        KeyError('Special column %s not found in message dict', 'url_md5')
        >>> message_dct['url_md5'] = 2**64 - 1
        >>> try:
        ...     consumer._message_to_table_values(pickle.dumps(message_dct))
        ... except Exception as ex:
        ...     ex
        ValueError('%s out of range: %s', 'url_md5', 18446744073709551615)
        >>> message_dct['url_md5'] = 1
        >>> message_dct['pattern'] = '[0-9]+'
        >>> consumer._message_to_table_values(pickle.dumps(message_dct))
        (1, 20, 200, '[0-9]+', 'NULL')
        """
        unpacked_dct = pickle.loads(msg)
        result = []
        for field in PostgresSettings.WEBSITES_TABLE_SCHEMA:
            if field.col_constraint and field.name not in unpacked_dct:
                raise KeyError('Special column %s not found in message dict', field.name)
            # todo: better way is to fill and validate some db-related structure from kafka message
            if field.type == 'bigint' and unpacked_dct[field.name] >= 2**63:
                raise ValueError('%s out of range: %s', field.name, unpacked_dct[field.name])
            result.append(unpacked_dct.get(field.name, 'NULL'))
        return tuple(result)

    @staticmethod
    def _compose_update_query(messages_cnt):
        # inner 'format' generates placeholders for fields names and values according to schema and len(batch)
        # outer 'format' plainly fills fields names that should be wrapped into sql.Identifiers for safety
        # sql_vars takes values to be inserted into table, which would be wrapped by cursor.execute()
        # todo: somehow hide usage of psycopg2.sql in db_worker.py
        sql_query = sql.SQL(
            'INSERT INTO {{}} ({columns}) VALUES {values} '
            'ON CONFLICT ({key_columns}) DO UPDATE SET {updates} RETURNING *'.format(
                columns=', '.join('{}' for _ in range(len(PostgresSettings.WEBSITES_TABLE_SCHEMA))),
                values=', '.join(
                    '('
                    + ', '.join('%s' for _ in range(len(PostgresSettings.WEBSITES_TABLE_SCHEMA)))
                    + ')'
                    for _ in range(messages_cnt)
                ),
                key_columns=', '.join(
                    '{}' for field in PostgresSettings.WEBSITES_TABLE_SCHEMA
                    if field.col_constraint
                ),
                updates=', '.join(
                    '{} = EXCLUDED.{}'
                    for field in PostgresSettings.WEBSITES_TABLE_SCHEMA
                    if not field.col_constraint
                )
            )
        ).format(
            sql.Identifier(PostgresSettings.WEBSITES_TABLE),
            *[sql.Identifier(field.name) for field in PostgresSettings.WEBSITES_TABLE_SCHEMA],  # values
            *[sql.Identifier(field.name) for field in PostgresSettings.WEBSITES_TABLE_SCHEMA if field.col_constraint],  # key_columns
            *list(itertools.chain(*[
                [sql.Identifier(field.name)] * 2
                for field in PostgresSettings.WEBSITES_TABLE_SCHEMA
                if not field.col_constraint
            ])),     # updates
        )
        return sql_query

    def process_batch(self, batch):
        total = 0
        skipped = 0
        messages_values = []

        for msg in batch:
            total += 1
            try:
                messages_values.append(self._message_to_table_values(msg.value))
            except Exception as ex:
                logging.info(ex)
                skipped += 1
                continue

        logging.warning('Got %s, skipped %s messages from kafka', total, skipped)
        if messages_values:
            unique_messages_values = unique_by_nth_field(
                messages_values,
                key_idx=[
                    idx for idx, field in enumerate(PostgresSettings.WEBSITES_TABLE_SCHEMA)
                    if field.col_constraint != ''
                ][0],
                keep_last=True,
            )  # rows for db update query should be unique by url_md5
            logging.warning('Unique messages in batch: %s', len(unique_messages_values))
            result = self.db_worker.execute(
                sql_query=self._compose_update_query(len(unique_messages_values)),
                sql_vars=list(itertools.chain(*unique_messages_values)),
                expect_result=True,
                commit=True,
            )
            logging.warning('Inserted/updated %s rows', len(result))
        return total

    def consume_from_kafka_to_db(self, max_records=4):
        raw_msgs = self.consumer.poll(timeout_ms=1000, max_records=max_records)
        total_consumed = 0
        for tp, msgs in raw_msgs.items():
            total_consumed += self.process_batch(msgs)
        return total_consumed


def check_or_create_websites_table(db_worker):
    table_exists_check = db_worker.execute(
        sql_query="SELECT to_regclass(%s)",
        sql_vars=[PostgresSettings.WEBSITES_TABLE],
        expect_result=True,
        commit=False,
    )
    if table_exists_check[0]['to_regclass'] is None:
        logging.warning('websites table does not exist in db')
        db_worker.execute(
            sql_query=(
                "CREATE TABLE {table} ({fields});".format(
                    table=PostgresSettings.WEBSITES_TABLE,
                    fields=', '.join(
                        f'{field.name} {field.type} {field.col_constraint}'
                        for field in PostgresSettings.WEBSITES_TABLE_SCHEMA
                    )
                )
            ),
            expect_result=False,
        )
        logging.info('websites table created')
    else:
        logging.warning('websites table exists')


def main():
    """
    Sample for an upstart daemon. Run for session_length_sec, then exit closing db connection.
    """
    logging.basicConfig(level=logging.WARNING)

    with DbWorker(
        user=PostgresSettings.USER,
        host=PostgresSettings.HOST,
        port=PostgresSettings.PORT,
        password_fname=PostgresSettings.PASSWORD_FNAME,
        db_name=PostgresSettings.DB_NAME,
        ssl_mode=PostgresSettings.SSL_MODE,
    ) as db_worker:
        check_or_create_websites_table(db_worker)

        consumer = Consumer(db_worker)
        session_start_ts = time.time()

        # try read next batch from kafka.
        # if it contains less than batch_size message, perhaps topic is empty, wait for upcoming messages.
        while True:
            consumed_cnt = consumer.consume_from_kafka_to_db(max_records=ConsumerSettings.BATCH_SIZE)
            if consumed_cnt < ConsumerSettings.BATCH_SIZE:
                logging.warning(
                    '%s < %s: processed last batch in topic, sleep %s sec',
                    consumed_cnt,
                    ConsumerSettings.BATCH_SIZE,
                    ConsumerSettings.SLEEP_INTERVAL_SEC
                )
                time.sleep(ConsumerSettings.SLEEP_INTERVAL_SEC)
            if time.time() - session_start_ts >= ConsumerSettings.SESSION_LENGTH_SEC:
                logging.info('Close current reading session')
                return


if __name__ == '__main__':
    main()
