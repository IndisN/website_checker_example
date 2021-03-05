# coding: utf-8
import os
import logging
from argparse import ArgumentParser

from kafka import KafkaProducer

from util.settings import KafkaSettings, ProducerSettings
from wsc_producer.urls_fetcher import UrlsFetcher, UrlData


class Producer:
    def __init__(self, batch_size=10):
        self.batch_size = batch_size
        self._counter = 0

        logging.warning('Check config files')
        for fname in KafkaSettings.SSL_CONFIG.values():
            if not os.path.exists(fname):
                raise FileNotFoundError(fname)

        self.kafka_producer = KafkaProducer(
            bootstrap_servers=KafkaSettings.SERVICE_URI,
            **KafkaSettings.SECURITY_CONFIG,
        )

    def __enter__(self):
        logging.info('Producer enter')
        return self

    def send_message(self, message):
        self.kafka_producer.send(KafkaSettings.WSC_TOPIC, message)
        self._counter += 1
        if self._counter % self.batch_size == 0:
            self.kafka_producer.flush()
            logging.warning('Flushed next %s messages to kafka', self.batch_size)

    def send_batch(self, messages):
        for msg in messages:
            self.send_message(msg)

    def __exit__(self, exc_type, exc_value, traceback):
        self.kafka_producer.flush()     # if last batch was incomplete
        logging.warning('Flushed last %s messages to kafka', self._counter % self.batch_size)

        self.kafka_producer.close()
        logging.warning('Closed kafka producer')


def main():
    logging.basicConfig(level=logging.WARNING)
    logging.warning('Start producer')

    arg_parser = ArgumentParser()
    arg_parser.add_argument('-i', '--input-file', help='tsv file with urls and regexp patterns')
    args = arg_parser.parse_args()

    url_fetcher = UrlsFetcher()

    with open(args.input_file) as fobj, Producer() as producer:
        while True:
            lines = fobj.readlines(ProducerSettings.BATCH_SIZE_BYTES)
            if not lines:
                break

            urls_data = [
                UrlData(*(line.rstrip('\n').split('\t', 1) + [''])[:2])  # fill maybe missing pattern with ''
                for line in lines
            ]

            logging.warning('Fetch %s urls', len(urls_data))
            responses = url_fetcher.process(urls_data)
            for response in responses:
                response.match_pattern()
                producer.send_message(response.to_msg())
    logging.warning('Processed all urls, exit')


if __name__ == '__main__':
    main()
