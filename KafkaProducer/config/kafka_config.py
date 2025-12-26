import copy

KAFKA_CONFIG = {
    # connection
    'bootstrap.servers': 'cp-kafka:9092',
}

TOPIC_MAPPING = {
    'chartevents': 'icu_chartevents',
    'microbiologyevents': 'hosp_microbiologyevents',
}

def get_kafka_config():
    return copy.deepcopy(KAFKA_CONFIG)

def get_topic_mapping():
    return copy.deepcopy(TOPIC_MAPPING)