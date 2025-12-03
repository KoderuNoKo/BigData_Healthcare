import copy

KAFKA_CONFIG = {
    # connection
    'bootstrap.servers': 'cp-kafka:9092',
}

TOPIC_MAPPING = {
    'chartevents': 'icu_chartevents',
    'labevents': 'hosp_labevents',
    'vitalsign': 'ed_vitalsign',
    'inputevents': 'icu_inputevents',
    'outputevents': 'icu_outputevents',
}

def get_kafka_config():
    return copy.deepcopy(KAFKA_CONFIG)

def get_topic_mapping():
    return copy.deepcopy(TOPIC_MAPPING)