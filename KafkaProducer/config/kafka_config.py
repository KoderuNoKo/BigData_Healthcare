KAFKA_CONFIG = {
    # connection
    'bootstrap_servers': ['cp-kafka:9092',],
}

TOPIC_MAPPING = {
    'chartevents': 'icu_chartevents',
    'labevents': 'hosp_labevents',
    'vitalsign': 'ed_vitalsign',
    'inputevents': 'icu_inputevents',
    'outputevents': 'icu_outputevents',
}