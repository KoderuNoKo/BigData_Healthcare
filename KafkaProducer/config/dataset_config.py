"""Configurations regarding the data sources, each sources corresponding to a producer
'data_source': {
    'path': 'path/to/data/source',
    'rate': mesage rate (msgs/sec)
    ...
}
"""
import copy

from .kafka_config import get_topic_mapping

topic_mapping = get_topic_mapping()

CHECKPOINT_DIR = './checkpoints'

DATASET_SOURCES = {
    'chartevents': {
        'file_path': '/home/dev/app/data/mimic-iv-3.1/icu/chartevents.csv',
        'topic': topic_mapping['chartevents'],
        'rate': 2000,
        'batch_size': 10000,
        'key_field': 'subject_id',
        'partition_key': 'stay_id',
        'enabled': True,
    },
    'microbiologyevents': {
        'file_path': '/home/dev/app/data/mimic-iv-3.1/hosp/microbiologyevents.csv',
        'topic': topic_mapping['microbiologyevents'],
        'rate': 200,
        'batch_size': 1000,
        'key_field': 'microevent_id',
        'partition_key': 'subject_id',
        'enabled': True,
    },
}

for name, config in DATASET_SOURCES.items():
    if not config.get('topic', False):
        config['topic'] = topic_mapping[name]

def get_producer_configs(enabled_only: bool = True):
    """Returns datesets contains"""
    dataset_configs = copy.deepcopy(DATASET_SOURCES)
    return {
        dataset: config for dataset, config in dataset_configs.items() if config.get('enabled', False)
    } if enabled_only else {
        dataset: config for dataset, config in dataset_configs.items()
    }
    
def get_single_producer_config(name: str):
    if name not in DATASET_SOURCES.keys():
        raise KeyError(f"Unable to get single producer '{name}', no such dataset source.")
    dataset_config = copy.deepcopy(DATASET_SOURCES[name])
    return dataset_config