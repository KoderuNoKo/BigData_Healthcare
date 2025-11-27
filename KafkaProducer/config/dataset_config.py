"""Configurations regarding the data sources, each sources corresponding to a producer
'data_source': {
    'path': 'path/to/data/source',
    'rate': mesage rate (msgs/sec)
    ...
}
"""
import copy

from .kafka_config import TOPIC_MAPPING

CHECKPOINT_DIR = './checkpoints'

DATASET_SOURCES = {
    'chartevents': {
        'file_path': '/data/mimic-iv-3.1/icu/chartevents.csv',
        'topic': TOPIC_MAPPING['chartevents'],
        'rate': 100,
        'batch_size': 5000,
        'key_field': 'subject_id',
        'partition_key': 'stay_id',
        
        # flags
        'enabled': True,
    },
    'labevents': {
        'file_path': '/data/mimic-iv-3.1/hosp/labevents.csv',
        'rate': 50,
        'batch_size': 3000,
        'key_field': 'subject_id',
        
        # flags
        'enabled': False,
    },
}

def get_producer_configs(enabled_only: bool = False):
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