"""
ProducerFactory: Factory pattern for creating MIMIC-IV Kafka producers.

Automatically instantiates the correct producer class based on configuration.
"""
import logging
from typing import Dict, Any, Type

from .base_producer import BaseProducer
from .chartevents_producer import CharteventsProducer
from .labevents_producer import LabeventsProducer
# from vitalsign_producer import VitalsignProducer
# from inputevents_producer import InputeventsProducer
# from outputevents_producer import OutputeventsProducer

logger = logging.getLogger(__name__)    


class ProducerFactory:
    """
    Factory for creating appropriate producer instances based on data type.
    
    Usage:
        factory = ProducerFactory()
        producer = factory.create_producer('chartevents', config, kafka_config)
    """
    
    # Registry mapping producer names to their classes
    _PRODUCER_REGISTRY: Dict[str, Type[BaseProducer]] = {
        'chartevents': CharteventsProducer,
        'labevents': LabeventsProducer,
        # 'vitalsign': VitalsignProducer,
        # 'inputevents': InputeventsProducer,
        # 'outputevents': OutputeventsProducer,
    }
    
    @classmethod
    def create_producer(cls,
                       name: str,
                       config: Dict[str, Any],
                       kafka_config: Dict[str, Any],
                       checkpoint_dir: str = "./checkpoints") -> BaseProducer:
        """
        Create a producer instance based on name.
        
        Args:
            name: Producer name (e.g., 'chartevents', 'labevents')
            config: Producer-specific configuration
                {
                    'file_path': str,
                    'topic': str,
                    'rate': int,
                    'batch_size': int,
                    'time_multiplier': float,
                    'partition_key': str (optional)
                }
            kafka_config: Kafka broker configuration
            checkpoint_dir: Directory for checkpoint files
            
        Returns:
            Instantiated producer subclass
            
        Raises:
            ValueError: If producer name not recognized
        """
        producer_class = cls._PRODUCER_REGISTRY.get(name)
        
        if producer_class is None:
            available = ', '.join(cls._PRODUCER_REGISTRY.keys())
            raise ValueError(
                f"Unknown producer type: '{name}'. "
                f"Available producers: {available}"
            )
        
        logger.info(f"Creating producer '{name}' of type {producer_class.__name__}")
        
        # Validate required config fields
        cls._validate_config(name, config)
        
        # Instantiate and return producer
        return producer_class(
            name=name,
            config=config,
            kafka_config=kafka_config,
            checkpoint_dir=checkpoint_dir
        )
    
    @classmethod
    def _validate_config(cls, name: str, config: Dict[str, Any]):
        """
        Validate that required configuration fields are present.
        
        Raises:
            ValueError: If required fields are missing
        """
        required_fields = ['file_path', 'topic']
        missing_fields = [field for field in required_fields if field not in config]
        
        if missing_fields:
            raise ValueError(
                f"Producer '{name}' config missing required fields: {missing_fields}"
            )
        
        # Validate file_path exists (optional but recommended)
        from pathlib import Path
        file_path = Path(config['file_path'])
        if not file_path.exists():
            logger.warning(f"Producer '{name}' file does not exist: {file_path}")
    
    @classmethod
    def register_producer(cls, name: str, producer_class: Type[BaseProducer]):
        """
        Register a new producer class.
        
        Allows dynamic registration of custom producer classes.
        
        Args:
            name: Producer name
            producer_class: Producer class (must inherit from BaseProducer)
        """
        if not issubclass(producer_class, BaseProducer):
            raise TypeError(
                f"Producer class must inherit from BaseProducer, "
                f"got {producer_class.__name__}"
            )
        
        cls._PRODUCER_REGISTRY[name] = producer_class
        logger.info(f"Registered producer '{name}' -> {producer_class.__name__}")
    
    @classmethod
    def list_producers(cls) -> list:
        """List all registered producer types"""
        return list(cls._PRODUCER_REGISTRY.keys())
    
    @classmethod
    def create_multiple(cls,
                       producer_configs: Dict[str, Dict[str, Any]],
                       kafka_config: Dict[str, Any],
                       checkpoint_dir: str = "./checkpoints") -> Dict[str, BaseProducer]:
        """
        Create multiple producers from a list of configurations.
        
        Convenience method for batch creation.
        
        Args:
            producer_configs: List of producer configurations
                [
                    {'name': 'chartevents', 'file_path': '...', 'topic': '...', ...},
                    {'name': 'labevents', 'file_path': '...', 'topic': '...', ...},
                    ...
                ]
            kafka_config: Shared Kafka configuration
            checkpoint_dir: Shared checkpoint directory
            
        Returns:
            Dictionary mapping producer names to instances
            
        Example:
            configs = [
                {'name': 'chartevents', 'file_path': '/data/chartevents.csv', ...},
                {'name': 'labevents', 'file_path': '/data/labevents.csv', ...}
            ]
            producers = ProducerFactory.create_multiple(configs, kafka_config)
            producers['chartevents'].stream()
        """
        producers = {}
        
        for name, config in producer_configs.items():            
            try:
                producer = cls.create_producer(name, config, kafka_config, checkpoint_dir)
                producers[name] = producer
            except Exception as e:
                logger.error(f"Failed to create producer '{name}': {e}")
                # Continue creating other producers even if one fails
        
        logger.info(f"Created {len(producers)} producers")
        return producers