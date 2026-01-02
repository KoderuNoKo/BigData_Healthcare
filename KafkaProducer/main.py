#!/usr/bin/env python3
"""
Main entry point for MIMIC-IV Kafka Producer System.

Usage:
    # Run all enabled producers
    python main.py
    
    # Run all producer (enabled or not)
    python main.py --all
    
    # Run specific producers only
    python main.py --producers chartevents labevents
"""
import argparse
import logging
import sys

from ._coordinator.producer_coordinator import ProducerCoordinator
from .config.kafka_config import get_kafka_config
from .config.dataset_config import get_producer_configs

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='MIMIC-IV Kafka Producer - Stream medical data to Kafka'
    )
    
    parser.add_argument(
        '--producers',
        nargs='+',
        help='Specific producers to run (e.g., chartevents labevents)'
    )
    
    parser.add_argument(
        '--all',
        action='store_true',
        help='Run all producers including disabled ones'
    )
    
    return parser.parse_args()


def filter_producers(configs, producer_names):
    """Filter configs to only include specified producers"""
    if not producer_names:
        return configs
    filtered = [c for c in configs if c['name'] in producer_names]
    if not filtered:
        logger.error(f"No matching producers found for: {producer_names}")
        available = [c['name'] for c in configs]
        logger.error(f"Available producers: {available}")
        sys.exit(1)
    return filtered


def print_banner(producer_configs, kafka_config):
    """Print startup banner"""
    print("\n" + "=" * 70)
    print("  MIMIC-IV Kafka Producer System")
    print("=" * 70)
    print(f"\nKafka Brokers: {kafka_config.get('bootstrap.servers', 'unknown')}")
    print(f"\nProducers ({len(producer_configs)}):")
    for name, config in producer_configs.items():
        print(f"  - {name:15} -> {config['topic']:25} @ {config['rate']:4} msg/s")
    print("\n" + "=" * 70)
    print("Press Ctrl+C to stop")
    print("=" * 70 + "\n")


def main():
    """Main entry point"""
    args = parse_args()
    
    try:
        # Get configurations
        kafka_config = get_kafka_config()
        if args.all:
            producer_configs = get_producer_configs(enabled_only=False)
        elif args.producers:
            producer_configs = get_producer_configs(enabled_only=False)
            producer_configs = filter_producers(producer_configs, args.producers)
        else:
            producer_configs = get_producer_configs(enabled_only=True)
        
        # Check if any producers to run
        if not producer_configs:
            logger.error("No producers configured to run")
            logger.info("Use --all to include disabled producers")
            sys.exit(1)
        
        # Print banner
        print_banner(producer_configs, kafka_config)
        
        # Create and start coordinator
        coordinator = ProducerCoordinator(producer_configs, kafka_config)
        coordinator.start()
        
        logger.info("All producers terminated")
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        raise e


if __name__ == "__main__":
    main()