#!/usr/bin/env python3
"""
Test script for producer infrastructure.

Tests BaseProducer, CharteventsProducer, and ProducerFactory.
"""
import logging
from pathlib import Path

# Import configurations
from config.kafka_config import KAFKA_CONFIG
from config.dataset_config import get_producer_configs, get_single_producer_config, CHECKPOINT_DIR

from producers.producer_factory import ProducerFactory

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_factory_creation():
    """Test 1: Can we create producers using the factory?"""
    logger.info("=" * 50)
    logger.info("TEST 1: Factory Creation")
    logger.info("=" * 50)
    
    try:
        # Get configurations
        kafka_config = KAFKA_CONFIG
        chartevents_config = get_single_producer_config('chartevents')
        
        # Create producer using factory
        producer = ProducerFactory.create_producer(
            name='chartevents',
            config=chartevents_config,
            kafka_config=kafka_config,
            checkpoint_dir=CHECKPOINT_DIR
        )
        
        logger.info(f"✓ Successfully created producer: {producer.name}")
        logger.info(f"✓ Producer type: {type(producer).__name__}")
        logger.info(f"✓ Topic: {producer.topic}")
        logger.info(f"✓ File: {producer.file_path}")
        logger.info(f"✓ Rate: {producer.rate} msg/s")
        
        # Check stats
        stats = producer.get_stats()
        logger.info(f"✓ Stats: {stats}")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Factory creation failed: {e}", exc_info=True)
        return False


def test_multiple_creation():
    """Test 2: Can we create multiple producers?"""
    logger.info("\n" + "=" * 50)
    logger.info("TEST 2: Multiple Producer Creation")
    logger.info("=" * 50)
    
    try:
        kafka_config = KAFKA_CONFIG
        producer_configs = get_producer_configs(enabled_only=True)
        
        # Create all enabled producers
        producers = ProducerFactory.create_multiple(
            producer_configs,
            kafka_config,
            CHECKPOINT_DIR
        )
        
        logger.info(f"✓ Created {len(producers)} producers")
        for name, producer in producers.items():
            logger.info(f"  - {name}: {producer.topic} @ {producer.rate} msg/s")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Multiple creation failed: {e}", exc_info=True)
        return False


def test_producer_validation():
    """Test 3: Does validation work correctly?"""
    logger.info("\n" + "=" * 50)
    logger.info("TEST 3: Producer Validation")
    logger.info("=" * 50)
    
    try:
        kafka_config = KAFKA_CONFIG
        
        # Test invalid producer name
        try:
            ProducerFactory.create_producer(
                name='nonexistent',
                config={'file_path': '/tmp/test.csv', 'topic': 'test'},
                kafka_config=kafka_config
            )
            logger.error("✗ Should have failed with invalid producer name")
            return False
        except ValueError as e:
            logger.info(f"✓ Correctly rejected invalid producer: {e}")
        
        # Test missing config field
        try:
            ProducerFactory.create_producer(
                name='chartevents',
                config={'file_path': '/tmp/test.csv'},  # Missing 'topic'
                kafka_config=kafka_config
            )
            logger.error("✗ Should have failed with missing config field")
            return False
        except ValueError as e:
            logger.info(f"✓ Correctly rejected incomplete config: {e}")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Validation test failed: {e}", exc_info=True)
        return False


def test_checkpoint_operations():
    """Test 4: Do checkpoints work?"""
    logger.info("\n" + "=" * 50)
    logger.info("TEST 4: Checkpoint Operations")
    logger.info("=" * 50)
    
    try:
        kafka_config = KAFKA_CONFIG
        config = get_single_producer_config('chartevents')
        
        producer = ProducerFactory.create_producer(
            'chartevents',
            config,
            kafka_config,
            CHECKPOINT_DIR
        )
        
        # Check initial checkpoint
        logger.info(f"✓ Initial checkpoint: row {producer.checkpoint_state.last_row_index}")
        
        # Simulate some progress
        producer.checkpoint_state.last_row_index = 1000
        producer.checkpoint_state.total_messages_sent = 500
        producer._save_checkpoint()
        logger.info("✓ Saved checkpoint at row 1000")
        
        # Create new producer instance - should load checkpoint
        producer2 = ProducerFactory.create_producer(
            'chartevents',
            config,
            kafka_config,
            CHECKPOINT_DIR
        )
        
        if producer2.checkpoint_state.last_row_index == 1000:
            logger.info(f"✓ Checkpoint loaded correctly: row {producer2.checkpoint_state.last_row_index}")
        else:
            logger.error(f"✗ Checkpoint not loaded: expected 1000, got {producer2.checkpoint_state.last_row_index}")
            return False
        
        # Reset checkpoint
        producer2.reset_checkpoint()
        logger.info("✓ Checkpoint reset successful")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Checkpoint test failed: {e}", exc_info=True)
        return False


def test_registry_operations():
    """Test 5: Does producer registry work?"""
    logger.info("\n" + "=" * 50)
    logger.info("TEST 5: Registry Operations")
    logger.info("=" * 50)
    
    try:
        # List available producers
        available = ProducerFactory.list_producers()
        logger.info(f"✓ Available producers: {available}")
        
        # Test custom producer registration (if needed in future)
        logger.info("✓ Registry operations working")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Registry test failed: {e}", exc_info=True)
        return False


def main():
    """Run all tests"""
    logger.info("\n")
    logger.info("#" * 60)
    logger.info("# MIMIC-IV Producer Infrastructure Tests")
    logger.info("#" * 60)
    
    tests = [
        ("Factory Creation", test_factory_creation),
        ("Multiple Producer Creation", test_multiple_creation),
        ("Producer Validation", test_producer_validation),
        ("Checkpoint Operations", test_checkpoint_operations),
        ("Registry Operations", test_registry_operations),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            logger.error(f"Test '{test_name}' crashed: {e}")
            results.append((test_name, False))
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("TEST SUMMARY")
    logger.info("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        logger.info(f"{status}: {test_name}")
    
    logger.info("=" * 60)
    logger.info(f"Results: {passed}/{total} tests passed")
    logger.info("=" * 60)
    
    return passed == total


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)