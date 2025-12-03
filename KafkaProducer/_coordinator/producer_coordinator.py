#!/usr/bin/env python3
"""
Minimal ProducerCoordinator: Orchestrates multiple Kafka producers.

Bare-bones implementation for demonstrating multi-producer streaming.
"""
import multiprocessing as mp
import multiprocessing.synchronize
import signal
import time
import logging
from typing import Dict, List

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ProducerCoordinator:
    """
    Minimal coordinator for running multiple producers in parallel.
    
    Features:
    - Spawn each producer in separate process
    - Monitor if processes are alive
    - Graceful shutdown on Ctrl+C
    """
    
    def __init__(self, producer_configs: Dict[str, Dict], kafka_config: Dict):
        """
        Args:
            producer_configs: Dict of producer and their configurations
            kafka_config: Kafka broker configuration
        """
        self.producer_configs = producer_configs
        self.kafka_config = kafka_config
        
        # Process tracking
        self.processes: Dict[str, mp.Process] = {}
        
        # Shutdown coordination
        self.shutdown_event = mp.Event()
        self._setup_signal_handlers()
        
        logger.info(f"Coordinator initialized with {len(self.producer_configs)} producers")
    
    def _setup_signal_handlers(self):
        """Setup handlers for graceful shutdown (Ctrl+C)"""
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.shutdown()
    
    def start(self):
        """Start all configured producers"""
        logger.info("Starting all producers...")
        
        for name, config in self.producer_configs.items():
            self._start_producer(name, config)
        
        logger.info(f"All producers started. Running {len(self.processes)} processes.")
        
        # Wait for all processes to finish
        self._wait_for_completion()
    
    def _start_producer(self, name: str, config: Dict):
        """Start a single producer in a separate process"""
        process = mp.Process(
            target=self._producer_worker,
            args=(name, config, self.kafka_config, self.shutdown_event),
            name=f"Producer-{name}"
        )
        process.start()
        
        self.processes[name] = process
        logger.info(f"Started producer '{name}' (PID: {process.pid})")
    
    @staticmethod
    def _producer_worker(name: str, config: Dict, kafka_config: Dict, shutdown_event: multiprocessing.synchronize.Event):
        """
        Worker function that runs in a separate process.
        Creates and runs the actual producer.
        """
        try:
            # Import here to avoid issues with multiprocessing
            from KafkaProducer.producers.producer_factory import ProducerFactory
            
            logger.info(f"[{name}] Worker started")
            
            # Create producer using factory
            producer = ProducerFactory.create_producer(
                name=name,
                config=config,
                kafka_config=kafka_config
            )
            
            # Stream data (will run until file exhausted or shutdown)
            producer.stream(
                control_flags=None,  # Not using pause/stop flags in minimal version
                shutdown_event=shutdown_event,
                metrics_dict=None    # Not tracking metrics in minimal version
            )
            
            logger.info(f"[{name}] Worker completed")
            
        except KeyboardInterrupt:
            logger.info(f"[{name}] Worker interrupted")
        except Exception as e:
            logger.error(f"[{name}] Worker failed: {e}", exc_info=True)
            raise e
    
    def _wait_for_completion(self):
        """Wait for all producer processes to complete"""
        try:
            # Check process status every 5 seconds
            while not self.shutdown_event.is_set():
                time.sleep(5)
                
                # Check if any process died
                alive_count = sum(1 for p in self.processes.values() if p.is_alive())
                
                if alive_count == 0:
                    logger.info("All producers finished")
                    break
                
                logger.info(f"Status: {alive_count}/{len(self.processes)} producers running")
        
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Gracefully shutdown all producers"""
        if self.shutdown_event.is_set():
            return  # Already shutting down
        
        logger.info("Shutting down coordinator...")
        self.shutdown_event.set()
        
        # Wait for processes to finish (with timeout)
        for name, process in self.processes.items():
            logger.info(f"Waiting for producer '{name}' to finish...")
            process.join(timeout=10)
            
            if process.is_alive():
                logger.warning(f"Force terminating producer '{name}'")
                process.terminate()
                process.join(timeout=5)
        
        logger.info("Coordinator shutdown complete")