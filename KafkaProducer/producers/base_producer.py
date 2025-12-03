import json
import time
from datetime import datetime
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Callable
from pathlib import Path
from dataclasses import dataclass, asdict
import pandas as pd
from confluent_kafka import Producer, KafkaException

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class Checkpoint:
    last_row_index: int = 0
    total_messages_sent: int = 0
    total_messages_failed: int = 0
    checkpoint_time: str = None
    
    def __post_init__(self):
        if not self.checkpoint_time:
            self.checkpoint_time = datetime.now().isoformat()
    
    def to_dict(self) -> Dict:
        if isinstance(self.checkpoint_time, datetime):
            self.checkpoint_time = self.checkpoint_time.isoformat()
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Checkpoint':
        return cls(**data)
        


class BaseProducer(ABC):
    """
    Abstract base producer class for streaming MIMIC-IV data to Kafka.
    
    Subclasses must implement:
    - parse_row(): Convert CSV row to message dictionary
    - get_partition_key(): Determine partition key for message
    - validate_row(): (Optional) Validate row before sending
    """
    
    def __init__(self, 
                 name: str,
                 config: Dict[str, Any],
                 kafka_config: Dict[str, Any],
                 checkpoint_dir: str = "./checkpoints"):
        """
        Args:
            name: Producer name (e.g., 'chartevents')
            producer_config: Producer-specific configuration
                {
                    'file_path': str,
                    'topic': str,
                    'rate': int (messages/sec),
                    'batch_size': int (rows to read per batch),
                    'time_multiplier': float (speed up streaming, default 1.0),
                    'partition_key': str (Optional) field to use for partitioning
                }
            kafka_config: Kafka broker configuration
            checkpoint_dir: Directory to store checkpoint files
        """
        self.name = name
        self.config = config
        self.kafka_config = kafka_config
        
        # Extract configuration
        self.file_path = config['file_path']
        self.topic = config['topic']
        self.rate = config.get('rate')
        self.batch_size = config.get('batch_size')
        self.time_multiplier = config.get('time_multiplier', 1.0)
        
        # Checkpointing
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_file = self.checkpoint_dir / f"{self.name}_checkpoint.json"
        self.checkpoint_state = self._load_checkpoint()
        
        # Kafka producer (initialized lazily)
        self._producer: Optional[Producer] = None
        
        # Metrics tracking
        self.messages_sent = self.checkpoint_state.total_messages_sent
        self.messages_failed = self.checkpoint_state.total_messages_failed
        self.bytes_sent = 0
        
        # Control flags (set by coordinator)
        self._should_stop = False
        self._is_paused = False
        
        logger.info(f"[{self.name}] Producer initialized, streaming from row {self.checkpoint_state.last_row_index}")
    
    def _load_checkpoint(self) -> Checkpoint:
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r') as file:
                    data = json.load(file)
                    checkpoint = Checkpoint.from_dict(data)
                    logger.info(f"[{self.name}] Loaded checkpoint: row {checkpoint.last_row_index}" f"sent: {checkpoint.total_messages_sent}")
                    return checkpoint
            except Exception as e:
                logger.warning(f"[{self.name}] Unable to load checkpoint: row {e}, starting fresh.")            
        return Checkpoint()
    
    def _save_checkpoint(self):
        self.checkpoint_state.checkpoint_time = datetime.now()
        try:
            with open(self.checkpoint_file, 'w') as file:
                json.dump(self.checkpoint_state.to_dict(), file, indent=2)
            logger.debug(f"[{self.name}] Checkpoint saved at row {self.checkpoint_state.last_row_index}")
        except Exception as e:
            logger.error(f'[{self.name}] Failed to save checkpoint: {e}')

    @abstractmethod
    def parse_row(self, row: pd.Series) -> Dict[str, Any]:
        """
        Parse a CSV row into a message dictionary.
        
        Must be implemented by subclass.
        
        Args:
            row: Pandas Series representing one CSV row
            
        Returns:
            Dictionary representation of the message
        """
        pass
    
    @abstractmethod
    def get_partition_key(self, message: Dict[str, Any]) -> str:
        """
        Determine the partition key for a message.
        
        For MIMIC-IV, typically use stay_id or hadm_id to ensure
        temporal consistency (events for same patient stay in order).
        
        Args:
            message: Parsed message dictionary
            
        Returns:
            Partition key as string
        """
        pass
    
    @abstractmethod
    def validate_row(self, row: pd.Series) -> bool:
        """
        Validate a row before processing.
        
        Can be overridden by subclass for custom validation.
        
        Args:
            row: Pandas Series representing one CSV row
            
        Returns:
            True if row is valid, False otherwise
        """
        # Default: check for required fields (subclass can override)
        return True
    
    def _init_producer(self):
        """Initialize Kafka producer"""
        if self._producer is not None:
            return
        
        try:
            self._producer = Producer(self.kafka_config)
            logger.info(f'[{self.name}] Kafka producer initialized')
        except KafkaException as ke:
            logger.error(f'[{self.name}] Kafka producer initialization failed: {ke}')
            raise ke
        
    def _send_message(self, message: Dict[str, Any]):
        """Send a single message to Kafka"""
        try:
            key = self.get_partition_key(message)
            value = json.dumps(message)
            self._producer.produce(
                self.topic,
                key=key.encode('utf-8') if key else None,
                value=value.encode('utf-8'),
                callback=self._delivery_callback
            )
            self._producer.poll(0)
        except BufferError:
            # local queue is full, wait and retry
            logger.warning(f"[{self.name}] Producer queue full, waiting...")
            self._producer.poll(1)
            self._send_message(message)
        except Exception as e:
            logger.error(f'[{self.name}] Error sending messages: {e}]')
            self.messages_failed += 1
            raise e
        
    def _delivery_callback(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            self.messages_failed += 1
            self.checkpoint_state.total_messages_failed += 1
            logger.error(f"[{self.name}] Message delivery failed: {err}")
        else:
            self.messages_sent += 1
            self.checkpoint_state.total_messages_sent += 1
            self.bytes_sent += len(msg.value())
            logger.debug(f"[{self.name}] Message delivered to {msg.topic()} [{msg.partition()}]")
        
    def stream(self,
               control_flags: Optional[Dict] = None,
               shutdown_event: Optional[Any] = None,
               metrics_dict: Optional[Dict] = None):
        """
        Main streaming loop - reads CSV and sends to Kafka.
        
        Args:
            control_flags: Dictionary with 'pause' and 'stop' flags (from coordinator)
            shutdown_event: Multiprocessing event for shutdown coordination
            metrics_dict: Shared dictionary for metrics reporting
        """
        self._init_producer()
        logger.info(f'[{self.name}] Starting stream from row {self.checkpoint_state.last_row_index}')
        
        chunk_iterator = pd.read_csv(
            self.file_path,
            chunksize=self.batch_size,
            skiprows = range(1, self.checkpoint_state.last_row_index + 1) if self.checkpoint_state else None
        )
        
        current_row_index = self.checkpoint_state.last_row_index
        checkpoint_interval = 1000  # save checkpoint every 1000 rows
        messages_since_checkpoint = 0
        
        try:
            for chunk in chunk_iterator:
                if shutdown_event and shutdown_event.is_set():
                    logger.info(f"[{self.name}] Shutdown signal received")
                    break
                
                if control_flags and control_flags.get('stop', False):
                    logger.info(f"[{self.name}] Stop flag set")
                    break
                
                for idx, row in chunk.iterrows():
                    while control_flags and control_flags.get('pause', False):
                        time.sleep(0.5)
                
                    # validate row 
                    if not self.validate_row(row):
                        logger.warning(f"[{self.name}] Invalid row at index {current_row_index}, skipping")
                        current_row_index += 1
                        continue
                    
                    # parse and send
                    try:
                        message = self.parse_row(row)
                        self._send_message(message)
                        
                        # rate limiting
                        time.sleep(1 / (self.rate * self.time_multiplier))
                        current_row_index += 1
                        messages_since_checkpoint += 1
                        
                        # periodic checkpoint
                        if messages_since_checkpoint == checkpoint_interval:
                            self.checkpoint_state.last_row_index = current_row_index
                            self._save_checkpoint()
                            messages_since_checkpoint = 0
                            
                        # coordinator
                        if metrics_dict:
                            metrics_dict.update(sent=1, bytes=len(json.dumps(message)))
                    except Exception as e:
                        logger.error(f"[{self.name}] Error processing row {current_row_index}: {e}, skipping")
                        current_row_index += 1
                        continue
                
                # flush after each chunk
                self._producer.flush()
                logger.info(f"[{self.name}] Processed chunk, current row: {current_row_index}, "
                           f"sent: {self.messages_sent}, failed: {self.messages_failed}")
                
            # final checkpoint
            self.checkpoint_state.last_row_index = current_row_index
            self._save_checkpoint()
            logger.info(f"[{self.name}] Streaming complete. Total sent: {self.messages_sent}, "
                       f"failed: {self.messages_failed}")
        
        except KeyboardInterrupt:
            logger.info(f'[{self.name}] Keyboard interrupted.')
        
        except Exception as e:
            logger.error(f'[{self.name}] Fatal error in streaming loop: {e}', exc_info=True)
            raise
        
        finally:
            self._cleanup()
            
    def _cleanup(self):
        """Clean up resources"""
        logger.info(f"[{self.name}] Cleaning up...")
        
        if self._producer is not None:
            # Flush any remaining messages
            remaining = self._producer.flush(timeout=30)
            if remaining > 0:
                logger.warning(f"[{self.name}] {remaining} messages not delivered")
            
        # Final checkpoint save
        self._save_checkpoint()
        
        logger.info(f"[{self.name}] Cleanup completed")
        
    def get_stats(self) -> Dict[str, Any]:
        """Get current producer statistics"""
        return {
            'name': self.name,
            'messages_sent': self.messages_sent,
            'messages_failed': self.messages_failed,
            'bytes_sent': self.bytes_sent,
            'current_row': self.checkpoint_state.last_row_index,
            'topic': self.topic,
            'status': 'initialized' if self._producer else 'uninitialized'
        }
        
    def reset_checkpoint(self):
        """Reset checkpoint to start from beginning"""
        self.checkpoint_state = Checkpoint()
        self._save_checkpoint()
        logger.info(f"[{self.name}] Checkpoint reset")