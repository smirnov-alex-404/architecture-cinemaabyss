import asyncio
import json
import logging

from kafka import KafkaProducer, KafkaConsumer

logger = logging.getLogger(__name__)


class KafkaProducerManager:
    def __init__(self, bootstrap_servers: str):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )

    async def send(self, topic: str, message: dict) -> None:
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(
                None,
                lambda: self.producer.send(topic, value=message).get(timeout=10)
            )
            logger.info(f'Produced message to {topic}: {message}')
        except Exception as e:
            logger.error(f'Failed to produce message: {str(e)}')
            raise

    async def close(self) -> None:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.producer.close)

class KafkaConsumerManager:
    def __init__(self, bootstrap_servers: str, group_id: str):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id

    async def consume(self, topic: str, timeout_ms: int = 5000) -> dict:
        loop = asyncio.get_event_loop()
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=None,
                auto_offset_reset='latest',
                enable_auto_commit=False,
                consumer_timeout_ms=timeout_ms,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            message = await loop.run_in_executor(
                None,
                lambda: next(iter(consumer), None)
            )
            consumer.close()

            if message:
                logger.info(f'Consumed message from {topic}: {message.value}')
                return message.value
            return None
        except Exception as e:
            logger.error(f'Failed to consume message: {str(e)}')
            raise
