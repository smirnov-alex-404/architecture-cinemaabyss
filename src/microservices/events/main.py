import os

from fastapi import APIRouter, FastAPI, HTTPException, status
from fastapi.responses import JSONResponse

from kafka_helper import KafkaConsumerManager, KafkaProducerManager


KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'localhost:9092')
MOVIE_EVENTS_TOPIC = 'movie-events'
USER_EVENTS_TOPIC = 'user-events'
PAYMENT_EVENTS_TOPIC = 'payment-events'

producer = KafkaProducerManager(KAFKA_BROKERS)
consumer = KafkaConsumerManager(
    KAFKA_BROKERS,
    group_id='group-id',
)


app = FastAPI()
router = APIRouter(prefix='/api/events')


@router.get('/health')
async def health():
    return {'status': True}


@router.post('/movie')
async def create_movie_event(data: dict):
    try:
        msg = {
            'type': 'movie_event',
            'data': data,
        }
        await producer.send(MOVIE_EVENTS_TOPIC, msg)
        _ = await consumer.consume(MOVIE_EVENTS_TOPIC)
        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content={
                'status': 'success',
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post('/user')
async def create_user_event(data: dict):
    try:
        msg = {
            'type': 'user_event',
            'data': data,
        }
        await producer.send(USER_EVENTS_TOPIC, msg)
        _ = await consumer.consume(USER_EVENTS_TOPIC)
        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content={
                'status': 'success',
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post('/payment')
async def create_payment_event(data: dict):
    try:
        msg = {
            'type': 'payment_event',
            'data': data,
        }
        await producer.send(PAYMENT_EVENTS_TOPIC, msg)
        _ = await consumer.consume(PAYMENT_EVENTS_TOPIC)
        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content=
            {
                'status': 'success',
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


app.include_router(router)


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(
        app,
        host='0.0.0.0',
        port=8082
    )