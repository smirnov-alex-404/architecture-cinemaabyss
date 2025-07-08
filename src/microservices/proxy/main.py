import os
import logging
from random import randint

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse


SERVER_PORT = int(os.getenv('PORT', '8000'))
MONOLITH_URL = os.getenv('MONOLITH_URL', 'http://monolith:8080').rstrip('/')
MOVIES_SERVICE_URL = os.getenv('MOVIES_SERVICE_URL', 'http://movies-service:8081').rstrip('/')
MOVIES_MIGRATION_PERCENT = int(os.getenv('MOVIES_MIGRATION_PERCENT', '50'))
GRADUAL_MIGRATION = os.getenv('GRADUAL_MIGRATION', 'false').lower() == 'true'

logger = logging.getLogger(__name__)

client = httpx.AsyncClient()
app = FastAPI()

@app.middleware('http')
async def route_movies_requests(request: Request, call_next):
    path = request.url.path
    method = request.method
    logger.info(f'Got method [{method}] path [{path}]')
    is_movies_request = path.startswith('/api/movies')

    target_url = f'{MONOLITH_URL}{path}'
    if is_movies_request and GRADUAL_MIGRATION:
        if randint(1, 100) <= MOVIES_MIGRATION_PERCENT:
            target_url = f'{MOVIES_SERVICE_URL}{path}'

    headers = {
        key: value
        for key, value in request.headers.items()
        if key.lower() not in ['host', 'content-length']
    }
    logger.info(f'Redirect url [{target_url}]')
    client_request = client.build_request(
        method,
        target_url,
        headers=headers,
        params=request.query_params,
        content=await request.body()
    )

    client_response = await client.send(client_request, stream=True)

    return StreamingResponse(
        client_response.aiter_bytes(),
        status_code=client_response.status_code,
        headers=dict(client_response.headers),
        media_type=client_response.headers.get('content-type')
    )


@app.get('/health')
async def health():
    return {'status': True}


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(
        app,
        host='0.0.0.0',
        port=SERVER_PORT
    )
