import os,json,uuid
from fastapi import FastAPI,Header,HTTPException,Depends
from pydantic import BaseModel,Field
from queue_app.infrastructure import submit,get,initialize,conn,broker
app=FastAPI(title='Distributed Job Queue')
class Request(BaseModel):
    kind:str=Field(pattern='^(sum|fibonacci|fail_once)$')
    payload:dict
    idempotency_key:str=Field(min_length=8,max_length=160)
def key(x_api_key:str|None=Header(None)):
    if x_api_key!=os.getenv('QUEUE_API_KEY','local-demo-key'):raise HTTPException(401,'API key required')
@app.on_event('startup')
def startup():initialize()
@app.get('/health')
def health():return {'status':'ok'}
@app.post('/jobs',dependencies=[Depends(key)])
def create(data:Request):return submit(data.kind,data.payload,data.idempotency_key)
@app.get('/jobs/{id}',dependencies=[Depends(key)])
def status(id:str):
    try:uuid.UUID(id)
    except ValueError:raise HTTPException(400,'Invalid UUID')
    row=get(id)
    if not row:raise HTTPException(404,'Not found')
    return {k:(str(v) if k=='id' else v) for k,v in row.items()}
@app.get('/stats',dependencies=[Depends(key)])
def stats():
    with conn() as db:counts=db.execute('SELECT status,count(*) total FROM jobs GROUP BY status').fetchall()
    return {'statuses':counts,'dead_stream_length':broker().xlen('jobs:dead')}
