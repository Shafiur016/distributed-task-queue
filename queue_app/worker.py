import os,time,json,socket,uuid
from queue_app.core import execute,backoff,disposition
from queue_app.infrastructure import initialize,broker,conn,STREAM,GROUP

def process(job_id,msg_id):
    r=broker()
    with conn() as db:
        row=db.execute("""UPDATE jobs SET attempts=attempts+1,status='running',updated_at=now()
          WHERE id=%s AND status NOT IN ('done','dead') RETURNING kind,payload,attempts""",(job_id,)).fetchone()
        if not row:r.xack(STREAM,GROUP,msg_id);return
        attempt=row['attempts']
    payload=dict(row['payload']);payload['attempt']=attempt
    try:
        result=execute(row['kind'],payload)
        with conn() as db:db.execute("UPDATE jobs SET status='done',result=%s::jsonb,error=NULL,updated_at=now() WHERE id=%s",(json.dumps(result),job_id))
        r.xack(STREAM,GROUP,msg_id)
    except Exception as e:
        state=disposition(attempt)
        with conn() as db:db.execute('UPDATE jobs SET status=%s,error=%s,updated_at=now() WHERE id=%s',(state,str(e)[:300],job_id))
        if state=='dead':r.xadd('jobs:dead',{'id':job_id,'reason':str(e)[:300]})
        else:r.zadd('jobs:delayed',{job_id:time.time()+backoff(attempt)})
        r.xack(STREAM,GROUP,msg_id)

def dispatch_due(r):
    due=r.zrangebyscore('jobs:delayed',0,time.time(),start=0,num=50)
    for job_id in due:
        if r.zrem('jobs:delayed',job_id):r.xadd(STREAM,{'id':job_id})

def run():
    initialize();r=broker();worker=f'{socket.gethostname()}-{uuid.uuid4().hex[:6]}'
    while True:
        dispatch_due(r)
        # Claim tasks from crashed workers after 30 seconds. Running tasks must be idempotent.
        claimed=r.xautoclaim(STREAM,GROUP,worker,min_idle_time=30000,start_id='0-0',count=10)
        stale=claimed[1]  # redis-py returns (next_start_id, messages[, deleted_ids])
        messages=[(STREAM,stale)] if stale else r.xreadgroup(GROUP,worker,{STREAM:'>'},count=10,block=1000)
        for _,items in messages:
            for mid,fields in items:process(fields['id'],mid)

if __name__=='__main__':run()
