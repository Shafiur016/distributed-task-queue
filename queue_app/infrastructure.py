import os,json,time,uuid
import psycopg,redis
from psycopg.rows import dict_row
DB=os.getenv('DATABASE_URL','postgresql://worker:workerlocal@localhost:5432/jobs')
RD=os.getenv('REDIS_URL','redis://localhost:6379/0')
STREAM='jobs:main';GROUP='workers'

def conn():return psycopg.connect(DB,row_factory=dict_row)
def broker():return redis.Redis.from_url(RD,decode_responses=True)
def initialize():
    with conn() as db:
        db.execute("""CREATE TABLE IF NOT EXISTS jobs(id uuid PRIMARY KEY, idem_key text UNIQUE NOT NULL,
          kind text NOT NULL, payload jsonb NOT NULL, status text NOT NULL DEFAULT 'queued',
          attempts integer NOT NULL DEFAULT 0, result jsonb, error text,created_at timestamptz DEFAULT now(),updated_at timestamptz DEFAULT now())""")
    r=broker()
    try:r.xgroup_create(STREAM,GROUP,id='0',mkstream=True)
    except redis.exceptions.ResponseError as e:
        if 'BUSYGROUP' not in str(e):raise

def submit(kind,payload,idem_key):
    with conn() as db:
        row=db.execute('INSERT INTO jobs(id,idem_key,kind,payload) VALUES(%s,%s,%s,%s::jsonb) ON CONFLICT(idem_key) DO NOTHING RETURNING id',(uuid.uuid4(),idem_key,kind,json.dumps(payload))).fetchone()
        if row is None:return db.execute('SELECT id,status FROM jobs WHERE idem_key=%s',(idem_key,)).fetchone()
        job_id=str(row['id'])
    # Postgres insert and Redis dispatch are not atomic. Use an outbox to close this failure window in production.
    broker().xadd(STREAM,{'id':job_id})
    return {'id':job_id,'status':'queued'}

def get(job_id):
    with conn() as db:return db.execute('SELECT id,kind,payload,status,attempts,result,error FROM jobs WHERE id=%s',(job_id,)).fetchone()
