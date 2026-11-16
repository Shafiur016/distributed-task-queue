import argparse,concurrent.futures,time,statistics,uuid,httpx
p=argparse.ArgumentParser();p.add_argument('--count',type=int,default=50);a=p.parse_args()
def send(i):
    t=time.perf_counter()
    r=httpx.post('http://localhost:8000/jobs',json={'kind':'sum','payload':{'values':[1,i]},'idempotency_key':str(uuid.uuid4())},headers={'x-api-key':'local-demo-key'},timeout=10);r.raise_for_status()
    return time.perf_counter()-t
start=time.perf_counter()
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:lat=list(ex.map(send,range(a.count)))
print({'accepted_jobs':len(lat),'submit_throughput_per_sec':len(lat)/(time.perf_counter()-start),'submission_p95_ms':sorted(lat)[int(len(lat)*.95)-1]*1000})
