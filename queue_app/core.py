"""Execution semantics and bounded task types independent of infrastructure."""
import math,time
MAX_ATTEMPTS=3

def backoff(attempt:int):return min(2**max(0,attempt-1),60)

def disposition(attempt:int):return 'dead' if attempt>=MAX_ATTEMPTS else 'retry'

def execute(kind,payload):
    if kind=='sum':
        values=payload.get('values')
        if not isinstance(values,list) or len(values)>1000 or any(type(x) not in (int,float) or not math.isfinite(x) for x in values):raise ValueError('values must be <=1000 finite numbers')
        return {'sum':sum(values)}
    if kind=='fibonacci':
        n=payload.get('n')
        if type(n)!=int or n<0 or n>1000:raise ValueError('n must be integer 0..1000')
        a,b=0,1
        for _ in range(n):a,b=b,a+b
        return {'value':str(a)}
    if kind=='fail_once':
        # Never add general shell command execution to a job queue.
        if payload.get('attempt',1)<2:raise RuntimeError('Demonstrated transient failure')
        return {'recovered':True}
    raise ValueError('Unsupported kind')
