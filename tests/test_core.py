import pytest
from queue_app.core import execute,backoff,disposition

def test_execution_and_validation():
    assert execute('sum',{'values':[2,3]})=={'sum':5}
    assert execute('fibonacci',{'n':10})=={'value':'55'}
    with pytest.raises(ValueError):execute('sum',{'values':['oops']})

def test_retry_dead_letter_decision():
    assert [disposition(n) for n in range(1,5)]==['retry','retry','dead','dead']
    assert backoff(1)==1 and backoff(3)==4

def test_transient_failure():
    with pytest.raises(RuntimeError):execute('fail_once',{'attempt':1})
    assert execute('fail_once',{'attempt':2})['recovered']
