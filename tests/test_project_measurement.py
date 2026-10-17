import json
import sqlite3
from copy import deepcopy
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from assurance.analytics import commercial_summary, oracle
from assurance.core import Ledger, DL_VALIDATOR, data_layer, mp_payload
from assurance.fixtures import CLOCK, SESSION, admit, event, seed, setup_sources
from assurance.server import create_app


@pytest.fixture
def ledger(tmp_path):
    db=Ledger(tmp_path/'test.sqlite')
    setup_sources(db)
    return db


@pytest.mark.parametrize('name',['qualify_lead','working_lead','close_convert_lead','purchase'])
def test_browser_cannot_claim_offline_authority(ledger,name):
    assert ledger.collect(event(name=name),CLOCK)['status']=='authority_denied'
    assert not ledger.snapshot()['events']


@pytest.mark.parametrize('name',['generate_lead','qualify_lead','working_lead','disqualify_lead','close_convert_lead','purchase'])
def test_event_mapping_and_value_basis(ledger,name):
    project='SYN-P05' if name=='disqualify_lead' else 'SYN-P01'
    e=event(name=name,project=project)
    assert admit(ledger,e)['status']=='accepted'
    p=mp_payload(e)['events'][0]
    assert p['name']==name
    if name in ('purchase','close_convert_lead'):
        assert p['params']['currency']=='USD' and p['params']['value']>0
    else:
        assert 'value' not in p['params'] and 'currency' not in p['params']
    if name=='purchase':
        assert p['params']['value']==19500
    if name=='close_convert_lead':
        assert p['params']['value']==65000 and 'items' not in p['params']


def test_proposal_is_not_contract(ledger):
    e=event(name='close_convert_lead',project='SYN-P04',ref='SYN-P04-proposal')
    assert ledger.collect_offline(e,CLOCK)['status']=='authority_denied'
    assert commercial_summary(ledger.snapshot())['signed_contract_cents']==43500000


def test_new_id_cannot_duplicate_contract_value(ledger):
    e=event(name='close_convert_lead')
    assert admit(ledger,e)['status']=='accepted'
    again=deepcopy(e);again['event_id']=str(uuid4())
    assert admit(ledger,again)['status']=='duplicate_conversion'
    again['value_cents']=12500000
    assert admit(ledger,again)['status']=='conversion_conflict'
    assert commercial_summary(ledger.snapshot())['measured_contract_cents']==6500000


def test_valid_payment_units_can_still_disagree_with_source(ledger):
    e=event(project='SYN-P03',value=24500000)
    assert admit(ledger,e)['status']=='accepted'
    r=next(r for r in oracle(ledger.snapshot()) if r['source_ref']=='SYN-P03-payment')
    assert (r['expected_cents'],r['measured_cents'],r['status'])==(7350000,24500000,'value_mismatch')


def test_multiple_actual_payments_do_not_repeat_contract(ledger):
    admit(ledger,event(name='close_convert_lead'))
    admit(ledger,event(2))
    admit(ledger,event(3,ref='SYN-P01-progress',value=1300000))
    s=commercial_summary(ledger.snapshot())
    assert s['measured_contract_cents']==6500000
    assert s['measured_payment_cents']==3250000


@pytest.mark.parametrize('change',[{'source_ref':'SYN-missing'},{'project_id':'SYN-P02'},
                                 {'occurred_at':'2026-01-14T12:00:00Z'}])
def test_offline_must_match_registered_source(ledger,change):
    e=event();e.update(change)
    assert ledger.collect_offline(e,CLOCK)['status']=='authority_denied'


@pytest.mark.parametrize('stage,value',[('payment',1),('signed_contract',1),('proposal',50000001),('inquiry',100)])
def test_invalid_source_economics(ledger,stage,value):
    with pytest.raises(ValueError):
        ledger.source('SYN-invalid','SYN-P04',stage,value,session_id=SESSION)


def test_append_only_sources_and_payment_cap(ledger):
    with pytest.raises(sqlite3.IntegrityError):
        ledger.source('SYN-P01-payment','SYN-P01','payment',1,session_id=SESSION)
    with pytest.raises(ValueError):
        ledger.source('SYN-excess','SYN-P01','payment',6500000,session_id=SESSION)
    with pytest.raises(ValueError):
        ledger.source('SYN-second-contract','SYN-P01','signed_contract',6500000,session_id=SESSION)


def test_inquiry_source_idempotency_and_denied_measurement(tmp_path):
    db=Ledger(tmp_path/'inquiry.sqlite')
    key=str(uuid4())
    first=db.inquiry(key,'custom_pool')
    assert db.inquiry(key,'custom_pool')==first
    assert len(db.snapshot()['projects'])==len(db.snapshot()['source_facts'])==1
    assert oracle(db.snapshot())[0]['status']=='not_eligible'
    with pytest.raises(ValueError): db.inquiry(key,'hardscaping')


def test_gtm_message_is_separate_from_private_collection(ledger):
    e=event(name='generate_lead')
    result=data_layer(e)
    assert DL_VALIDATOR.is_valid(result)
    assert result['event']=='generate_lead'
    assert result['event_params']==mp_payload(e)['events'][0]['params']
    assert 'session_id' not in json.dumps(result) and 'source_ref' not in json.dumps(result)
    bad=deepcopy(result);bad['event_name']=bad.pop('event')
    assert not DL_VALIDATOR.is_valid(bad)
    with pytest.raises(ValueError): data_layer(event())


def test_legacy_workspace_refused_without_modification(tmp_path):
    path=tmp_path/'old.sqlite'
    with sqlite3.connect(path) as db: db.execute('CREATE TABLE orders(id TEXT)')
    original=path.read_bytes()
    with pytest.raises(ValueError,match='legacy workspace'): Ledger(path)
    assert path.read_bytes()==original


def test_api_offline_claim_is_rejected(tmp_path):
    path=tmp_path/'http.sqlite';db=Ledger(path);setup_sources(db)
    client=TestClient(create_app(path))
    headers={'content-type':'application/json','x-assurance-demo':'synthetic'}
    r=client.post('/api/events',json=event(),headers=headers)
    assert r.json()['status']=='authority_denied'
    assert client.post('/api/source',json={},headers=headers).status_code==404
    assert client.post('/api/payment',json={},headers=headers).status_code==404
    assert client.post('/api/contract',json={},headers=headers).status_code==404


def test_revocation_blocks_offline_replay_without_erasing_history(ledger):
    e=event();admit(ledger,e)
    ledger.revoke(SESSION)
    assert admit(ledger,e)['status']=='consent_denied'
    assert len(ledger.snapshot()['events'])==1


@pytest.mark.parametrize('name',['generate_lead','qualify_lead','working_lead','disqualify_lead'])
def test_lead_stages_cannot_invent_monetary_value(ledger,name):
    e=event(name=name,value=6500000,project='SYN-P05' if name=='disqualify_lead' else 'SYN-P01')
    assert admit(ledger,e)['status']=='contract_rejected'


def test_payment_cannot_claim_contract_value_basis(ledger):
    e=event();e['value_basis']='contract'
    assert admit(ledger,e)['status']=='contract_rejected'


def test_project_oracle_detects_wrong_cash_without_count_change(ledger):
    from assurance.analytics import validate_models
    snapshot=ledger.snapshot()
    bad=dict(snapshot['projects'][0],proposal_cents=6500000,signed_contract_cents=6500000,
             payment_cents=6500000,measured_contract_cents=0,measured_payment_cents=0)
    with pytest.raises(RuntimeError,match='project values'):
        validate_models({'mart_reconciliation':oracle(snapshot),'mart_project_values':[bad]},snapshot)


def test_rollup_cannot_combine_payment_and_contract(ledger):
    from assurance.analytics import validate_models
    admit(ledger,event())
    snapshot=ledger.snapshot()
    with pytest.raises(RuntimeError,match='value-basis'):
        validate_models({'mart_reconciliation':oracle(snapshot),
                         'mart_channels':[{'value_basis':'contract','conversions':1,'measured_cents':1950000}]},snapshot)
