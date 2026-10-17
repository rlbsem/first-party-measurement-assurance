"""Exact synthetic project lifecycle; not a CRM or accounting implementation."""
from copy import deepcopy
from uuid import UUID
from .core import Ledger, STAGES

CLOCK = '2026-01-15T12:00:00+00:00'
SESSION = str(UUID(int=900))
TIMES = {'inquiry':'2025-09-01T12:00:00+00:00','qualified':'2025-09-03T12:00:00+00:00',
         'disqualified':'2025-09-03T12:00:00+00:00','working':'2025-09-06T12:00:00+00:00',
         'proposal':'2025-10-01T12:00:00+00:00','signed_contract':'2026-01-08T12:00:00+00:00','payment':CLOCK}
PROJECTS = [('SYN-P01','hardscaping',6500000),('SYN-P02','outdoor_kitchen',12500000),
            ('SYN-P03','custom_pool',24500000),('SYN-P04','premium_renovation',48500000),
            ('SYN-P05','landscape_construction',5000000)]


def source_records():
    result=[]
    for p,kind,value in PROJECTS:
        stages=['inquiry','disqualified'] if p=='SYN-P05' else ['inquiry','qualified','working','proposal']
        if p in ('SYN-P01','SYN-P02','SYN-P03'):
            stages+=['signed_contract','payment']
        for stage in stages:
            amount=value if stage in ('proposal','signed_contract') else value*3//10 if stage=='payment' else 0
            result.append({'source_ref':p+'-'+stage,'project_id':p,'stage':stage,'value_cents':amount,
                           'occurred_at':TIMES[stage],'session_id':SESSION})
    result.append({'source_ref':'SYN-P01-progress','project_id':'SYN-P01','stage':'payment',
                   'value_cents':1300000,'occurred_at':CLOCK,'session_id':SESSION})
    return result


def event(n=1,session=None,name='purchase',tx=None,ref=None,project='SYN-P01',value=None):
    kind=next(p[1] for p in PROJECTS if p[0]==project)
    stage=next(k for k,v in STAGES.items() if v==name)
    ref=ref or tx or project+'-'+stage
    amount=next(p[2] for p in PROJECTS if p[0]==project)
    basis='payment' if name=='purchase' else 'contract' if name=='close_convert_lead' else 'unvalued'
    value=(amount*3//10 if basis=='payment' else amount if basis=='contract' else 0) if value is None else value
    result={'schema_version':2,'event_id':str(UUID(int=n)),'session_id':session or SESSION,
            'origin':'browser' if name=='generate_lead' else 'offline','occurred_at':TIMES[stage],'event_name':name,
            'currency':'USD','value_cents':value,'value_basis':basis,'campaign':'design-journal',
            'project_type':kind,'project_id':project,'source_ref':ref}
    if name=='purchase':
        result.update(transaction_id=ref,items=[{'item_id':kind,'price_cents':value,'quantity':1}])
    return result


def setup_sources(ledger):
    with ledger.connect() as db:
        db.execute('INSERT INTO consent VALUES (?,1)',(SESSION,))
    for args in PROJECTS:
        ledger.project(*args)
    for record in source_records():
        ledger.source(**record)


def admit(ledger,e,time=None,**kwargs):
    return (ledger.collect if e['origin']=='browser' else ledger.collect_offline)(e,time or e['occurred_at'],**kwargs)


def seed(ledger:Ledger,late=True):
    if ledger.snapshot()['source_facts'] or ledger.snapshot()['decisions']:
        raise ValueError('fixture requires a fresh ledger')
    setup_sources(ledger)
    for n,r in enumerate(source_records(),1):
        if r['stage']=='proposal' or r['source_ref'] in ('SYN-P03-signed_contract','SYN-P01-progress','SYN-P02-payment'):
            continue
        amount=24500000 if r['source_ref']=='SYN-P03-payment' else r['value_cents']
        admit(ledger,event(n,name=STAGES[r['stage']],ref=r['source_ref'],project=r['project_id'],value=amount))
    first=event(100,name='generate_lead')
    # Existing original inquiry has a different event ID: same conversion, no second lead.
    admit(ledger,first)
    original=next(e['payload'] for e in ledger.snapshot()['events'] if e['payload']['source_ref']=='SYN-P01-payment')
    admit(ledger,original)
    duplicate=deepcopy(original);duplicate['event_id']=str(UUID(int=101));admit(ledger,duplicate)
    conflict=deepcopy(original);conflict['campaign']='direct';admit(ledger,conflict)
    bad=event(102);bad['email']='synthetic@example.invalid';admit(ledger,bad)
    bad=event(103);bad['schema_version']=3;admit(ledger,bad)
    bad=event(104);bad['value_cents']=1;admit(ledger,bad)
    bad=event(105,session=str(UUID(int=901)));admit(ledger,bad)
    # Browser cannot promote a lead or assert payment, even when it claims offline origin.
    ledger.collect(event(106),CLOCK)
    bad=event(107,name='close_convert_lead',project='SYN-P04',ref='SYN-P04-proposal')
    ledger.collect_offline(bad,CLOCK)
    if late:
        add_late(ledger)


def add_late(ledger):
    ledger.collect_offline(event(200,project='SYN-P02'),'2026-01-17T12:00:00+00:00')
