"""Atomic admission, independent synthetic commercial facts, and source authority."""
import hashlib
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4
from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = json.loads((ROOT / 'contracts/event-v2.json').read_text())
VALIDATOR = Draft202012Validator(SCHEMA, format_checker=FormatChecker())
DL_SCHEMA = json.loads((ROOT / 'contracts/data-layer-v1.json').read_text())
DL_VALIDATOR = Draft202012Validator(DL_SCHEMA, format_checker=FormatChecker())
SERVICES = ('landscape_construction','custom_pool','outdoor_kitchen','hardscaping','premium_renovation')
STAGES = {'inquiry':'generate_lead','qualified':'qualify_lead','working':'working_lead',
          'disqualified':'disqualify_lead','signed_contract':'close_convert_lead','payment':'purchase'}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def stamp():
    return datetime.now(timezone.utc).isoformat()


def instant(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


class Ledger:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            existing = db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            if existing and not db.execute("SELECT name FROM sqlite_master WHERE name='ledger_version'").fetchone():
                raise ValueError('legacy workspace: use a new v2 workspace; existing data remains untouched')
            db.executescript('''
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS ledger_version(version INTEGER PRIMARY KEY CHECK(version=2));
            INSERT OR IGNORE INTO ledger_version VALUES(2);
            CREATE TABLE IF NOT EXISTS consent(session_id TEXT PRIMARY KEY, active INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS projects(project_id TEXT PRIMARY KEY, project_type TEXT NOT NULL,
              opportunity_cents INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS source_facts(source_ref TEXT PRIMARY KEY, project_id TEXT NOT NULL,
              stage TEXT NOT NULL, value_cents INTEGER NOT NULL, currency TEXT NOT NULL,
              occurred_at TEXT NOT NULL, session_id TEXT, measurement_eligible INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS events(event_id TEXT PRIMARY KEY, payload TEXT NOT NULL,
              hash TEXT NOT NULL, received_at TEXT NOT NULL, late INTEGER NOT NULL,
              transaction_id TEXT UNIQUE, conversion_key TEXT UNIQUE NOT NULL);
            CREATE TABLE IF NOT EXISTS decisions(receipt INTEGER PRIMARY KEY, received_at TEXT NOT NULL,
              status TEXT NOT NULL, event_id TEXT, detail TEXT NOT NULL);
            ''')

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=15)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def grant(self):
        session = str(uuid4())
        with self.connect() as db:
            db.execute('INSERT INTO consent VALUES (?, 1)', (session,))
        return session

    def revoke(self, session):
        with self.connect() as db:
            db.execute('UPDATE consent SET active=0 WHERE session_id=?', (session,))

    def inquiry(self, request_id, project_type, session_id=None):
        from uuid import UUID
        request_id=str(UUID(request_id))
        if project_type not in SERVICES:
            raise ValueError('invalid inquiry service')
        guidance = dict(zip(SERVICES,(18_500_000,24_500_000,12_500_000,6_500_000,48_500_000)))
        pid, ref = 'SYN-LIVE-'+request_id, 'SYN-INQ-'+request_id
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            existing = db.execute('SELECT * FROM source_facts WHERE source_ref=?',(ref,)).fetchone()
            if existing:
                kind=db.execute('SELECT project_type FROM projects WHERE project_id=?',(pid,)).fetchone()[0]
                if kind != project_type or existing['session_id'] != session_id:
                    raise ValueError('inquiry identity conflict')
                return dict(existing)
            active=bool(session_id and db.execute('SELECT 1 FROM consent WHERE session_id=? AND active=1',(session_id,)).fetchone())
            # Do not persist an unverified session assertion on the business source.
            if session_id and not active:
                raise ValueError('inquiry permission expired')
            db.execute('INSERT INTO projects VALUES(?,?,?)',(pid,project_type,guidance[project_type]))
            db.execute('INSERT INTO source_facts VALUES(?,?,?,?,?,?,?,?)',(ref,pid,'inquiry',0,'USD',stamp(),session_id,int(active)))
            return dict(db.execute('SELECT * FROM source_facts WHERE source_ref=?',(ref,)).fetchone())

    def project(self, project_id, project_type, opportunity_cents):
        if (not isinstance(project_id,str) or not project_id.startswith('SYN-') or project_type not in SERVICES
                or type(opportunity_cents) is not int or not 5_000_000 <= opportunity_cents <= 50_000_000):
            raise ValueError('invalid synthetic project')
        with self.connect() as db:
            db.execute('INSERT INTO projects VALUES(?,?,?)', (project_id,project_type,opportunity_cents))

    def source(self, source_ref, project_id, stage, value_cents=0, occurred_at=None, session_id=None):
        """Trusted Python fixture/adapter boundary; never exposed as a commercial HTTP write."""
        if (not isinstance(source_ref,str) or not source_ref.startswith('SYN-') or stage not in (*STAGES,'proposal')
                or type(value_cents) is not int or value_cents < 0):
            raise ValueError('invalid source fact')
        if stage in ('signed_contract','proposal') and not 5_000_000 <= value_cents <= 50_000_000:
            raise ValueError('project value outside synthetic policy')
        if stage not in ('signed_contract','proposal','payment') and value_cents != 0:
            raise ValueError('lead stages must be unvalued')
        when = occurred_at or stamp()
        instant(when)
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if not db.execute('SELECT 1 FROM projects WHERE project_id=?',(project_id,)).fetchone():
                raise ValueError('unknown source project')
            if stage in ('signed_contract','proposal') and db.execute(
                    'SELECT 1 FROM source_facts WHERE project_id=? AND stage=?',(project_id,stage)).fetchone():
                raise ValueError('one immutable proposal/contract per project in this bounded scenario')
            if stage == 'payment':
                contract = db.execute("SELECT value_cents FROM source_facts WHERE project_id=? AND stage='signed_contract'",(project_id,)).fetchone()
                paid = db.execute("SELECT coalesce(sum(value_cents),0) FROM source_facts WHERE project_id=? AND stage='payment'",(project_id,)).fetchone()[0]
                if not contract or value_cents <= 0 or paid + value_cents > contract[0]:
                    raise ValueError('payment requires a signed contract and cannot exceed remaining commitment')
            eligible = bool(session_id and db.execute('SELECT 1 FROM consent WHERE session_id=? AND active=1',(session_id,)).fetchone())
            db.execute('INSERT INTO source_facts VALUES(?,?,?,?,?,?,?,?)',
                       (source_ref,project_id,stage,value_cents,'USD',when,session_id,int(eligible)))

    def collect_offline(self,event,received_at=None,crash=False):
        return self.collect(event,received_at,crash,trusted_source='offline')

    def collect(self,event,received_at=None,crash=False,*,trusted_source='browser'):
        received_at = received_at or stamp()
        reason = 'contract_rejected' if not VALIDATOR.is_valid(event) else None
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            event_id = None
            if reason is None:
                consent = db.execute('SELECT active FROM consent WHERE session_id=?',(event['session_id'],)).fetchone()
                if not consent or not consent[0]:
                    reason = 'consent_denied'
            if reason is None:
                fact = db.execute('SELECT * FROM source_facts WHERE source_ref=?',(event['source_ref'],)).fetchone()
                allowed = (trusted_source == event['origin'] and
                           ((trusted_source == 'browser' and event['event_name'] == 'generate_lead') or
                            (trusted_source == 'offline' and event['event_name'] != 'generate_lead')))
                if (not allowed or not fact or STAGES.get(fact['stage']) != event['event_name'] or
                        fact['project_id'] != event['project_id'] or fact['session_id'] != event['session_id'] or
                        not fact['measurement_eligible'] or instant(fact['occurred_at']) != instant(event['occurred_at'])):
                    reason = 'authority_denied'
            if reason is None:
                age = (instant(received_at)-instant(event['occurred_at'])).total_seconds()
                if age < -300 or age > 7*86400:
                    reason = 'time_rejected'
                project = db.execute('SELECT project_type FROM projects WHERE project_id=?',(event['project_id'],)).fetchone()
                if reason is None and (event['project_type'] != project[0] or any(i['item_id'] != project[0] for i in event.get('items',[]))):
                    reason = 'service_rejected'
                if reason is None and event['event_name']=='purchase' and event['value_cents'] != sum(i['price_cents']*i['quantity'] for i in event['items']):
                    reason = 'value_rejected'
                if reason is None and event['event_name']=='purchase' and event['transaction_id'] != event['source_ref']:
                    reason = 'authority_denied'
            if reason is None:
                event_id = event['event_id']
                old = db.execute('SELECT hash FROM events WHERE event_id=?',(event_id,)).fetchone()
                key = event['event_name']+'|'+event['source_ref']
                previous = db.execute('SELECT payload FROM events WHERE conversion_key=?',(key,)).fetchone()
                if old:
                    reason = 'duplicate_event' if old[0]==digest(event) else 'identity_conflict'
                elif previous:
                    original = json.loads(previous[0])
                    fields = ('project_id','value_cents','currency','value_basis','items')
                    same = all(original.get(k)==event.get(k) for k in fields)
                    reason = ('duplicate_transaction' if same else 'transaction_conflict') if event['event_name']=='purchase' else ('duplicate_conversion' if same else 'conversion_conflict')
                    event_id = original['event_id']
                else:
                    db.execute('INSERT INTO events VALUES(?,?,?,?,?,?,?)',
                               (event_id,canonical(event),digest(event),received_at,int(age>86400),event.get('transaction_id'),key))
                    reason = 'accepted_late' if age>86400 else 'accepted'
            if crash:
                raise RuntimeError('injected failure before commit')
            cursor = db.execute('INSERT INTO decisions(received_at,status,event_id,detail) VALUES(?,?,?,?)',
                                (received_at,reason,event_id,'payload_not_retained' if event_id is None else 'see_admitted_event'))
            return {'receipt':cursor.lastrowid,'status':reason}

    def snapshot(self):
        with self.connect() as db:
            db.execute('BEGIN')
            events = [dict(r) for r in db.execute('SELECT * FROM events ORDER BY event_id')]
            for e in events:
                e['payload'] = json.loads(e['payload'])
            return {'events':events,'projects':[dict(r) for r in db.execute('SELECT * FROM projects ORDER BY project_id')],
                    'source_facts':[dict(r) for r in db.execute('SELECT * FROM source_facts ORDER BY source_ref')],
                    'decisions':[dict(r) for r in db.execute('SELECT * FROM decisions ORDER BY receipt')]}


def ga4_params(event):
    VALIDATOR.validate(event)
    params = {'lead_source':event['campaign'],'project_type':event['project_type'],'value_basis':event['value_basis']}
    if event['value_basis'] != 'unvalued':
        params.update(currency='USD',value=float(Decimal(event['value_cents'])/100))
    if event['event_name']=='purchase':
        params['transaction_id'] = event['transaction_id']
        params['items'] = [{'item_id':i['item_id'],'price':float(Decimal(i['price_cents'])/100),'quantity':i['quantity']} for i in event['items']]
    return params


def data_layer(event):
    if event['origin'] != 'browser':
        raise ValueError('offline conversions never enter the browser data layer')
    result = {'event':event['event_name'],'event_params':ga4_params(event),
              'measurement':{'data_layer_version':1,'event_id':event['event_id']}}
    DL_VALIDATOR.validate(result)
    return result


def mp_payload(event):
    return {'client_id':'123456789.1234567890','validation_behavior':'ENFORCE_RECOMMENDATIONS',
            'events':[{'name':event['event_name'],'params':ga4_params(event)}]}
