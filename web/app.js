/* GTM-compatible messages and private collection envelopes have independent versions. */
const $ = id => document.getElementById(id);
let session=null,last=null,queue=[],loseAck=false,busy=false,consentEpoch=0;
window.dataLayer=window.dataLayer||[];
const usd=cents=>new Intl.NumberFormat('en-US',{style:'currency',currency:'USD',maximumFractionDigits:0}).format(cents/100);
const log=message=>{$('activity').textContent=message+'\n'+$('activity').textContent;};
const showQueue=()=>{$('queue').textContent=`Queue: ${queue.length} | three attempts per delivery cycle`;};
async function post(path,body){const r=await fetch(path,{method:'POST',headers:{'Content-Type':'application/json','X-Assurance-Demo':'synthetic'},body:JSON.stringify(body),signal:AbortSignal.timeout(5000)});if(!r.ok)throw new Error(`HTTP ${r.status}`);return r.json();}
function cell(row,text,cls){const td=document.createElement('td');td.textContent=text;if(cls)td.className=cls;row.append(td);return td;}
async function health(){
 const r=await fetch('/api/health');if(!r.ok)throw new Error('Health unavailable');const h=await r.json();
 $('attempts').textContent=h.attempts;$('accepted').textContent=h.accepted;
 $('duplicates').textContent=['duplicate_event','duplicate_transaction','duplicate_conversion'].reduce((a,k)=>a+(h.statuses[k]||0),0);
 const financial=h.reconciliation.filter(r=>r.value_basis!=='unvalued');
 $('exceptions').textContent=financial.filter(r=>!['matched','not_eligible'].includes(r.status)).length;
 $('late').textContent=`Late arrivals: ${h.late}`;
 for(const [id,key] of [['signed','signed_contract_cents'],['measured-contract','measured_contract_cents'],['cash','payment_cents'],['measured-payment','measured_payment_cents'],['proposals','proposal_cents']])$(id).textContent=usd(h.commercial[key]);
 $('reconciliation').replaceChildren();
 for(const row of financial){const tr=document.createElement('tr');const td=cell(tr,'','financial-name');const title=document.createElement('strong');title.textContent=row.project_id+' / '+(row.value_basis==='contract'?'Signed contract':row.source_ref.endsWith('progress')?'Progress payment':'Deposit');const ref=document.createElement('small');ref.textContent=row.source_ref;td.append(title,ref);cell(tr,row.expected_cents===null?'--':usd(row.expected_cents));cell(tr,row.measured_cents===null?'--':usd(row.measured_cents));cell(tr,row.status.replaceAll('_',' '),`finding ${row.status}`);$('reconciliation').append(tr);}
 $('stages').replaceChildren();for(const stage of ['inquiry','qualified','disqualified','working','proposal','signed_contract','payment']){const s=document.createElement('span');s.textContent=`${stage.replaceAll('_',' ')}: ${h.stage_counts[stage]||0}`;$('stages').append(s);}
 $('decisions').replaceChildren();for(const [status,n] of Object.entries(h.statuses)){const row=document.createElement('div');row.className='bar-row';const name=document.createElement('span');name.textContent=status.replaceAll('_',' ');const count=document.createElement('b');count.textContent=n;const track=document.createElement('div');track.className='track';const fill=document.createElement('div');fill.className='fill';fill.style.width=`${100*n/Math.max(h.attempts,1)}%`;track.append(fill);row.append(name,count,track);$('decisions').append(row);}
 $('receipts').replaceChildren();for(const r of h.recent){const b=document.createElement('button');b.className='receipt';b.textContent=r.status.replaceAll('_',' ');b.dataset.receipt=r.receipt;const s=document.createElement('span');s.textContent=`Receipt ${String(r.receipt).padStart(3,'0')} | ${r.detail==='payload_not_retained'?'payload discarded':'event linked'}`;b.append(s);b.addEventListener('click',async()=>{try{const response=await fetch(`/api/receipts/${r.receipt}`);if(!response.ok)throw new Error('Receipt unavailable');$('receipt-detail').textContent=JSON.stringify(await response.json(),null,2);$('receipt-detail').classList.remove('hidden');$('close-detail').classList.remove('hidden');}catch(e){log(e.message);}});$('receipts').append(b);}
}
async function flush(){
 if(busy)return;busy=true;
 try{while(queue.length&&session){const event=queue[0];let delivered=false;
  for(let attempt=1;attempt<=3&&session===event.session_id;attempt++){
   try{const receipt=await post('/api/events',event);if(loseAck){loseAck=false;log('Fault: server committed; acknowledgement discarded locally. Retrying the same identity.');throw new Error('simulated lost acknowledgement');}log(`${event.event_name} -> ${receipt.status} | receipt ${receipt.receipt}`);delivered=true;break;}
   catch(error){log(`Attempt ${attempt}/3: ${error.message}`);if(attempt<3)await new Promise(r=>setTimeout(r,150*attempt));}
  }
  if(!delivered){log('Delivery exhausted or permission changed. No automatic unbounded retry.');break;}
  if(queue[0]===event)queue.shift();
 }}finally{busy=false;showQueue();await health();}
}
async function inquiry(){
 const current=session,epoch=consentEpoch,projectType=$('service').value;
 const fact=await post('/api/inquiry',{synthetic:true,request_id:crypto.randomUUID(),project_type:projectType,session_id:current});
 log(`Unvalued consultation request recorded | ${fact.source_ref}`);
 if(!current||session!==current||epoch!==consentEpoch){log('generate_lead suppressed in browser | no measurement event queued or sent');await health();return;}
 const param=new URLSearchParams(location.search).get('campaign');const campaign=['design-journal','architect-referral','organic-search'].includes(param)?param:'direct';
 const message={event:'generate_lead',event_params:{lead_source:campaign,project_type:projectType,value_basis:'unvalued'},measurement:{data_layer_version:1,event_id:crypto.randomUUID()}};
 window.dataLayer.push(structuredClone(message));$('data-layer').textContent=JSON.stringify(message,null,2);
 const event={schema_version:2,event_id:message.measurement.event_id,session_id:current,origin:'browser',occurred_at:fact.occurred_at,event_name:message.event,currency:'USD',value_cents:0,value_basis:'unvalued',campaign,project_type:message.event_params.project_type,project_id:fact.project_id,source_ref:fact.source_ref};
 last=structuredClone(event);queue.push(event);showQueue();await flush();
}
function bind(id,fn){$(id).addEventListener('click',async()=>{const b=$(id);b.disabled=true;try{await fn();}catch(e){log(`Action failed: ${e.message}`);}finally{b.disabled=false;}});}
bind('grant',async()=>{if(session)return;const epoch=consentEpoch;const r=await post('/api/consent',{grant:true});if(epoch!==consentEpoch){await post('/api/consent',{revoke:r.session_id});return;}session=r.session_id;$('consent-state').textContent='Analytics allowed';log('Permission granted | new in-memory session');});
bind('revoke',async()=>{consentEpoch++;const old=session;session=null;queue=[];last=null;window.dataLayer.splice(0);$('data-layer').textContent='Revoked. In-memory messages and pending events cleared.';showQueue();$('consent-state').textContent='Denied by default';log('Local permission revoked | queue and identifiers cleared');if(old)await post('/api/consent',{revoke:old});});
bind('inquiry',inquiry);
bind('replay',async()=>{if(!session||!last){log('No permitted event available to replay.');return;}queue.push(structuredClone(last));await flush();});
bind('drift',async()=>{if(!session||!last){log('Allow analytics and create an inquiry first.');return;}const bad=structuredClone(last);bad.event_id=crypto.randomUUID();bad.schema_version=3;const r=await post('/api/events',bad);log(`Schema v3 probe -> ${r.status} | payload not retained`);await health();});
bind('lost',()=>{loseAck=true;log('Next event acknowledgement will be discarded after the server response.');});
bind('retry',flush);
bind('spoof',async()=>{if(!session){log('Allow analytics before running the authority probe.');return;}const e={schema_version:2,event_id:crypto.randomUUID(),session_id:session,origin:'offline',occurred_at:'2026-01-15T12:00:00+00:00',event_name:'purchase',currency:'USD',value_cents:1950000,value_basis:'payment',campaign:'direct',project_type:'hardscaping',project_id:'SYN-P01',source_ref:'SYN-P01-payment',transaction_id:'SYN-P01-payment',items:[{item_id:'hardscaping',price_cents:1950000,quantity:1}]};const r=await post('/api/events',e);log(`Browser payment claim -> ${r.status} | no financial event admitted`);await health();});
bind('refresh',health);bind('close-detail',()=>{$('receipt-detail').classList.add('hidden');$('close-detail').classList.add('hidden');});
document.querySelectorAll('[data-tab]').forEach(b=>b.addEventListener('click',()=>{document.querySelectorAll('.panel').forEach(p=>p.classList.toggle('hidden',p.id!==b.dataset.tab));document.querySelectorAll('.nav').forEach(n=>n.classList.toggle('active',n===b));window.scrollTo(0,0);if(b.dataset.tab==='health')health().catch(e=>log(e.message));}));
health().catch(e=>log(e.message));
fetch('/api/recorded').then(r=>r.json()).then(r=>{if(!r.final)return;$('models').textContent=Object.keys(r.final.models).length;$('dbt-tests').textContent=r.final.dbt_results.filter(x=>x.node.startsWith('test.')).length;$('parity').textContent=r.equivalence?'Matched':'Failed';$('reconciled').textContent=r.equivalence?'All nine model rowsets match the independent full build.':'Evidence incomplete';$('oracle').textContent=r.final.reconciliation;}).catch(()=>{$('oracle').textContent='Unavailable';});
