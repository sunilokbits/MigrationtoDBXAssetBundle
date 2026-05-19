/* ── Global Error Boundary ── */
(function(){
  var _errCount=0, _MAX_ERRS=5;
  function _showErr(msg){
    if(++_errCount>_MAX_ERRS) return;
    var d=document.createElement('div');
    d.setAttribute('role','alert');
    d.style.cssText='position:fixed;top:'+(8+(_errCount-1)*56)+'px;left:50%;transform:translateX(-50%);z-index:99999;background:#fee2e2;color:#b91c1c;border:2px solid #f87171;padding:12px 18px;border-radius:8px;font:13px/1.4 monospace;max-width:80vw;white-space:pre-wrap;box-shadow:0 8px 32px rgba(0,0,0,.2);cursor:pointer;';
    d.textContent=msg;
    d.onclick=function(){d.remove();_errCount--;};
    document.body.appendChild(d);
    setTimeout(function(){if(d.parentNode){d.remove();_errCount--;}},15000);
  }
  window.onerror=function(msg,src,line){_showErr('JS ERROR (line '+line+'): '+msg);return false;};
  window.addEventListener('unhandledrejection',function(e){
    var reason=e.reason;
    var msg=(reason&&reason.message)||String(reason)||'Unknown promise rejection';
    _showErr('Unhandled Promise: '+msg);
  });
})();
const G = id => document.getElementById(id);
let ALL_OBJECTS=[], HELPER_RESULT=null, ACTIVE_FILE=null, UC_TABLE=null;

function toast(msg,type='tinfo',dur=3200){
  const icons={tok:'✓',terr:'✕',tinfo:'ℹ'};
  const el=document.createElement('div');
  el.className='toast '+type;
  el.innerHTML='<span style="font-weight:700;font-size:13px;flex-shrink:0;">'+(icons[type]||'ℹ')+'</span><span>'+msg+'</span>';
  G('toasts').prepend(el);
  setTimeout(()=>{el.classList.add('hiding');setTimeout(()=>el.remove(),220);},dur);
}
function showToast(msg,type='info'){
  const map={success:'tok',error:'terr',warning:'tinfo',info:'tinfo'};
  toast(msg,map[type]||'tinfo');
}

const TAB_META={
  convert:{title:'Convert SQL Objects to PySpark',sub:'One .py per SP/View · All UDFs bundled into HelperFunction.py',step:1},
  deploy:{title:'Deploy Notebooks',sub:'Connect to Databricks & upload notebooks to your workspace',step:2},
  uc:{title:'Databricks SQL Editor',sub:'Browse catalogs, run SQL queries & preview table data',step:3},
  healer:{title:'System Health Check',sub:'Intelligent failure detection, auto-recovery, and system health monitoring',step:4},
  'wf-dashboard':{title:'AI Workflow Manager',sub:'Dashboard — metadata-driven pipeline orchestration overview',step:5},
  'wf-metadata':{title:'MetadataFlow',sub:'Configure Databricks connection & provision Delta metadata tables',step:5},
  'wf-pipelines':{title:'Pipeline Studio',sub:'Connect data sources, create & manage medallion pipelines',step:5},
  'wf-jobs':{title:'Job Manager',sub:'Create workflow jobs, monitor runs & track watermarks',step:5},
  'wf-scheduler':{title:'Job Scheduler',sub:'Schedule migration jobs with cron, interval or one-time triggers',step:5},
  'wf-reports':{title:'Reports & Analytics',sub:'Interactive dashboards, charts & exportable reports for migration pipeline',step:5},
  'wf-progress':{title:'Migration Progress Tracker',sub:'Track overall migration completion — tables, stages, blockers & ETA',step:5},
  'wf-audit':{title:'Audit & Compliance Log',sub:'Track every migration action, config change & security event with full compliance scoring',step:5},
  'wf-dq':{title:'Data Quality Dashboard',sub:'Validate completeness, accuracy, consistency & freshness across all migrated tables',step:5},
  'wf-schema':{title:'Schema Comparison',sub:'Compare source SQL Server & target Databricks schemas — column types, nullability & drift detection',step:5},
  'wf-recon':{title:'Reconciliation Report',sub:'Source vs Bronze aggregate reconciliation — row counts, numeric sums & variance analysis',step:5},
  'wf-datamodel':{title:'AI Data Modeling',sub:'Auto-generate Star & Snowflake schemas with ER diagrams & Databricks DDL',step:5},
  'wf-settings':{title:'Settings',sub:'Configure Azure infrastructure, storage, connectors & Unity Catalog deployment',step:5},
  'wf-admin':{title:'User Management',sub:'Add, edit & remove users — assign role-based access (Admin only)',step:5},
  'wf-discovery':{title:'Discovery',sub:'Scan & analyse SQL objects — complexity scoring, dependency graph & migration readiness',step:5},
};
const WF_LBL=['Convert SQL Objects','Deploy Notebooks','Databricks SQL Editor','System Health Check','AI Workflow Manager'];
const TAB_ICONS={
  convert:'<polyline points="16 3 21 3 21 8"/><line x1="4" y1="20" x2="21" y2="3"/><polyline points="21 16 21 21 16 21"/><line x1="15" y1="15" x2="21" y2="21"/>',
  deploy:'<polyline points="16 16 12 12 8 16"/><line x1="12" y1="12" x2="12" y2="21"/><path d="M20.39 18.39A5 5 0 0018 9h-1.26A8 8 0 103 16.3"/>',
  uc:'<ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>',
  healer:'<path d="M19.69 14a6.9 6.9 0 00.31-2 6.9 6.9 0 00-.31-2l2.15-1.68a.51.51 0 00.12-.64l-2.04-3.53a.51.51 0 00-.61-.22l-2.54 1.02a6.76 6.76 0 00-3.46-2l-.39-2.7a.5.5 0 00-.49-.42h-4.08a.5.5 0 00-.49.42l-.39 2.7a6.76 6.76 0 00-3.46 2L1.73 3.93a.5.5 0 00-.61.22L.09 7.68a.5.5 0 00.12.64L2.36 10a6.9 6.9 0 000 4L.21 15.68a.51.51 0 00-.12.64l2.04 3.53c.12.22.39.3.61.22l2.54-1.02a6.76 6.76 0 003.46 2l.39 2.7c.04.24.25.42.49.42h4.08c.24 0 .45-.18.49-.42l.39-2.7a6.76 6.76 0 003.46-2l2.54 1.02c.22.08.49 0 .61-.22l2.04-3.53a.51.51 0 00-.12-.64L19.69 14zM12 15.5A3.5 3.5 0 1115.5 12 3.5 3.5 0 0112 15.5z"/>',
  'wf-dashboard':'<rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/>',
  'wf-metadata':'<ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>',
  'wf-pipelines':'<path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/>',
  'wf-settings':'<path d="M12.22 2h-.44a2 2 0 00-2 2v.18a2 2 0 01-1 1.73l-.43.25a2 2 0 01-2 0l-.15-.08a2 2 0 00-2.73.73l-.22.38a2 2 0 00.73 2.73l.15.1a2 2 0 011 1.72v.51a2 2 0 01-1 1.74l-.15.09a2 2 0 00-.73 2.73l.22.38a2 2 0 002.73.73l.15-.08a2 2 0 012 0l.43.25a2 2 0 011 1.73V20a2 2 0 002 2h.44a2 2 0 002-2v-.18a2 2 0 011-1.73l.43-.25a2 2 0 012 0l.15.08a2 2 0 002.73-.73l.22-.39a2 2 0 00-.73-2.73l-.15-.08a2 2 0 01-1-1.74v-.5a2 2 0 011-1.74l.15-.09a2 2 0 00.73-2.73l-.22-.38a2 2 0 00-2.73-.73l-.15.08a2 2 0 01-2 0l-.43-.25a2 2 0 01-1-1.73V4a2 2 0 00-2-2z"/><circle cx="12" cy="12" r="3"/>',
  'wf-admin':'<path d="M17 21v-2a4 4 0 00-4-4H5a4 4 0 00-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 00-3-3.87"/><path d="M16 3.13a4 4 0 010 7.75"/>',
  'wf-jobs':'<rect x="2" y="3" width="20" height="14" rx="2" ry="2"/><line x1="8" y1="21" x2="16" y2="21"/><line x1="12" y1="17" x2="12" y2="21"/>',
  'wf-reports':'<line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/>',
  'wf-progress':'<path d="M22 12h-4l-3 9L9 3l-3 9H2"/>',
  'wf-audit':'<path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/>',
  'wf-dq':'<path d="M9 11l3 3L22 4"/><path d="M21 12v7a2 2 0 01-2 2H5a2 2 0 01-2-2V5a2 2 0 012-2h11"/>',
  'wf-schema':'<path d="M16 3h5v5"/><path d="M8 3H3v5"/><path d="M12 22v-8.5"/><path d="M20 9.5V12l-8 4.5L4 12V9.5"/><path d="M4 3l8 4.5L20 3"/>',
  'wf-recon':'<path d="M9 5H2v7l6.29 6.29c.94.94 2.48.94 3.42 0l4.58-4.58c.94-.94.94-2.48 0-3.42L9 5z"/><path d="M6 9h.01"/><path d="M22 5l-4.72 4.72"/>',
  'wf-datamodel':'<circle cx="12" cy="5" r="3"/><line x1="12" y1="8" x2="12" y2="14"/><circle cx="6" cy="19" r="3"/><circle cx="18" cy="19" r="3"/><line x1="12" y1="14" x2="6" y2="16"/><line x1="12" y1="14" x2="18" y2="16"/>',
  'wf-discovery':'<circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/><line x1="11" y1="8" x2="11" y2="14"/><line x1="8" y1="11" x2="14" y2="11"/>',
};

function switchTab(id,btn){
  document.querySelectorAll('.tab-pane').forEach(p=>p.classList.remove('active'));
  document.querySelectorAll('.nav-btn').forEach(b=>b.classList.remove('active'));
  G('pane-'+id).classList.add('active');
  G('nav-'+id).classList.add('active');

  // Persist current tab in URL hash for page refresh
  if(id!=='aiworkflow') history.replaceState(null,'','#'+id);

  if(id==='healer'){
    _hlSyncFromConfig();
    hlFetchRecentRuns();
  }
  if(id==='uc' && typeof ucInit==='function') ucInit();
  if(id==='aiworkflow'){
    switchTab('wf-dashboard',G('nav-wf-dashboard'));return;
  }
  const _wfIds=['wf-dashboard','wf-metadata','wf-pipelines','wf-jobs','wf-settings','wf-progress','wf-audit','wf-dq','wf-schema','wf-recon','wf-datamodel','wf-admin','wf-discovery'];
  if(_wfIds.includes(id)){
    // Auto-sync hidden wfDbr* fields from Settings / deployconfig
    _wfSyncHiddenFields();
    if(id==='wf-dashboard'||id==='wf-pipelines') wfRefreshAll();
    if(id==='wf-jobs') wfRefreshJobs(),wfRefreshAuditHistory();
    if(id==='wf-settings') loadDeployConfig();
    if(id==='wf-admin' && typeof adminRefresh==='function') adminRefresh();
    if(id==='wf-progress' && typeof mptRefresh==='function') mptRefresh();
    if(id==='wf-audit' && typeof auditRefresh==='function') auditRefresh();
    if(id==='wf-dq' && typeof dqRefresh==='function') dqRefresh();
    if(id==='wf-schema' && typeof scRefresh==='function') scRefresh();
    if(id==='wf-recon' && typeof reconRefresh==='function') reconRefresh();
    if(id==='wf-datamodel' && typeof dmInit==='function') dmInit();
    if(id==='wf-discovery' && typeof discInit==='function') discInit();
  }
  const m=TAB_META[id];
  if(m){
    G('topIco').innerHTML=TAB_ICONS[id]||TAB_ICONS.convert;
    G('topTitle').textContent=m.title;
    G('topSub').textContent=m.sub;
    if(G('topBcCur')) G('topBcCur').textContent=m.title;
    for(let i=1;i<=5;i++){const s=G('wf'+i);if(s) s.className='wf-step'+(i<m.step?' done':i===m.step?' active':'');}
    G('wfLbl').textContent='Step '+m.step+' of 5 \u2014 '+WF_LBL[m.step-1];
  }
}

async function loadObjects(){
  try{
    const r=await fetch('/api/v1/all-objects');
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Load failed');
    const MAP={stored_procedure:'SP',view:'VIEW',udf:'UDF'};
    ALL_OBJECTS=[];
    const C={SP:0,VIEW:0,UDF:0};
    for(const[ot,items]of Object.entries(d.grouped||{})){
      items.forEach(o=>{
        const t=MAP[ot]||ot.toUpperCase();
        ALL_OBJECTS.push({key:o.key,name:o.name,description:o.description,object_type:ot,type:t});
        if(C[t]!==undefined)C[t]++;
      });
    }
    G('statSP').textContent=C.SP;G('statVW').textContent=C.VIEW;G('statUDF').textContent=C.UDF;
    renderObjects();
  }catch(e){G('objList').innerHTML='<div class="alert a-err"><span class="a-ico">✕</span>Failed: '+e.message+'</div>';}
}

function renderObjects(){
  const GRPS={SP:[],VIEW:[],UDF:[]};
  ALL_OBJECTS.forEach(o=>{if(GRPS[o.type])GRPS[o.type].push(o);});
  const M={
    SP:{label:'Stored Procedures',dot:'var(--sp-c)',lc:'var(--sp-c)',tc:'sp',desc:'Stored procedure'},
    VIEW:{label:'SQL Views',dot:'var(--vw-c)',lc:'var(--vw-c)',tc:'vw',desc:'SQL View'},
    UDF:{label:'User-Defined Functions',dot:'var(--udf-c)',lc:'var(--udf-c)',tc:'udf',desc:'User-defined function'},
  };
  let html='';
  for(const[t,items]of Object.entries(GRPS)){
    if(!items.length)continue;
    const m=M[t];
    html+=`<div class="grp-hd" onclick="toggleGrp('${t}')">
      <span class="grp-dot" style="background:${m.dot}"></span>
      <span class="grp-lbl" style="color:${m.lc}">${m.label}</span>
      <span class="grp-badge">${items.length}</span>
      <span class="grp-chv" id="gc${t}">▾</span>
    </div><div id="gi${t}" style="padding:4px 0;">`;
    items.forEach(o=>{
      html+=`<div class="obj-item" id="oi${o.key}" onclick="togItem('${o.key}',event)">
        <input type="checkbox" id="ck${o.key}">
        <span class="chk" id="cb${o.key}">✓</span>
        <span class="badge b${m.tc}">${t==='VIEW'?'VIEW':t}</span>
        <div class="obj-info"><div class="obj-name" title="${o.key}">${o.key}</div><div class="obj-desc">${o.description||m.desc}</div></div>
        <button class="sql-btn" onclick="event.stopPropagation();loadSrc('${o.key}')">SQL</button>
      </div>`;
    });
    html+='</div>';
  }
  G('objList').innerHTML=html||'<div class="empty"><div class="empty-s">No objects loaded.</div></div>';
  updSelCnt();
}

function toggleGrp(t){const el=G('gi'+t),chv=G('gc'+t);if(!el)return;const hide=el.style.display!=='none';el.style.display=hide?'none':'';if(chv)chv.textContent=hide?'▸':'▾';}
function togItem(key,e){if(e.target.classList.contains('sql-btn'))return;const c=G('ck'+key);if(c){c.checked=!c.checked;updSel();}}
function updSel(){ALL_OBJECTS.forEach(o=>{const c=G('ck'+o.key),item=G('oi'+o.key),cb=G('cb'+o.key);const s=c&&c.checked;if(item)item.classList.toggle('selected',s);if(cb)cb.style.color=s?'#fff':'transparent';});updSelCnt();}
function updSelCnt(){const n=getSel().length;G('selCnt').textContent=n+' / '+ALL_OBJECTS.length;const b=G('btnConvert');if(b)b.disabled=(n===0);updAnalysisSel();}
function getSel(){return ALL_OBJECTS.filter(o=>{const c=G('ck'+o.key);return c&&c.checked;});}

/* ── Source Analysis ── */
function renderAnalysis(objects){
  const card=G('analysisCard');
  if(!objects||!objects.length){card.style.display='none';return;}
  const byType={SP:[],VIEW:[],UDF:[]};
  objects.forEach(o=>{const t=o.type;if(byType[t])byType[t].push(o);else byType[t]=[o];});
  const total=objects.length;
  const cnts={SP:byType.SP.length,VIEW:byType.VIEW.length,UDF:byType.UDF.length};
  // Distribution bar
  const BGMAP={SP:'#FB923C',VIEW:'#38BDF8',UDF:'#A78BFA'};
  G('anDistBar').innerHTML=['SP','VIEW','UDF'].map(t=>{
    const w=total?Math.round(cnts[t]/total*100):0;
    return w?`<div class="an-bar-seg" style="width:${w}%;background:${BGMAP[t]}" title="${cnts[t]} ${t}"></div>`:'';
  }).join('');
  // Type counts
  ['SP','VIEW','UDF'].forEach(t=>{const el=G('anCnt'+t);if(el)el.textContent=cnts[t];});
  // Show first non-empty tab
  window._AN_DATA=byType;
  const first=['SP','VIEW','UDF'].find(t=>cnts[t]>0)||'SP';
  showAnTab(first);
  card.style.display='';
  updAnalysisSel();
}
function showAnTab(type){
  const d=window._AN_DATA;
  if(!d)return;
  ['SP','VIEW','UDF'].forEach(t=>{const btn=G('anTab'+t);if(btn)btn.classList.toggle('active',t===type);});
  const items=d[type]||[];
  const DOTMAP={SP:'#C2410C',VIEW:'#0369A1',UDF:'#6D28D9'};
  if(!items.length){G('anList').innerHTML='<div class="an-empty">No '+type+' objects loaded</div>';return;}
  G('anList').innerHTML=items.map(o=>{
    const len=(o.code||'').length;
    const cx=len===0?null:len<500?'LOW':len<1500?'MED':'HIGH';
    const cxS=cx==='LOW'?'background:#DCFCE7;color:#15803D':cx==='MED'?'background:#FEF9C3;color:#92400E':cx==='HIGH'?'background:#FEE2E2;color:#B91C1C':'';
    return `<div class="an-item" onclick="loadSrc('${o.key}')" title="Click to view SQL">`+
      `<span class="an-item-dot" style="background:${DOTMAP[type]}"></span>`+
      `<span class="an-item-name">${o.name}</span>`+
      (cx?`<span class="an-cx" style="${cxS}">${cx}</span>`:'')+
      `</div>`;
  }).join('');
}
function updAnalysisSel(){
  const sel=typeof getSel==='function'?getSel():[];
  const total=ALL_OBJECTS?ALL_OBJECTS.length:0;
  const el=G('anSelSummary');
  if(el)el.textContent=sel.length+' selected · '+total+' total';
  ['SP','VIEW','UDF'].forEach(t=>{
    const n=ALL_OBJECTS?ALL_OBJECTS.filter(o=>o.type===t&&(()=>{const c=G('ck'+o.key);return c&&c.checked;})()).length:0;
    const badge=G('anSel'+t);
    if(badge){badge.textContent=n?(n+''):'';badge.style.opacity=n?'1':'0';}
  });
}

/* ── Source Connection ── */
const IDD_OPTS = {
  sqlserver: {label:'SQL Server'},
  azuresql: {label:'Azure SQL'},
  synapse:  {label:'Synapse SQL'},
  sqlmi:    {label:'SQL Managed Instance'}
};
// toggleIDD / pickSrcType no longer needed — native <select> handles it
function toggleIDD(){}
function pickSrcType(){}

/* Auto-populate source connection hidden fields from deployconfig.json */
async function _srcSyncFromConfig(){
  const cfg=await _ensureDeployConfig();
  const src=cfg.source||{};
  G('srcType').value=src.source_type||'azuresql';
  G('srcServer').value=src.server||'';
  G('srcDb').value=src.database||'';
  G('srcUser').value=src.username||'';
  G('srcPass').value=src.password||'';
  // Update info bar
  if(G('srcCfgServer'))G('srcCfgServer').textContent=src.server||'—';
  if(G('srcCfgDb'))G('srcCfgDb').textContent=src.database||'—';
  if(G('srcCfgType'))G('srcCfgType').textContent=(IDD_OPTS[src.source_type]||{}).label||src.source_type||'—';
  if(G('srcCfgUser'))G('srcCfgUser').textContent=src.username||'—';
}

async function _dbrSyncFromConfig(){
  const cfg=await _ensureDeployConfig();
  G('dbHost').value=cfg.databricks_host||'';
  G('dbToken').value=cfg.databricks_token||'';
  if(G('dbrCfgHost'))G('dbrCfgHost').textContent=cfg.databricks_host||'—';
  if(G('dbrCfgToken'))G('dbrCfgToken').textContent=cfg.databricks_token?'Configured ✓':'—';
}

function toggleSrcConn(){
  const body=G('srcConnBody'),chv=G('srcConnChv');
  body.classList.toggle('collapsed');
  chv.classList.toggle('open');
}

async function testSourceConn(){
  const btn=G('btnTestSrc');
  const server=G('srcServer').value.trim(),db=G('srcDb').value.trim(),user=G('srcUser').value.trim();
  if(!server||!db||!user){toast('Server, database and username are required.','terr');return;}
  const origHTML=btn.innerHTML;
  btn.disabled=true;btn.innerHTML='<div class="spin"></div>';
  const msg=G('srcConnMsg');
  msg.style.display='none';
  try{
    const r=await fetch('/api/v1/source/test-connection',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({source_type:G('srcType').value,server,database:db,username:user,password:G('srcPass').value})});
    const d=await r.json();
    msg.style.display='';
    if(d.success){
      msg.style.cssText='display:block;background:var(--green-light);color:var(--green-fg);border:1px solid var(--green-border);font-size:11.5px;padding:8px 10px;border-radius:var(--r);';
      msg.textContent='✓ '+d.server_version;
      G('srcConnDot').className='src-conn-dot ok';
      G('btnLoadSrc').disabled=false;
      toast('Source connected successfully!','tok');
    }else{
      msg.style.cssText='display:block;background:var(--red-light);color:var(--red-fg);border:1px solid var(--red-border);font-size:11.5px;padding:8px 10px;border-radius:var(--r);';
      msg.textContent='✕ '+d.error;
      G('srcConnDot').className='src-conn-dot err';
      G('btnLoadSrc').disabled=true;
      toast(d.error,'terr',5000);
    }
  }catch(e){
    msg.style.cssText='display:block;background:var(--red-light);color:var(--red-fg);border:1px solid var(--red-border);font-size:11.5px;padding:8px 10px;border-radius:var(--r);';
    msg.textContent='✕ '+e.message;
    G('srcConnDot').className='src-conn-dot err';
    toast(e.message,'terr');
  }finally{btn.disabled=false;btn.innerHTML=origHTML;}
}

async function loadFromSource(){
  const btn=G('btnLoadSrc');
  const server=G('srcServer').value.trim(),db=G('srcDb').value.trim(),user=G('srcUser').value.trim();
  const origHTML=btn.innerHTML;
  btn.disabled=true;btn.innerHTML='<div class="spin"></div> Loading…';
  G('objList').innerHTML='<div class="loading-state"><div class="spin spin-lg"></div><span>Fetching objects from source…</span></div>';
  try{
    const r=await fetch('/api/v1/source/load-objects',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({source_type:G('srcType').value,server,database:db,username:user,password:G('srcPass').value})});
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Load failed');
    const MAP={stored_procedure:'SP',view:'VIEW',udf:'UDF'};
    ALL_OBJECTS=[];
    const C={SP:0,VIEW:0,UDF:0};
    for(const[ot,items]of Object.entries(d.grouped||{})){
      items.forEach(o=>{
        const t=MAP[ot]||ot.toUpperCase();
        ALL_OBJECTS.push({key:o.key,name:o.name,description:o.description||'',object_type:ot,type:t,code:o.code||''});
        if(C[t]!==undefined)C[t]++;
      });
    }
    G('statSP').textContent=C.SP;
    G('statVW').textContent=C.VIEW;
    G('statUDF').textContent=C.UDF;
    renderObjects();
    renderAnalysis(ALL_OBJECTS);
    toast('Loaded '+ALL_OBJECTS.length+' objects from '+db+'.','tok',4000);
    const body=G('srcConnBody'),chv=G('srcConnChv');
    if(!body.classList.contains('collapsed')){body.classList.add('collapsed');chv.classList.remove('open');}
  }catch(e){
    G('objList').innerHTML='<div class="alert a-err"><span class="a-ico">✕</span>'+e.message+'</div>';
    toast(e.message,'terr');
  }finally{btn.disabled=false;btn.innerHTML=origHTML;}
}

function setFilterActive(id){['fbAll','fbNone','fbSP','fbVW','fbUDF'].forEach(i=>{const b=G(i);if(b)b.classList.remove('active');});if(id){const b=G(id);if(b)b.classList.add('active');}}
function selectAll(){
  const allChecked=ALL_OBJECTS.length>0&&ALL_OBJECTS.every(o=>{const c=G('ck'+o.key);return c&&c.checked;});
  ALL_OBJECTS.forEach(o=>{const c=G('ck'+o.key);if(c)c.checked=!allChecked;});
  updSel();setFilterActive(allChecked?'fbNone':'fbAll');
}
function deselectAll(){ALL_OBJECTS.forEach(o=>{const c=G('ck'+o.key);if(c)c.checked=false;});updSel();setFilterActive('fbNone');}
function selType(t){
  const MAP={SP:'fbSP',VIEW:'fbVW',UDF:'fbUDF'};
  const allOfType=ALL_OBJECTS.filter(o=>o.type===t);
  const alreadyActive=allOfType.length>0&&allOfType.every(o=>{const c=G('ck'+o.key);return c&&c.checked;})&&ALL_OBJECTS.filter(o=>o.type!==t).every(o=>{const c=G('ck'+o.key);return c&&!c.checked;});
  if(alreadyActive){ALL_OBJECTS.forEach(o=>{const c=G('ck'+o.key);if(c)c.checked=false;});updSel();setFilterActive('fbNone');}
  else{ALL_OBJECTS.forEach(o=>{const c=G('ck'+o.key);if(c)c.checked=(o.type===t);});updSel();setFilterActive(MAP[t]||null);}
}

async function loadSrc(key){
  const local=ALL_OBJECTS.find(o=>o.key===key);
  if(local&&local.code&&local.code.trim()){
    G('srcName').textContent=key+'  ['+(local.object_type||local.type||'').toUpperCase()+']';
    G('srcBody').textContent=local.code.trim();
    G('srcPanel').style.display='';
    return;
  }
  try{
    const r=await fetch('/api/v1/object-code/'+encodeURIComponent(key));
    const d=await r.json();
    if(d.success){G('srcName').textContent=key+'  ['+(d.object_type||'').toUpperCase()+']';G('srcBody').textContent=d.code;G('srcPanel').style.display='';}
    else toast('Source not found: '+(d.error||'?'),'terr');
  }catch(e){toast(e.message,'terr');}
}
async function previewSource(){const sel=getSel();if(!sel.length){toast('Select at least one object first.','tinfo');return;}loadSrc(sel[0].key);}
function closeSrc(){G('srcPanel').style.display='none';}

async function convertSelected(){
  const sel=getSel();
  if(!sel.length){toast('Select at least one object.','tinfo');return;}
  const btn=G('btnConvert');btn.disabled=true;
  G('bci').style.display='none';
  const spinEl=document.createElement('span');spinEl.className='spin';spinEl.style.cssText='border-top-color:#fff;margin-right:0';
  btn.insertBefore(spinEl,btn.querySelector('#bct'));
  G('bct').textContent='Converting…';
  let pct=0;const prog=G('bprog');
  const iv=setInterval(()=>{pct=Math.min(pct+6,82);prog.style.width=pct+'%';},120);
  const cnt=sel.length;
  G('codeOut').innerHTML='<div class="loading-state"><div class="spin spin-lg"></div><span>Building notebooks for '+cnt+' object'+(cnt>1?'s':'')+'…</span></div>';
  G('nbTabs').innerHTML='';G('nbBar').style.display='none';
  G('notesCard').style.display='none';G('pyBadge').style.display='none';
  HELPER_RESULT=null;
  try{
    const r=await fetch('/api/v1/convert-separate',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({
      object_names: sel.map(o=>o.key),
      objects_with_code: Object.fromEntries(sel.filter(o=>o.code).map(o=>[o.key,{type:o.object_type||o.type,code:o.code}]))
    })});
    const d=await r.json();
    clearInterval(iv);prog.style.width='100%';
    setTimeout(()=>prog.style.width='0%',450);
    if(!d.success){G('codeOut').innerHTML='<div class="alert a-err" style="margin:14px;"><span class="a-ico">✕</span>'+(d.error||'Conversion failed')+'</div>';toast(d.error||'Conversion failed','terr');return;}
    HELPER_RESULT=d;renderSeparateFiles(d);updDeployList();
    const msg=(d.sp_view_count||0)+' file'+((d.sp_view_count||0)!==1?'s':'')+(d.udf_count?' + HelperFunction.py ('+d.udf_count+' UDF'+(d.udf_count!==1?'s':'')+')'  :'')+' — '+d.object_count+' objects converted';
    toast(msg,'tok',4500);G('wf1').className='wf-step done';
  }catch(e){
    clearInterval(iv);prog.style.width='0%';
    G('codeOut').innerHTML='<div class="alert a-err" style="margin:14px;"><span class="a-ico">✕</span>'+e.message+'</div>';
    toast('Error: '+e.message,'terr');
  }finally{
    btn.disabled=false;
    if(spinEl.parentNode)spinEl.remove();
    G('bci').style.display='';
    G('bct').textContent='Convert - SQL → PySpark';
    updSelCnt();
  }
}

function renderSeparateFiles(d){
  const udfLbl=d.udf_count?d.udf_count+' UDF'+(d.udf_count!==1?'s':''):'Shared';
  let th=`<button class="nb-tab helper active" id="nbt___helper" onclick="showFile('__helper__')">⚙ HelperFunction.py <span style="opacity:.5;font-size:9px">${udfLbl}</span></button>`;
  (d.files||[]).forEach(f=>{
    const cls=f.object_type==='stored_procedure'?'sp':f.object_type==='view'?'vw':'ud';
    const ico=f.object_type==='stored_procedure'?'▸':f.object_type==='view'?'◉':'ƒ';
    th+=`<button class="nb-tab ${cls}" id="nbt_${f.name}" onclick="showFile('${f.name}')">${ico} ${f.name}.py</button>`;
  });
  G('nbTabs').innerHTML=th;G('nbBar').style.display='';
  ACTIVE_FILE='__helper__';G('codeTitle').textContent='HelperFunction.py';G('pyBadge').style.display='';
  G('codeOut').textContent=d.helper_code;
  const btnDL=G('btnDL'),btnALL=G('btnDLAll');
  if(btnDL)btnDL.disabled=false;if(btnALL)btnALL.disabled=false;G('btnCopy').disabled=false;
  const notes=d.conversion_notes||{};let nh='';
  Object.entries(notes).forEach(([name,ns])=>{
    const o=ALL_OBJECTS.find(x=>x.key===name)||{type:'SP'};const tc=o.type.toLowerCase();
    nh+=`<div class="note-grp"><span class="badge b${tc}">${o.type}</span> ${name}</div><div>`;
    (ns||[]).forEach(n=>{nh+=`<div class="note-item">${n}</div>`;});nh+='</div>';
  });
  if(nh){G('notesList').innerHTML=nh;G('notesCard').style.display='';}
}

function showFile(name){
  if(!HELPER_RESULT)return;
  document.querySelectorAll('.nb-tab').forEach(t=>t.classList.remove('active'));
  ACTIVE_FILE=name;
  if(name==='__helper__'){const t=G('nbt___helper');if(t)t.classList.add('active');G('codeTitle').textContent='HelperFunction.py';G('codeOut').textContent=HELPER_RESULT.helper_code;}
  else{const t=G('nbt_'+name);if(t)t.classList.add('active');const f=(HELPER_RESULT.files||[]).find(x=>x.name===name);G('codeTitle').textContent=name+'.py';G('codeOut').textContent=f?f.code:'# File "'+name+'" not found.';}
  G('btnCopy').disabled=false;const btnDL=G('btnDL');if(btnDL)btnDL.disabled=false;
}

function copyCode(){const c=G('codeOut').textContent;if(!c||c.trim().length<5){toast('Nothing to copy.','tinfo');return;}navigator.clipboard.writeText(c).then(()=>{toast('Copied to clipboard!','tok',2000);const b=G('btnCopy'),orig=b.innerHTML;b.textContent='✓ Copied!';setTimeout(()=>b.innerHTML=orig,1800);});}
function dlCode(){const c=G('codeOut').textContent;if(!c||c.trim().length<5){toast('Nothing to download.','tinfo');return;}const fn=(!ACTIVE_FILE||ACTIVE_FILE==='__helper__')?'HelperFunction.py':(ACTIVE_FILE+'.py');Object.assign(document.createElement('a'),{href:URL.createObjectURL(new Blob([c],{type:'text/plain'})),download:fn}).click();toast('Downloaded '+fn,'tok',2000);}
function dlAllFiles(){
  if(!HELPER_RESULT){toast('Convert first.','tinfo');return;}
  const allFiles=[{filename:'HelperFunction.py',code:HELPER_RESULT.helper_code},...(HELPER_RESULT.files||[]).map(f=>({filename:f.filename,code:f.code}))];
  allFiles.forEach((f,i)=>{setTimeout(()=>{Object.assign(document.createElement('a'),{href:URL.createObjectURL(new Blob([f.code],{type:'text/plain'})),download:f.filename}).click();},i*350);});
  toast('Downloading '+allFiles.length+' files…','tok',2500);
}

function updDeployList(){
  if(!HELPER_RESULT){G('deployList').innerHTML='<div class="empty" style="padding:12px;"><div class="empty-ico">📓</div><div class="empty-s">Convert objects in Step 1 first.</div></div>';if(G('depCnt'))G('depCnt').textContent='0 files';return;}
  const files=HELPER_RESULT.files||[];
  const total=files.length+(HELPER_RESULT.udf_count>0?1:0);
  if(G('depCnt'))G('depCnt').textContent=total+' file'+(total!==1?'s':'');
  let h='<div style="display:flex;flex-direction:column;gap:6px;">';
  if(HELPER_RESULT.udf_count>0){
    h+=`<div style="display:flex;align-items:center;gap:9px;padding:8px 6px;border-bottom:1px solid var(--border);margin-bottom:2px;">
    <div style="flex:1;"><div style="font-size:12.5px;font-weight:700;color:var(--blue-fg);">⚙ HelperFunction.py</div><div style="font-size:10.5px;color:var(--t3);">${HELPER_RESULT.udf_count||0} UDFs · ${HELPER_RESULT.helper_lines||0} lines</div></div>
  </div>`;
  }
  files.forEach(f=>{const cls=f.object_type==='stored_procedure'?'bsp':f.object_type==='view'?'bvw':'budf';const lbl=f.object_type==='stored_procedure'?'SP':f.object_type==='view'?'VIEW':'UDF';h+=`<div style="display:flex;align-items:center;gap:8px;padding:6px 4px;"><span class="badge ${cls}">${lbl}</span><span style="font-size:12.5px;color:var(--t2);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${f.filename}</span><span style="font-size:10.5px;color:var(--t3);flex-shrink:0;">${f.lines} lines</span></div>`;});
  G('deployList').innerHTML=h+'</div>';
}

async function deployAll(){
  const host=G('dbHost').value.trim(),token=G('dbToken').value.trim(),path=G('depPath').value.trim()||'/Shared/Migrations';
  if(!host||!token){toast('Enter Workspace Host and Access Token.','terr');return;}
  if(!HELPER_RESULT){toast('Convert objects in Step 1 first.','tinfo');return;}
  const btn=G('btnDeployAll'),lbl=G('depBtnTxt'),prog=G('depProg');
  btn.disabled=true;lbl.textContent='Deploying…';
  let pct=0;const iv=setInterval(()=>{pct=Math.min(pct+4,88);prog.style.width=pct+'%';},200);
  G('deployLog').innerHTML='<div class="loading-state"><div class="spin spin-lg"></div><span>Uploading notebooks to Databricks…</span></div>';
  const notebooks=[{name:'HelperFunction',code:HELPER_RESULT.helper_code},...(HELPER_RESULT.files||[]).map(f=>({name:f.name,code:f.code}))];
  try{
    const r=await fetch('/api/v1/databricks/upload-multiple',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({host,token,workspace_path:path,notebooks})});
    const d=await r.json();
    clearInterval(iv);prog.style.width='100%';setTimeout(()=>prog.style.width='0%',500);
    if(d.results&&d.results.length){
      let logHtml='';
      d.results.forEach(res=>{const ok=res.success;logHtml+=`<div class="dep-row ${ok?'ok':'err'}"><div class="blt ${ok?'blt-ok':'blt-err'}"></div><span class="dep-name">${res.name}.py</span><span class="dep-path">${res.path||res.error||''}</span></div>`;});
      if(d.uploaded===notebooks.length){logHtml+=`<div class="alert a-ok" style="margin-top:10px;"><span class="a-ico">✓</span> All ${d.uploaded} notebooks deployed to <strong>${path}</strong></div>`;G('wf2').className='wf-step done';lbl.textContent='✓ Deployed!';toast('All '+d.uploaded+' notebooks deployed!','tok');}
      else{logHtml+=`<div class="alert a-warn" style="margin-top:10px;"><span class="a-ico">⚠</span> ${d.uploaded} of ${d.total} notebooks uploaded.</div>`;lbl.textContent='Deploy All to Databricks';toast(d.uploaded+'/'+d.total+' deployed.','tinfo');}
      G('deployLog').innerHTML=logHtml;
    }else{G('deployLog').innerHTML=`<div class="alert a-err"><span class="a-ico">✕</span>${d.error||'Upload failed'}</div>`;lbl.textContent='Deploy All to Databricks';toast('Deploy failed.','terr');}
  }catch(e){
    clearInterval(iv);prog.style.width='0%';
    G('deployLog').innerHTML=`<div class="alert a-err"><span class="a-ico">✕</span>${e.message}</div>`;lbl.textContent='Deploy All to Databricks';toast('Deploy error: '+e.message,'terr');
  }finally{btn.disabled=false;}
}


async function testConn(){
  const host=G('dbHost').value.trim(),token=G('dbToken').value.trim();
  if(!host||!token){toast('Host and token required.','terr');return;}
  G('connStatus').innerHTML='<div class="alert a-info"><span class="spin" style="border-top-color:var(--blue-fg)"></span> Connecting…</div>';
  G('connInfo').innerHTML='<div class="loading-state"><div class="spin spin-lg"></div><span>Fetching workspace info…</span></div>';
  try{
    const r=await fetch('/api/v1/databricks/test-connection',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({host,token})});
    const d=await r.json();
    if(d.success){
      G('connStatus').innerHTML='<div class="alert a-ok"><span class="a-ico">✓</span> Connected to Databricks</div>';
      let h=`<div class="alert a-info" style="margin-bottom:12px;"><span class="a-ico">🌐</span><span><strong>Host:</strong> ${host}</span></div>`;
      (d.clusters||[]).forEach(c=>{const sc=c.state==='RUNNING'?'tag-run':c.state==='TERMINATED'?'tag-stop':'tag-pend';h+=`<div class="cl-card"><div class="cl-name">${c.cluster_name} <span class="tag ${sc}">${c.state}</span></div><div class="cl-meta"><span>ID: ${c.cluster_id}</span><span>DBR ${c.spark_version||'N/A'}</span></div></div>`;});
      if(!(d.clusters||[]).length)h+='<div class="alert a-warn"><span class="a-ico">⚠</span> No clusters found.</div>';
      G('connInfo').innerHTML=h;toast('Connected to Databricks!','tok');G('wf2').className='wf-step done';
    }else{G('connStatus').innerHTML=`<div class="alert a-err"><span class="a-ico">✕</span>${d.error}</div>`;G('connInfo').innerHTML=`<pre style="font-size:11px;color:var(--t3);padding:10px;">${JSON.stringify(d,null,2)}</pre>`;toast('Connection failed: '+d.error,'terr');}
  }catch(e){G('connStatus').innerHTML=`<div class="alert a-err"><span class="a-ico">✕</span>${e.message}</div>`;toast('Error: '+e.message,'terr');}
}

// ── UC Config Loader ─────────────────────────────────────────────────────────
let _ucCatalogSchemas = [];

async function ucInit(){
  try{
    const r=await fetch('/api/v1/uc/config');
    const d=await r.json();
    if(d.success){
      G('ucHostDisplay').textContent=d.host||'Not configured';
      G('ucHostDisplay').style.color=d.host?'var(--t1)':'#EF4444';
      G('ucTokenDisplay').innerHTML=d.has_token?'<span style="color:#10B981;">✓ Token configured (hidden)</span>':'<span style="color:#EF4444;">✕ No token in deployconfig.json</span>';
      _ucCatalogSchemas=d.catalog_schemas||[];
      const sel=G('ucCatalog');
      sel.innerHTML='<option value="">— Select catalog —</option>';
      const cats=[...new Set(_ucCatalogSchemas.map(c=>c.catalog))];
      cats.forEach(c=>{const o=document.createElement('option');o.value=c;o.textContent=c;sel.appendChild(o);});
    }
  }catch(e){console.error('ucInit',e);}
}

function ucOnCatalogChange(){
  const cat=G('ucCatalog').value;
  const sel=G('ucSchema');
  sel.innerHTML='<option value="">— Select schema —</option>';
  if(!cat)return;
  _ucCatalogSchemas.filter(c=>c.catalog===cat).forEach(cs=>{
    const o=document.createElement('option');o.value=cs.schema;o.textContent=cs.schema;sel.appendChild(o);
  });
}

async function loadUCTables(){
  const cat=G('ucCatalog').value,sch=G('ucSchema').value;
  if(!cat||!sch){toast('Select catalog and schema first','terr');return;}
  G('ucResults').innerHTML='<div class="loading-state"><div class="spin spin-lg"></div><span>Loading…</span></div>';
  try{
    const r=await fetch('/api/v1/unity-catalog/tables',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({catalog:cat,schema:sch})});
    const d=await r.json();
    if(d.success){
      const tbls=d.tables||[];
      G('ucTableList').innerHTML=tbls.length?tbls.map(t=>`<div class="tbl-item" id="utbl${t.table_name}" onclick="selUCTbl('${t.table_name}')"><span style="color:var(--t4);font-size:11px;">⊞</span> ${cat}.${sch}.<strong>${t.table_name}</strong>${t.table_type&&t.table_type!=='N/A'?`<span style="margin-left:auto;font-size:10px;color:var(--t3);">${t.table_type}</span>`:''}</div>`).join(''):'<div class="empty" style="padding:10px;"><div class="empty-s">No tables found.</div></div>';
      G('ucWarehouse').innerHTML=(d.warehouses||[]).map(w=>`<option value="${w.id}">${w.name} (${w.state})</option>`).join('')||'<option value="">No warehouses</option>';
      G('ucResults').innerHTML=`<div class="alert a-ok"><span class="a-ico">✓</span> Loaded ${tbls.length} tables, ${(d.warehouses||[]).length} warehouses.</div>`;
      toast('Loaded '+tbls.length+' tables!','tok');
    }else{G('ucResults').innerHTML=`<div class="alert a-err"><span class="a-ico">✕</span>${d.error}</div>`;}
  }catch(e){G('ucResults').innerHTML=`<div class="alert a-err"><span class="a-ico">✕</span>${e.message}</div>`;}
}

function selUCTbl(t){UC_TABLE=t;G('ucTable').value=t;document.querySelectorAll('.tbl-item').forEach(el=>el.classList.remove('selected'));const el=G('utbl'+t);if(el)el.classList.add('selected');}

function _renderUCResult(d){
  if(!d.success) return `<div class="alert a-err"><span class="a-ico">✕</span>${d.error||d.message||'Unknown error'}</div>`;
  if(d.sql_type==='query'){
    if(!d.columns||d.columns.length===0) return '<div class="alert a-ok"><span class="a-ico">✓</span> Statement executed successfully (no rows returned).</div>';
    const colDefs=d.columns.map(c=>`<th>${c}</th>`).join('');
    const rowDefs=(d.rows||[]).map(r=>`<tr>${r.map(v=>`<td title="${String(v??'').replace(/"/g,'&quot;')}">${v??'<em style="color:var(--t3)">null</em>'}</td>`).join('')}</tr>`).join('');
    const info=`<div style="font-size:11px;color:var(--t3);margin-top:6px;padding:0 2px;">${d.row_count} row${d.row_count!==1?'s':''} · ${d.columns.length} column${d.columns.length!==1?'s':''}</div>`;
    return `<div style="overflow-x:auto;"><table class="uc-tbl"><thead><tr>${colDefs}</tr></thead><tbody>${rowDefs}</tbody></table></div>${info}`;
  }
  if(d.sql_type==='statement') return '<div class="alert a-ok"><span class="a-ico">✓</span> Statement executed successfully.</div>';
  if(d.steps){
    const cfg={PASS:{ico:'✓',col:'var(--green)',bg:'var(--green-light)'},FAIL:{ico:'✕',col:'var(--red)',bg:'var(--red-light)'},WARN:{ico:'⚠',col:'var(--amber)',bg:'var(--amber-light)'},INFO:{ico:'ℹ',col:'var(--blue)',bg:'var(--blue-light)'}};
    const stepsHtml=d.steps.map(s=>{
      const c=cfg[s.status]||{ico:'·',col:'var(--t2)',bg:'var(--surface-2)'};
      let extra='';
      if(s.columns&&s.sample_rows&&s.sample_rows.length>0){
        const h=s.columns.map(c=>`<th>${c}</th>`).join('');
        const b=s.sample_rows.map(r=>`<tr>${r.map(v=>`<td>${v??''}</td>`).join('')}</tr>`).join('');
        extra=`<div style="overflow-x:auto;margin-top:6px;"><table class="uc-tbl"><thead><tr>${h}</tr></thead><tbody>${b}</tbody></table></div>`;
      }
      return `<div class="uc-step"><span class="uc-step-icon" style="color:${c.col};">${c.ico}</span><div style="flex:1;"><div class="uc-step-title">${s.step}</div><div class="uc-step-detail">${s.detail}</div>${extra}</div><span class="uc-step-badge" style="color:${c.col};background:${c.bg};">${s.status}</span></div>`;
    }).join('');
    const meta=`<div style="font-size:10.5px;color:var(--t3);margin-top:8px;padding:0 2px;">Table: <code>${d.table}</code> · ${d.executed_at||''}</div>`;
    return `<div>${stepsHtml}</div>${meta}`;
  }
  if(d.columns&&d.rows){
    const h=d.columns.map(c=>`<th>${c}</th>`).join('');
    const b=(d.rows||[]).map(r=>`<tr>${r.map(v=>`<td>${v??''}</td>`).join('')}</tr>`).join('');
    return `<div style="overflow-x:auto;"><table class="uc-tbl"><thead><tr>${h}</tr></thead><tbody>${b}</tbody></table></div><div style="font-size:11px;color:var(--t3);margin-top:6px;">${d.preview_rows} preview rows · ${d.total_rows} total</div>`;
  }
  return `<pre style="font-size:11px;color:var(--t2);white-space:pre-wrap;">${JSON.stringify(d,null,2)}</pre>`;
}

async function _ucPost(endpoint,extra={}){
  const cat=G('ucCatalog').value,sch=G('ucSchema').value,wh=G('ucWarehouse').value;
  G('ucResults').innerHTML='<div class="loading-state"><div class="spin spin-lg"></div><span>Running…</span></div>';
  try{
    const r=await fetch(endpoint,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({catalog:cat,schema:sch,warehouse_id:wh,...extra})});
    const d=await r.json();
    G('ucResults').innerHTML=_renderUCResult(d);
    if(d.success!==false)toast('Done!','tok');
  }catch(e){G('ucResults').innerHTML=`<div class="alert a-err"><span class="a-ico">✕</span>${e.message}</div>`;}
}

function previewTable(){const t=UC_TABLE||G('ucTable').value,wh=G('ucWarehouse').value;if(!t||!wh){toast('Select a table and warehouse.','tinfo');return;}_ucPost('/api/v1/unity-catalog/preview',{table_name:t});}
function executeTable(){const t=UC_TABLE||G('ucTable').value,wh=G('ucWarehouse').value;if(!t||!wh){toast('Select a table and warehouse.','tinfo');return;}_ucPost('/api/v1/unity-catalog/execute',{table_name:t});}
function runCustomSQL(){const sql=G('ucSQL').value.trim(),wh=G('ucWarehouse').value;if(!sql||!wh){toast('Enter SQL and select a warehouse.','tinfo');return;}_ucPost('/api/v1/unity-catalog/execute',{table_name:'__custom__',execute_sql:sql});}

// loadObjects() removed — counts stay at 0 until user clicks "Load SQL Objects"

// ═══════════════════════════════════════════════════════
// SYSTEM HEALTH CHECK
// ═══════════════════════════════════════════════════════
let HL_SEV_FILTER=null, HL_MONITOR_IVS={};

async function _hlSyncFromConfig(){
  try{
    const r=await fetch('/api/v1/deploy-config');
    const cfg=await r.json();
    G('hlHost').value=cfg.databricks_host||'';
    G('hlToken').value=cfg.databricks_token||'';
    const src=cfg.source||{};
    G('hlSrcServer').value=src.server||'';
    G('hlSrcDb').value=src.database||'';
    G('hlSrcUser').value=src.username||'';
    G('hlSrcPass').value=src.password||'';
    const lbl=G('hlConnLabel');
    if(lbl) lbl.textContent=(cfg.databricks_host||'').replace('https://','').replace(/\/$/,'') + ' \u2022 ' + (src.server||'').split('.')[0] + '/' + (src.database||'');
  }catch(e){console.warn('hlSyncFromConfig',e);}
}

async function hlFetchRecentRuns(){
  const sel=G('hlRunId');
  const prev=sel.value;
  sel.innerHTML='<option value="">Loading runs\u2026</option>';
  try{
    const r=await fetch('/api/v1/healer/recent-runs');
    const d=await r.json();
    if(!d.success||!d.runs||!d.runs.length){
      sel.innerHTML='<option value="">-- No runs found --</option>';
      return;
    }
    sel.innerHTML='<option value="">-- Select a Run ID --</option>';
    d.runs.forEach(run=>{
      const st=run.start_time?new Date(run.start_time).toLocaleString():'';
      const state=run.result_state||run.life_cycle||'';
      const stateColor=state==='SUCCESS'?'\u2705':state==='FAILED'?'\u274c':state==='RUNNING'?'\u23f3':'\u2022';
      const name=run.run_name||('Job '+run.job_id);
      const opt=document.createElement('option');
      opt.value=run.run_id;
      opt.textContent=`${stateColor} #${run.run_id} \u2014 ${name} (${state}) ${st}`;
      sel.appendChild(opt);
    });
    if(prev) sel.value=prev;
  }catch(e){
    sel.innerHTML='<option value="">-- Error loading runs --</option>';
    console.warn('hlFetchRecentRuns',e);
  }
}

function _hlCreds(){
  return {
    host:        G('hlHost').value.trim(),
    token:       G('hlToken').value.trim(),
    server:      G('hlSrcServer').value.trim(),
    database:    G('hlSrcDb').value.trim(),
    source_type: 'sqlserver',
    username:    G('hlSrcUser').value.trim(),
    password:    G('hlSrcPass').value.trim(),
  };
}

// ── Health Check ───────────────────────────────────
async function hlRunHealthCheck(){
  const btn=G('btnHlCheck');
  btn.disabled=true; btn.textContent='Checking…';
  G('hlChecks').innerHTML='<div class="loading-state"><div class="spin spin-lg"></div><span>Running health diagnostics…</span></div>';
  try{
    await _hlSyncFromConfig();
    const c=_hlCreds();
    const r=await fetch('/api/v1/healer/health-check',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(c)});
    const d=await r.json();
    if(!d.success){G('hlChecks').innerHTML=`<div class="alert a-err"><span class="a-ico">✕</span>${d.error||'Health check failed'}</div>`;return;}

    // Update overall status
    const pulse=G('hlPulse');
    const overall=G('hlOverall');
    pulse.className='heal-pulse '+(d.overall||'unknown');
    overall.className='heal-overall '+(d.overall||'unknown');
    overall.textContent=d.overall||'Unknown';

    // Render checks
    const checks=d.checks||[];
    G('hlChecks').innerHTML=checks.map(c=>
      `<div class="hl-check-row">
        <div class="hl-check-dot ${c.status}"></div>
        <div class="hl-check-name">${escHtml(c.name)}</div>
        <div class="hl-check-detail">${escHtml(c.detail)}</div>
      </div>`
    ).join('');

    toast(`Health: ${d.overall} (${checks.length} checks)`,'tok');
    hlRefreshHistory();
  }catch(e){G('hlChecks').innerHTML=`<div class="alert a-err"><span class="a-ico">✕</span>${e.message}</div>`;toast(e.message,'terr');}
  finally{
    btn.disabled=false;
    btn.innerHTML='<svg viewBox="0 0 24 24"><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></svg> Run Health Check';
  }
}

// ── Diagnose ───────────────────────────────────────
async function hlDiagnose(){
  const text=G('hlErrorText').value.trim();
  if(!text){toast('Paste an error message to diagnose.','tinfo');return;}
  const btn=G('btnHlDiagnose');
  btn.disabled=true; btn.textContent='Analyzing…';
  G('hlDiagResult').style.display='none';
  try{
    const r=await fetch('/api/v1/healer/diagnose',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({error_text:text})});
    const d=await r.json();
    if(!d.success){G('hlDiagResult').style.display='';G('hlDiagResult').innerHTML=`<div class="alert a-err"><span class="a-ico">✕</span>${d.error||'Diagnosis failed'}</div>`;return;}

    const sev=d.severity||'info';
    const sevIcons={info:'ℹ',warning:'⚠',error:'✕',critical:'⚡'};
    G('hlDiagResult').style.display='';
    G('hlDiagResult').innerHTML=`
      <div class="hl-diag-card ${sev}">
        <div class="hl-diag-hdr">
          <span class="hl-diag-sev ${sev}">${sevIcons[sev]||'·'} ${sev}</span>
          <span class="hl-diag-cat">${d.category||'UNKNOWN'}</span>
        </div>
        <div class="hl-diag-desc">${escHtml(d.description||'')}</div>
        <div class="hl-diag-rec">
          <strong>🔧 Recommendation:</strong> ${escHtml(d.recommendation||'')}
        </div>
        <div style="margin-top:8px;display:flex;gap:6px;">
          <button class="btn btn-primary btn-xs" onclick="hlExecuteHeal('${d.action||'notify'}','${d.category||''}')">
            ⚡ Auto-Heal: ${d.action||'notify'}
          </button>
          <button class="btn btn-ghost btn-xs" onclick="hlExecuteHeal('skip_table','${d.category||''}')">
            ⏭ Skip & Continue
          </button>
        </div>
      </div>`;
    toast(`Diagnosed: ${d.category} (${sev})`,'tok');
    hlRefreshHistory();
  }catch(e){
    G('hlDiagResult').style.display='';
    G('hlDiagResult').innerHTML=`<div class="alert a-err"><span class="a-ico">✕</span>${e.message}</div>`;
  }finally{
    btn.disabled=false;
    btn.innerHTML='<svg viewBox="0 0 24 24"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg> Diagnose & Recommend';
  }
}

// ── Execute Heal ───────────────────────────────────
async function hlExecuteHeal(action, category){
  const c=_hlCreds();
  toast(`Executing heal: ${action}…`,'tinfo');
  try{
    const r=await fetch('/api/v1/healer/heal',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({action, host:c.host, token:c.token, context:{category, job_key:'manual_'+Date.now()}})});
    const d=await r.json();
    if(d.success!==false){
      toast(`✓ Heal "${action}": ${d.message||'done'}`,'tok',4000);
    }else{
      toast(`✕ Heal failed: ${d.message||d.error||'unknown'}`,'terr',4000);
    }
    hlRefreshHistory();
  }catch(e){toast(e.message,'terr');}
}

// ── Restore Points ─────────────────────────────────
async function hlCreateRp(){
  const name=G('hlRpName').value.trim();
  if(!name){toast('Enter a restore point name.','tinfo');return;}
  try{
    const r=await fetch('/api/v1/healer/restore-point',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({key:name, metadata:{created_by:'user',source:'ui'}})});
    const d=await r.json();
    if(d.success){toast(`Restore point "${name}" created!`,'tok');G('hlRpName').value='';hlLoadRps();}
    else toast(d.error||'Failed','terr');
  }catch(e){toast(e.message,'terr');}
}

async function hlLoadRps(){
  try{
    const r=await fetch('/api/v1/healer/restore-points');
    const d=await r.json();
    const rps=d.restore_points||[];
    G('hlRpCount').style.display=rps.length?'':'none';
    G('hlRpCount').textContent=rps.length+' saved';
    if(!rps.length){G('hlRpList').innerHTML='<div class="empty" style="padding:20px;"><div class="empty-ico">📌</div><div class="empty-t">No Restore Points</div><div class="empty-s">Create one before running pipelines.</div></div>';return;}
    G('hlRpList').innerHTML=rps.map(rp=>
      `<div class="hl-rp-row">
        <div style="color:var(--blue);flex-shrink:0;">📌</div>
        <div class="hl-rp-name">${escHtml(rp.key)}</div>
        <div class="hl-rp-time">${new Date(rp.timestamp).toLocaleString()}</div>
        <div class="hl-rp-del" onclick="hlDeleteRp('${rp.key}')">✕</div>
      </div>`
    ).join('');
  }catch(e){toast(e.message,'terr');}
}

async function hlDeleteRp(key){
  try{
    await fetch(`/api/v1/healer/restore-point/${encodeURIComponent(key)}`,{method:'DELETE'});
    toast(`Restore point "${key}" deleted.`,'tok');
    hlLoadRps();
  }catch(e){toast(e.message,'terr');}
}

// ── Monitors ───────────────────────────────────────
async function hlStartMonitor(){
  const runId=G('hlRunId').value.trim();
  if(!runId){toast('Select a Databricks Run to monitor.','tinfo');return;}
  await _hlSyncFromConfig();
  const c=_hlCreds();
  const autoHeal=G('hlAutoHeal').checked;
  try{
    const r=await fetch('/api/v1/healer/monitor/start',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({run_id:parseInt(runId), host:c.host, token:c.token, auto_heal:autoHeal})});
    const d=await r.json();
    if(d.success){
      toast(`Monitor started for run ${runId}`,'tok');
      G('hlRunId').value='';
      const mon=d.monitor;
      // Start polling
      HL_MONITOR_IVS[mon.monitor_id]=setInterval(()=>hlPollMonitor(mon.monitor_id),5000);
      hlRefreshMonitors();
    }else{toast(d.error||'Failed','terr');}
  }catch(e){toast(e.message,'terr');}
}

async function hlPollMonitor(monitorId){
  const c=_hlCreds();
  try{
    const r=await fetch(`/api/v1/healer/monitor/check/${monitorId}`,{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({host:c.host,token:c.token})});
    const d=await r.json();
    if(d.success){
      const mon=d.monitor;
      if(mon.status==='completed'||mon.status==='stopped'||mon.status==='failed'){
        clearInterval(HL_MONITOR_IVS[monitorId]);
        delete HL_MONITOR_IVS[monitorId];
        if(mon.status==='completed')toast(`Run ${mon.run_id} completed!`,'tok');
        else if(mon.status==='failed')toast(`Run ${mon.run_id} failed!`,'terr');
      }
      hlRefreshMonitors();
      hlRefreshHistory();
    }
  }catch(e){/* silently retry on next poll */}
}

async function hlRefreshMonitors(){
  try{
    const r=await fetch('/api/v1/healer/monitors');
    const d=await r.json();
    const mons=d.monitors||[];
    if(!mons.length){G('hlMonitors').innerHTML='<div style="font-size:12px;color:var(--t4);text-align:center;padding:10px;">No active monitors</div>';return;}
    G('hlMonitors').innerHTML=mons.map(m=>{
      const heals=m.heals||[];
      const lastEvt=(m.events||[]).slice(-1)[0];
      return `<div class="hl-monitor-row">
        <div class="hl-mon-status ${m.status||'watching'}"></div>
        <div style="flex:1;">
          <div style="font-size:12px;font-weight:600;color:var(--t1);">Run #${m.run_id}</div>
          <div style="font-size:10.5px;color:var(--t3);">${m.status||'watching'}${heals.length?' · '+heals.length+' heal(s)':''}${lastEvt?' · '+lastEvt.msg.substring(0,60):''}</div>
        </div>
        <button class="btn btn-ghost btn-xs" onclick="hlStopMonitor('${m.monitor_id}')">Stop</button>
      </div>`;
    }).join('');
  }catch(e){/* ignore */}
}

async function hlStopMonitor(monitorId){
  try{
    await fetch(`/api/v1/healer/monitor/stop/${monitorId}`,{method:'POST'});
    if(HL_MONITOR_IVS[monitorId]){clearInterval(HL_MONITOR_IVS[monitorId]);delete HL_MONITOR_IVS[monitorId];}
    hlRefreshMonitors();
    toast('Monitor stopped.','tok');
  }catch(e){toast(e.message,'terr');}
}

// ── Healing Rules ──────────────────────────────────
async function hlLoadRules(){
  try{
    const r=await fetch('/api/v1/healer/rules');
    const d=await r.json();
    const rules=d.rules||[];
    G('hlRuleCount').style.display=rules.length?'':'none';
    G('hlRuleCount').textContent=rules.filter(r=>r.enabled).length+' active';
    G('hlRules').innerHTML=rules.map(rule=>
      `<div class="hl-rule-row">
        <div class="hl-rule-toggle ${rule.enabled?'on':''}" onclick="hlToggleRule(${rule.id},${!rule.enabled},this)" title="${rule.enabled?'Disable':'Enable'}"></div>
        <div class="hl-rule-name" title="${escHtml(rule.description||'')}">${escHtml(rule.name)}</div>
        <div class="hl-rule-cat">${rule.category}</div>
      </div>`
    ).join('');
  }catch(e){toast(e.message,'terr');}
}

async function hlToggleRule(id, enabled, el){
  try{
    const r=await fetch('/api/v1/healer/rules/toggle',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({rule_id:id, enabled})});
    const d=await r.json();
    if(d.success){
      el.className='hl-rule-toggle '+(enabled?'on':'');
      toast(`Rule ${d.rule.name}: ${enabled?'enabled':'disabled'}`,'tok');
      hlLoadRules();
    }
  }catch(e){toast(e.message,'terr');}
}

// ── Audit History ──────────────────────────────────
function hlFilterSev(sev, btn){
  HL_SEV_FILTER=sev;
  document.querySelectorAll('.hl-sev-btn').forEach(b=>b.classList.remove('active'));
  if(btn)btn.classList.add('active');
  hlRefreshHistory();
}

async function hlRefreshHistory(){
  try{
    const url='/api/v1/healer/history?limit=100'+(HL_SEV_FILTER?'&severity='+HL_SEV_FILTER:'');
    const r=await fetch(url);
    const d=await r.json();
    const items=d.history||[];
    G('hlEventCount').style.display=items.length?'':'none';
    G('hlEventCount').textContent=items.length+' events';
    if(!items.length){G('hlHistory').innerHTML='<div style="color:var(--t4);text-align:center;padding:16px;">No events yet — run a health check to start.</div>';return;}
    G('hlHistory').innerHTML=items.map(h=>{
      const t=new Date(h.timestamp).toLocaleTimeString();
      return `<div class="hl-evt">
        <div class="hl-evt-sev ${h.severity}"></div>
        <div class="hl-evt-time">${t}</div>
        <div class="hl-evt-msg">${escHtml(h.message)}${h.action_taken?' <span style="color:var(--blue);font-weight:600;">→ '+h.action_taken+'</span>':''}${h.success===true?' <span style="color:var(--green-fg);">✓</span>':h.success===false?' <span style="color:var(--red-fg);">✕</span>':''}</div>
      </div>`;
    }).join('');
    G('hlHistory').scrollTop=0;
  }catch(e){/* ignore */}
}

async function hlClearHistory(){
  try{
    await fetch('/api/v1/healer/history/clear',{method:'POST'});
    G('hlHistory').innerHTML='<div style="color:var(--t4);text-align:center;padding:16px;">History cleared.</div>';
    G('hlEventCount').style.display='none';
    toast('History cleared.','tok');
  }catch(e){toast(e.message,'terr');}
}

async function hlRefreshStats(){
  try{
    const r=await fetch('/api/v1/healer/stats');
    const d=await r.json();
    const box=G('hlStatsBox');
    box.style.display='';
    box.innerHTML=`
      <div class="hl-stats-grid">
        <div class="hl-stat-box"><div class="n" style="color:var(--blue-fg);">${d.total_events||0}</div><div class="l">Total Events</div></div>
        <div class="hl-stat-box"><div class="n" style="color:var(--green-fg);">${d.heals_succeeded||0}</div><div class="l">Heals OK</div></div>
        <div class="hl-stat-box"><div class="n" style="color:var(--red-fg);">${d.heals_failed||0}</div><div class="l">Heals Failed</div></div>
        <div class="hl-stat-box"><div class="n" style="color:var(--amber-fg);">${d.active_monitors||0}</div><div class="l">Monitors</div></div>
        <div class="hl-stat-box"><div class="n" style="color:var(--t1);">${d.restore_points||0}</div><div class="l">Restore Pts</div></div>
        <div class="hl-stat-box"><div class="n" style="color:var(--accent-primary);">${d.active_rules||0}</div><div class="l">Active Rules</div></div>
      </div>`;
  }catch(e){toast(e.message,'terr');}
}

// Auto-load rules & restore points when healer tab is first opened
(function(){
  const origSwitch=switchTab;
  let healerLoaded=false;
  switchTab=function(id,btn){
    origSwitch(id,btn);
    if(id==='healer'&&!healerLoaded){
      healerLoaded=true;
      hlLoadRules();
      hlLoadRps();
    }
    if(id==='wf-scheduler'){
      if(typeof schLoadJobs==='function') schLoadJobs();
      if(typeof schRefresh==='function') schRefresh();
    }
    if(id==='wf-metadata'){
      // Restore Deploy Notebooks card visibility on page reload / tab switch
      if(typeof wfCheckMetaStatus==='function') wfCheckMetaStatus();
    }
  };
})();

// ═════════════════════════════════════════════════════════════════════════════
// AI WORKFLOW MANAGER — JAVASCRIPT
// ═════════════════════════════════════════════════════════════════════════════

/* ─── MetadataFlow — Databricks Persistence ─── */
let _wfMetaReady=false;
let _wfSelectedGroups=new Set(); // multi-select for batch Databricks run

let _cachedDeployConfig=null;
async function _ensureDeployConfig(){
  if(_cachedDeployConfig) return _cachedDeployConfig;
  try{
    const r=await fetch('/api/v1/deploy-config');const d=await r.json();
    if(d.success&&d.config) _cachedDeployConfig=d.config;
  }catch(e){}
  return _cachedDeployConfig||{};
}
/* ── Sync hidden wfDbr* fields from Settings / deployconfig and update label ── */
async function _wfSyncHiddenFields(){
  const cfg=await _ensureDeployConfig();
  const h=G('cfgDbrHost')?.value?.trim()||cfg.databricks_host||G('dbHost')?.value?.trim()||'';
  const t=G('cfgDbrToken')?.value?.trim()||cfg.databricks_token||G('dbToken')?.value?.trim()||'';
  const c=G('cfgMetaCatalog')?.value?.trim()||cfg.metadata_catalog||'admin_source';
  const s=G('cfgMetaSchema')?.value?.trim()||cfg.metadata_schema||'configtables';
  if(G('wfDbrHost'))G('wfDbrHost').value=h;
  if(G('wfDbrToken'))G('wfDbrToken').value=t;
  if(G('wfDbrCatalog'))G('wfDbrCatalog').value=c;
  if(G('wfDbrSchema'))G('wfDbrSchema').value=s;
  // Auto-populate Landing Path from volume_path config
  const vPath=G('cfgVolPath')?.value?.trim()||cfg.volume_path||'';
  if(vPath){
    if(G('wfNbLandingPath'))G('wfNbLandingPath').value=vPath;
    if(G('mdlLandingPath'))G('mdlLandingPath').value=vPath;
  }
  // Auto-populate Pipeline Mode from cdc.dlt_mode config
  const dltMode=G('cfgDltMode')?.value?.trim()||(cfg.cdc&&cfg.cdc.dlt_mode)||'standard';
  const pmSel=G('wfNbPipelineMode');
  if(pmSel){
    pmSel.value=dltMode;
    pmSel.dispatchEvent(new Event('change'));
  }
  // Update mode hint text
  const hint=G('wfNbModeHint');
  if(hint) hint.textContent=dltMode==='dlt'?'DLT: 3 notebooks (Extract, DLT Pipeline, DLT Orchestrator) \u2014 Auto Loader + Expectations':'Standard: 4 notebooks (Extract, Bronze, Silver, Orchestrator)';
  _wfUpdateConnLabel();
}
function _wfUpdateConnLabel(){
  const lbl=G('wfMetaConnLabel');if(!lbl)return;
  const c=_wfDbrCreds();
  if(c.host){
    const short=c.host.replace(/^https?:\/\//,'').replace(/\/$/,'');
    lbl.textContent=short+'  •  '+c.catalog+'.'+c.schema;
    lbl.style.color='var(--t2)';
  } else {
    lbl.innerHTML='<span style=\"color:var(--red);\">No connection configured — go to <strong>Settings</strong></span>';
  }
}
function _wfDbrCreds(){
  // Read from Settings fields → hidden fields → deployconfig cache
  const host= G('cfgDbrHost')?.value?.trim() || G('wfDbrHost')?.value?.trim() || '';
  const token= G('cfgDbrToken')?.value?.trim() || G('wfDbrToken')?.value?.trim() || '';
  const catalog= G('cfgMetaCatalog')?.value?.trim() || G('wfDbrCatalog')?.value?.trim() || 'admin_source';
  const schema= G('cfgMetaSchema')?.value?.trim() || G('wfDbrSchema')?.value?.trim() || 'configtables';
  return {host,token,catalog,schema};
}
async function _wfDbrCredsWithFallback(){
  let c=_wfDbrCreds();
  if(!c.host||!c.token){
    const cfg=await _ensureDeployConfig();
    c.host=c.host||cfg.databricks_host||'';
    c.token=c.token||cfg.databricks_token||'';
    c.catalog=c.catalog||cfg.metadata_catalog||'admin_source';
    c.schema=c.schema||cfg.metadata_schema||'configtables';
    // Populate hidden fields + Settings fields so subsequent calls work
    if(c.host){if(G('wfDbrHost'))G('wfDbrHost').value=c.host; if(G('cfgDbrHost')&&!G('cfgDbrHost').value)G('cfgDbrHost').value=c.host;}
    if(c.token){if(G('wfDbrToken'))G('wfDbrToken').value=c.token; if(G('cfgDbrToken')&&!G('cfgDbrToken').value)G('cfgDbrToken').value=c.token;}
    if(c.catalog){if(G('wfDbrCatalog'))G('wfDbrCatalog').value=c.catalog; if(G('cfgMetaCatalog')&&!G('cfgMetaCatalog').value)G('cfgMetaCatalog').value=c.catalog;}
    if(c.schema){if(G('wfDbrSchema'))G('wfDbrSchema').value=c.schema; if(G('cfgMetaSchema')&&!G('cfgMetaSchema').value)G('cfgMetaSchema').value=c.schema;}
    _wfUpdateConnLabel();
  }
  return c;
}
function _wfSourceConfig(){
  return {
    source_type: G('wfSrcType')?.value||'sqlserver',
    server:      G('wfSrcServer')?.value?.trim()||'',
    database:    G('wfSrcDb')?.value?.trim()||'',
    username:    G('wfSrcUser')?.value?.trim()||'',
  };
}
function _wfTargetConfig(){
  const c=_wfDbrCreds();
  // Multi-catalog fields (volumes / bronze / silver)
  const vc=G('wfVolCatalog')?.value?.trim()||'';
  const bc=G('wfBrzCatalog')?.value?.trim()||'';
  const sc=G('wfSlvCatalog')?.value?.trim()||'';
  const ts=G('wfTgtSchema')?.value?.trim()||'';
  const cfg = {
    host:             c.host,
    metadata_catalog: c.catalog,
    metadata_schema:  c.schema,
    catalog:          bc || c.catalog,
    schema:           ts || c.schema,
    workspace_path:   G('wfNbWsPath')?.value?.trim()||'/Shared/MetadataPipeline',
    landing_path:     G('wfNbLandingPath')?.value?.trim()||'/mnt/landing',
  };
  // Always include multi-catalog keys so notebooks know the real targets
  if(vc) cfg.volumes_catalog=vc;
  if(bc) cfg.bronze_catalog=bc;
  if(sc) cfg.silver_catalog=sc;
  if(ts) cfg.target_schema=ts;
  return cfg;
}

async function wfCreateMetadataFlow(){
  const c=_wfDbrCreds();
  if(!c.host||!c.token){toast('Enter Databricks host and token','terr');return;}
  const btn=G('btnWfMeta');btn.disabled=true;btn.textContent='Provisioning tables…';
  const dot=G('wfMetaDot'),lbl=G('wfMetaLabel'),msg=G('wfMetaMsg');
  dot.style.background='#f59e0b';lbl.textContent='Provisioning…';
  msg.innerHTML='<span style="color:var(--amber);">Creating 5 Delta tables in '+c.catalog+'.'+c.schema+'…</span>';
  try{
    const r=await fetch('/api/v1/workflow/metadata/init',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(c)});
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Failed');
    dot.style.background='#10b981';lbl.textContent='Initialized — '+c.catalog+'.'+c.schema;
    msg.innerHTML='<span style="color:var(--green);">✓ '+d.message+'</span>';
    toast(d.message,'tok');
    _wfMetaReady=true;
    G('btnWfMetaLoad').style.display='';
    G('btnWfMetaSync').style.display='';
    G('wfMetaTablesInfo').style.display='block';
    if(G('wfNotebookCard'))G('wfNotebookCard').style.display='';
    wfCheckMetaStatus();
  }catch(e){
    dot.style.background='#ef4444';lbl.textContent='Failed';
    msg.innerHTML='<span style="color:var(--red);">'+e.message+'</span>';
    toast(e.message,'terr');
  }
  btn.disabled=false;
  btn.innerHTML='<svg viewBox="0 0 24 24" style="width:14px;height:14px;"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg> Create MetadataFlow';
}

async function wfCheckMetaStatus(){
  try{
    const r=await fetch('/api/v1/workflow/metadata/status');
    const d=await r.json();
    if(!d.success)return;
    const dot=G('wfMetaDot'),lbl=G('wfMetaLabel');
    if(d.initialized){
      dot.style.background='#10b981';lbl.textContent='Active — '+(d.catalog||'main')+'.'+(d.schema||'default');
      _wfMetaReady=true;
      G('btnWfMetaLoad').style.display='';
      G('btnWfMetaSync').style.display='';
      G('wfMetaTablesInfo').style.display='block';
      if(G('wfNotebookCard'))G('wfNotebookCard').style.display='';
      // Update table counts
      const tbls=d.tables||{};
      const m={wf_pipeline_metadata:'wfMetaTblPipelines',wf_job_metadata:'wfMetaTblJobs',wf_job_metadatahis:'wfMetaTblJobHis',wf_run_history:'wfMetaTblRuns',wf_watermark_metadata:'wfMetaTblWm',wf_source_tables:'wfMetaTblSrc',wf_scheduler_config:'wfMetaTblSchCfg',wf_scheduler_history:'wfMetaTblSchHis'};
      for(const[t,info]of Object.entries(tbls)){
        const el=G(m[t]);
        if(el){
          if(info.exists) el.textContent=info.rows;
          else el.textContent='✕';
        }
      }
    }else{
      dot.style.background='#6b7280';lbl.textContent='Not Configured';
    }
  }catch(e){console.error('wfCheckMetaStatus',e);}
}

async function wfLoadMetadata(){
  const btn=G('btnWfMetaLoad');btn.disabled=true;btn.textContent='Loading…';
  try{
    const r=await fetch('/api/v1/workflow/metadata/load',{method:'POST'});
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Failed');
    const l=d.loaded;
    toast('Loaded '+l.pipelines+' pipelines, '+l.jobs+' jobs, '+l.watermarks+' watermarks from Databricks','tok');
    G('wfMetaMsg').innerHTML='<span style="color:var(--green);">✓ Loaded: '+l.pipelines+' pipelines, '+l.jobs+' jobs, '+l.watermarks+' watermarks</span>';
    wfRefreshAll();
  }catch(e){
    toast(e.message,'terr');
    G('wfMetaMsg').innerHTML='<span style="color:var(--red);">'+e.message+'</span>';
  }
  btn.disabled=false;btn.innerHTML='<svg viewBox="0 0 24 24" style="width:12px;height:12px;"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 11-2.12-9.36L23 10"/></svg> Load Existing Metadata';
}

async function wfSyncMetadata(){
  const btn=G('btnWfMetaSync');btn.disabled=true;btn.textContent='Syncing…';
  try{
    // Fix 3: dispatch async; poll /sync-status until done.
    const r=await fetch('/api/v1/workflow/metadata/sync',{method:'POST'});
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Failed');
    // If backend returned synced counts directly (sync mode fallback), finish.
    if(d.synced){
      const s=d.synced;
      toast('Synced '+s.pipelines+' pipelines, '+s.jobs+' jobs, '+s.runs+' runs to Databricks','tok');
      G('wfMetaMsg').innerHTML='<span style="color:var(--green);">\u2713 Synced: '+s.pipelines+' pipelines, '+s.jobs+' jobs, '+s.runs+' runs</span>';
    }else if(d.task_id){
      const taskId=d.task_id;
      G('wfMetaMsg').innerHTML='<span style="color:var(--muted);">\u23F3 Sync running in background\u2026</span>';
      // Poll every 2s, max 5 minutes
      let task=null;
      for(let i=0;i<150;i++){
        await new Promise(res=>setTimeout(res,2000));
        const pr=await fetch('/api/v1/workflow/metadata/sync-status/'+encodeURIComponent(taskId));
        const pd=await pr.json();
        if(!pd.success){throw new Error(pd.error||'Poll failed');}
        task=pd.task;
        G('wfMetaMsg').innerHTML='<span style="color:var(--muted);">\u23F3 '+(task.progress||task.status)+'\u2026</span>';
        if(task.status==='succeeded'||task.status==='failed')break;
      }
      if(!task||task.status!=='succeeded'){throw new Error((task&&task.error)||'Sync did not complete');}
      const s=task.synced||{};
      toast('Synced '+(s.pipelines||0)+' pipelines, '+(s.jobs||0)+' jobs, '+(s.runs||0)+' runs to Databricks','tok');
      G('wfMetaMsg').innerHTML='<span style="color:var(--green);">\u2713 Synced: '+(s.pipelines||0)+' pipelines, '+(s.jobs||0)+' jobs, '+(s.runs||0)+' runs</span>';
    }
    wfCheckMetaStatus();
  }catch(e){
    toast(e.message,'terr');
    G('wfMetaMsg').innerHTML='<span style="color:var(--red);">'+e.message+'</span>';
  }
  btn.disabled=false;btn.innerHTML='<svg viewBox="0 0 24 24" style="width:12px;height:12px;"><polyline points="16 16 12 12 8 16"/><line x1="12" y1="12" x2="12" y2="21"/><path d="M20.39 18.39A5 5 0 0018 9h-1.26A8 8 0 103 16.3"/></svg> Sync to Databricks';
}

/* Save discovered source tables to Databricks when Discover is clicked */
async function _wfSaveSourcesToDatabricks(){
  if(!_wfMetaReady||!WF_SRC_TABLES.length)return;
  try{
    const c=_wfSrcCreds();
    await fetch('/api/v1/workflow/metadata/save-sources',{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({tables:WF_SRC_TABLES,source_config:c})
    });
  }catch(e){console.error('_wfSaveSourcesToDatabricks',e);}
}

/* ─── Deploy Metadata Notebooks ─── */
let _wfNbDeployed=false;

// Pipeline mode hint + DQ panel toggle
(function(){
  const sel=document.getElementById('wfNbPipelineMode');
  if(sel)sel.addEventListener('change',function(){
    const h=document.getElementById('wfNbModeHint');
    const dqStd=document.getElementById('wfDqStandard');
    const dqDlt=document.getElementById('wfDqDlt');
    const dqTag=document.getElementById('wfDqModeTag');
    const isDlt=this.value==='dlt';
    if(h)h.textContent=isDlt
      ?'DLT: 3 notebooks (Extract, DLT Pipeline, DLT Orchestrator) — Auto Loader + Expectations'
      :'Standard: 4 notebooks (Extract, Bronze, Silver, Orchestrator)';
    if(dqStd)dqStd.style.display=isDlt?'none':'block';
    if(dqDlt)dqDlt.style.display=isDlt?'block':'none';
    if(dqTag){dqTag.textContent=isDlt?'DLT':'STANDARD';dqTag.style.background=isDlt?'#f59e0b':'var(--blue)';}
    // Update pipeline mode tag in Active Pipeline Groups
    const pTag=document.getElementById('wfPipelineModeTag');
    if(pTag){
      pTag.textContent=isDlt?'⚡ Delta Live Tables (DLT)':'🔥 Apache Spark';
      pTag.style.background=isDlt?'#f59e0b':'#3b82f6';
    }
    // Update Quick Create pipeline preview & button text
    const preview=document.getElementById('wfQPipelinePreview');
    if(preview){
      if(isDlt){
        preview.innerHTML='<div style="color:var(--t4);font-weight:600;margin-bottom:2px;">Per table creates:</div>'+
          '<div><span style="color:var(--t4);">1.</span> <span style="color:#2563eb;">ExtractTo_</span><span style="color:var(--amber);font-weight:600;">TableName</span></div>'+
          '<div><span style="color:var(--t4);">2.</span> <span style="color:#f59e0b;">DLT_BronzeSilver_</span><span style="color:var(--amber);font-weight:600;">TableName</span></div>';
      }else{
        preview.innerHTML='<div style="color:var(--t4);font-weight:600;margin-bottom:2px;">Per table creates:</div>'+
          '<div><span style="color:var(--t4);">1.</span> <span style="color:#2563eb;">SqlExtract_</span><span style="color:var(--amber);font-weight:600;">TableName</span></div>'+
          '<div><span style="color:var(--t4);">2.</span> <span style="color:#d97706;">LandingToBronze_</span><span style="color:var(--amber);font-weight:600;">TableName</span></div>'+
          '<div><span style="color:var(--t4);">3.</span> <span style="color:#059669;">BronzeToSilver_</span><span style="color:var(--amber);font-weight:600;">TableName</span></div>';
      }
    }
    const btnLbl=document.getElementById('btnWfQuickLabel');
    if(btnLbl)btnLbl.textContent=isDlt?'Create 2-Stage DLT Pipeline':'Create 3-Stage Medallion Pipeline';
    // Re-render pipelines to update per-group badges
    if(typeof wfRefreshPipelines==='function')wfRefreshPipelines();
  });
  // Fire once on load to sync initial state
  sel.dispatchEvent(new Event('change'));
})();

async function wfDeployNotebooks(){
  const c=await _wfDbrCredsWithFallback();
  if(!c.host||!c.token){toast('Configure Databricks connection in Settings first','terr');return;}
  const btn=G('btnWfDeployNb');btn.disabled=true;btn.textContent='Deploying…';
  const dot=G('wfNbDot'),lbl=G('wfNbLabel'),msg=G('wfNbMsg');
  const pipelineMode=(G('wfNbPipelineMode')||{}).value||'standard';
  dot.style.background='#f59e0b';lbl.textContent='Deploying…';
  try{
    const r=await fetch('/api/v1/workflow/notebooks/deploy',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({
        host:c.host,token:c.token,catalog:c.catalog,schema:c.schema,
        workspace_path:G('wfNbWsPath').value.trim()||'/Shared/MetadataPipeline',
        landing_path:G('wfNbLandingPath').value.trim()||'/mnt/landing',
        pipeline_mode:pipelineMode,
        cdc_mode:(G('cfgCdcMode')||{}).value||'watermark',
        primary_keys:(G('cfgPrimaryKeys')||{}).value ? G('cfgPrimaryKeys').value.split(',').map(s=>s.trim()).filter(Boolean) : [],
        recon_catalog:G('cfgReconCatalog')?.value?.trim()||'reconciliation',
        recon_schema:G('cfgReconSchema')?.value?.trim()||'hr',
        recon_table:G('cfgReconTable')?.value?.trim()||'ReconcilationDetails',
        recon_location:G('cfgReconLocation')?.value?.trim()||'',
        log_catalog:G('cfgLogCatalog')?.value?.trim()||'logging',
        log_schema:G('cfgLogSchema')?.value?.trim()||'hr',
        log_table:G('cfgLogTable')?.value?.trim()||'ExecutionLog',
        log_location:G('cfgLogLocation')?.value?.trim()||'',
      })
    });
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Deploy failed');
    dot.style.background='#10b981';lbl.textContent='Deployed — '+d.workspace_path;
    msg.innerHTML='<span style="color:var(--green);">✓ '+d.message+'</span>';
    toast(d.message,'tok');
    _wfNbDeployed=true;
    // Show results grid
    const grid=G('wfNbResultGrid');
    G('wfNbResults').style.display='block';
    grid.innerHTML=(d.results||[]).map(nb=>{
      const ok=nb.success;
      return `<div style="padding:8px;background:${ok?'var(--green)':'var(--red)'}11;border:1px solid ${ok?'var(--green)':'var(--red)'}33;border-radius:var(--r-xs);text-align:center;">
        <div style="font-size:10px;font-weight:600;color:${ok?'var(--green)':'var(--red)'};">${ok?'✅':'❌'} ${nb.name}</div>
        <div style="font-size:9px;color:var(--t4);margin-top:2px;">${nb.layer} · ${nb.lines} lines</div>
        ${nb.path?'<div style="font-size:8px;color:var(--t4);margin-top:1px;">'+nb.path+'</div>':''}
      </div>`;
    }).join('');
  }catch(e){
    dot.style.background='#ef4444';lbl.textContent='Failed';
    msg.innerHTML='<span style="color:var(--red);">'+e.message+'</span>';
    toast(e.message,'terr');
  }
  btn.disabled=false;
  btn.innerHTML='<svg viewBox="0 0 24 24" style="width:14px;height:14px;"><polyline points="16 16 12 12 8 16"/><line x1="12" y1="12" x2="12" y2="21"/><path d="M20.39 18.39A5 5 0 0018 9h-1.26A8 8 0 103 16.3"/></svg> Deploy Notebooks';
}

async function wfCheckNbStatus(){
  try{
    const r=await fetch('/api/v1/workflow/notebooks/status');const d=await r.json();
    const dot=G('wfNbDot'),lbl=G('wfNbLabel');
    if(d.deployed){
      dot.style.background='#10b981';lbl.textContent='Deployed — '+d.workspace_path;
      _wfNbDeployed=true;
    }else{
      dot.style.background='#6b7280';lbl.textContent='Not Deployed';
    }
  }catch(e){console.error('wfCheckNbStatus',e);}
}

async function wfRunOnDatabricks(groupId, pwd){
  // Auto-detect readiness — if config has host/token, we can proceed
  if(!_wfNbDeployed||!_wfMetaReady){
    try{
      const cfgr=await fetch('/api/v1/deploy-config');const cfgd=await cfgr.json();
      if(cfgd.success&&cfgd.config?.databricks_host&&cfgd.config?.databricks_token){
        _wfMetaReady=true;
        _wfNbDeployed=true;
      }
    }catch(e){}
  }
  if(!_wfMetaReady){
    toast('Configure Databricks host & token in Settings or MetadataFlow first','terr');return false;
  }
  // Get Databricks credentials — try UI fields first, fallback to deployconfig
  let c=_wfDbrCreds();
  if(!c.host||!c.token){
    try{
      const cfgr=await fetch('/api/v1/deploy-config');const cfgd=await cfgr.json();
      if(cfgd.success&&cfgd.config){
        c.host=c.host||cfgd.config.databricks_host||'';
        c.token=c.token||cfgd.config.databricks_token||'';
        const cats=cfgd.config.catalogs||{};
        const firstCat=Object.keys(cats)[0]||'';
        c.catalog=c.catalog||firstCat||'main';
        c.schema=c.schema||(cats[firstCat]?.schemas?.[0])||'default';
      }
    }catch(e){}
  }
  if(!c.host||!c.token){toast('Databricks host & token required — configure in MetadataFlow or Settings','terr');return false;}
  // Get cluster — try UI dropdown first, fallback to auto-detect running cluster
  let clusterId=(G('wfClusterSelect')||{}).value||'';
  if(!clusterId){
    try{
      const clr=await fetch('/api/v1/workflow/clusters?host='+encodeURIComponent(c.host)+'&token='+encodeURIComponent(c.token));
      const cld=await clr.json();
      if(cld.success&&cld.clusters){
        const running=cld.clusters.find(cl=>cl.state==='RUNNING');
        if(running)clusterId=running.cluster_id;
      }
    }catch(e){}
  }
  if(!clusterId){toast('No running cluster found — start a cluster in Pipeline Studio or MetadataFlow','terr');return false;}
  // Get password — try UI field, then deployconfig
  if(pwd===undefined||pwd===''){
    pwd=(G('wfSrcPass')||{}).value||'';
    if(!pwd){
      try{
        const cfgr=await fetch('/api/v1/deploy-config');const cfgd=await cfgr.json();
        if(cfgd.success&&cfgd.config?.source)pwd=cfgd.config.source.password||'';
      }catch(e){}
    }
  }
  toast('Submitting pipeline to Databricks…','tinfo');
  try{
    const r=await fetch('/api/v1/workflow/pipelines/'+groupId+'/run-databricks',{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({
        host:c.host,token:c.token,catalog:c.catalog,schema:c.schema,
        cluster_id:clusterId,
        password:pwd,
        workspace_path:G('wfNbWsPath')?.value?.trim()||'/Shared/MetadataPipeline',
        landing_path:G('wfNbLandingPath')?.value?.trim()||'/mnt/landing',
        recon_catalog:G('cfgReconCatalog')?.value?.trim()||'reconciliation',
        recon_schema:G('cfgReconSchema')?.value?.trim()||'hr',
        recon_table:G('cfgReconTable')?.value?.trim()||'ReconcilationDetails',
        recon_location:G('cfgReconLocation')?.value?.trim()||'',
        log_catalog:G('cfgLogCatalog')?.value?.trim()||'logging',
        log_schema:G('cfgLogSchema')?.value?.trim()||'hr',
        log_table:G('cfgLogTable')?.value?.trim()||'ExecutionLog',
        log_location:G('cfgLogLocation')?.value?.trim()||'',
      })
    });
    const d=await r.json();
    if(!d.success)throw new Error(d.error||d.message||'Failed to submit');
    toast('Pipeline submitted to Databricks! Run ID: '+d.run_id,'tok');
    if(d.run_url){
      const msgEl=G('wfNbMsg');
      if(msgEl)msgEl.innerHTML='<span style="color:var(--green);">✓ Running → <a href="'+d.run_url+'" target="_blank" style="color:var(--blue);">View Run</a></span>';
    }
    // Auto-open pipeline logs panel if on Pipeline Studio
    if(typeof wfShowPipelineLogs==='function'){
      try{wfShowPipelineLogs(groupId, '');}catch(e){}
    }
    setTimeout(()=>wfRefreshAll(),2000);
    return true;
  }catch(e){
    console.error('wfRunOnDatabricks error:',e);
    toast(e.message,'terr');
    if(typeof wfShowPipelineLogs==='function'){
      try{wfShowPipelineLogs(groupId, '');}catch(ex){}
    }
    return false;
  }
}

/* ─── Multi-Select Pipeline Group Helpers ─── */
function wfToggleGroupSelect(groupId, checked){
  if(checked) _wfSelectedGroups.add(groupId);
  else _wfSelectedGroups.delete(groupId);
  const total=document.querySelectorAll('.wfGrpChk').length;
  _wfUpdateGroupToolbar(total);
}
function wfToggleSelectAllGroups(checked){
  document.querySelectorAll('.wfGrpChk').forEach(cb=>{
    cb.checked=checked;
    const gid=cb.dataset.gid;
    if(checked) _wfSelectedGroups.add(gid);
    else _wfSelectedGroups.delete(gid);
  });
  const total=document.querySelectorAll('.wfGrpChk').length;
  _wfUpdateGroupToolbar(total);
}
function _wfUpdateGroupToolbar(total){
  const n=_wfSelectedGroups.size;
  const lbl=G('wfGroupSelCount');
  if(lbl) lbl.textContent=n+' selected';
  const btn=G('btnRunSelectedDbr');
  if(btn) btn.style.display=n>0?'inline-flex':'none';
  const allCb=G('wfGroupSelectAll');
  if(allCb) allCb.checked=(total>0&&n===total);
}
async function wfRunSelectedOnDatabricks(){
  const ids=[..._wfSelectedGroups];
  if(!ids.length){toast('Select at least one pipeline group','terr');return;}
  toast('Submitting '+ids.length+' pipeline(s) to Databricks…','tinfo');
  // Submit ALL pipelines in parallel — don't await one-by-one
  const results=await Promise.allSettled(ids.map(gid=>wfRunOnDatabricks(gid)));
  const ok=results.filter(r=>r.status==='fulfilled'&&r.value===true).length;
  const fail=results.length-ok;
  _wfSelectedGroups.clear();
  wfRefreshPipelines();
  const msg=ok+' submitted'+(fail?' · '+fail+' failed':'');
  toast(msg, fail?'terr':'tok');
}

function switchAiSubTab(tab,btn){
  // Legacy redirect — sub-tabs replaced by separate pages
  if(tab==='jobs') switchTab('wf-jobs',G('nav-wf-jobs'));
  else switchTab('wf-pipelines',G('nav-wf-pipelines'));
}

function wfToggleWatermark(){
  const v=G('wfLoadType').value;
  G('wfWatermarkWrap').style.display=v==='incremental'?'block':'none';
}

/* ─── Data Source Connection & Table Discovery ─── */
let WF_SRC_TABLES=[];
let _wfSelectedQ=null; // legacy compat — multi-select uses _wfQSelected[]
let _wfSelectedJ=null; // selected table for Job Workflow

function _wfSrcCreds(){
  return {
    source_type: G('wfSrcType').value,
    server:      G('wfSrcServer').value.trim(),
    database:    G('wfSrcDb').value.trim(),
    username:    G('wfSrcUser').value.trim(),
    password:    G('wfSrcPass').value,
  };
}

async function _wfSaveSourceToConfig(){
  try{
    // Load existing config, merge source info, save back
    const lr=await fetch('/api/v1/deploy-config');
    const ld=await lr.json();
    const cfg=ld.success?ld.config:{};
    cfg.source={
      source_type: (G('wfSrcType')||{}).value||'sqlserver',
      server:      (G('wfSrcServer')||{}).value?.trim()||'',
      database:    (G('wfSrcDb')||{}).value?.trim()||'',
      username:    (G('wfSrcUser')||{}).value?.trim()||'',
      password:    (G('wfSrcPass')||{}).value||'',
    };
    await fetch('/api/v1/deploy-config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(cfg)});
  }catch(e){console.warn('Could not save source to config:',e);}
}

async function wfTestConnection(){
  const c=_wfSrcCreds();
  if(!c.server||!c.database||!c.username){toast('Enter server, database and username','terr');return;}
  const btn=G('btnWfTest');btn.disabled=true;btn.textContent='Testing…';
  const st=G('wfSrcStatus');
  st.innerHTML='<span style="color:var(--amber);">⏳ Testing…</span>';
  try{
    const r=await fetch('/api/v1/workflow/list-tables',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(c)});
    const d=await r.json();
    if(!r.ok||d.error)throw new Error(d.error||'Connection failed');
    st.innerHTML='<span style="color:var(--green);">🟢 Connected — '+d.total+' tables found</span>';
    toast('Connection successful! '+d.total+' tables available.','tok');
    // Auto-populate tables
    WF_SRC_TABLES=d.tables||[];
    G('wfSrcMsg').innerHTML='<span style="color:var(--green);">✓ '+WF_SRC_TABLES.length+' tables loaded</span>';
    // Auto-save source info to deployconfig.json
    _wfSaveSourceToConfig();
  }catch(e){
    st.innerHTML='<span style="color:var(--red);">🔴 Failed</span>';
    G('wfSrcMsg').innerHTML='<span style="color:var(--red);">'+e.message+'</span>';
    toast(e.message,'terr');
  }
  btn.disabled=false;btn.innerHTML='<svg viewBox="0 0 24 24" style="width:12px;height:12px;"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Test Connectivity';
}

async function wfFetchTables(){
  const c=_wfSrcCreds();
  if(!c.server||!c.database||!c.username){toast('Enter server, database and username','terr');return;}
  const btn=G('btnWfFetch');btn.disabled=true;btn.textContent='Discovering…';
  const st=G('wfSrcStatus');
  try{
    const r=await fetch('/api/v1/workflow/list-tables',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(c)});
    const d=await r.json();
    if(!r.ok||d.error)throw new Error(d.error||'Failed');
    WF_SRC_TABLES=d.tables||[];
    st.innerHTML='<span style="color:var(--green);">🟢 Connected — '+WF_SRC_TABLES.length+' tables</span>';
    G('wfSrcMsg').innerHTML='<span style="color:var(--green);">✓ '+WF_SRC_TABLES.length+' tables ready for pipeline creation</span>';
    toast('Discovered '+WF_SRC_TABLES.length+' tables from source.','tok');
    // Auto-populate inline table picker
    _wfQFiltered=[...WF_SRC_TABLES];
    _renderTableItems(G('wfQTableDropdown'),_wfQFiltered,'wfSelectQTable');
    _wfPopulateSchemaFilter();
    _wfSaveSourcesToDatabricks();
  }catch(e){
    st.innerHTML='<span style="color:var(--red);">🔴 Failed</span>';
    G('wfSrcMsg').innerHTML='<span style="color:var(--red);">'+e.message+'</span>';
    toast(e.message,'terr');
  }
  btn.disabled=false;btn.innerHTML='<svg viewBox="0 0 24 24" style="width:12px;height:12px;"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 11-2.12-9.36L23 10"/></svg> Discover Tables';
}

/* ─── Quick Create Table Picker (Multi-Table) ─── */
let _wfQSelected = [];  // array of selected table objects

/* ─── Target Schema Mapping Cache ─── */
let _wfCatalogSchemas = [];  // [{catalog:'bronze', schemas:['hr','default']}, ...]
let _wfTargetSchemaList = [];  // unique schema names: ['hr','default','dbo']
let _wfCatSchemaLoaded = false;
async function _wfLoadCatalogSchemas() {
  if (_wfCatSchemaLoaded) return;
  try {
    const r = await fetch('/api/v1/uc/catalog-schemas');
    const d = await r.json();
    if (d.success && d.catalogs) {
      _wfCatalogSchemas = d.catalogs;
      // Collect unique schema names across all catalogs
      const schSet = new Set();
      _wfCatalogSchemas.forEach(c => c.schemas.forEach(s => schSet.add(s)));
      // Always include 'dbo' as a common source schema mapping option
      schSet.add('dbo');
      _wfTargetSchemaList = [...schSet].sort();
      _wfCatSchemaLoaded = true;
      // Populate default target and bulk target dropdowns
      ['wfQDefaultTarget', 'wfQBulkTarget'].forEach(id => {
        const sel = G(id);
        if (sel) {
          const first = id === 'wfQBulkTarget' ? '<option value="">Schema…</option>' : '<option value="">— schema —</option>';
          sel.innerHTML = first;
          _wfTargetSchemaList.forEach(s => {
            sel.innerHTML += `<option value="${s}">${s}</option>`;
          });
        }
      });
    }
  } catch (e) { console.warn('Could not load catalog schemas:', e); }
}
function _wfTargetOptions(selectedVal) {
  let html = '<option value="">— schema —</option>';
  _wfTargetSchemaList.forEach(s => {
    html += `<option value="${s}"${s === selectedVal ? ' selected' : ''}>${s}</option>`;
  });
  return html;
}
function wfQUpdateItemTarget(idx, val) {
  if (_wfQSelected[idx]) _wfQSelected[idx]._target = val;
}
function wfQApplyDefaultTarget(val) {
  _wfQSelected.forEach(t => { if (!t._target) t._target = val; });
  _wfQRenderSelected();
}
function wfQBulkApplyTarget() {
  const val = G('wfQBulkTarget')?.value || '';
  _wfQChecked.forEach(i => { if (_wfQSelected[i]) _wfQSelected[i]._target = val; });
  _wfQRenderSelected();
}

let _wfQPage=0;
const _wfQPageSize=10;
function _wfGetConfiguredTableNames(){
  const names=new Set();
  (_wfPipelineData||[]).forEach(g=>{if(g.full_table)names.add(g.full_table);});
  return names;
}
function _renderTableItems(container, tables, onSelectFn){
  if(!tables.length){container.innerHTML='<div style="padding:24px;text-align:center;color:var(--t4);font-size:11px;"><div style="font-size:20px;margin-bottom:4px;">📋</div>No tables found</div>';return;}
  const selNames = new Set(_wfQSelected.map(t=>t.full_name));
  const configuredNames = _wfGetConfiguredTableNames();
  const ac=G('wfQAvailCount'); if(ac) ac.textContent=tables.length+' tables';
  const totalPages=Math.ceil(tables.length/_wfQPageSize);
  if(_wfQPage>=totalPages) _wfQPage=Math.max(0,totalPages-1);
  const start=_wfQPage*_wfQPageSize;
  const pageItems=tables.slice(start,start+_wfQPageSize);
  let html=pageItems.map((t,pi)=>{
    const i=start+pi;
    const isSel = selNames.has(t.full_name);
    const isConfigured = configuredNames.has(t.full_name);
    const schema = t.full_name.includes('.') ? t.full_name.split('.')[0]+'.' : '';
    const tName = t.table || t.full_name.split('.').pop();
    return `<div class="wf-tbl-item" style="padding:7px 12px;cursor:pointer;font-size:12px;border-bottom:1px solid var(--border);transition:all .15s;display:flex;align-items:center;gap:8px;${isSel?'background:rgba(37,99,235,.06);':''}" onmouseover="this.style.background='${isSel?'rgba(37,99,235,.1)':'var(--surface-2)'}'" onmouseout="this.style.background='${isSel?'rgba(37,99,235,.06)':''}'" onclick="${onSelectFn}(${i})">
      <div style="width:18px;height:18px;display:flex;align-items:center;justify-content:center;border-radius:4px;border:2px solid ${isSel?'var(--blue)':'var(--border)'};background:${isSel?'var(--blue)':'transparent'};transition:all .15s;flex-shrink:0;">
        ${isSel?'<svg viewBox="0 0 24 24" style="width:12px;height:12px;stroke:#fff;stroke-width:3;fill:none;"><polyline points="20 6 9 17 4 12"/></svg>':''}
      </div>
      <div style="flex:1;min-width:0;">
        <div style="font-weight:600;color:var(--t1);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;font-size:11.5px;"><span style="color:var(--t4);font-weight:400;">${schema}</span>${tName}</div>
        <div style="font-size:10px;color:var(--t4);margin-top:1px;">~${Number(t.row_estimate||0).toLocaleString()} rows · ${t.col_count||'?'} cols</div>
      </div>
      ${isConfigured?'<span style="font-size:9px;padding:2px 7px;border-radius:4px;background:#dcfce7;color:#16a34a;font-weight:600;white-space:nowrap;">✓ Configured</span>':''}
      ${isSel&&!isConfigured?'<span style="font-size:9px;padding:2px 6px;border-radius:4px;background:var(--blue);color:#fff;font-weight:600;">✓</span>':''}
    </div>`;
  }).join('');
  // Pagination controls
  if(totalPages>1){
    html+=`<div style="display:flex;align-items:center;justify-content:space-between;padding:6px 12px;border-top:1px solid var(--border);background:var(--surface-2);user-select:none;">
      <button onclick="_wfQPagePrev()" ${_wfQPage===0?'disabled':''} style="font-size:11px;padding:3px 10px;border:1px solid var(--border);border-radius:4px;background:${_wfQPage===0?'var(--surface)':'#fff'};color:${_wfQPage===0?'var(--t4)':'var(--t1)'};cursor:${_wfQPage===0?'default':'pointer'};font-weight:500;">← Prev</button>
      <span style="font-size:10px;color:var(--t3);font-weight:500;">Page ${_wfQPage+1} of ${totalPages}</span>
      <button onclick="_wfQPageNext()" ${_wfQPage>=totalPages-1?'disabled':''} style="font-size:11px;padding:3px 10px;border:1px solid var(--border);border-radius:4px;background:${_wfQPage>=totalPages-1?'var(--surface)':'#fff'};color:${_wfQPage>=totalPages-1?'var(--t4)':'var(--t1)'};cursor:${_wfQPage>=totalPages-1?'default':'pointer'};font-weight:500;">Next →</button>
    </div>`;
  }
  container.innerHTML=html;
}
function _wfQPagePrev(){if(_wfQPage>0){_wfQPage--;_renderTableItems(G('wfQTableDropdown'),_wfQFiltered,'wfSelectQTable');}}
function _wfQPageNext(){const tp=Math.ceil(_wfQFiltered.length/_wfQPageSize);if(_wfQPage<tp-1){_wfQPage++;_renderTableItems(G('wfQTableDropdown'),_wfQFiltered,'wfSelectQTable');}}

let _wfQChecked=new Set();
function _wfQRenderSelected(){
  const el=G('wfQSelectedList');
  const badge=G('wfQSelCount');
  const summary=G('wfQSelSummary');
  const selAllChk=G('wfQSelAllChk');
  // Clean up checked set — remove indices that no longer exist
  _wfQChecked=new Set([..._wfQChecked].filter(i=>i<_wfQSelected.length));
  _wfQUpdateBulkBar();
  if(!_wfQSelected.length){
    el.innerHTML='<div style="padding:30px 16px;text-align:center;color:var(--t4);font-size:11px;"><div style="font-size:24px;margin-bottom:6px;">📂</div>No tables selected<br><span style="font-size:10px;">Click tables on the left to add them</span></div>';
    badge.style.display='none';
    if(summary) summary.textContent='';
    if(selAllChk){selAllChk.checked=false;selAllChk.indeterminate=false;}
    return;
  }
  badge.style.display='';
  badge.textContent=_wfQSelected.length+' selected';
  if(summary) summary.textContent=_wfQSelected.length+' table'+(_wfQSelected.length>1?'s':'');
  // Update Select All checkbox state
  if(selAllChk){
    selAllChk.checked=_wfQChecked.size===_wfQSelected.length&&_wfQSelected.length>0;
    selAllChk.indeterminate=_wfQChecked.size>0&&_wfQChecked.size<_wfQSelected.length;
  }
  el.innerHTML=_wfQSelected.map((t,i)=>{
    const tbl=t.table||t.full_name.split('.').pop();
    const schema = t.full_name.includes('.') ? t.full_name.split('.')[0] : '';
    const chk=_wfQChecked.has(i);
    const tgtVal = t._target || '';
    const tgtLabel = tgtVal ? tgtVal.split('.').map(p=>'<span style="color:#7c3aed;font-weight:600;">'+p+'</span>').join('<span style="color:var(--t4);">.</span>') : '';
    return `<div data-tbl-name="${t.full_name}" style="display:flex;align-items:flex-start;gap:6px;padding:7px 10px;border-bottom:1px solid var(--border);font-size:11px;transition:background .12s;${chk?'background:#eff6ff;':''}" onmouseover="if(!${chk})this.style.background='var(--surface-2)'" onmouseout="if(!${chk})this.style.background=''">
      <input type="checkbox" class="wfQRowChk" data-idx="${i}" ${chk?'checked':''} onchange="wfQToggleRowChk(${i},this.checked)" style="width:14px;height:14px;accent-color:#3b82f6;cursor:pointer;flex-shrink:0;margin-top:3px;">
      <div style="width:22px;height:22px;border-radius:5px;background:linear-gradient(135deg,#3b82f6,#2563eb);display:flex;align-items:center;justify-content:center;color:#fff;font-size:9px;font-weight:700;flex-shrink:0;">${i+1}</div>
      <div style="flex:1;min-width:0;">
        <div style="display:flex;align-items:center;gap:6px;">
          <span style="font-weight:600;color:var(--t1);font-size:11px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:120px;" title="${t.full_name}">${tbl}</span>
          <span style="font-size:9px;color:var(--t4);">${schema?schema+' · ':''}~${Number(t.row_estimate||0).toLocaleString()} rows</span>
        </div>
        <div style="display:flex;align-items:center;gap:4px;margin-top:3px;">
          <span style="font-size:8px;color:var(--t4);white-space:nowrap;">→</span>
          <select class="inp wfQItemTarget" data-idx="${i}" style="font-size:9px;padding:2px 4px;max-width:100px;border-radius:4px;color:#7c3aed;font-weight:600;border-color:#c4b5fd;" onchange="wfQUpdateItemTarget(${i},this.value)" title="Target schema on Databricks">
            ${_wfTargetOptions(tgtVal)}
          </select>
        </div>
      </div>
      <select class="inp wfQItemLoad" data-idx="${i}" style="font-size:9px;padding:2px 4px;max-width:70px;border-radius:4px;" onchange="wfQUpdateItemLoad(${i},this.value)">
        <option value="full"${(t._loadType||'full')==='full'?' selected':''}>Full</option>
        <option value="incremental"${t._loadType==='incremental'?' selected':''}>Incr</option>
      </select>
      <input class="inp wfQItemWm" data-idx="${i}" placeholder="WM col" style="font-size:9px;padding:2px 4px;width:60px;border-radius:4px;display:${t._loadType==='incremental'?'block':'none'};" value="${t._wmCol||''}" oninput="wfQUpdateItemWm(${i},this.value)">
      <button onclick="wfQRemoveItem(${i})" style="background:none;border:none;color:var(--t4);cursor:pointer;font-size:14px;padding:0 2px;flex-shrink:0;transition:color .15s;" onmouseover="this.style.color='var(--red)'" onmouseout="this.style.color='var(--t4)'" title="Remove">×</button>
    </div>`;
  }).join('');
}
function wfQToggleRowChk(idx,checked){
  if(checked) _wfQChecked.add(idx); else _wfQChecked.delete(idx);
  const selAllChk=G('wfQSelAllChk');
  if(selAllChk){
    selAllChk.checked=_wfQChecked.size===_wfQSelected.length&&_wfQSelected.length>0;
    selAllChk.indeterminate=_wfQChecked.size>0&&_wfQChecked.size<_wfQSelected.length;
  }
  _wfQUpdateBulkBar();
  // Update row highlight without full re-render
  const row=document.querySelectorAll('.wfQRowChk[data-idx="'+idx+'"]')[0];
  if(row&&row.closest('div[style]'))row.closest('div[style*="border-bottom"]').style.background=checked?'#eff6ff':'';
}
function wfQToggleSelectAll(checked){
  _wfQChecked.clear();
  if(checked) _wfQSelected.forEach((_,i)=>_wfQChecked.add(i));
  _wfQRenderSelected();
}
function _wfQUpdateBulkBar(){
  const bar=G('wfQBulkBar');
  const cnt=G('wfQBulkCount');
  if(!bar)return;
  if(_wfQChecked.size>0){
    bar.style.display='flex';
    cnt.textContent=_wfQChecked.size+' checked';
  } else {
    bar.style.display='none';
  }
  // Update Create button label with checked count
  const btnLbl=G('btnWfQuickLabel');
  if(btnLbl){
    const isDlt=((G('wfNbPipelineMode')||{}).value||'standard')==='dlt';
    const n=_wfQChecked.size;
    if(n>0) btnLbl.textContent=(isDlt?'Create 2-Stage DLT Pipeline':'Create 3-Stage Medallion Pipeline')+' ('+n+' table'+(n>1?'s':'')+')';
    else btnLbl.textContent=isDlt?'Create 2-Stage DLT Pipeline':'Create 3-Stage Medallion Pipeline';
  }
}
function wfQBulkRemove(){
  const indices=[..._wfQChecked].sort((a,b)=>b-a);
  indices.forEach(i=>_wfQSelected.splice(i,1));
  _wfQChecked.clear();
  _wfQRenderSelected();
  _renderTableItems(G('wfQTableDropdown'),_wfQFiltered,'wfSelectQTable');
}
function wfQBulkApplyLoad(){
  const loadType=G('wfQBulkLoadType')?.value||'full';
  _wfQChecked.forEach(i=>{
    if(_wfQSelected[i]) _wfQSelected[i]._loadType=loadType;
  });
  _wfQRenderSelected();
}

function wfQUpdateItemLoad(idx,val){
  if(_wfQSelected[idx]){
    _wfQSelected[idx]._loadType=val;
    _wfQRenderSelected();
  }
}
function wfQUpdateItemWm(idx,val){
  if(_wfQSelected[idx]) _wfQSelected[idx]._wmCol=val;
}
function wfQRemoveItem(idx){
  _wfQSelected.splice(idx,1);
  _wfQRenderSelected();
  _renderTableItems(G('wfQTableDropdown'),_wfQFiltered,'wfSelectQTable');
}
function wfQSelectAll(){
  if(!WF_SRC_TABLES.length){toast('Discover tables first','terr');return;}
  const selNames=new Set(_wfQSelected.map(t=>t.full_name));
  const defaultLoad=G('wfQLoadType').value;
  WF_SRC_TABLES.forEach(t=>{
    if(!selNames.has(t.full_name)){
      _wfQSelected.push({...t, _loadType:defaultLoad, _wmCol:''});
      _wfQChecked.add(_wfQSelected.length-1);  // auto-check on add
    }
  });
  _wfQRenderSelected();
  _renderTableItems(G('wfQTableDropdown'),_wfQFiltered,'wfSelectQTable');
}
function wfQClearAll(){
  _wfQSelected=[];
  _wfQChecked.clear();
  _wfQRenderSelected();
  _renderTableItems(G('wfQTableDropdown'),_wfQFiltered,'wfSelectQTable');
}

let _wfQFiltered=[];
function wfShowQDropdown(){
  if(!WF_SRC_TABLES.length) return;
  if(!_wfQFiltered.length) _wfQFiltered=[...WF_SRC_TABLES];
  _renderTableItems(G('wfQTableDropdown'),_wfQFiltered,'wfSelectQTable');
}
function wfFilterQTables(val){
  if(!WF_SRC_TABLES.length){
    G('wfQTableDropdown').innerHTML='<div style="padding:24px;text-align:center;color:var(--t4);font-size:11px;"><div style="font-size:20px;margin-bottom:4px;">⚠</div>Click "Discover Tables" above first</div>';
    return;
  }
  const q=(val||'').toLowerCase();
  const schemaFilter=(G('wfQSchemaFilter')||{}).value||'';
  _wfQFiltered=WF_SRC_TABLES.filter(t=>{
    if(q && !t.full_name.toLowerCase().includes(q)) return false;
    if(schemaFilter && (t.schema||t.full_name.split('.')[0])!==schemaFilter) return false;
    return true;
  });
  _wfQPage=0;  // reset to first page on filter change
  _renderTableItems(G('wfQTableDropdown'),_wfQFiltered,'wfSelectQTable');
  const info=G('wfQFilteredInfo');
  if(info) info.textContent=(q||schemaFilter)?_wfQFiltered.length+' of '+WF_SRC_TABLES.length:'';
}
/* Populate schema dropdown after table discovery */
function _wfPopulateSchemaFilter(){
  const sel=G('wfQSchemaFilter');
  if(!sel) return;
  const schemas=new Set(WF_SRC_TABLES.map(t=>t.schema||t.full_name.split('.')[0]).filter(Boolean));
  sel.innerHTML='<option value="">All Schemas ('+schemas.size+')</option>'+
    [...schemas].sort().map(s=>'<option value="'+s+'">'+s+'</option>').join('');
}
/* Filter selected table list */
function wfFilterSelectedTables(val){
  const q=(val||'').toLowerCase();
  const rows=G('wfQSelectedList')?.querySelectorAll('[data-tbl-name]');
  if(!rows) return;
  rows.forEach(r=>{
    const name=(r.dataset.tblName||'').toLowerCase();
    r.style.display=(!q||name.includes(q))?'':'none';
  });
}
function wfSelectQTable(idx){
  const t=_wfQFiltered[idx];if(!t)return;
  // Toggle selection
  const existIdx=_wfQSelected.findIndex(s=>s.full_name===t.full_name);
  if(existIdx>=0){
    _wfQChecked.delete(existIdx);
    _wfQSelected.splice(existIdx,1);
    // Re-index checked set after splice
    const updated=new Set();
    _wfQChecked.forEach(i=>{if(i>existIdx)updated.add(i-1);else updated.add(i);});
    _wfQChecked=updated;
  }else{
    const defaultLoad=G('wfQLoadType').value;
    _wfQSelected.push({...t, _loadType:defaultLoad, _wmCol:''});
    _wfQChecked.add(_wfQSelected.length-1);  // auto-check on add
  }
  _wfQRenderSelected();
  // Re-render available list to update checkmarks
  _renderTableItems(G('wfQTableDropdown'),_wfQFiltered,'wfSelectQTable');
  // Keep dropdown open for multi-select
}
function wfClearQSelection(){
  wfQClearAll();
}

/* ─── Job Workflow Table Picker (Sub-tab 2) ─── */
let _wfJFiltered=[];
function wfShowJobDropdown(){
  if(!WF_SRC_TABLES.length){
    const dd=G('wfTableDropdown');
    dd.innerHTML='<div style="padding:12px;text-align:center;color:var(--t4);font-size:11px;">⚠ Connect data source in Medallion Architecture tab first</div>';
    dd.style.display='block';
    return;
  }
  _wfJFiltered=[...WF_SRC_TABLES];
  const dd=G('wfTableDropdown');
  _renderTableItems(dd,_wfJFiltered,'wfSelectJobTable');
  dd.style.display='block';
}
function wfFilterJobTables(val){
  if(!WF_SRC_TABLES.length)return;
  const q=val.toLowerCase();
  _wfJFiltered=q?WF_SRC_TABLES.filter(t=>t.full_name.toLowerCase().includes(q)):WF_SRC_TABLES;
  const dd=G('wfTableDropdown');
  _renderTableItems(dd,_wfJFiltered,'wfSelectJobTable');
  dd.style.display='block';
}
function wfSelectJobTable(idx){
  const t=_wfJFiltered[idx];if(!t)return;
  _wfSelectedJ=t;
  G('wfTableSearch').value='';
  G('wfTableDropdown').style.display='none';
  G('wfSelectedTable').style.display='block';
  G('wfSelName').textContent=t.full_name;
  G('wfSelMeta').textContent='~'+Number(t.row_estimate||0).toLocaleString()+' rows · '+(t.col_count||'?')+' cols';
  // Update preview
  const n=t.table||t.full_name.split('.').pop();
  const prev=document.getElementById('wfPreviewName');
  if(prev)prev.textContent=n;
  document.querySelectorAll('.wfPrevTbl').forEach(e=>e.textContent=n);
}
function wfClearJobSelection(){
  _wfSelectedJ=null;
  G('wfSelectedTable').style.display='none';
  G('wfTableSearch').value='';
  const prev=document.getElementById('wfPreviewName');
  if(prev)prev.textContent='TableName';
  document.querySelectorAll('.wfPrevTbl').forEach(e=>e.textContent='TableName');
}

// Close dropdowns when clicking outside (job table picker only — Quick Create stays open)
document.addEventListener('click',function(e){
  if(!e.target.closest('#wfTableSearch')&&!e.target.closest('#wfTableDropdown')){
    const dd=G('wfTableDropdown');if(dd)dd.style.display='none';
  }
});

async function wfCreatePipeline(){
  if(!_wfSelectedJ){toast('Select a table from the dropdown — connect data source first','terr');return;}
  /* ── Cluster gate ── */
  const _sel=G('wfClusterSelect');
  if(!_sel||!_sel.value){toast('Please select a Databricks cluster first','terr');return;}
  const _cOpt=_sel.options[_sel.selectedIndex];
  if(_cOpt&&_cOpt.dataset.state!=='RUNNING'){
    toast('Cluster is '+(_cOpt.dataset.state||'not running')+' — please start the cluster before creating a pipeline','terr');return;
  }
  const schema=_wfSelectedJ.schema||'dbo';
  const table=_wfSelectedJ.table||_wfSelectedJ.full_name.split('.').pop();
  const loadType=G('wfLoadType').value;
  const wmCol=loadType==='incremental'?G('wfWatermarkCol').value.trim():'';
  if(loadType==='incremental'&&!wmCol){toast('Enter watermark column for incremental load','terr');return;}
  const btn=G('btnWfCreate');btn.disabled=true;btn.textContent='Creating…';
  try{
    const r=await fetch('/api/v1/workflow/create-pipeline',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({table_schema:schema,table_name:table,load_type:loadType,watermark_column:wmCol,source_config:_wfSourceConfig(),target_config:_wfTargetConfig(),pipeline_mode:(G('wfNbPipelineMode')||{}).value||'standard',cdc_mode:(G('cfgCdcMode')||{}).value||'watermark',primary_keys:(G('cfgPrimaryKeys')||{}).value?G('cfgPrimaryKeys').value.split(',').map(s=>s.trim()).filter(Boolean):[]})});
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Failed');
    toast('Pipeline created: '+d.jobs.length+' jobs for '+table,'tok');
    wfClearJobSelection();
    G('wfLoadType').value='full';wfToggleWatermark();
    wfRefreshAll();
  }catch(e){toast(e.message,'terr');}
  btn.disabled=false;btn.innerHTML='<svg viewBox="0 0 24 24"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg> Create 3-Stage Pipeline';
}

/* ─── Quick Create from Medallion Tab ─── */
function wfUpdateQuickPreview(){
  // no-op — multi-table mode handles preview via _wfQRenderSelected()
}
function wfQToggleWm(){
  // no-op — per-table load type is now inline in the selected list
}
async function wfQuickCreate(){
  if(!_wfQSelected.length){toast('Select one or more tables from the dropdown','terr');return;}
  // Only migrate tables whose checkboxes are checked
  const checkedTables=_wfQChecked.size>0
    ? [..._wfQChecked].sort((a,b)=>a-b).map(i=>_wfQSelected[i]).filter(Boolean)
    : [];
  if(!checkedTables.length){toast('Check the tables you want to migrate using the checkboxes','terr');return;}
  /* ── Cluster gate ── */
  const _sel=G('wfClusterSelect');
  if(!_sel||!_sel.value){toast('Please select a Databricks cluster first','terr');return;}
  const _cOpt=_sel.options[_sel.selectedIndex];
  if(_cOpt&&_cOpt.dataset.state!=='RUNNING'){
    toast('Cluster is '+(_cOpt.dataset.state||'not running')+' — please start the cluster before creating a pipeline','terr');return;
  }
  // Validate incremental tables have watermark columns (skip for Change Tracking CDC — uses SYS_CHANGE_VERSION)
  const _cdcMode=(G('cfgCdcMode')||{}).value||'watermark';
  for(const t of checkedTables){
    if(t._loadType==='incremental'&&_cdcMode!=='change_tracking'&&!(t._wmCol||'').trim()){
      toast('Enter watermark column for '+t.full_name+' (incremental)','terr');return;
    }
  }
  const btn=G('btnWfQuick');btn.disabled=true;btn.textContent='Creating '+checkedTables.length+' pipeline(s)…';
  try{
    const tables=checkedTables.map(t=>{
      const obj = {
        schema: t.schema||'dbo',
        table: t.table||t.full_name.split('.').pop(),
        load_type: t._loadType||G('wfQLoadType').value,
        watermark_column: t._wmCol||'',
      };
      // Per-table target schema mapping (schema only, not catalog)
      if(t._target){
        obj.target_schema=t._target;
      }
      return obj;
    });
    const r=await fetch('/api/v1/workflow/create-pipelines-bulk',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({tables:tables,source_config:_wfSourceConfig(),target_config:_wfTargetConfig(),pipeline_mode:(G('wfNbPipelineMode')||{}).value||'standard',cdc_mode:(G('cfgCdcMode')||{}).value||'watermark',primary_keys:(G('cfgPrimaryKeys')||{}).value?G('cfgPrimaryKeys').value.split(',').map(s=>s.trim()).filter(Boolean):[]})});
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Failed');
    const archCount=(d.groups||[]).reduce((s,g)=>(s+(g.archived_jobs||[]).length),0);
    let msg='Created '+d.created+' pipeline(s) with '+d.total_jobs+' jobs';
    if(archCount>0) msg+=' ('+archCount+' old job(s) archived to history)';
    toast(msg,'tok');
    wfQClearAll();
    G('wfQLoadType').value='full';
    wfRefreshAll();
  }catch(e){toast(e.message,'terr');}
  btn.disabled=false;const _isDlt=((G('wfNbPipelineMode')||{}).value||'standard')==='dlt';btn.innerHTML='<svg viewBox="0 0 24 24" style="width:14px;height:14px;"><path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg> <span id="btnWfQuickLabel">'+(_isDlt?'Create 2-Stage DLT Pipeline':'Create 3-Stage Medallion Pipeline')+'</span>';
}

/* ─── Layer Detail Panel ─── */
let _openLayer=null;
function wfToggleLayerDetail(layer){
  const panel=G('mdlDetailPanel');
  if(_openLayer===layer){wfCloseLayerDetail();return;}
  _openLayer=layer;
  const titles={source:'SQL Source — Extract Jobs',landing:'Landing Zone — Ingested Data',bronze:'Bronze Layer — Raw Delta Tables',silver:'Silver Layer — Cleansed Tables'};
  const stages={source:'extract',landing:'landing_to_bronze',bronze:'landing_to_bronze',silver:'bronze_to_silver'};
  G('mdlDetailTitle').textContent=titles[layer]||'Layer Details';
  // Highlight selected layer
  document.querySelectorAll('.mdl-layer').forEach(el=>el.style.outline='none');
  const layerEl=G('mdlLayer'+layer.charAt(0).toUpperCase()+layer.slice(1));
  if(layerEl)layerEl.style.outline='2px solid rgba(255,255,255,.5)';
  // Load jobs for this stage
  wfLoadLayerJobs(layer,stages[layer]);
  panel.classList.add('open');
}
function wfCloseLayerDetail(){
  _openLayer=null;
  G('mdlDetailPanel').classList.remove('open');
  document.querySelectorAll('.mdl-layer').forEach(el=>el.style.outline='none');
}
async function wfLoadLayerJobs(layer,stage){
  const content=G('mdlDetailContent');
  content.innerHTML='<div style="text-align:center;color:var(--t4);padding:12px;">Loading jobs…</div>';
  try{
    const r=await fetch('/api/v1/workflow/jobs?stage='+encodeURIComponent(stage));
    const d=await r.json();
    if(!d.success||!d.jobs.length){
      content.innerHTML='<div style="text-align:center;color:var(--t4);padding:12px;">No jobs at this stage yet. Create a pipeline using Quick Create.</div>';
      return;
    }
    const jc={created:'#94a3b8',running:'#3b82f6',success:'#10b981',failed:'#ef4444'};
    const ji={created:'⏸',running:'🔄',success:'✅',failed:'❌'};
    content.innerHTML=d.jobs.map(j=>
      `<div class="mdl-detail-job">
        <div style="display:flex;align-items:center;gap:8px;">
          <span style="font-size:12px;">${ji[j.status]||'⏸'}</span>
          <div>
            <div style="font-weight:600;color:var(--t1);">${j.job_name}</div>
            <div style="font-size:10px;color:var(--t4);">${j.load_type.toUpperCase()} · ${j.run_count||0} runs</div>
          </div>
        </div>
        <div style="display:flex;align-items:center;gap:6px;">
          <span style="font-size:10px;font-weight:600;color:${jc[j.status]||'#94a3b8'};">${j.status.toUpperCase()}</span>
          <button class="btn btn-primary btn-xs" onclick="wfRunSingleJob('${j.job_id}')" title="Run">▶</button>
        </div>
      </div>`
    ).join('');
  }catch(e){
    content.innerHTML='<div style="text-align:center;color:var(--red);padding:12px;">Error: '+e.message+'</div>';
  }
}

async function wfRefreshAll(){
  wfRefreshStats();
  wfRefreshPipelines();
  wfRefreshJobs();
  wfRefreshHistory();
  wfRefreshAuditHistory();
  wfRefreshWatermarks();
  if(!_wfClustersLoaded)wfFetchClusters();
  if(!_wfCatSchemaLoaded) _wfLoadCatalogSchemas();
  // Re-render open layer detail if any
  if(_openLayer){
    const stages={source:'extract',landing:'landing_to_bronze',bronze:'landing_to_bronze',silver:'bronze_to_silver'};
    wfLoadLayerJobs(_openLayer,stages[_openLayer]);
  }
}

async function wfRefreshStats(){
  try{
    const r=await fetch('/api/v1/workflow/stats');const d=await r.json();
    let s=d.success?d.stats:null;

    // If local stats are all zeros, fallback to Databricks metadata
    if(!s || (s.total_jobs===0 && s.total_pipelines===0)){
      try{
        const dbxR=await fetch('/api/v1/reports/jobs').then(r=>r.json());
        if(dbxR.success && dbxR.jobs && dbxR.jobs.length){
          const jobs=dbxR.jobs;
          const stages=new Set(jobs.map(j=>j.stage).filter(Boolean));
          const successJobs=jobs.filter(j=>(j.status||'').toLowerCase()==='success').length;
          const failedJobs=jobs.filter(j=>(j.status||'').toLowerCase()==='failed').length;
          const runningJobs=jobs.filter(j=>(j.status||'').toLowerCase()==='running').length;
          const totalRows=jobs.reduce((sum,j)=>sum+(parseInt(j.rows_processed)||0),0);
          s={
            total_pipelines: stages.size || Math.ceil(jobs.length/2),
            total_jobs: jobs.length,
            success_jobs: successJobs,
            failed_jobs: failedJobs,
            running_jobs: runningJobs,
            total_rows_processed: totalRows,
            extract_jobs: jobs.filter(j=>(j.stage||'').includes('extract')).length,
            ingest_jobs: jobs.filter(j=>(j.stage||'').includes('dlt')||((j.stage||'').includes('bronze'))).length,
            cleanse_jobs: jobs.filter(j=>(j.stage||'').includes('silver')).length
          };
        }
      }catch(dbxErr){console.warn('Dashboard Databricks fallback failed',dbxErr);}
    }
    if(!s) return;

    G('wfStatPipelines').textContent=s.total_pipelines;
    G('wfStatJobs').textContent=s.total_jobs;
    G('wfStatSuccess').textContent=s.success_jobs;
    G('wfStatFailed').textContent=s.failed_jobs;
    const rowsEl=G('wfStatRows');if(rowsEl)rowsEl.textContent=s.total_rows_processed||0;
    const b=G('navBadgeWf');if(b)b.textContent=s.total_jobs;
    // Update medallion layer counts
    const sc=G('mdlSrcCount');if(sc)sc.textContent=s.total_pipelines+' tables';
    const lc=G('mdlLandingCount');if(lc)lc.textContent=s.total_pipelines+' tables';
    const bc=G('mdlBronzeCount');if(bc)bc.textContent=s.total_pipelines+' tables';
    const slc=G('mdlSilverCount');if(slc)slc.textContent=s.total_pipelines+' tables';
    // Toggle has-data class on layers
    ['Source','Landing','Bronze','Silver'].forEach(layer=>{
      const el=G('mdlLayer'+layer);
      if(el){ if(s.total_pipelines>0) el.classList.add('has-data'); else el.classList.remove('has-data'); }
    });
    // Update arrow job counts
    const a1=G('mdlArrow1Count');if(a1)a1.textContent=(s.extract_jobs||s.total_pipelines)+' jobs';
    const a2=G('mdlArrow2Count');if(a2)a2.textContent=(s.ingest_jobs||s.total_pipelines)+' jobs';
    const a3=G('mdlArrow3Count');if(a3)a3.textContent=(s.cleanse_jobs||s.total_pipelines)+' jobs';
    // Animate arrows when there are running jobs
    ['mdlArrow1','mdlArrow2','mdlArrow3','mdlArrow4'].forEach(id=>{
      const el=G(id);
      if(el){ if(s.running_jobs>0) el.classList.add('active'); else el.classList.remove('active'); }
    });
  }catch(e){console.error('wfRefreshStats',e);}
}

/* ─── Fetch Databricks Clusters ─── */
let _wfClustersLoaded=false;
async function wfFetchClusters(){
  const c=await _wfDbrCredsWithFallback();
  if(!c.host||!c.token) return;  // silently skip — no credentials configured yet
  const sel=G('wfClusterSelect'), stat=G('wfClusterStatus'), info=G('wfClusterInfo');
  stat.textContent='Loading…';stat.style.color='var(--t4)';
  try{
    const r=await fetch(`/api/v1/workflow/clusters?host=${encodeURIComponent(c.host)}&token=${encodeURIComponent(c.token)}`);
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Failed to list clusters');
    const clusters=d.clusters||[];
    // Keep current selection if possible
    const prev=sel.value;
    sel.innerHTML='<option value="">— Select a cluster —</option>';
    let runCount=0;
    clusters.forEach(cl=>{
      const st=cl.state||'UNKNOWN';
      const isRun=st==='RUNNING';
      if(isRun)runCount++;
      const icon=isRun?'🟢':st==='TERMINATED'?'🔴':st==='PENDING'?'🟡':'⚪';
      const opt=document.createElement('option');
      opt.value=cl.cluster_id;
      opt.textContent=`${icon} ${cl.cluster_name}  (${st} · DBR ${cl.spark_version||'N/A'})`;
      opt.dataset.state=st;
      sel.appendChild(opt);
    });
    // Restore previous selection if still present
    if(prev){sel.value=prev;}
    stat.textContent=`${clusters.length} cluster${clusters.length!==1?'s':''} found (${runCount} running)`;
    stat.style.color='var(--green)';
    _wfClustersLoaded=true;
    _updateClusterInfo();
  }catch(e){
    stat.textContent=e.message;stat.style.color='var(--red)';
    console.error('wfFetchClusters',e);
  }
}
function _updateClusterInfo(){
  const sel=G('wfClusterSelect'),info=G('wfClusterInfo');
  const opt=sel.options[sel.selectedIndex];
  if(opt&&opt.value){
    info.style.display='block';
    info.innerHTML=`<span style="font-weight:600;">ID:</span> <code style="font-size:9px;">${opt.value}</code>`;
  }else{info.style.display='none';}
  // Show/hide Start Cluster button based on cluster state
  const btn=G('btnStartCluster');
  if(btn){
    if(opt&&opt.value&&opt.dataset.state&&opt.dataset.state!=='RUNNING'){
      btn.style.display='';
    }else{
      btn.style.display='none';
    }
  }
}

async function wfStartCluster(){
  const sel=G('wfClusterSelect');
  if(!sel||!sel.value){toast('Select a cluster first','terr');return;}
  const opt=sel.options[sel.selectedIndex];
  if(opt&&opt.dataset.state==='RUNNING'){toast('Cluster is already running','tinfo');return;}
  const c=await _wfDbrCredsWithFallback();
  if(!c.host||!c.token){toast('Configure Databricks connection in Settings first','terr');return;}
  const btn=G('btnStartCluster');
  btn.disabled=true;btn.textContent='Starting…';
  try{
    const r=await fetch('/api/v1/workflow/clusters/start',{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({host:c.host,token:c.token,cluster_id:sel.value})
    });
    const d=await r.json();
    if(!d.success)throw new Error(d.error||d.message||'Failed');
    toast('Cluster start initiated — may take 2–5 minutes','tok');
    // Poll for cluster status update
    let polls=0;
    const poller=setInterval(async()=>{
      polls++;
      await wfFetchClusters();
      const updated=sel.options[sel.selectedIndex];
      if(updated&&updated.dataset.state==='RUNNING'){
        clearInterval(poller);
        toast('Cluster is now RUNNING ✔','tok');
        btn.style.display='none';
      }
      if(polls>=40)clearInterval(poller); // stop after ~5 min
    },8000);
  }catch(e){toast(e.message,'terr');}
  btn.disabled=false;btn.innerHTML='<svg viewBox="0 0 24 24" style="width:12px;height:12px;fill:currentColor;"><polygon points="5 3 19 12 5 21 5 3"/></svg> Start Cluster';
}
(function(){
  const sel=document.getElementById('wfClusterSelect');
  if(sel)sel.addEventListener('change',_updateClusterInfo);
})();

let _wfPipelineData=[];  // cached for filtering
async function wfRefreshPipelines(){
  try{
    const r=await fetch('/api/v1/workflow/pipelines');const d=await r.json();
    if(!d.success)return;
    _wfPipelineData=d.groups||[];
    _wfRenderFilteredPipelines();
  }catch(e){console.error('wfRefreshPipelines',e);}
}
function wfFilterPipelineGroups(val){
  _wfRenderFilteredPipelines();
}
function _wfRenderFilteredPipelines(){
    const el=G('wfPipelineList');
    const infoBar=G('wfPipelineListInfo');
    let groups=_wfPipelineData;
    // Apply text filter
    const q=((G('wfPipelineFilter')||{}).value||'').toLowerCase();
    const sf=((G('wfPipelineStatusFilter')||{}).value||'');
    if(q) groups=groups.filter(g=>(g.full_table||'').toLowerCase().includes(q)||(g.table_name||'').toLowerCase().includes(q));
    if(sf) groups=groups.filter(g=>g.status===sf);

    if(!groups.length){
      el.innerHTML='<div class="empty" style="padding:24px;"><div class="empty-ico">🔗</div><div class="empty-t">'+(q||sf?'No pipelines match filter':'No pipelines yet — use Quick Create to get started')+'</div></div>';
      if(infoBar)infoBar.style.display='none';
      const _toolbar=G('wfGroupToolbar');if(_toolbar)_toolbar.style.display='none';
      return;
    }
    // Show / hide toolbar depending on group count
    const _toolbar=G('wfGroupToolbar');
    if(_toolbar)_toolbar.style.display=groups.length?'flex':'none';
    // Prune stale selections
    const _gids=new Set(groups.map(g=>g.group_id));
    _wfSelectedGroups.forEach(id=>{if(!_gids.has(id))_wfSelectedGroups.delete(id);});
    _wfUpdateGroupToolbar(groups.length);

    /* Helper: format datetime for display */
    function _fmtDt(ts){
      if(!ts)return '';
      try{
        const d=new Date(ts);
        if(isNaN(d.getTime()))return '';
        const pad=n=>String(n).padStart(2,'0');
        return pad(d.getDate())+'/'+pad(d.getMonth()+1)+'/'+d.getFullYear()+' '+pad(d.getHours())+':'+pad(d.getMinutes());
      }catch(e){return '';}
    }

    el.innerHTML=groups.map(g=>{
      const _pm=g.pipeline_mode||(G('wfNbPipelineMode')||{}).value||'standard';
      const _pmIsDlt=_pm==='dlt';
      const _lastAct=_fmtDt(g.last_activity);
      const _pmBadge=_pmIsDlt
        ?'<span style="font-size:8px;padding:1px 5px;border-radius:6px;background:#f59e0b;color:#fff;font-weight:700;">⚡DLT</span>'
        :'<span style="font-size:8px;padding:1px 5px;border-radius:6px;background:#3b82f6;color:#fff;font-weight:700;">Spark</span>';
      const _chk=_wfSelectedGroups.has(g.group_id)?'checked':'';
      const _SL={extract:'Extract',landing_to_bronze:'→Bronze',bronze_to_silver:'→Silver',dlt_bronze_silver:'⚡DLT'};
      const _statusBg={created:'#f1f5f9',running:'#dbeafe',success:'#d1fae5',failed:'#fee2e2'};
      const _statusFg={created:'#64748b',running:'#2563eb',success:'#059669',failed:'#dc2626'};
      const _statusIco={created:'⏸',running:'🔄',success:'✅',failed:'❌'};
      /* Compact inline stage indicators */
      const stageIndicators=(g.jobs||[]).sort((a,b)=>a.order-b.order).map((j,i,arr)=>{
        const jc={created:'#94a3b8',running:'#3b82f6',success:'#10b981',failed:'#ef4444'};
        const c=jc[j.status]||'#94a3b8';
        const sl=_SL[j.stage]||j.stage;
        const sep=i<arr.length-1?' <span style="color:var(--t4);font-size:10px;">→</span> ':'';
        const _jt=_fmtDt(j.last_run_at);
        return '<span style="display:inline-flex;align-items:center;gap:3px;padding:1px 6px;border-radius:4px;background:'+c+'14;border:1px solid '+c+'33;font-size:9px;font-weight:600;color:'+c+';" title="'+j.job_name+(_jt?' | '+_jt:'')+'">'
          +(_statusIco[j.status]||'⏸')+' '+sl
          +(j.status==='failed'?' <button onclick="event.stopPropagation();wfRunSingleJob(\''+j.job_id+'\')" style="padding:0 3px;font-size:8px;background:none;border:none;color:'+c+';cursor:pointer;font-weight:700;" title="Rerun">↻</button>':'')
          +'</span>'+sep;
      }).join('');
      return `<div style="padding:8px 12px;border:1px solid var(--border);border-radius:var(--r-sm);margin-bottom:4px;background:var(--surface);transition:box-shadow .15s;" onmouseover="this.style.boxShadow='0 2px 8px rgba(0,0,0,.06)'" onmouseout="this.style.boxShadow=''">
        <div style="display:flex;align-items:center;gap:6px;">
          <input type="checkbox" class="wfGrpChk" data-gid="${g.group_id}" ${_chk} onchange="wfToggleGroupSelect('${g.group_id}',this.checked)" style="accent-color:var(--blue);width:14px;height:14px;cursor:pointer;flex-shrink:0;">
          <div style="flex:1;min-width:0;">
            <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;">
              <span style="font-weight:700;font-size:12px;color:var(--t1);">${g.full_table}</span>
              ${_pmBadge}
              <span style="display:inline-flex;align-items:center;gap:3px;padding:1px 6px;border-radius:99px;font-size:9px;font-weight:600;background:${_statusBg[g.status]||'#f1f5f9'};color:${_statusFg[g.status]||'#64748b'};">${_statusIco[g.status]||'⏸'} ${g.status.toUpperCase()}</span>
              ${_lastAct?'<span style="font-size:9px;color:var(--t4);">🕐 '+_lastAct+'</span>':''}
            </div>
            <div style="display:flex;align-items:center;gap:4px;margin-top:4px;flex-wrap:wrap;">
              <span style="font-size:9px;color:var(--t3);">${g.load_type.toUpperCase()} · ${g.job_ids.length} jobs${g.watermark_column?' · WM:'+g.watermark_column:''}</span>
              <span style="font-size:9px;color:var(--t4);">|</span>
              ${stageIndicators}
            </div>
          </div>
          <div style="display:flex;align-items:center;gap:4px;flex-shrink:0;">
            <button class="btn btn-primary btn-xs" onclick="wfRunOnDatabricks('${g.group_id}')" title="Run on Databricks" style="background:var(--accent-gradient);padding:3px 8px;font-size:10px;">⚡ Run</button>
            <button class="btn btn-ghost btn-xs" onclick="wfRerunPipeline('${g.group_id}')" title="Rerun from failure" style="padding:3px 6px;font-size:10px;">🔄</button>
            <button class="btn btn-ghost btn-xs" onclick="wfShowPipelineLogs('${g.group_id}','${g.full_table}')" title="Show logs" style="padding:3px 6px;font-size:10px;">📋</button>
            <button class="btn btn-ghost btn-xs" onclick="wfDeletePipeline('${g.group_id}')" title="Delete" style="color:var(--red);padding:3px 6px;font-size:10px;">✕</button>
          </div>
        </div>
      </div>`;
    }).join('');

    // Info bar
    if(infoBar){
      if(q||sf){
        infoBar.style.display='block';
        infoBar.textContent='Showing '+groups.length+' of '+_wfPipelineData.length+' pipeline(s)';
      }else{
        infoBar.style.display='block';
        infoBar.textContent=groups.length+' pipeline group(s)';
      }
    }
}

const WF_STAGE_LABELS={extract:'Extract',landing_to_bronze:'Landing→Bronze',bronze_to_silver:'Bronze→Silver',dlt_bronze_silver:'⚡ DLT Bronze+Silver'};
const WF_STATUS_BADGES={
  created:'<span style="display:inline-block;padding:2px 8px;border-radius:99px;font-size:10px;font-weight:600;background:#f1f5f9;color:#64748b;">CREATED</span>',
  running:'<span style="display:inline-block;padding:2px 8px;border-radius:99px;font-size:10px;font-weight:600;background:#dbeafe;color:#2563eb;">RUNNING</span>',
  success:'<span style="display:inline-block;padding:2px 8px;border-radius:99px;font-size:10px;font-weight:600;background:#d1fae5;color:#059669;">SUCCESS</span>',
  failed:'<span style="display:inline-block;padding:2px 8px;border-radius:99px;font-size:10px;font-weight:600;background:#fee2e2;color:#dc2626;">FAILED</span>',
};

/* ─── Job Multi-Select State ─── */
let _wfSelectedJobs=new Set();
let _wfAllJobs=[];

function wfToggleJobSelect(jobId, checked){
  if(checked) _wfSelectedJobs.add(jobId);
  else _wfSelectedJobs.delete(jobId);
  _wfUpdateJobToolbar();
}
function wfToggleAllJobs(checked){
  document.querySelectorAll('.wfJobChk').forEach(cb=>{
    cb.checked=checked;
    if(checked) _wfSelectedJobs.add(cb.dataset.jid);
    else _wfSelectedJobs.delete(cb.dataset.jid);
  });
  const allHead=G('wfJobSelectAllHead');
  if(allHead)allHead.checked=checked;
  const allTb=G('wfJobSelectAll');
  if(allTb)allTb.checked=checked;
  _wfUpdateJobToolbar();
}
function _wfUpdateJobToolbar(){
  const n=_wfSelectedJobs.size;
  const tb=G('wfJobToolbar');
  if(tb)tb.style.display=n>0?'flex':'none';
  const lbl=G('wfJobSelCount');
  if(lbl)lbl.textContent=n+' selected';
  const runBtn=G('btnWfRunSelectedJobs');
  if(runBtn)runBtn.style.display=n>0?'inline-flex':'none';
  // Show "Rerun Failed" only if any selected job has failed status
  const rerunBtn=G('btnWfRerunFailedJobs');
  if(rerunBtn){
    const hasFailed=_wfAllJobs.some(j=>_wfSelectedJobs.has(j.job_id)&&j.status==='failed');
    rerunBtn.style.display=hasFailed?'inline-flex':'none';
  }
}

async function wfRunSelectedJobs(){
  const ids=[..._wfSelectedJobs];
  if(!ids.length){toast('Select at least one job','terr');return;}
  // Find unique pipeline groups for selected jobs and submit to Databricks
  const groupIds=new Set();
  for(const jid of ids){
    const job=_wfAllJobs.find(j=>j.job_id===jid);
    if(job&&job.group_id)groupIds.add(job.group_id);
  }
  if(!groupIds.size){toast('No pipeline groups found for selected jobs','terr');return;}
  toast('Submitting '+groupIds.size+' pipeline(s) to Databricks…','tinfo');
  let ok=0,fail=0;
  for(const gid of groupIds){
    const success=await wfRunOnDatabricks(gid);
    if(success)ok++;else fail++;
  }
  _wfSelectedJobs.clear();
  _wfUpdateJobToolbar();
  const msg=ok+' submitted'+(fail?' · '+fail+' failed':'');
  toast(msg,fail?'terr':'tok');
  setTimeout(()=>{wfRefreshJobs();wfRefreshHistory();},2000);
}

async function wfRerunFailedJobs(){
  const ids=[..._wfSelectedJobs];
  const failedIds=ids.filter(jid=>_wfAllJobs.some(j=>j.job_id===jid&&j.status==='failed'));
  if(!failedIds.length){toast('No failed jobs selected','terr');return;}
  // Find unique pipeline groups for failed jobs and resubmit to Databricks
  const groupIds=new Set();
  for(const jid of failedIds){
    const job=_wfAllJobs.find(j=>j.job_id===jid);
    if(job&&job.group_id)groupIds.add(job.group_id);
  }
  if(!groupIds.size){toast('No pipeline groups found','terr');return;}
  toast('Rerunning '+groupIds.size+' pipeline(s) on Databricks…','tinfo');
  let ok=0,fail=0;
  for(const gid of groupIds){
    const success=await wfRunOnDatabricks(gid);
    if(success)ok++;else fail++;
  }
  _wfSelectedJobs.clear();
  _wfUpdateJobToolbar();
  toast(ok+' resubmitted'+(fail?' · '+fail+' failed':''),'tok');
  setTimeout(()=>{wfRefreshJobs();wfRefreshHistory();},2000);
}

async function wfRefreshJobs(){
  try{
    const stage=G('wfFilterStage')?G('wfFilterStage').value:'';
    const status=G('wfFilterStatus')?G('wfFilterStatus').value:'';
    let url='/api/v1/workflow/jobs?';
    if(stage)url+='stage='+stage+'&';
    if(status)url+='status='+status+'&';
    const r=await fetch(url);const d=await r.json();
    if(!d.success)return;
    _wfAllJobs=d.jobs||[];
    const tb=G('wfJobTbody');
    if(!_wfAllJobs.length){
      tb.innerHTML='<tr><td colspan="9" style="padding:32px;text-align:center;color:var(--t4);">No jobs found</td></tr>';
      G('wfJobToolbar').style.display='none';
      return;
    }
    tb.innerHTML=_wfAllJobs.map(j=>{
      const isSel=_wfSelectedJobs.has(j.job_id);
      const lastRun=j.last_run_at?new Date(j.last_run_at).toLocaleString('en-US',{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}):'—';
      return `<tr style="border-bottom:1px solid var(--border);transition:background .15s;${isSel?'background:var(--surface-2);':''}" onmouseover="this.style.background='var(--surface-2)'" onmouseout="if(!${isSel})this.style.background=''">
      <td style="padding:8px 6px;text-align:center;"><input type="checkbox" class="wfJobChk" data-jid="${j.job_id}" ${isSel?'checked':''} onchange="wfToggleJobSelect('${j.job_id}',this.checked)"></td>
      <td style="padding:8px 10px;font-weight:600;font-family:var(--font-mono);font-size:11px;">${j.job_name}</td>
      <td style="padding:8px 10px;">${WF_STAGE_LABELS[j.stage]||j.stage}</td>
      <td style="padding:8px 10px;font-size:11px;">${j.full_table}</td>
      <td style="padding:8px 10px;"><span style="font-size:10px;font-weight:600;text-transform:uppercase;color:${j.load_type==='incremental'?'var(--amber)':'var(--blue)'};">${j.load_type}</span></td>
      <td style="padding:8px 10px;text-align:center;">${WF_STATUS_BADGES[j.status]||j.status}</td>
      <td style="padding:8px 10px;text-align:center;font-size:10px;color:var(--t3);">${lastRun}</td>
      <td style="padding:8px 10px;text-align:center;font-size:11px;">${j.run_count}${j.fail_count?' <span style="color:var(--red);">('+j.fail_count+' fail)</span>':''}</td>
      <td style="padding:8px 10px;text-align:center;white-space:nowrap;">
        <button class="btn btn-primary btn-xs" onclick="wfRunSingleJob('${j.job_id}')" title="Run" style="padding:3px 8px;">▶</button>
        ${j.status==='failed'?'<button class="btn btn-xs" onclick="wfRunSingleJob(\''+j.job_id+'\')" title="Rerun failed" style="padding:3px 8px;background:#fee2e2;color:#dc2626;border:1px solid #fca5a5;">↺</button>':''}
        <button class="btn btn-ghost btn-xs" onclick="wfViewJobLogs('${j.job_id}')" title="Logs" style="padding:3px 8px;">📋</button>
        <button class="btn btn-ghost btn-xs" onclick="wfDeleteJob('${j.job_id}')" title="Delete" style="padding:3px 8px;color:var(--red);">✕</button>
      </td>
    </tr>`;
    }).join('');
    _wfUpdateJobToolbar();
    // Auto-poll while any jobs are running
    _wfScheduleAutoRefresh();
  }catch(e){console.error('wfRefreshJobs',e);}
}

let _wfAutoRefreshTimer=null;
function _wfScheduleAutoRefresh(){
  clearTimeout(_wfAutoRefreshTimer);
  const hasRunning=_wfAllJobs.some(j=>j.status==='running');
  if(hasRunning){
    _wfAutoRefreshTimer=setTimeout(()=>{wfRefreshJobs();wfRefreshHistory();},10000);
  }
}

/* ─── Execution History ─── */
async function wfRefreshHistory(){
  try{
    const status=G('wfHistFilterStatus')?G('wfHistFilterStatus').value:'';
    let url='/api/v1/workflow/runs?limit=50';
    if(status)url+='&status='+status;
    const r=await fetch(url);const d=await r.json();
    if(!d.success)return;
    const tb=G('wfHistoryTbody');
    const runs=d.runs||[];
    if(!runs.length){
      tb.innerHTML='<tr><td colspan="9" style="padding:24px;text-align:center;color:var(--t4);">No execution history yet</td></tr>';
      return;
    }
    tb.innerHTML=runs.map(run=>{
      const dur=run.duration_sec!=null?run.duration_sec+'s':'—';
      const rows=run.rows_processed!=null&&run.rows_processed>0?Number(run.rows_processed).toLocaleString():'—';
      const started=run.started_at?new Date(run.started_at).toLocaleString('en-US',{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit',second:'2-digit'}):'—';
      const runIdShort=(run.run_id||'').substring(0,8)+'…';
      return `<tr style="border-bottom:1px solid var(--border);transition:background .15s;" onmouseover="this.style.background='var(--surface-2)'" onmouseout="this.style.background=''">
        <td style="padding:6px 10px;font-family:var(--font-mono);font-size:10px;color:var(--t3);" title="${run.run_id}">${runIdShort}</td>
        <td style="padding:6px 10px;font-weight:600;font-size:11px;">${run.job_name||'—'}</td>
        <td style="padding:6px 10px;font-size:11px;">${WF_STAGE_LABELS[run.stage]||run.stage||'—'}</td>
        <td style="padding:6px 10px;font-size:11px;">${run.full_table||'—'}</td>
        <td style="padding:6px 10px;text-align:center;">${WF_STATUS_BADGES[run.status]||run.status}</td>
        <td style="padding:6px 10px;text-align:center;font-size:11px;">${dur}</td>
        <td style="padding:6px 10px;text-align:center;font-size:11px;">${rows}</td>
        <td style="padding:6px 10px;font-size:10px;color:var(--t3);">${started}</td>
        <td style="padding:6px 10px;text-align:center;white-space:nowrap;">
          <button class="btn btn-ghost btn-xs" onclick="wfViewRunLog('${run.run_id}')" title="View logs" style="padding:3px 8px;">📋</button>
          ${run.status==='failed'?'<button class="btn btn-xs" onclick="wfRerunFromHistory(\''+run.job_id+'\')" title="Rerun" style="padding:3px 8px;background:#fee2e2;color:#dc2626;border:1px solid #fca5a5;">↺ Rerun</button>':''}
        </td>
      </tr>`;
    }).join('');
  }catch(e){console.error('wfRefreshHistory',e);}
}

async function wfRefreshAuditHistory(){
  try{
    const tbl=(G('wfAuditFilterTable')||{}).value||'';
    let url='/api/v1/workflow/jobs/history';
    if(tbl)url+='?table_name='+encodeURIComponent(tbl);
    const r=await fetch(url);const d=await r.json();
    if(!d.success){console.error('audit history',d.error);return;}
    const tb=G('wfAuditTbody');
    const rows=d.history||[];
    if(!rows.length){
      tb.innerHTML='<tr><td colspan="9" style="padding:24px;text-align:center;color:var(--t4);">No archived jobs yet</td></tr>';
      return;
    }
    tb.innerHTML=rows.map(h=>{
      const created=h.created_at?new Date(h.created_at).toLocaleString('en-US',{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}):'—';
      const archived=h.archived_at?new Date(h.archived_at).toLocaleString('en-US',{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}):'—';
      const lt=h.load_type==='incremental'?'<span style="background:#dbeafe;color:#1d4ed8;padding:2px 6px;border-radius:4px;font-size:10px;font-weight:600;">INCREMENTAL</span>':'<span style="background:#e0e7ff;color:#4338ca;padding:2px 6px;border-radius:4px;font-size:10px;font-weight:600;">FULL</span>';
      const st=WF_STATUS_BADGES[h.status]||h.status||'—';
      return `<tr style="border-bottom:1px solid var(--border);transition:background .15s;" onmouseover="this.style.background='var(--surface-2)'" onmouseout="this.style.background=''">
        <td style="padding:6px 10px;font-weight:600;font-size:11px;">${h.job_name||'—'}</td>
        <td style="padding:6px 10px;font-size:11px;">${h.full_table||h.table_name||'—'}</td>
        <td style="padding:6px 10px;font-size:11px;">${WF_STAGE_LABELS[h.stage]||h.stage||'—'}</td>
        <td style="padding:6px 10px;text-align:center;">${lt}</td>
        <td style="padding:6px 10px;text-align:center;">${st}</td>
        <td style="padding:6px 10px;text-align:center;font-size:11px;">${h.run_count||0}</td>
        <td style="padding:6px 10px;font-size:10px;color:var(--t3);">${created}</td>
        <td style="padding:6px 10px;font-size:10px;color:var(--t3);">${archived}</td>
        <td style="padding:6px 10px;font-size:10px;color:var(--amber);">${h.archive_reason||'—'}</td>
      </tr>`;
    }).join('');
  }catch(e){console.error('wfRefreshAuditHistory',e);}
}

async function wfViewRunLog(runId){
  try{
    const r=await fetch('/api/v1/workflow/runs/'+runId);const d=await r.json();
    if(!d.success)return;
    const run=d.run;
    const logEl=G('wfRunLogs');
    const sc={success:'#a6e3a1',failed:'#f38ba8',running:'#89b4fa'};
    let html='<div style="color:'+(sc[run.status]||'#cdd6f4')+';font-weight:600;margin-bottom:6px;">'+run.job_name+' — '+run.status.toUpperCase()+'</div>';
    html+='<div style="color:#6c7086;margin-bottom:8px;">Run: '+run.run_id+'  ·  Started: '+(run.started_at||'—')+'</div>';
    (run.logs||[]).forEach(l=>html+='<div>'+l+'</div>');
    if(run.error)html+='<div style="color:#f38ba8;margin-top:6px;">ERROR: '+run.error+'</div>';
    logEl.innerHTML=html;
    logEl.scrollTop=logEl.scrollHeight;
  }catch(e){console.error('wfViewRunLog',e);}
}

async function wfRerunFromHistory(jobId){
  if(!jobId){toast('No job ID to rerun','terr');return;}
  // Find the group_id for this job and submit to Databricks
  try{
    const jr=await fetch('/api/v1/workflow/jobs/'+jobId);const jd=await jr.json();
    if(!jd.success){toast('Job not found','terr');return;}
    const groupId=jd.job?.group_id;
    if(!groupId){toast('No pipeline group for this job','terr');return;}
    toast('Resubmitting pipeline to Databricks…','tinfo');
    await wfRunOnDatabricks(groupId);
    setTimeout(()=>{wfRefreshJobs();wfRefreshHistory();},2000);
  }catch(e){toast(e.message,'terr');}
}

async function wfRunSingleJob(jobId){
  // Find the group_id for this job and submit to Databricks
  try{
    const job=_wfAllJobs.find(j=>j.job_id===jobId);
    const groupId=job?.group_id;
    if(!groupId){
      // Fallback: fetch job info from API
      const jr=await fetch('/api/v1/workflow/jobs/'+jobId);const jd=await jr.json();
      if(!jd.success){toast('Job not found','terr');return;}
      const gid=jd.job?.group_id;
      if(!gid){toast('No pipeline group for this job','terr');return;}
      toast('Submitting pipeline to Databricks…','tinfo');
      const pwd=(G('wfSrcPass')||{}).value||'';
      await wfRunOnDatabricks(gid, pwd);
    }else{
      toast('Submitting pipeline to Databricks…','tinfo');
      const pwd=(G('wfSrcPass')||{}).value||'';
      await wfRunOnDatabricks(groupId, pwd);
    }
    setTimeout(()=>{wfRefreshJobs();wfRefreshHistory();},2000);
  }catch(e){toast(e.message,'terr');}
}

async function wfRunPipelineGroup(groupId){
  try{
    toast('Starting pipeline…','tinfo');
    const r=await fetch('/api/v1/workflow/pipelines/'+groupId+'/run',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({})});
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Failed');
    toast('Pipeline started: '+d.total_jobs+' jobs queued','tok');
    (d.runs||[]).forEach(run=>{if(run.run_id)wfPollRun(run.run_id);});
    // Auto-open logs for this pipeline
    wfShowPipelineLogs(groupId, d.runs?.[0]?.full_table||groupId);
    setTimeout(()=>{wfRefreshAll();},1000);
  }catch(e){toast(e.message,'terr');
}
}

async function wfRerunPipeline(groupId){
  try{
    toast('Rerunning from first failed stage…','tinfo');
    const r=await fetch('/api/v1/workflow/pipelines/'+groupId+'/rerun',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({})});
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'No failed jobs found');
    const _SL={1:'Extract',2:'Landing→Bronze',3:'Bronze→Silver'};
    toast('Rerun started from '+(_SL[d.rerun_from]||'stage '+d.rerun_from)+': '+d.total_reran+' jobs','tok');
    // Auto-open logs for this pipeline
    wfShowPipelineLogs(groupId, '');
    setTimeout(()=>wfRefreshAll(),1000);
  }catch(e){toast(e.message,'terr');}
}

async function wfDeleteJob(jobId){
  if(!confirm('Delete this job?'))return;
  try{
    const r=await fetch('/api/v1/workflow/jobs/'+jobId,{method:'DELETE'});
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Failed');
    toast('Deleted: '+d.job_name,'tok');
    wfRefreshAll();
  }catch(e){toast(e.message,'terr');}
}

async function wfDeletePipeline(groupId){
  if(!confirm('Delete this entire pipeline group and all its jobs?'))return;
  try{
    const r=await fetch('/api/v1/workflow/pipelines/'+groupId,{method:'DELETE'});
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Failed');
    toast('Pipeline deleted ('+d.deleted_jobs.length+' jobs)','tok');
    wfClosePipelineLogs();
    wfRefreshAll();
  }catch(e){toast(e.message,'terr');}
}

/* ─── Pipeline Studio: Show Logs ─── */
let _wfLogGroupId='';
let _wfLogPollTimer=null;

async function wfShowPipelineLogs(groupId, tableName){
  _wfLogGroupId=groupId;
  const card=G('wfPipelineLogCard');
  const nameEl=G('wfPipelineLogName');
  card.style.display='';
  nameEl.textContent=tableName||groupId;
  card.scrollIntoView({behavior:'smooth',block:'nearest'});
  await _wfFetchPipelineLogs(groupId);
  // Auto-poll if any run is still running
  _wfStartLogPoll();
}

function wfClosePipelineLogs(){
  _wfLogGroupId='';
  G('wfPipelineLogCard').style.display='none';
  G('wfPipelineLogs').innerHTML='<div style="color:#6c7086;">// Select a pipeline to view execution logs…</div>';
  _wfStopLogPoll();
}

function wfRefreshPipelineLogs(){
  if(_wfLogGroupId) _wfFetchPipelineLogs(_wfLogGroupId);
}

async function _wfFetchPipelineLogs(groupId){
  try{
    const r=await fetch('/api/v1/workflow/runs?group_id='+encodeURIComponent(groupId)+'&limit=30');
    const d=await r.json();
    if(!d.success)return;
    const logEl=G('wfPipelineLogs');
    const runs=d.runs||[];
    if(!runs.length){
      logEl.innerHTML='<div style="color:#6c7086;">// No runs recorded yet for this pipeline — click ⚡ Databricks to start</div>';
      return;
    }
    let html='<div style="color:#89dceb;margin-bottom:10px;font-weight:600;">// Pipeline Execution Logs — '+runs.length+' run'+(runs.length>1?'s':'')+'</div>';
    const _SL={extract:'Extract',landing_to_bronze:'Landing→Bronze',bronze_to_silver:'Bronze→Silver',dlt_bronze_silver:'⚡ DLT Bronze+Silver'};
    let hasRunning=false;
    runs.forEach(run=>{
      const sc={success:'#a6e3a1',failed:'#f38ba8',running:'#89b4fa',created:'#6c7086'};
      const icon={success:'✅',failed:'❌',running:'🔄',created:'⏸'};
      if(run.status==='running') hasRunning=true;
      const stageLabel=run.stage?(' <span style="font-size:9px;padding:1px 5px;border-radius:4px;background:#45475a;color:#89dceb;margin-left:6px;">'+(_SL[run.stage]||run.stage)+'</span>'):'';
      html+='<div style="margin-bottom:12px;padding:8px 10px;border:1px solid #313244;border-radius:6px;background:#181825;">';
      html+='<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px;">';
      html+='<span style="color:'+(sc[run.status]||'#cdd6f4')+';font-weight:700;">'+(icon[run.status]||'•')+' '+run.job_name+stageLabel+' — '+run.status.toUpperCase()+'</span>';
      html+='<span style="color:#585b70;font-size:10px;">'+run.run_id+'</span>';
      html+='</div>';
      // Timing info
      if(run.started_at){
        html+='<div style="color:#585b70;font-size:10px;margin-bottom:4px;">⏱ Started: '+run.started_at;
        if(run.duration_sec!=null) html+=' · Duration: '+run.duration_sec+'s';
        if(run.rows_processed) html+=' · Rows: '+Number(run.rows_processed).toLocaleString();
        html+='</div>';
      }
      // Logs
      const logs=run.logs||[];
      if(logs.length){
        logs.forEach(l=>{
          let color='#cdd6f4';
          if(l.includes('✅')||l.includes('complete')||l.includes('SUCCESS')) color='#a6e3a1';
          else if(l.includes('❌')||l.includes('FAIL')||l.includes('ERROR')) color='#f38ba8';
          else if(l.includes('🔄')||l.includes('Running')||l.includes('🚀')) color='#89b4fa';
          else if(l.includes('⚠️')) color='#fab387';
          html+='<div style="color:'+color+';line-height:1.6;">'+l+'</div>';
        });
      }
      // Error message
      if(run.error){
        html+='<div style="color:#f38ba8;margin-top:4px;padding:4px 8px;background:#f38ba822;border-radius:4px;font-size:10px;">ERROR: '+run.error+'</div>';
      }
      // Rerun button for failed jobs
      if(run.status==='failed'&&run.job_id){
        html+='<button onclick="wfRunSingleJob(\''+run.job_id+'\')" style="margin-top:6px;margin-right:6px;padding:2px 10px;font-size:10px;background:#f38ba822;color:#f38ba8;border:1px solid #f38ba8;border-radius:4px;cursor:pointer;font-weight:600;" title="Rerun this failed job">🔄 Rerun Job</button>';
      }
      // Fetch Databricks Output button (for terminal runs with dbr_run_id)
      if(run.dbr_run_id && (run.status==='success'||run.status==='failed')){
        html+='<button onclick="wfFetchDbrOutput(\''+run.run_id+'\')" style="margin-top:6px;padding:2px 10px;font-size:10px;background:#45475a;color:#cdd6f4;border:1px solid #585b70;border-radius:4px;cursor:pointer;" title="Fetch notebook output from Databricks">📋 Fetch Databricks Output</button>';
      }
      html+='</div>';
    });
    logEl.innerHTML=html;
    logEl.scrollTop=logEl.scrollHeight;
    // Store running state for poll decision
    logEl.dataset.hasRunning=hasRunning?'1':'0';
  }catch(e){console.error('_wfFetchPipelineLogs',e);}
}

function _wfStartLogPoll(){
  _wfStopLogPoll();
  _wfLogPollTimer=setInterval(()=>{
    if(!_wfLogGroupId){_wfStopLogPoll();return;}
    const logEl=G('wfPipelineLogs');
    if(logEl&&logEl.dataset.hasRunning==='1'){
      _wfFetchPipelineLogs(_wfLogGroupId);
      wfRefreshPipelines();
    }else{
      _wfStopLogPoll();
    }
  },2000);
}
function _wfStopLogPoll(){
  if(_wfLogPollTimer){clearInterval(_wfLogPollTimer);_wfLogPollTimer=null;}
}

/* ── Auto-refresh pipeline status when any pipeline is running ── */
let _wfPipelineAutoPoll=null;
function _wfStartPipelineAutoPoll(){
  if(_wfPipelineAutoPoll) return;
  _wfPipelineAutoPoll=setInterval(()=>{
    const hasRunning=_wfPipelineData.some(g=>g.status==='running'||(g.jobs||[]).some(j=>j.status==='running'));
    if(hasRunning){
      wfRefreshPipelines();
    } else {
      clearInterval(_wfPipelineAutoPoll);
      _wfPipelineAutoPoll=null;
    }
  },5000);
}
// Start auto-poll after any pipeline refresh if running pipelines exist
const _patchedRefreshPipelines=wfRefreshPipelines;
wfRefreshPipelines=async function(){
  await _patchedRefreshPipelines();
  const hasRunning=_wfPipelineData.some(g=>g.status==='running'||(g.jobs||[]).some(j=>j.status==='running'));
  if(hasRunning) _wfStartPipelineAutoPoll();
};

async function wfFetchDbrOutput(runId){
  const host=(G('wfDbrHost')||{}).value||'';
  const token=(G('wfDbrToken')||{}).value||'';
  if(!host||!token){_toast('Enter Databricks host & token first','warn');return;}
  try{
    const r=await fetch('/api/v1/workflow/runs/'+runId+'/databricks-output',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({host,token})});
    const d=await r.json();
    if(!d.success){_toast(d.message||'Failed to fetch output','error');return;}
    let msg='';
    if(d.notebook_result) msg+='📄 Result: '+d.notebook_result+'\n';
    if(d.error) msg+='🔴 Error: '+d.error+'\n';
    if(d.error_trace) msg+='📋 Trace:\n'+d.error_trace+'\n';
    if(d.tasks&&d.tasks.length){
      msg+='\n📌 Tasks:\n';
      d.tasks.forEach(t=>{msg+='  '+t.task_key+': '+(t.result_state||t.life_cycle)+(t.state_message?' — '+t.state_message:'')+'\n';});
    }
    if(!msg) msg='No output available for this run.';
    alert(msg);
  }catch(e){_toast('Error fetching output: '+e.message,'error');}
}
/* ─── / Pipeline Studio Logs ─── */

async function wfViewJobLogs(jobId){
  try{
    const r=await fetch('/api/v1/workflow/jobs/'+jobId);const d=await r.json();
    if(!d.success)return;
    const logEl=G('wfRunLogs');
    const job=d.job;
    const runs=d.runs||[];
    let html='<div style="color:#a6e3a1;margin-bottom:8px;">// === '+job.job_name+' — Run History ===</div>';
    if(!runs.length)html+='<div style="color:#6c7086;">No runs yet</div>';
    runs.forEach(run=>{
      const sc={success:'#a6e3a1',failed:'#f38ba8',running:'#89b4fa'};
      html+='<div style="margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid #313244;">';
      html+='<div style="color:'+(sc[run.status]||'#cdd6f4')+';font-weight:600;">Run '+run.run_id+' — '+run.status.toUpperCase()+'</div>';
      (run.logs||[]).forEach(l=>html+='<div>'+l+'</div>');
      if(run.error)html+='<div style="color:#f38ba8;">ERROR: '+run.error+'</div>';
      html+='</div>';
    });
    logEl.innerHTML=html;
    // Auto-scroll sub-tab to jobs and switch
    switchAiSubTab('jobs',G('aiSubJobs'));
  }catch(e){console.error('wfViewJobLogs',e);}
}

function wfPollRun(runId){
  const poll=async()=>{
    try{
      const r=await fetch('/api/v1/workflow/runs/'+runId);const d=await r.json();
      if(!d.success)return;
      const run=d.run;
      // Update log panel live
      const logEl=G('wfRunLogs');
      let html='<div style="color:#89b4fa;font-weight:600;">▶ '+run.job_name+' — LIVE</div>';
      (run.logs||[]).forEach(l=>html+='<div>'+l+'</div>');
      logEl.innerHTML=html;
      logEl.scrollTop=logEl.scrollHeight;
      if(run.status==='running'){
        setTimeout(poll,1500);
      }else{
        wfRefreshAll();
      }
    }catch(e){console.error('wfPollRun',e);}
  };
  setTimeout(poll,800);
}

async function wfRefreshRuns(){
  try{
    const r=await fetch('/api/v1/workflow/runs?limit=20');const d=await r.json();
    if(!d.success)return;
    const logEl=G('wfRunLogs');
    if(!d.runs.length){logEl.innerHTML='<div style="color:#6c7086;">// No runs yet</div>';return;}
    let html='';
    d.runs.forEach(run=>{
      const sc={success:'#a6e3a1',failed:'#f38ba8',running:'#89b4fa'};
      html+='<div style="margin-bottom:10px;padding-bottom:6px;border-bottom:1px solid #313244;">';
      html+='<div style="color:'+(sc[run.status]||'#cdd6f4')+';font-weight:600;">'+run.job_name+' — '+run.status.toUpperCase()+'</div>';
      const lastLog=(run.logs||[]).slice(-3);
      lastLog.forEach(l=>html+='<div style="font-size:10px;">'+l+'</div>');
      html+='</div>';
    });
    logEl.innerHTML=html;
  }catch(e){console.error('wfRefreshRuns',e);}
}

async function wfRefreshWatermarks(){
  try{
    const r=await fetch('/api/v1/workflow/watermarks');const d=await r.json();
    if(!d.success)return;
    const el=G('wfWatermarks');
    const wms=d.watermarks;
    const keys=Object.keys(wms);
    if(!keys.length){el.innerHTML='<div style="color:var(--t4);text-align:center;padding:12px;">No watermarks — use incremental loads to track</div>';return;}
    el.innerHTML=keys.map(k=>{
      const w=wms[k];
      return `<div style="display:flex;justify-content:space-between;align-items:center;padding:8px;border:1px solid var(--border);border-radius:var(--r-sm);margin-bottom:6px;background:var(--surface-2);">
        <div>
          <div style="font-weight:600;font-size:12px;color:var(--t1);">${k}</div>
          <div style="font-size:10px;color:var(--t3);">${w.column} = ${w.last_value||'<em>not set</em>'}</div>
        </div>
        <button class="btn btn-ghost btn-xs" onclick="wfResetWatermark('${k}')" title="Reset watermark" style="color:var(--amber);">↺ Reset</button>
      </div>`;
    }).join('');
  }catch(e){console.error('wfRefreshWatermarks',e);}
}

async function wfResetWatermark(table){
  if(!confirm('Reset watermark for '+table+'? Next run will do a full load.'))return;
  try{
    const r=await fetch('/api/v1/workflow/watermarks/reset',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({table:table})});
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Failed');
    toast(d.message,'tok');
    wfRefreshWatermarks();
  }catch(e){toast(e.message,'terr');}
}

// ═══════════ SETTINGS / DEPLOY CONFIG ═══════════
function _collectConfig(){
  const extLocs={};
  document.querySelectorAll('#cfgExtLocList [data-extloc]').forEach(row=>{
    const n=row.querySelector('.cfg-extloc-name').value.trim();
    const u=row.querySelector('.cfg-extloc-url').value.trim();
    if(n&&u) extLocs[n]=u;
  });
  const catalogs={};
  document.querySelectorAll('#cfgCatalogList [data-catalog]').forEach(row=>{
    const n=row.querySelector('.cfg-cat-name').value.trim();
    const l=row.querySelector('.cfg-cat-loc').value.trim();
    const s=(row.querySelector('.cfg-cat-schemas')?row.querySelector('.cfg-cat-schemas').value.trim():'default')||'default';
    if(n&&l) catalogs[n]={location:l, schemas:s.split(',').map(x=>x.trim()).filter(Boolean)};
  });
  const folders=(G('cfgFolders').value||'').split('\n').map(s=>s.trim()).filter(Boolean);
  return {
    subscription_id: G('cfgSubId').value.trim(),
    resource_group:  G('cfgResourceGroup').value.trim(),
    region:          G('cfgRegion').value,
    databricks_host: G('cfgDbrHost').value.trim(),
    databricks_token: G('cfgDbrToken').value.trim(),
    storage_account: G('cfgStorageAcct').value.trim(),
    container:       G('cfgContainer').value.trim(),
    folders:         folders,
    access_connector:G('cfgAccessConnector').value.trim(),
    storage_credential_name:G('cfgStorageCredName')?.value?.trim()||'',
    role_assignment: G('cfgRole').value,
    external_locations: extLocs,
    catalogs:        catalogs,
    volume_name:     G('cfgVolName').value.trim(),
    volume_catalog:  G('cfgVolCatalog').value.trim(),
    volume_schema:   G('cfgVolSchema').value.trim()||'default',
    volume_path:     G('cfgVolPath').value.trim(),
    reconciliation: {
      catalog:  G('cfgReconCatalog').value.trim()||'reconciliation',
      schema:   G('cfgReconSchema').value.trim()||'hr',
      table:    G('cfgReconTable').value.trim()||'ReconcilationDetails',
      location: G('cfgReconLocation').value.trim(),
    },
    logging: {
      catalog:  G('cfgLogCatalog').value.trim()||'logging',
      schema:   G('cfgLogSchema').value.trim()||'hr',
      table:    G('cfgLogTable').value.trim()||'ExecutionLog',
      location: G('cfgLogLocation').value.trim(),
    },
    cdc: {
      cdc_mode: (G('cfgCdcMode')||{}).value||'watermark',
      dlt_mode: (G('cfgDltMode')||{}).value||'standard',
      primary_keys: (G('cfgPrimaryKeys')||{}).value ? G('cfgPrimaryKeys').value.split(',').map(s=>s.trim()).filter(Boolean) : [],
    },
    source: {
      source_type: (G('cfgSrcType')||{}).value||(G('wfSrcType')||{}).value||'sqlserver',
      server:      G('cfgSrcServer')?.value?.trim()||(G('wfSrcServer')||{}).value?.trim()||'',
      database:    G('cfgSrcDb')?.value?.trim()||(G('wfSrcDb')||{}).value?.trim()||'',
      username:    G('cfgSrcUser')?.value?.trim()||(G('wfSrcUser')||{}).value?.trim()||'',
      password:    G('cfgSrcPass')?.value||(G('wfSrcPass')||{}).value||'',
    },
    metadata_catalog: G('cfgMetaCatalog')?.value?.trim()||'',
    metadata_schema:  G('cfgMetaSchema')?.value?.trim()||'',
    azure_tenant_id:  G('cfgTenantId')?.value?.trim()||'',
    azure_client_id:  G('cfgClientId')?.value?.trim()||'',
    azure_client_secret: G('cfgClientSecret')?.value||'',
  };
}

function _populateConfig(c){
  if(!c) return;
  G('cfgSubId').value=c.subscription_id||'';
  G('cfgResourceGroup').value=c.resource_group||'';
  G('cfgRegion').value=c.region||'centralindia';
  G('cfgDbrHost').value=c.databricks_host||'';
  G('cfgDbrToken').value=c.databricks_token||'';
  G('cfgStorageAcct').value=c.storage_account||'';
  G('cfgContainer').value=c.container||'';
  G('cfgFolders').value=(c.folders||[]).join('\n');
  G('cfgAccessConnector').value=c.access_connector||'';
  if(G('cfgStorageCredName')) G('cfgStorageCredName').value=c.storage_credential_name||'';
  G('cfgRole').value=c.role_assignment||'Storage Blob Data Owner';
  G('cfgTenantId').value=c.azure_tenant_id||'';
  G('cfgClientId').value=c.azure_client_id||'';
  G('cfgClientSecret').value=c.azure_client_secret||'';
  if(c.azure_tenant_id&&c.azure_client_id&&c.azure_client_secret){
    const spSt=G('cfgSpStatus');if(spSt){spSt.innerHTML='<span style="color:#16a34a;">✓ Service Principal configured</span>';}
  }
  // External locations
  const elList=G('cfgExtLocList'); elList.innerHTML='';
  const elEntries=Object.entries(c.external_locations||{});
  (elEntries.length?elEntries:[['','']]).forEach(([n,u])=>{
    _addExtLocRow(n,u);
  });
  // Catalogs
  const catList=G('cfgCatalogList'); catList.innerHTML='';
  const catEntries=Object.entries(c.catalogs||{});
  (catEntries.length?catEntries:[['',{location:'',schemas:['default']}]]).forEach(([n,v])=>{
    if(typeof v==='string') _addCatalogRow(n,v,'default');
    else _addCatalogRow(n,v.location||'',(v.schemas||['default']).join(','));
  });
  G('cfgVolName').value=c.volume_name||'';
  G('cfgVolCatalog').value=c.volume_catalog||'';
  G('cfgVolSchema').value=c.volume_schema||'default';
  G('cfgVolPath').value=c.volume_path||'';
  // Reconciliation
  const rc=c.reconciliation||{};
  G('cfgReconCatalog').value=rc.catalog||'reconciliation';
  G('cfgReconSchema').value=rc.schema||'hr';
  G('cfgReconTable').value=rc.table||'ReconcilationDetails';
  G('cfgReconLocation').value=rc.location||'';
  // Logging
  const lc=c.logging||{};
  G('cfgLogCatalog').value=lc.catalog||'logging';
  G('cfgLogSchema').value=lc.schema||'hr';
  G('cfgLogTable').value=lc.table||'ExecutionLog';
  G('cfgLogLocation').value=lc.location||'';
  // CDC / DLT
  const cc=c.cdc||{};
  if(G('cfgCdcMode')) G('cfgCdcMode').value=cc.cdc_mode||'watermark';
  if(G('cfgDltMode')) G('cfgDltMode').value=cc.dlt_mode||'standard';
  if(G('cfgPrimaryKeys')) G('cfgPrimaryKeys').value=(cc.primary_keys||[]).join(', ');
  if(typeof cfgCdcModeChange==='function') cfgCdcModeChange();
  // Source connection — populate BOTH Settings fields AND Pipeline Studio fields
  const src=c.source||{};
  // Settings page source fields
  if(G('cfgSrcType')&&src.source_type) G('cfgSrcType').value=src.source_type;
  if(G('cfgSrcServer')) G('cfgSrcServer').value=src.server||'';
  if(G('cfgSrcDb'))     G('cfgSrcDb').value=src.database||'';
  if(G('cfgSrcUser'))   G('cfgSrcUser').value=src.username||'';
  if(G('cfgSrcPass'))   G('cfgSrcPass').value=src.password||'';
  // Pipeline Studio source fields (hidden)
  if(G('wfSrcType')&&src.source_type) G('wfSrcType').value=src.source_type;
  if(G('wfSrcServer')) G('wfSrcServer').value=src.server||'';
  if(G('wfSrcDb'))     G('wfSrcDb').value=src.database||'';
  if(G('wfSrcUser'))   G('wfSrcUser').value=src.username||'';
  if(G('wfSrcPass'))   G('wfSrcPass').value=src.password||'';
  // Show source info in Pipeline Studio compact card
  if(G('wfSrcConnInfo')&&src.server) G('wfSrcConnInfo').textContent='🟢 '+src.server+' / '+(src.database||'');
  // Metadata catalog/schema — Settings fields AND MetadataFlow fields
  const metaCat=c.metadata_catalog||'';
  const metaSch=c.metadata_schema||'';
  if(G('cfgMetaCatalog')) G('cfgMetaCatalog').value=metaCat;
  if(G('cfgMetaSchema'))  G('cfgMetaSchema').value=metaSch;
  if(G('wfDbrCatalog')&&metaCat) G('wfDbrCatalog').value=metaCat;
  if(G('wfDbrSchema')&&metaSch)  G('wfDbrSchema').value=metaSch;
  // MetadataFlow Databricks host/token fields (populated from Settings config)
  if(G('wfDbrHost')&&c.databricks_host)  G('wfDbrHost').value=c.databricks_host;
  if(G('wfDbrToken')&&c.databricks_token) G('wfDbrToken').value=c.databricks_token;
  // Schema Comparison — auto-populate source/target from config
  const scSrc=G('scSourceSchema'), scTgt=G('scTargetSchema');
  if(scSrc&&!scSrc.value&&src.database) scSrc.value=src.database+'.dbo';
  if(scTgt&&!scTgt.value){
    const cats=Object.keys(c.catalogs||{});
    const bronzeCat=cats.find(k=>k.toLowerCase().includes('bronze'))||cats[0]||'';
    const bronzeSchema=bronzeCat&&c.catalogs[bronzeCat]?(c.catalogs[bronzeCat].schemas||[])[0]||'default':'default';
    if(bronzeCat) scTgt.value=bronzeCat+'.'+bronzeSchema;
  }
  /* refresh interactive status after populate */
  if(typeof cfgUpdateStatus==='function') cfgUpdateStatus();
  if(typeof cfgDeriveAbfss==='function') cfgDeriveAbfss();
  if(typeof cfgCheckAzureAuth==='function') cfgCheckAzureAuth();
}

/* ── Settings Accordion & Interactive Helpers ── */
window.cfgToggleAccordion=function(id){
  const el=G(id); if(!el)return;
  el.classList.toggle('open');
};
window.cfgUpdateStatus=function(){
  const filled=id=>{const e=G(id);return e&&e.value.trim().length>0;};
  const dot=(pillId,ok)=>{const p=G(pillId);if(!p)return;const d=p.querySelector('.cfg-dot');if(d){d.className='cfg-dot'+(ok?' ok':'');}};
  dot('cfgStatAzure',filled('cfgSubId')&&filled('cfgDbrHost')&&filled('cfgDbrToken'));
  dot('cfgStatSrc',filled('cfgSrcServer')&&filled('cfgSrcDb'));
  dot('cfgStatStorage',filled('cfgStorageAcct')&&filled('cfgContainer'));
  dot('cfgStatUC',G('cfgCatalogList')&&G('cfgCatalogList').children.length>0);
  dot('cfgStatCDC',filled('cfgCdcMode'));
  const h=G('cfgHintAzure');if(h){const host=G('cfgDbrHost');h.textContent=host&&host.value?host.value.replace(/^https?:\/\//,'').slice(0,30):'';};
  const hs=G('cfgHintSrc');if(hs){const sv=G('cfgSrcServer');hs.textContent=sv&&sv.value?sv.value.slice(0,30):'';};
};
window.cfgDeriveAbfss=function(){
  const acct=(G('cfgStorageAcct')||{}).value||'';
  const cont=(G('cfgContainer')||{}).value||'';
  const preview=G('cfgAbfssPreview');
  const base=G('cfgAbfssBase');
  if(acct&&cont){
    const url='abfss://'+cont+'@'+acct+'.dfs.core.windows.net';
    if(base) base.textContent=url;
    if(preview) preview.style.display='block';
  } else {
    if(preview) preview.style.display='none';
  }
};
window.cfgAutoFillVolPath=function(){
  const acct=(G('cfgStorageAcct')||{}).value||'';
  const cont=(G('cfgContainer')||{}).value||'';
  if(!acct||!cont){alert('Please fill Storage Account and Container first.');return;}
  const f=G('cfgVolPath');if(f)f.value='abfss://'+cont+'@'+acct+'.dfs.core.windows.net/dev/landing';
};
window.cfgAutoFillLoc=function(fieldId){
  const acct=(G('cfgStorageAcct')||{}).value||'';
  const cont=(G('cfgContainer')||{}).value||'';
  if(!acct||!cont){alert('Please fill Storage Account and Container first.');return;}
  const f=G(fieldId);if(f)f.value='abfss://'+cont+'@'+acct+'.dfs.core.windows.net/dev/uc-managed';
};
window.cfgAutoFillInput=function(inputEl){
  const acct=(G('cfgStorageAcct')||{}).value||'';
  const cont=(G('cfgContainer')||{}).value||'';
  if(!acct||!cont){alert('Please fill Storage Account and Container first.');return;}
  if(inputEl)inputEl.value='abfss://'+cont+'@'+acct+'.dfs.core.windows.net';
};
window.cfgAutoFillCatLoc=function(row){
  const acct=(G('cfgStorageAcct')||{}).value||'';
  const cont=(G('cfgContainer')||{}).value||'';
  if(!acct||!cont){alert('Please fill Storage Account and Container first.');return;}
  const nameInp=row.querySelector('.cfg-cat-name');
  const catName=(nameInp&&nameInp.value.trim())||'';
  const locInp=row.querySelector('.cfg-cat-loc');
  if(locInp) locInp.value='abfss://'+cont+'@'+acct+'.dfs.core.windows.net/dev/uc-managed'+(catName?'/'+catName:'');
};
window.cfgSwitchSubTab=function(tab){
  const recon=G('cfgSubReconPanel'),log=G('cfgSubLogPanel');
  const tRecon=G('cfgSubRecon'),tLog=G('cfgSubLog');
  if(tab==='recon'){
    if(recon)recon.style.display='';if(log)log.style.display='none';
    if(tRecon)tRecon.classList.add('active');if(tLog)tLog.classList.remove('active');
  } else {
    if(recon)recon.style.display='none';if(log)log.style.display='';
    if(tRecon)tRecon.classList.remove('active');if(tLog)tLog.classList.add('active');
  }
};
window.cfgTogglePw=function(fieldId,btn){
  const f=G(fieldId);if(!f)return;
  const isP=f.type==='password';f.type=isP?'text':'password';
  if(btn)btn.title=isP?'Hide':'Show';
};

function _addExtLocRow(name,url){
  const d=document.createElement('div');
  d.className='cfg-grid'; d.style.marginBottom='8px'; d.setAttribute('data-extloc','');
  d.innerHTML='<div><label class="lbl">Location Name</label><input class="inp cfg-extloc-name" placeholder="e.g. landing_loc_mig" value="'+(name||'')+'"></div>'+
    '<div style="display:flex;gap:6px;align-items:flex-end;"><div style="flex:1;"><label class="lbl">ABFSS URL <button type=button class=cfg-auto-btn onclick="cfgAutoFillInput(this.closest(\'[data-extloc]\').querySelector(\'.cfg-extloc-url\'))">Auto-fill</button></label><input class="inp cfg-extloc-url" placeholder="abfss://..." value="'+(url||'')+'"></div>'+
    '<button class="btn btn-ghost btn-xs" onclick="this.closest(\'.cfg-grid\').remove()" style="margin-bottom:2px;color:var(--red);" title="Remove">&times;</button></div>';
  G('cfgExtLocList').appendChild(d);
}
function cfgAddExtLoc(){ _addExtLocRow('',''); }

function _addCatalogRow(name,loc,schemas){
  const d=document.createElement('div');
  d.className='cfg-grid-3'; d.style.marginBottom='8px'; d.setAttribute('data-catalog','');
  d.innerHTML='<div><label class="lbl">Catalog Name</label><input class="inp cfg-cat-name" placeholder="e.g. bronze" value="'+(name||'')+'"></div>'+
    '<div><label class="lbl">Managed Location (ABFSS) <button type=button class=cfg-auto-btn onclick="cfgAutoFillCatLoc(this.closest(\'[data-catalog]\'))">Auto-fill</button></label><input class="inp cfg-cat-loc" placeholder="abfss://..." value="'+(loc||'')+'"></div>'+
    '<div style="display:flex;gap:6px;align-items:flex-end;"><div style="flex:1;"><label class="lbl">Schemas</label><input class="inp cfg-cat-schemas" placeholder="default,hr,raw" value="'+(schemas||'default')+'"></div>'+
    '<button class="btn btn-ghost btn-xs" onclick="this.closest(\'.cfg-grid-3\').remove()" style="margin-bottom:2px;color:var(--red);" title="Remove">&times;</button></div>';
  G('cfgCatalogList').appendChild(d);
}
function cfgAddCatalog(){ _addCatalogRow('','','default'); }

function cfgCdcModeChange(){
  const mode=(G('cfgCdcMode')||{}).value||'watermark';
  const ctCfg=G('cfgCtConfig');
  if(ctCfg) ctCfg.style.display=(mode==='change_tracking'?'block':'none');
  const badge=G('cfgCdcBadge');
  if(badge) badge.textContent=(mode==='change_tracking'?'CT':'CDC');
  const silverInfo=G('cfgSilverDltInfo');
  if(silverInfo) silverInfo.textContent=(mode==='change_tracking'?'dlt.apply_changes() — SCD Type 1':'@dlt.table + DQ validation');
}

async function cfgTestSourceConn(){
  const badge=G('cfgSrcConnBadge');
  const server=G('cfgSrcServer')?.value?.trim();
  const db=G('cfgSrcDb')?.value?.trim();
  const user=G('cfgSrcUser')?.value?.trim();
  const pwd=G('cfgSrcPass')?.value||'';
  const srcType=G('cfgSrcType')?.value||'sqlserver';
  if(!server||!db||!user){toast('Fill in server, database and username','terr');return;}
  badge.textContent='Testing…';badge.style.background='#f59e0b';badge.style.color='#fff';
  try{
    const r=await fetch('/api/v1/source/test-connection',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({source_type:srcType,server:server,database:db,username:user,password:pwd})});
    const ct = r.headers.get('content-type')||'';
    let d;
    if(ct.includes('application/json')){
      d = await r.json();
    } else {
      const txt = await r.text();
      // Detect login redirect (session expired)
      if(r.status===401 || r.status===302 || /<title>\s*Login/i.test(txt) || /name=["']password["']/i.test(txt)){
        throw new Error('Session expired — please log in again and retry.');
      }
      // Strip HTML tags for a readable message
      const plain = txt.replace(/<[^>]+>/g,' ').replace(/\s+/g,' ').trim().slice(0,300);
      throw new Error('Server returned HTML (HTTP '+r.status+'): '+(plain||'check server logs'));
    }
    if(d.success){
      badge.textContent='Connected ✓';badge.style.background='#10b981';badge.style.color='#fff';
      badge.className='cfg-conn-badge pass';
      toast('Source connection successful!'+(d.server_version?' — '+d.server_version:''),'tok');
      // Auto-copy to Pipeline Studio fields
      if(G('wfSrcType')) G('wfSrcType').value=srcType;
      if(G('wfSrcServer')) G('wfSrcServer').value=server;
      if(G('wfSrcDb')) G('wfSrcDb').value=db;
      if(G('wfSrcUser')) G('wfSrcUser').value=user;
      if(G('wfSrcPass')) G('wfSrcPass').value=pwd;
    }else{
      throw new Error(d.error||('Connection failed (HTTP '+r.status+')'));
    }
  }catch(e){
    badge.textContent='Failed ✕';badge.style.background='#ef4444';badge.style.color='#fff';
    badge.className='cfg-conn-badge fail';
    toast(e.message,'terr');
    console.error('[Test Connection] error:', e);
  }
}

async function cfgTestDatabricksConn(){
  const badge=G('cfgDbrConnBadge');
  const info=G('cfgDbrConnInfo');
  const host=G('cfgDbrHost')?.value?.trim();
  const token=G('cfgDbrToken')?.value?.trim();
  if(!host||!token){toast('Fill in Databricks Host URL and PAT Token','terr');return;}
  badge.textContent='Testing…';badge.style.background='#f59e0b';badge.style.color='#fff';
  if(info) info.textContent='';
  try{
    const r=await fetch('/api/v1/test-databricks',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({databricks_host:host,databricks_token:token})});
    const ct=r.headers.get('content-type')||'';
    let d;
    if(ct.includes('application/json')){
      d=await r.json();
    }else{
      const txt=await r.text();
      if(r.status===401||r.status===302||/<title>\s*Login/i.test(txt)||/name=["']password["']/i.test(txt)){
        throw new Error('Session expired — please log in again and retry.');
      }
      const plain=txt.replace(/<[^>]+>/g,' ').replace(/\s+/g,' ').trim().slice(0,300);
      throw new Error('Server returned HTML (HTTP '+r.status+'): '+(plain||'check server logs'));
    }
    if(d.success){
      badge.textContent='Connected ✓';badge.style.background='#10b981';badge.style.color='#fff';
      badge.className='cfg-conn-badge pass';
      const detail=d.total_clusters!=null?d.running_clusters+'/'+d.total_clusters+' clusters running':'';
      if(info) info.textContent=detail;
      toast('Databricks workspace connected!'+(detail?' — '+detail:''),'tok');
    }else{
      throw new Error(d.error||('Connection failed (HTTP '+r.status+')'));
    }
  }catch(e){
    badge.textContent='Failed ✕';badge.style.background='#ef4444';badge.style.color='#fff';
    badge.className='cfg-conn-badge fail';
    if(info) info.textContent=e.message;
    toast(e.message,'terr');
    console.error('[Databricks Test] error:',e);
  }
}

function cfgPreview(){
  const w=G('cfgJsonWrap');
  if(w) w.style.display=w.style.display==='none'?'':'none';
  G('cfgJsonPreview').textContent=JSON.stringify(_collectConfig(),null,2);
}

/* ── Azure Device Code Authentication ── */
let _azureAuthPollTimer = null;

async function cfgStartAzureAuth(){
  const btn = G('btnAzureAuth');
  const status = G('azureAuthStatus');
  const codeBox = G('azureDeviceCodeBox');
  btn.disabled = true; btn.textContent = 'Starting…';
  if(status) status.textContent = '';
  try {
    const r = await fetch('/api/v1/azure-auth', { method: 'POST' });
    const d = await r.json();
    if(d.status === 'success'){
      // Already authenticated
      if(status){ status.style.cssText='font-size:10px;color:#16a34a;font-weight:600;'; status.textContent = '✅ Already authenticated'; }
      G('btnAzureLogout').style.display = '';
      btn.style.background = '#16a34a';
      btn.innerHTML = '<svg viewBox="0 0 24 24" style="width:12px;height:12px;margin-right:4px;stroke:currentColor;fill:none;stroke-width:2;"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Authenticated';
      btn.disabled = false;
      toast('Already authenticated with Azure','tok');
      return;
    }
    if(!d.user_code){ toast(d.error||'Failed to start auth','terr'); return; }
    // Show device code box
    G('azureDeviceCode').textContent = d.user_code;
    if(d.verification_uri){ G('azureDeviceCodeLink').href = d.verification_uri; }
    codeBox.style.display = 'block';
    if(status) status.textContent = 'Waiting for you to complete login…';
    toast('Enter the code at the Microsoft login page','tinfo',8000);
    // Start polling for completion
    _azureAuthPollTimer = setInterval(async () => {
      try {
        const sr = await fetch('/api/v1/azure-auth/status');
        const sd = await sr.json();
        if(sd.status === 'success' || sd.authenticated){
          clearInterval(_azureAuthPollTimer); _azureAuthPollTimer = null;
          codeBox.style.display = 'none';
          if(status){ status.style.cssText='font-size:10px;color:#16a34a;font-weight:600;'; status.textContent = '✅ Authenticated as Azure admin'; }
          G('btnAzureLogout').style.display = '';
          btn.style.background = '#16a34a';
          btn.innerHTML = '<svg viewBox="0 0 24 24" style="width:12px;height:12px;margin-right:4px;stroke:currentColor;fill:none;stroke-width:2;"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Authenticated';
          toast('Azure authentication successful!','tok');
        } else if(sd.status === 'error'){
          clearInterval(_azureAuthPollTimer); _azureAuthPollTimer = null;
          codeBox.style.display = 'none';
          if(status){ status.style.cssText='font-size:10px;color:#dc2626;'; status.textContent = '❌ ' + (sd.error||'Authentication failed'); }
          toast('Azure authentication failed','terr');
          btn.disabled = false;
          btn.innerHTML = '<svg viewBox="0 0 24 24" style="width:12px;height:12px;margin-right:4px;stroke:currentColor;fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round;"><path d="M15 3h4a2 2 0 012 2v14a2 2 0 01-2 2h-4"/><polyline points="10 17 15 12 10 7"/><line x1="15" y1="12" x2="3" y2="12"/></svg> Authenticate with Azure';
          btn.style.background = '#0078D4';
        }
      } catch(e) { /* poll errors are transient, keep polling */ }
    }, 3000);
  } catch(e) {
    toast('Error: '+e.message,'terr');
    if(status) status.textContent = '❌ ' + e.message;
  } finally {
    if(!_azureAuthPollTimer){
      btn.disabled = false;
      btn.innerHTML = '<svg viewBox="0 0 24 24" style="width:12px;height:12px;margin-right:4px;stroke:currentColor;fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round;"><path d="M15 3h4a2 2 0 012 2v14a2 2 0 01-2 2h-4"/><polyline points="10 17 15 12 10 7"/><line x1="15" y1="12" x2="3" y2="12"/></svg> Authenticate with Azure';
    }
  }
}

async function cfgAzureLogout(){
  try {
    await fetch('/api/v1/azure-auth/logout', { method: 'POST' });
    G('btnAzureLogout').style.display = 'none';
    const btn = G('btnAzureAuth');
    btn.disabled = false; btn.style.background = '#0078D4';
    btn.innerHTML = '<svg viewBox="0 0 24 24" style="width:12px;height:12px;margin-right:4px;stroke:currentColor;fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round;"><path d="M15 3h4a2 2 0 012 2v14a2 2 0 01-2 2h-4"/><polyline points="10 17 15 12 10 7"/><line x1="15" y1="12" x2="3" y2="12"/></svg> Authenticate with Azure';
    const status = G('azureAuthStatus');
    if(status){ status.style.cssText='font-size:10px;color:var(--t3);'; status.textContent = 'Logged out'; }
    toast('Azure session cleared','tinfo');
  } catch(e) { toast('Error: '+e.message,'terr'); }
}

async function cfgCheckAzureAuth(){
  try {
    const r = await fetch('/api/v1/azure-auth/status');
    const d = await r.json();
    if(d.status === 'success' || d.authenticated){
      const status = G('azureAuthStatus');
      if(status){ status.style.cssText='font-size:10px;color:#16a34a;font-weight:600;'; status.textContent = '✅ Authenticated as Azure admin'; }
      const btn = G('btnAzureAuth');
      btn.style.background = '#16a34a';
      btn.innerHTML = '<svg viewBox="0 0 24 24" style="width:12px;height:12px;margin-right:4px;stroke:currentColor;fill:none;stroke-width:2;"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Authenticated';
      G('btnAzureLogout').style.display = '';
    }
  } catch(e) { /* status check failed, that's fine */ }
}

async function cfgApplyRbac(){
  const role=G('cfgRole')?.value||'Storage Blob Data Owner';
  const sa=G('cfgStorageAcct')?.value?.trim();
  if(!sa){toast('Enter a Storage Account Name first','terr');return;}
  if(!confirm(`Assign "${role}" role to the App Service managed identity on storage account "${sa}"?\n\nThis also assigns to the Access Connector if configured.`)) return;
  const btn=G('btnApplyRbac');
  const status=G('rbacStatus');
  btn.disabled=true;btn.textContent='Saving config & applying…';
  if(status){status.textContent='';status.style.cssText='font-size:10px;color:var(--t3);';}
  try{
    // Auto-save current UI config first so backend uses latest values
    const cfg=_collectConfig();
    const sr=await fetch('/api/v1/deploy-config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(cfg)});
    const sd=await sr.json();
    if(!sd.success){toast('Failed to save config: '+(sd.error||''),'terr');return;}
    _cachedDeployConfig=cfg;
    const r=await fetch('/api/v1/apply-rbac',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({role_name:role})});
    const d=await r.json();
    if(d.success){
      toast('RBAC role assigned successfully','tok');
      if(status){status.style.cssText='font-size:10px;color:#16a34a;white-space:pre-wrap;max-width:800px;';status.textContent='✅ '+d.message;}
    }else{
      // Show CLI fallback command if provided
      if(d.cli_command){
        if(status){
          status.style.cssText='font-size:10px;color:#dc2626;white-space:pre-wrap;max-width:800px;background:#fef2f2;padding:8px 10px;border-radius:6px;border:1px solid #fecaca;margin-top:6px;';
          status.innerHTML='⚠️ '+d.error.replace(/\n/g,'<br>')+'<br><br><code style="background:#1e293b;color:#e2e8f0;padding:6px 10px;border-radius:4px;display:block;margin-top:4px;font-size:10px;word-break:break-all;cursor:pointer;" title="Click to copy" onclick="navigator.clipboard.writeText(this.textContent);this.style.outline=\'2px solid #10b981\';">'+d.cli_command+'</code>';
        }
        toast('Run the CLI command shown below','terr',6000);
      }else{
        toast(d.error||'Failed to apply RBAC','terr');
        if(status){status.style.cssText='font-size:10px;color:#dc2626;white-space:pre-wrap;max-width:800px;';status.textContent='❌ '+(d.error||'Failed');}
      }
    }
  }catch(e){toast('Error: '+e.message,'terr');if(status){status.style.cssText='font-size:10px;color:#dc2626;';status.textContent='❌ '+e.message;}}
  finally{btn.disabled=false;btn.innerHTML='<svg viewBox="0 0 24 24" style="width:12px;height:12px;margin-right:4px;stroke:currentColor;fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round;"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg> Apply RBAC to App Service Identity';}
}

async function cfgTestStorageCredential(){
  const credName=(G('cfgStorageCredName')?.value||G('cfgAccessConnector')?.value||'').trim();
  if(!credName){toast('Enter a Storage Credential Name (or Access Connector Name) first','terr');return;}
  const host=(G('cfgDbrHost')?.value||'').trim();
  const token=(G('cfgDbrToken')?.value||'').trim();
  if(!host||!token){toast('Configure Databricks Host and Token first','terr');return;}
  // Build test URL from storage account + container
  const sa=(G('cfgStorageAcct')?.value||'').trim();
  const ct=(G('cfgContainer')?.value||'').trim();
  const testUrl=sa&&ct?'abfss://'+ct+'@'+sa+'.dfs.core.windows.net':'';
  const btn=G('btnTestStorageCred');
  const status=G('storageCredStatus');
  const detail=G('storageCredDetail');
  btn.disabled=true;btn.textContent='Testing…';
  status.textContent='';status.style.cssText='font-size:10px;color:var(--t3);';
  detail.style.display='none';detail.textContent='';
  try{
    const r=await fetch('/api/v1/test-storage-credential',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({
      databricks_host:host,databricks_token:token,storage_credential_name:credName,test_url:testUrl
    })});
    const d=await r.json();
    if(d.success){
      status.style.cssText='font-size:10px;color:#16a34a;font-weight:600;';
      status.textContent='✅ '+(d.message||'Credential valid');
      let lines=['Credential: '+d.credential_name,'ID: '+d.credential_id,'Owner: '+d.owner];
      if(d.access_connector_id) lines.push('Access Connector: '+d.access_connector_id);
      if(d.external_location) lines.push('External Location: '+d.external_location+' (covers this path)');
      if(d.validation&&d.validation.passed) lines.push('Validation: '+(d.validation.overlap?'PASSED (path already managed by external location)':'PASSED for '+d.validation.url));
      detail.textContent=lines.join('\n');
      detail.style.display='block';detail.style.borderColor='#16a34a';
      toast('Storage credential is valid','tok');
    }else{
      status.style.cssText='font-size:10px;color:#dc2626;font-weight:600;';
      status.textContent='❌ '+(d.error||'Validation failed');
      let lines=[d.error||'Failed'];
      if(d.detail) lines.push(d.detail);
      if(d.failed_checks) lines.push('','Failed checks:',  ...d.failed_checks);
      if(d.credential_id) lines.push('','Credential ID: '+d.credential_id,'Owner: '+(d.owner||'?'));
      if(d.access_connector_id) lines.push('Access Connector: '+d.access_connector_id);
      detail.textContent=lines.join('\n');
      detail.style.display='block';detail.style.borderColor='#dc2626';
      toast(d.error||'Storage credential test failed','terr');
    }
  }catch(e){
    status.style.cssText='font-size:10px;color:#dc2626;';status.textContent='❌ '+e.message;
    toast('Error: '+e.message,'terr');
  }finally{
    btn.disabled=false;
    btn.innerHTML='<svg viewBox="0 0 24 24" style="width:12px;height:12px;margin-right:4px;stroke:currentColor;fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round;"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Test Storage Credential';
  }
}

async function cfgCleanMetadata(){
  const inp=G('cleanConfirmInput');
  if(!inp||inp.value.trim()!=='CLEAN'){
    toast('Type "CLEAN" in the confirmation box to proceed','terr');return;
  }
  const cleanAdls=G('cleanChkAdls')?.checked??true;
  const cleanTables=G('cleanChkTables')?.checked??true;
  if(!cleanAdls&&!cleanTables){toast('Select at least one option','terr');return;}
  const btn=G('btnCleanMeta');
  const logsEl=G('cleanMetaLogs');
  btn.disabled=true;btn.textContent='Cleaning…';
  logsEl.style.display='block';logsEl.textContent='Starting metadata cleanup…\n';
  try{
    const r=await fetch('/api/v1/settings/clean-metadata',{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({clean_adls:cleanAdls,clean_tables:cleanTables})
    });
    const txt=await r.text();
    if(!r.ok){
      throw new Error(txt.startsWith('<')?'Server error ('+r.status+') — the operation may have timed out. Try cleaning tables and ADLS separately.':txt);
    }
    if(!txt){
      throw new Error('Server returned empty response — the worker may have run out of memory. Please retry.');
    }
    let d;
    try{d=JSON.parse(txt);}catch(pe){throw new Error('Invalid response from server: '+txt.substring(0,200));}
    if(!d.success) throw new Error(d.error||'Cleanup failed');
    logsEl.textContent=(d.log||[]).join('\n')+'\n\n✅ '+d.summary;
    toast('Metadata cleaned successfully','tok');
  }catch(e){
    logsEl.textContent+='\n❌ Error: '+e.message;
    toast(e.message,'terr');
  }finally{
    btn.disabled=false;btn.innerHTML='<svg viewBox="0 0 24 24" style="width:13px;height:13px;margin-right:4px;"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg> Clean Metadata';
    inp.value='';
  }
}

async function saveDeployConfig(){
  const cfg=_collectConfig();
  if(!cfg.subscription_id){toast('Subscription ID is required','terr');return;}
  try{
    const r=await fetch('/api/v1/deploy-config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(cfg)});
    const d=await r.json();
    if(!d.success) throw new Error(d.error||'Save failed');
    toast('Configuration saved to deployconfig.json','tok');
    _cachedDeployConfig=cfg; // update cache
    const banner=G('cfgSavedBanner'); banner.style.display='block';
    setTimeout(()=>{banner.style.display='none';},4000);
    cfgPreview();
    // Auto-populate all pages from saved config
    _populateConfig(cfg);
    // Silently auto-init MetadataFlow if credentials are complete
    if(cfg.databricks_host&&cfg.databricks_token&&cfg.metadata_catalog&&cfg.metadata_schema){
      try{
        const ir=await fetch('/api/v1/workflow/auto-init',{method:'POST'});
        const id=await ir.json();
        if(id.success){_wfMetaReady=true;toast('MetadataFlow auto-initialized ✓','tok');}
      }catch(e){}
    }
  }catch(e){toast(e.message,'terr');}
}

async function deployInfrastructure(){
  const btn=G('btnDeployInfra');
  const prog=G('cfgDeployProgress');
  const stepsEl=G('cfgDeploySteps');
  const logsEl=G('cfgDeployLogs');
  const summaryEl=G('cfgDeploySummary');

  // First save the config
  const cfg=_collectConfig();
  if(!cfg.subscription_id||!cfg.storage_account){
    toast('Subscription ID and Storage Account are required','terr');return;
  }
  try{
    const sr=await fetch('/api/v1/deploy-config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(cfg)});
    const sd=await sr.json();
    if(!sd.success) throw new Error(sd.error||'Save failed');
  }catch(e){toast('Failed to save config: '+e.message,'terr');return;}

  // Confirm
  if(!confirm('This will create Azure infrastructure resources (Storage Account, Access Connector, External Locations, Catalogs, Volume).\n\nSubscription: '+cfg.subscription_id+'\nStorage: '+cfg.storage_account+'\nRegion: '+cfg.region+'\n\nProceed?')) return;

  // Show progress panel
  prog.style.display='block';
  stepsEl.innerHTML='';
  logsEl.textContent='Connecting to Azure…\n';
  summaryEl.textContent='Running…';
  summaryEl.style.color='var(--amber)';
  btn.disabled=true; btn.textContent='Deploying…';

  // SSE URL — all config (including creds) is read from deployconfig.json on the server
  const sseUrl='/api/v1/deploy-infra-stream';

  const statusIcons={
    running:'<span style="color:var(--amber);font-weight:700;" class="cfg-spin">&#9881;</span>',
    success:'<span style="color:var(--green);font-weight:700;">&#10003;</span>',
    error:'<span style="color:var(--red);font-weight:700;">&#10007;</span>',
    skipped:'<span style="color:var(--t4);font-weight:700;">&#8722;</span>'
  };

  // Track rendered steps by step number
  const stepMap={};

  function renderStep(s){
    const id='cfgStep_'+s.step;
    const html='<div id="'+id+'" style="display:flex;align-items:center;gap:10px;padding:8px 10px;border-radius:var(--r-xs);border:1px solid var(--border);background:var(--surface-2);font-size:12px;margin-bottom:4px;">'+
      (statusIcons[s.status]||'')+'<span style="font-weight:600;flex:1;">Step '+s.step+': '+s.name+'</span>'+
      '<span style="font-size:11px;color:'+(s.status==='error'?'var(--red)':'var(--t3)')+';">'+s.message+'</span></div>';
    if(stepMap[s.step]){
      stepMap[s.step].outerHTML=html;
      stepMap[s.step]=G(id);
    } else {
      stepsEl.insertAdjacentHTML('beforeend',html);
      stepMap[s.step]=G(id);
    }
    // Append logs
    if(s.logs) logsEl.textContent+=s.logs;
  }

  const evtSource=new EventSource(sseUrl);
  evtSource.onmessage=function(e){
    try{
      const d=JSON.parse(e.data);
      if(d.event==='step'){
        renderStep(d);
        logsEl.scrollTop=logsEl.scrollHeight;
      } else if(d.event==='done'){
        evtSource.close();
        summaryEl.textContent=d.summary||'';
        summaryEl.style.color=d.success?'var(--green)':'var(--red)';
        if(d.success){
          toast('Infrastructure deployed successfully!','tok',5000);
        } else {
          toast('Deployment completed with errors — check logs','terr',6000);
        }
        btn.disabled=false;
        btn.innerHTML='<svg viewBox="0 0 24 24" style="width:14px;height:14px;"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Deploy Infrastructure';
      }
    }catch(ex){console.error('SSE parse error',ex);}
  };
  evtSource.onerror=function(){
    evtSource.close();
    summaryEl.textContent='Connection lost';
    summaryEl.style.color='var(--red)';
    toast('Deploy stream connection lost','terr');
    btn.disabled=false;
    btn.innerHTML='<svg viewBox="0 0 24 24" style="width:14px;height:14px;"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Deploy Infrastructure';
  };
}

async function loadDeployConfig(){
  try{
    const r=await fetch('/api/v1/deploy-config');
    const d=await r.json();
    if(d.success&&d.config){
      _populateConfig(d.config);
      cfgPreview();
    }
  }catch(e){ /* no saved config yet — ignore */ }
}

// ── Startup self-check ──
(function(){
  const fns=['testSourceConn','loadFromSource','convertSelected','switchTab','toast','toggleSrcConn'];
  const missing=fns.filter(f=>typeof window[f]!=='function');
  if(missing.length){
    const d=document.createElement('div');
    d.style.cssText='position:fixed;top:8px;left:50%;transform:translateX(-50%);z-index:99999;background:#fef3c7;color:#92400e;border:2px solid #f59e0b;padding:12px 18px;border-radius:8px;font:13px/1.4 monospace;max-width:80vw;box-shadow:0 8px 32px rgba(0,0,0,.2);';
    d.textContent='MISSING FUNCTIONS: '+missing.join(', ');
    document.body.appendChild(d);
  }else{
    console.log('[Migration Studio] All '+fns.length+' core functions loaded OK');
  }
})();

// ── Auto-init from deployconfig.json on page load ──
(async function _autoInitFromConfig(){
  try{
    const r=await fetch('/api/v1/deploy-config');
    const d=await r.json();
    if(!d.success||!d.config) return;
    // Populate all UI fields from saved config
    _populateConfig(d.config);
    // If we have Databricks credentials, silently initialize MetadataFlow
    const host=d.config.databricks_host||'';
    const token=d.config.databricks_token||'';
    const metaCat=d.config.metadata_catalog||'';
    const metaSch=d.config.metadata_schema||'';
    if(host&&token&&metaCat&&metaSch){
      const ir=await fetch('/api/v1/workflow/auto-init',{method:'POST',headers:{'Content-Type':'application/json'}});
      const id=await ir.json();
      if(id.success){
        _wfMetaReady=true;
        console.log('[Auto-Init] MetadataFlow initialized:',metaCat+'.'+metaSch);
        // Refresh pipeline list now that metadata is loaded
        if(typeof wfRefreshPipelines==='function') wfRefreshPipelines();
      }
    }
    // Check notebook deployment status
    try{
      const nbr=await fetch('/api/v1/workflow/notebooks/status');const nbd=await nbr.json();
      if(nbd.deployed) _wfNbDeployed=true;
    }catch(e){}
    // Auto-fetch clusters if credentials available
    if(host&&token&&!_wfClustersLoaded){
      try{await wfFetchClusters();}catch(e){}
    }
  }catch(e){console.log('[Auto-Init] No saved config:',e);}
})();

// ── Restore last visited tab from URL hash ──
(function(){
  const hash=location.hash.replace('#','');
  const validTabs=Object.keys(TAB_META);
  if(hash&&validTabs.includes(hash)){
    switchTab(hash,G('nav-'+hash));
  }else{
    switchTab('wf-dashboard',G('nav-wf-dashboard'));
  }
  // Auto-populate source connection from deployconfig.json
  _srcSyncFromConfig();
  // Auto-populate Databricks credentials from deployconfig.json
  _dbrSyncFromConfig();
})();

// ═══════════════════════════════════════════════════════════════════════════════
//  DATA MODELING — AI-driven Star / Snowflake Schema Builder
// ═══════════════════════════════════════════════════════════════════════════════

let _dmModel = null;    // current model from backend
let _dmModelId = null;  // cache key
let _dmErJson = null;   // ER nodes/edges
let _dmDdl = '';        // DDL text
let _dmZoomLevel = 1;
let _dmCatalogSchemas = [];
let _dmAllTables = [];      // Full list currently loaded (unfiltered)
let _dmSelectedTables = new Set();  // Persistent selection across filters
let _dmTplVisible = true;
let _dmInsightsVisible = true;

// ── AI role hints (pure client-side heuristic, no backend call) ────────────
const _DM_FACT_RX = /(transaction|order|sale|invoice|payment|event|log|detail|line_?item|entry|fact|history|audit|session)/i;
const _DM_DIM_RX  = /(customer|employee|product|department|location|region|store|category|status|type|dim|lookup|geo|channel|vendor|supplier|account|currency|calendar|date)/i;
function _dmRoleHint(name){
  const n=(name||'').toLowerCase();
  if(_DM_FACT_RX.test(n)) return {role:'fact',color:'#3B82F6',label:'FACT-LIKELY'};
  if(_DM_DIM_RX.test(n))  return {role:'dim', color:'#10B981',label:'DIM-LIKELY'};
  return {role:'?',color:'#94A3B8',label:'?'};
}

// Load catalog/schema dropdown
async function dmInit(){
  try{
    const r=await fetch('/api/v1/datamodel/catalogs-schemas');
    const d=await r.json();
    if(d.success){
      _dmCatalogSchemas=d.catalog_schemas||[];
      const sel=G('dmCatalog');
      sel.innerHTML='<option value="">— Select catalog —</option>';
      const cats=[...new Set(_dmCatalogSchemas.map(c=>c.catalog))];
      cats.forEach(c=>{const o=document.createElement('option');o.value=c;o.textContent=c;sel.appendChild(o);});
    }
  }catch(e){console.error('dmInit',e);}
  _dmRefreshRecentList();
  _dmInstallShortcuts();
}

function dmOnCatalogChange(){
  const cat=G('dmCatalog').value;
  const sel=G('dmSchema');
  sel.innerHTML='<option value="">— Select schema —</option>';
  if(!cat)return;
  _dmCatalogSchemas.filter(c=>c.catalog===cat).forEach(cs=>{
    const o=document.createElement('option');o.value=cs.schema;o.textContent=cs.schema;sel.appendChild(o);
  });
  _dmAllTables=[];_dmSelectedTables.clear();_dmRenderTableList();
}

async function dmLoadTables(){
  const cat=G('dmCatalog').value, sch=G('dmSchema').value;
  if(!cat||!sch){toast('Select catalog and schema first','terr');return;}
  const box=G('dmTableList');
  box.innerHTML='<div style="padding:24px;text-align:center;color:var(--t4);font-size:11px;">Loading tables…</div>';
  try{
    const r=await fetch('/api/v1/datamodel/tables',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({catalog:cat,schema:sch})});
    const d=await r.json();
    if(d.success&&d.tables&&d.tables.length){
      _dmAllTables=d.tables.slice();
      _dmSelectedTables.clear();
      _dmRenderTableList();
    }else{
      _dmAllTables=[];
      box.innerHTML='<div style="padding:24px;text-align:center;color:var(--t4);font-size:11px;">No tables found</div>';
    }
  }catch(e){box.innerHTML='<div style="padding:24px;text-align:center;color:#EF4444;font-size:11px;">Error loading tables</div>';toast('Failed to load tables','terr');}
}

// ── Render checkbox table list with AI role hints ──────────────────────────
function _dmRenderTableList(){
  const box=G('dmTableList');
  const q=(G('dmTableSearch').value||'').toLowerCase().trim();
  const filtered=_dmAllTables.filter(t=>!q || t.toLowerCase().includes(q));
  if(!_dmAllTables.length){
    box.innerHTML='<div style="padding:24px;text-align:center;color:var(--t4);font-size:11px;">Select catalog and schema to load tables…</div>';
    _dmSyncHiddenSelect();_dmUpdateSelBadge();return;
  }
  if(!filtered.length){
    box.innerHTML='<div style="padding:24px;text-align:center;color:var(--t4);font-size:11px;">No tables match “'+q+'”</div>';
    _dmSyncHiddenSelect();_dmUpdateSelBadge();return;
  }
  let html='';
  filtered.forEach(t=>{
    const hint=_dmRoleHint(t);
    const checked=_dmSelectedTables.has(t);
    const tesc=t.replace(/"/g,'&quot;');
    html+='<label class="dm-tbl-row" style="display:flex;align-items:center;gap:8px;padding:6px 8px;border-radius:6px;cursor:pointer;font-size:12px;transition:background .15s;'+
      (checked?'background:rgba(245,158,11,.08);':'')+'" '+
      'onmouseover="this.style.background=\'rgba(148,163,184,.08)\'" '+
      'onmouseout="this.style.background=\''+(checked?'rgba(245,158,11,.08)':'transparent')+'\'">'+
      '<input type="checkbox" data-tname="'+tesc+'" '+(checked?'checked':'')+' onchange="_dmOnTableToggle(this)" style="margin:0;cursor:pointer;">'+
      '<span style="flex:1;font-family:\'SF Mono\',Consolas,monospace;color:var(--t1);">'+t+'</span>'+
      '<span style="font-size:9px;font-weight:700;padding:1px 6px;border-radius:8px;background:'+hint.color+'22;color:'+hint.color+';letter-spacing:.05em;">'+hint.label+'</span>'+
      '</label>';
  });
  box.innerHTML=html;
  _dmSyncHiddenSelect();_dmUpdateSelBadge();
}

function _dmOnTableToggle(el){
  const t=el.getAttribute('data-tname');
  if(el.checked) _dmSelectedTables.add(t); else _dmSelectedTables.delete(t);
  _dmSyncHiddenSelect();_dmUpdateSelBadge();
  // Light row highlight
  const row=el.closest('.dm-tbl-row');
  if(row) row.style.background = el.checked ? 'rgba(245,158,11,.08)' : 'transparent';
}

function _dmSyncHiddenSelect(){
  // Keep the hidden <select multiple id="dmTableSelect"> in sync so legacy
  // paths (dmGenerate selectedOptions, dmLoadSample) still work if referenced.
  const sel=G('dmTableSelect');if(!sel)return;
  sel.innerHTML='';
  _dmSelectedTables.forEach(t=>{
    const o=document.createElement('option');o.value=t;o.textContent=t;o.selected=true;sel.appendChild(o);
  });
}

function _dmUpdateSelBadge(){
  const b=G('dmSelBadge');if(!b)return;
  const n=_dmSelectedTables.size;
  if(n>0){b.style.display='';b.textContent=n+' selected';}
  else{b.style.display='none';}
}

function dmFilterTables(q){ _dmRenderTableList(); }
function dmSelectAll(){
  const q=(G('dmTableSearch').value||'').toLowerCase().trim();
  _dmAllTables.filter(t=>!q || t.toLowerCase().includes(q)).forEach(t=>_dmSelectedTables.add(t));
  _dmRenderTableList();
}
function dmSelectNone(){ _dmSelectedTables.clear(); _dmRenderTableList(); }
function dmInvertSelection(){
  const q=(G('dmTableSearch').value||'').toLowerCase().trim();
  _dmAllTables.filter(t=>!q || t.toLowerCase().includes(q)).forEach(t=>{
    if(_dmSelectedTables.has(t)) _dmSelectedTables.delete(t); else _dmSelectedTables.add(t);
  });
  _dmRenderTableList();
}

// ── Template Gallery ───────────────────────────────────────────────────────
const _DM_TEMPLATES = {
  'sales-star':   {schema:'star',      match:/^(orders?|orderdetails?|customers?|products?|geolocations?|date_?dim)$/i},
  'hr-star':      {schema:'star',      match:/^(employees?|employeesessions?|departments?|date_?dim)$/i},
  'finance-snow': {schema:'snowflake', match:/^(transactions?|accounts?|currencies|customers?|products?|categor(y|ies))$/i},
  'blank':        {schema:'auto',      match:null},
};
function dmApplyTemplate(key){
  const tpl=_DM_TEMPLATES[key];if(!tpl){return;}
  G('dmSchemaChoice').value=tpl.schema;
  if(key==='blank'){
    _dmSelectedTables.clear();_dmRenderTableList();
    toast('Blank canvas — pick your tables below','tok');return;
  }
  // If no tables loaded yet, fall back to sample demo
  if(!_dmAllTables.length){
    dmLoadSample();
    toast('Template "'+key+'" will apply using sample data…','tok');
    return;
  }
  _dmSelectedTables.clear();
  _dmAllTables.forEach(t=>{ if(tpl.match.test(t)) _dmSelectedTables.add(t); });
  _dmRenderTableList();
  if(_dmSelectedTables.size){
    toast('Template applied — '+_dmSelectedTables.size+' matching tables selected. Click Generate.','tok');
  }else{
    toast('No matching tables in this schema. Try "Load Sample Data" to see the template.','terr');
  }
}
function dmToggleTemplateCard(){
  _dmTplVisible=!_dmTplVisible;
  G('dmTemplateGrid').style.display = _dmTplVisible?'grid':'none';
  G('dmTplToggle').textContent = _dmTplVisible?'Hide':'Show';
}

// ── Recent Models (localStorage) ───────────────────────────────────────────
const _DM_RECENT_KEY = 'dm_recent_models_v1';
const _DM_RECENT_MAX = 8;
function _dmReadRecent(){ try{ return JSON.parse(localStorage.getItem(_DM_RECENT_KEY)||'[]'); }catch(e){return [];} }
function _dmWriteRecent(list){ try{ localStorage.setItem(_DM_RECENT_KEY, JSON.stringify(list.slice(0,_DM_RECENT_MAX))); }catch(e){} }
function _dmRefreshRecentList(){
  const sel=G('dmRecentSelect');if(!sel)return;
  const list=_dmReadRecent();
  sel.innerHTML='<option value="">📂 Recent models…</option>';
  list.forEach((m,i)=>{const o=document.createElement('option');o.value=i;o.textContent=m.name+' · '+m.schema_type;sel.appendChild(o);});
}
function dmSaveCurrent(){
  if(!_dmModel){toast('Generate a model first','terr');return;}
  const name=prompt('Name for this model snapshot:', (G('dmCatalog').value||'model')+'_'+new Date().toISOString().slice(0,10));
  if(!name)return;
  const list=_dmReadRecent();
  list.unshift({
    name:name, ts:new Date().toISOString(),
    catalog:G('dmCatalog').value, schema:G('dmSchema').value,
    schema_choice:G('dmSchemaChoice').value,
    schema_type:_dmModel.schema_type,
    tables:Array.from(_dmSelectedTables),
  });
  _dmWriteRecent(list);_dmRefreshRecentList();
  toast('Saved "'+name+'" to Recent Models','tok');
}
function dmLoadRecent(idx){
  if(idx===''||idx==null)return;
  const list=_dmReadRecent();const m=list[parseInt(idx)];if(!m)return;
  if(m.catalog){G('dmCatalog').value=m.catalog;dmOnCatalogChange();}
  if(m.schema){G('dmSchema').value=m.schema;}
  if(m.schema_choice){G('dmSchemaChoice').value=m.schema_choice;}
  (async()=>{
    await dmLoadTables();
    _dmSelectedTables.clear();
    (m.tables||[]).forEach(t=>_dmSelectedTables.add(t));
    _dmRenderTableList();
    toast('Loaded "'+m.name+'" — click Generate to rebuild','tok');
  })();
  G('dmRecentSelect').value='';
}

// ── Keyboard shortcuts ─────────────────────────────────────────────────────
function _dmInstallShortcuts(){
  if(window._dmKbdInstalled)return;window._dmKbdInstalled=true;
  document.addEventListener('keydown',e=>{
    // Only active on Data Modeling pane
    const pane=G('pane-wf-datamodel');if(!pane||!pane.classList.contains('active'))return;
    const tag=(e.target.tagName||'').toLowerCase();
    const inField=(tag==='input'||tag==='textarea'||tag==='select');
    // "/" → focus search
    if(!inField && e.key==='/' && !e.ctrlKey && !e.metaKey){e.preventDefault();const s=G('dmTableSearch');if(s)s.focus();}
    // "g" → generate
    else if(!inField && (e.key==='g'||e.key==='G') && !e.ctrlKey && !e.metaKey){e.preventDefault();dmGenerate();}
    // Ctrl+A in search → select all visible
    else if((e.ctrlKey||e.metaKey) && e.key==='a' && e.target.id==='dmTableSearch'){e.preventDefault();dmSelectAll();}
  });
}

// ── AI Insights (client-side derivation from generated model) ─────────────
function _dmDeriveInsights(d){
  const out=[];
  const facts=d.facts||[], dims=d.dimensions||[], rels=d.relationships||[];
  // 1. No date dimension detected
  const hasDate = dims.some(x=>/(date|calendar|time)/i.test(x.table_name));
  if(facts.length && !hasDate){
    out.push({icon:'📅',tone:'warn',title:'No date dimension detected',
      desc:'Facts typically need a Date/Calendar dim for time-based analytics. Consider adding one.'});
  }
  // 2. Fact table without measures
  facts.forEach(f=>{
    const measures=(f.columns||[]).filter(c=>/^(int|bigint|decimal|numeric|float|double|money)/i.test(c.data_type||''));
    if(measures.length<2){
      out.push({icon:'📊',tone:'warn',title:'Thin measures in '+f.table_name,
        desc:'Only '+measures.length+' numeric column(s) detected. Fact tables usually carry 2+ measures (amount, qty, etc.).'});
    }
  });
  // 3. Suggested grain per fact (first PK/date combo)
  facts.forEach(f=>{
    const pk=(f.columns||[]).find(c=>c.is_pk);
    const dt=(f.columns||[]).find(c=>/(date|time|_at$)/i.test(c.name));
    if(pk){
      out.push({icon:'🎯',tone:'info',title:'Grain for '+f.table_name,
        desc:'One row per <b>'+pk.name+'</b>'+(dt?' at <b>'+dt.name+'</b>':'')+'.'});
    }
  });
  // 4. SCD suggestions for dims
  dims.slice(0,5).forEach(dm=>{
    const hasAudit=(dm.columns||[]).some(c=>/(updated_at|modified|effective_date|valid_from)/i.test(c.name));
    out.push({icon:'🔁',tone:hasAudit?'ok':'info',
      title:'SCD for '+dm.table_name,
      desc:hasAudit?'Audit columns found — SCD Type 2 recommended (track history).':'No audit columns — SCD Type 1 (overwrite) is simplest.'});
  });
  // 5. Orphan dims (no relationship)
  const related=new Set(rels.flatMap(r=>[r.from,r.to]));
  dims.forEach(dm=>{
    if(!related.has(dm.table_name)){
      out.push({icon:'⚠️',tone:'warn',title:dm.table_name+' has no relationships',
        desc:'This dimension is not connected to any fact. Add an FK to connect it, or remove it.'});
    }
  });
  // 6. Large fact count
  if(facts.length>=3){
    out.push({icon:'💡',tone:'info',title:'Multiple facts — consider a bus matrix',
      desc:facts.length+' fact tables detected. Conformed dimensions across facts will boost analytics.'});
  }
  // 7. Star schema recommendation
  if(d.schema_type==='snowflake' && dims.length<5){
    out.push({icon:'⭐',tone:'info',title:'Star may be simpler',
      desc:'Snowflake chosen but only '+dims.length+' dims. Star schema often performs better at this scale.'});
  }
  return out;
}
function _dmRenderInsights(d){
  const panel=G('dmInsightsPanel'),list=G('dmInsightsList'),badge=G('dmInsightBadge');
  if(!panel||!list)return;
  const items=_dmDeriveInsights(d);
  if(!items.length){
    list.innerHTML='<div style="color:var(--t3);font-size:11px;padding:6px;">✓ No issues detected — model looks clean.</div>';
    badge.textContent='0 findings';return;
  }
  badge.textContent=items.length+' findings';
  const palette={warn:{bg:'rgba(245,158,11,.08)',bd:'rgba(245,158,11,.3)',c:'#B45309'},
                 info:{bg:'rgba(59,130,246,.06)',bd:'rgba(59,130,246,.25)',c:'#1D4ED8'},
                 ok:  {bg:'rgba(16,185,129,.06)',bd:'rgba(16,185,129,.25)',c:'#047857'}};
  list.innerHTML=items.map(it=>{
    const p=palette[it.tone]||palette.info;
    return '<div style="background:'+p.bg+';border:1px solid '+p.bd+';border-radius:8px;padding:8px 10px;">'+
      '<div style="display:flex;align-items:center;gap:6px;margin-bottom:3px;">'+
      '<span style="font-size:14px;">'+it.icon+'</span>'+
      '<span style="font-size:11px;font-weight:700;color:'+p.c+';">'+it.title+'</span></div>'+
      '<div style="font-size:10px;color:var(--t3);line-height:1.4;">'+it.desc+'</div></div>';
  }).join('');
}
function dmToggleInsights(){
  _dmInsightsVisible=!_dmInsightsVisible;
  G('dmInsightsList').style.display=_dmInsightsVisible?'grid':'none';
  G('dmInsightToggle').textContent=_dmInsightsVisible?'Hide':'Show';
}

async function dmGenerate(){
  const cat=G('dmCatalog').value, sch=G('dmSchema').value;
  const tables=Array.from(_dmSelectedTables);
  if(!tables.length){toast('Select at least one table','terr');return;}
  const schemaChoice=G('dmSchemaChoice').value;
  G('dmStatusMsg').textContent='Analyzing tables...';
  G('dmGenerateBtn').disabled=true;
  try{
    const r=await fetch('/api/v1/datamodel/generate',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({catalog:cat,schema:sch,tables:tables,schema_choice:schemaChoice})});
    const d=await r.json();
    if(d.success){
      _dmModel=d;_dmModelId=d.model_id;_dmErJson=d.er_json;_dmDdl=d.ddl;
      G('dmResultArea').style.display='';
      G('dmKpiTables').textContent=tables.length;
      G('dmKpiFacts').textContent=d.facts.length;
      G('dmKpiDims').textContent=d.dimensions.length;
      G('dmKpiSchema').textContent=d.schema_type==='star'?'⭐ Star':'❄ Snowflake';
      G('dmSchemaTypeBadge').textContent=d.schema_type.toUpperCase()+' SCHEMA';
      G('dmSchemaTypeBadge').style.background=d.schema_type==='star'?'rgba(245,158,11,.15)':'rgba(59,130,246,.15)';
      G('dmSchemaTypeBadge').style.color=d.schema_type==='star'?'#F59E0B':'#3B82F6';
      dmRenderER(d.er_json);
      dmRenderDetails(d);
      _dmRenderInsights(d);
      G('dmDdlCode').textContent=d.ddl;
      G('dmStatusMsg').textContent='Model generated successfully!';
      toast('Data model generated — '+d.schema_type.toUpperCase()+' schema with '+d.facts.length+' facts & '+d.dimensions.length+' dims','tok');
    }else{
      toast(d.error||'Generation failed','terr');
      G('dmStatusMsg').textContent=d.error||'Failed';
    }
  }catch(e){toast('Error: '+e.message,'terr');G('dmStatusMsg').textContent='Error';}
  G('dmGenerateBtn').disabled=false;
}

// ── Load Sample / Demo Data (no Databricks needed) ──────────────────────────
async function dmLoadSample(){
  G('dmSampleBtn').disabled=true;
  G('dmStatusMsg').textContent='Loading sample data...';
  try{
    // 1. Fetch sample table list
    const lr=await fetch('/api/v1/datamodel/sample-tables');
    const ld=await lr.json();
    if(ld.success&&ld.tables){
      _dmAllTables=ld.tables.slice();
      _dmSelectedTables=new Set(ld.tables);
      G('dmCatalog').innerHTML='<option value="sample_catalog" selected>sample_catalog</option>';
      G('dmSchema').innerHTML='<option value="sample_schema" selected>sample_schema</option>';
      _dmRenderTableList();
    }
    // 2. Auto-generate model
    const schemaChoice=G('dmSchemaChoice').value;
    const r=await fetch('/api/v1/datamodel/sample-generate',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({tables:[],schema_choice:schemaChoice})});
    const d=await r.json();
    if(d.success){
      _dmModel=d;_dmModelId=d.model_id;_dmErJson=d.er_json;_dmDdl=d.ddl;
      G('dmResultArea').style.display='';
      const totalTables=d.facts.length+d.dimensions.length;
      G('dmKpiTables').textContent=totalTables;
      G('dmKpiFacts').textContent=d.facts.length;
      G('dmKpiDims').textContent=d.dimensions.length;
      G('dmKpiSchema').textContent=d.schema_type==='star'?'⭐ Star':'❄ Snowflake';
      G('dmSchemaTypeBadge').textContent=d.schema_type.toUpperCase()+' SCHEMA';
      G('dmSchemaTypeBadge').style.background=d.schema_type==='star'?'rgba(245,158,11,.15)':'rgba(59,130,246,.15)';
      G('dmSchemaTypeBadge').style.color=d.schema_type==='star'?'#F59E0B':'#3B82F6';
      dmRenderER(d.er_json);
      dmRenderDetails(d);
      _dmRenderInsights(d);
      G('dmDdlCode').textContent=d.ddl;
      G('dmStatusMsg').textContent='Sample model generated!';
      toast('Sample data model generated — '+d.schema_type.toUpperCase()+' schema with '+d.facts.length+' facts & '+d.dimensions.length+' dims','tok');
    }else{
      toast(d.error||'Sample generation failed','terr');
      G('dmStatusMsg').textContent='Failed';
    }
  }catch(e){toast('Error: '+e.message,'terr');G('dmStatusMsg').textContent='Error';}
  G('dmSampleBtn').disabled=false;
}

// Sub-tab switching
function dmSwitchSubTab(tab,btn){
  document.querySelectorAll('.dmSubTab').forEach(b=>{b.classList.remove('active');b.style.borderBottom='2px solid transparent';b.style.fontWeight='';});
  btn.classList.add('active');btn.style.borderBottom='2px solid var(--accent-primary)';btn.style.fontWeight='700';
  G('dmSubER').style.display=tab==='er'?'':'none';
  G('dmSubDetails').style.display=tab==='details'?'':'none';
  G('dmSubDDL').style.display=tab==='ddl'?'':'none';
}

// ── ER Diagram Rendering (SVG) ──────────────────────────────────────────────
function dmRenderER(er){
  const g=G('dmErGroup');
  g.innerHTML='';
  if(!er||!er.nodes) return;
  const svg=G('dmErSvg');
  _dmZoomLevel=1;
  g.setAttribute('transform','translate(0,0) scale(1)');

  // Auto-layout: facts in center row, dims around
  const facts=er.nodes.filter(n=>n.type==='fact');
  const dims=er.nodes.filter(n=>n.type==='dimension');
  const W=Math.max(svg.clientWidth||1100,900);
  const H=460;

  // Position facts horizontally centered
  const factW=180, factGap=40;
  const totalFactW=facts.length*factW+(facts.length-1)*factGap;
  let fx=(W-totalFactW)/2;
  facts.forEach((n,i)=>{n.x=fx+i*(factW+factGap);n.y=H/2-40;});

  // Position dims in a arc around facts
  const cx=W/2, cy=H/2-20;
  dims.forEach((n,i)=>{
    const angle=Math.PI+Math.PI*(i+1)/(dims.length+1);
    n.x=cx+Math.cos(angle)*Math.min(W/2.5,380)-90;
    n.y=cy+Math.sin(angle)*Math.min(H/2.5,180)-30;
  });

  // Draw edges first (behind nodes)
  er.edges.forEach(e=>{
    const from=er.nodes.find(n=>n.id===e.from);
    const to=er.nodes.find(n=>n.id===e.to);
    if(!from||!to)return;
    const x1=from.x+90,y1=from.y+30,x2=to.x+90,y2=to.y+30;
    const line=document.createElementNS('http://www.w3.org/2000/svg','line');
    line.setAttribute('x1',x1);line.setAttribute('y1',y1);line.setAttribute('x2',x2);line.setAttribute('y2',y2);
    line.setAttribute('stroke','#94A3B8');line.setAttribute('stroke-width','1.5');line.setAttribute('marker-end','url(#dmArrow)');
    g.appendChild(line);
    // Label
    const lx=(x1+x2)/2,ly=(y1+y2)/2;
    const txt=document.createElementNS('http://www.w3.org/2000/svg','text');
    txt.setAttribute('x',lx);txt.setAttribute('y',ly-4);txt.setAttribute('text-anchor','middle');
    txt.setAttribute('fill','#64748B');txt.setAttribute('font-size','9');txt.setAttribute('font-family','system-ui');
    txt.textContent=e.label||'';g.appendChild(txt);
  });

  // Draw nodes
  er.nodes.forEach(n=>{
    const isFact=n.type==='fact';
    const ng=document.createElementNS('http://www.w3.org/2000/svg','g');
    ng.setAttribute('transform','translate('+n.x+','+n.y+')');
    ng.style.cursor='move';

    // Node box
    const cols=n.columns||[];
    const boxH=Math.max(60, 28+cols.length*14+8);
    const rect=document.createElementNS('http://www.w3.org/2000/svg','rect');
    rect.setAttribute('width','180');rect.setAttribute('height',boxH);
    rect.setAttribute('rx','8');rect.setAttribute('fill',isFact?'#EFF6FF':'#F0FDF4');
    rect.setAttribute('stroke',isFact?'#3B82F6':'#10B981');rect.setAttribute('stroke-width','1.5');
    ng.appendChild(rect);

    // Header bar
    const hdr=document.createElementNS('http://www.w3.org/2000/svg','rect');
    hdr.setAttribute('width','180');hdr.setAttribute('height','26');hdr.setAttribute('rx','8');
    hdr.setAttribute('fill',isFact?'#3B82F6':'#10B981');
    ng.appendChild(hdr);
    // Clip bottom corners of header
    const hdr2=document.createElementNS('http://www.w3.org/2000/svg','rect');
    hdr2.setAttribute('y','12');hdr2.setAttribute('width','180');hdr2.setAttribute('height','14');
    hdr2.setAttribute('fill',isFact?'#3B82F6':'#10B981');
    ng.appendChild(hdr2);

    // Table name
    const title=document.createElementNS('http://www.w3.org/2000/svg','text');
    title.setAttribute('x','90');title.setAttribute('y','17');title.setAttribute('text-anchor','middle');
    title.setAttribute('fill','white');title.setAttribute('font-size','11');title.setAttribute('font-weight','700');
    title.setAttribute('font-family','system-ui');
    title.textContent=(isFact?'⊞ ':'◈ ')+n.label;ng.appendChild(title);

    // Columns
    cols.forEach((c,ci)=>{
      const ct=document.createElementNS('http://www.w3.org/2000/svg','text');
      ct.setAttribute('x','10');ct.setAttribute('y',40+ci*14);ct.setAttribute('fill','#334155');
      ct.setAttribute('font-size','10');ct.setAttribute('font-family','monospace');
      let prefix=c.is_pk?'🔑 ':c.fk_table?'🔗 ':'   ';
      ct.textContent=prefix+c.name+' : '+(c.data_type||'STRING');
      ng.appendChild(ct);
    });

    // Drag support
    let dragging=false, dx=0, dy=0, ox=n.x, oy=n.y;
    ng.addEventListener('mousedown',ev=>{dragging=true;dx=ev.clientX-n.x;dy=ev.clientY-n.y;ev.preventDefault();});
    svg.addEventListener('mousemove',ev=>{
      if(!dragging)return;
      n.x=ev.clientX-dx;n.y=ev.clientY-dy;
      ng.setAttribute('transform','translate('+n.x+','+n.y+')');
      // Redraw edges
      dmUpdateEdges(er);
    });
    svg.addEventListener('mouseup',()=>{dragging=false;});

    g.appendChild(ng);
  });
}

function dmUpdateEdges(er){
  const g=G('dmErGroup');
  // Remove old lines/text, keep node groups
  g.querySelectorAll('line,text').forEach(el=>{
    if(!el.closest('g[transform]'))el.remove();
    else if(el.tagName==='line')el.remove();
  });
  // Re-insert above first g
  const firstG=g.querySelector('g');
  er.edges.forEach(e=>{
    const from=er.nodes.find(n=>n.id===e.from);
    const to=er.nodes.find(n=>n.id===e.to);
    if(!from||!to)return;
    const line=document.createElementNS('http://www.w3.org/2000/svg','line');
    line.setAttribute('x1',from.x+90);line.setAttribute('y1',from.y+30);
    line.setAttribute('x2',to.x+90);line.setAttribute('y2',to.y+30);
    line.setAttribute('stroke','#94A3B8');line.setAttribute('stroke-width','1.5');
    line.setAttribute('marker-end','url(#dmArrow)');
    if(firstG) g.insertBefore(line,firstG); else g.appendChild(line);
  });
}

function dmZoom(factor){
  if(factor===0){_dmZoomLevel=1;}else{_dmZoomLevel*=factor;}
  _dmZoomLevel=Math.max(0.3,Math.min(3,_dmZoomLevel));
  G('dmErGroup').setAttribute('transform','scale('+_dmZoomLevel+')');
}

// ── Table Details Rendering ─────────────────────────────────────────────────
function dmRenderDetails(d){
  // Facts
  const fDiv=G('dmFactsList');fDiv.innerHTML='';
  (d.facts||[]).forEach(f=>{ fDiv.innerHTML+=dmTableCard(f,'fact'); });
  // Dimensions
  const dDiv=G('dmDimsList');dDiv.innerHTML='';
  (d.dimensions||[]).forEach(dim=>{ dDiv.innerHTML+=dmTableCard(dim,'dimension'); });
  // Relationships
  const rb=G('dmRelsBody');rb.innerHTML='';
  (d.relationships||[]).forEach((r,i)=>{
    rb.innerHTML+='<tr style="border-bottom:1px solid var(--border);">'+
      '<td style="padding:6px 10px;">'+r.from+'</td>'+
      '<td style="padding:6px 10px;">'+r.to+'</td>'+
      '<td style="padding:6px 10px;text-align:center;"><span style="font-size:10px;padding:2px 8px;border-radius:8px;background:rgba(99,102,241,.1);color:#6366F1;font-weight:600;">'+r.type+'</span></td>'+
      '<td style="padding:6px 10px;text-align:center;"><button class="btn btn-ghost btn-xs" onclick="dmRemoveRel('+i+')" style="color:#EF4444;font-size:10px;">\u2715 Remove</button></td>'+
      '</tr>';
  });
}

function dmTableCard(tbl,role){
  const color=role==='fact'?'#3B82F6':'#10B981';
  const bgColor=role==='fact'?'rgba(59,130,246,.06)':'rgba(16,185,129,.06)';
  const tn=tbl.table_name;
  const tnEsc=tn.replace(/'/g,"\\'");
  let html='<div style="background:'+bgColor+';border:1px solid '+color+'22;border-radius:8px;padding:10px;position:relative;" id="dmCard_'+tn+'">';
  // Header: table name + role badge + actions
  html+='<div style="display:flex;align-items:center;gap:6px;margin-bottom:6px;flex-wrap:wrap;">';
  html+='<span style="font-weight:700;font-size:12px;color:var(--t1);">'+tn+'</span>';
  html+='<button class="btn btn-ghost btn-xs" onclick="dmToggleRole(\''+tnEsc+'\',\''+(role==='fact'?'dimension':'fact')+'\')" style="font-size:9px;padding:1px 6px;border:1px solid '+color+';color:'+color+';border-radius:10px;cursor:pointer;" title="Toggle role">'+role.toUpperCase()+'</button>';
  html+='<div style="margin-left:auto;display:flex;gap:4px;">';
  html+='<button class="btn btn-ghost btn-xs" onclick="dmRenameTableDialog(\''+tnEsc+'\')" style="font-size:9px;color:var(--t3);" title="Rename table">\u270E Rename</button>';
  html+='<button class="btn btn-ghost btn-xs" onclick="dmRemoveTable(\''+tnEsc+'\')" style="font-size:9px;color:#EF4444;" title="Remove table">\u2715 Remove</button>';
  html+='</div></div>';
  // Column table
  html+='<table style="width:100%;font-size:10px;border-collapse:collapse;">';
  html+='<thead><tr style="background:rgba(0,0,0,.03);"><th style="padding:3px 4px;text-align:left;font-weight:600;color:var(--t3);">Column</th><th style="padding:3px 4px;text-align:left;font-weight:600;color:var(--t3);">Type</th><th style="padding:3px 4px;text-align:center;font-weight:600;color:var(--t3);">PK</th><th style="padding:3px 4px;text-align:center;font-weight:600;color:var(--t3);">Null</th><th style="padding:3px 4px;text-align:center;font-weight:600;color:var(--t3);">Actions</th></tr></thead>';
  html+='<tbody>';
  (tbl.columns||[]).forEach(c=>{
    const cnEsc=c.name.replace(/'/g,"\\'");
    const pk=c.is_pk;
    const nl=c.is_nullable;
    html+='<tr style="border-bottom:1px solid rgba(0,0,0,.05);">';
    html+='<td style="padding:3px 4px;">'+(pk?'<span style="color:#F59E0B;" title="PK">\ud83d\udd11</span> ':'')+c.name+'</td>';
    html+='<td style="padding:3px 4px;color:var(--t3);">'+c.data_type+'</td>';
    html+='<td style="padding:3px 4px;text-align:center;"><button class="btn btn-ghost btn-xs" onclick="dmToggleColPK(\''+tnEsc+'\',\''+cnEsc+'\','+(!pk)+')" style="font-size:9px;padding:0 4px;color:'+(pk?'#F59E0B':'var(--t4)')+';">'+(pk?'\u2605':'\u2606')+'</button></td>';
    html+='<td style="padding:3px 4px;text-align:center;"><button class="btn btn-ghost btn-xs" onclick="dmToggleColNull(\''+tnEsc+'\',\''+cnEsc+'\','+(!nl)+')" style="font-size:9px;padding:0 4px;color:'+(nl?'#10B981':'var(--t4)')+';">'+(nl?'\u2714':'\u2718')+'</button></td>';
    html+='<td style="padding:3px 4px;text-align:center;display:flex;gap:2px;justify-content:center;">';
    html+='<button class="btn btn-ghost btn-xs" onclick="dmEditColDialog(\''+tnEsc+'\',\''+cnEsc+'\')" style="font-size:9px;padding:0 3px;color:var(--t3);" title="Edit">\u270E</button>';
    html+='<button class="btn btn-ghost btn-xs" onclick="dmRemoveCol(\''+tnEsc+'\',\''+cnEsc+'\')" style="font-size:9px;padding:0 3px;color:#EF4444;" title="Remove">\u2715</button>';
    html+='</td></tr>';
  });
  html+='</tbody></table>';
  // Add column button
  html+='<button class="btn btn-ghost btn-xs" onclick="dmAddColDialog(\''+tnEsc+'\')" style="margin-top:4px;font-size:9px;color:'+color+';border:1px dashed '+color+'44;width:100%;padding:3px;">+ Add Column</button>';
  html+='</div>';
  return html;
}

// ── Generic Edit API Helper ─────────────────────────────────────────────────
async function _dmEdit(edits, successMsg){
  if(!_dmModelId){toast('Generate a model first','terr');return;}
  try{
    const r=await fetch('/api/v1/datamodel/edit',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({
      model_id:_dmModelId, edits:edits,
      catalog:G('dmCatalog').value, schema:G('dmSchema').value
    })});
    const d=await r.json();
    if(d.success){
      _dmModel=d;_dmModelId=d.model_id;_dmErJson=d.er_json;_dmDdl=d.ddl;
      G('dmKpiFacts').textContent=d.facts.length;
      G('dmKpiDims').textContent=d.dimensions.length;
      G('dmKpiTables').textContent=d.facts.length+d.dimensions.length;
      G('dmKpiSchema').textContent=d.schema_type==='star'?'\u2b50 Star':'\u2744 Snowflake';
      G('dmSchemaTypeBadge').textContent=d.schema_type.toUpperCase()+' SCHEMA';
      G('dmSchemaTypeBadge').style.background=d.schema_type==='star'?'rgba(245,158,11,.15)':'rgba(59,130,246,.15)';
      G('dmSchemaTypeBadge').style.color=d.schema_type==='star'?'#F59E0B':'#3B82F6';
      dmRenderER(d.er_json);dmRenderDetails(d);G('dmDdlCode').textContent=d.ddl;
      if(typeof _dmRenderInsights==='function') _dmRenderInsights(d);
      if(successMsg)toast(successMsg,'tok');
    }else{toast(d.error||'Edit failed','terr');}
  }catch(e){toast('Edit failed: '+e.message,'terr');}
}

// ── Manual Edits ────────────────────────────────────────────────────────────
// ── Role Toggle ─────────────────────────────────────────────────────────────
async function dmToggleRole(tableName,newRole){
  await _dmEdit({role_changes:[{table_name:tableName,new_role:newRole}]},tableName+' moved to '+newRole);
}

// ── Relationship Remove / Add ───────────────────────────────────────────────
async function dmRemoveRel(idx){
  if(!_dmModel||!_dmModel.relationships)return;
  const rel=_dmModel.relationships[idx];if(!rel)return;
  await _dmEdit({relationship_removes:[{from:rel.from,to:rel.to}]},'Relationship removed');
}

function dmShowAddRelDialog(){
  if(!_dmModel)return;
  const allTables=[..._dmModel.facts,..._dmModel.dimensions].map(t=>t.table_name);
  const fromOpts=allTables.map(t=>'<option value="'+t+'">'+t+'</option>').join('');
  const html='<div style="display:flex;gap:8px;align-items:end;margin-top:8px;padding:10px;background:var(--bg2);border-radius:8px;" id="dmAddRelRow">'+
    '<div><label style="font-size:10px;font-weight:600;">From</label><select class="inp" id="dmNewRelFrom" style="font-size:11px;">'+fromOpts+'</select></div>'+
    '<div><label style="font-size:10px;font-weight:600;">To</label><select class="inp" id="dmNewRelTo" style="font-size:11px;">'+fromOpts+'</select></div>'+
    '<div><label style="font-size:10px;font-weight:600;">Type</label><select class="inp" id="dmNewRelType" style="font-size:11px;"><option>many-to-one</option><option>one-to-one</option><option>many-to-many</option></select></div>'+
    '<button class="btn btn-primary btn-xs" onclick="dmAddRel()">Add</button>'+
    '<button class="btn btn-ghost btn-xs" onclick="G(\'dmAddRelRow\').remove()">Cancel</button>'+
    '</div>';
  const existing=G('dmAddRelRow');if(existing)existing.remove();
  G('dmRelsBody').closest('div').insertAdjacentHTML('beforeend',html);
}

async function dmAddRel(){
  const from=G('dmNewRelFrom').value,to=G('dmNewRelTo').value,type=G('dmNewRelType').value;
  if(from===to){toast('From and To must be different','terr');return;}
  await _dmEdit({relationship_adds:[{from,to,type}]},'Relationship added');
  const row=G('dmAddRelRow');if(row)row.remove();
}

// ── Remove Table ────────────────────────────────────────────────────────────
async function dmRemoveTable(tableName){
  if(!confirm('Remove table "'+tableName+'" from the model?'))return;
  await _dmEdit({table_removes:[tableName]},tableName+' removed');
}

// ── Rename Table Dialog ─────────────────────────────────────────────────────
function dmRenameTableDialog(tableName){
  const newName=prompt('New name for table "'+tableName+'":',tableName);
  if(!newName||newName===tableName)return;
  _dmEdit({table_renames:[{old_name:tableName,new_name:newName}]},tableName+' renamed to '+newName);
}

// ── Column Editing ──────────────────────────────────────────────────────────
function dmEditColDialog(tableName,colName){
  // Find current column data
  const allTbls=[...(_dmModel.facts||[]),...(_dmModel.dimensions||[])];
  const tbl=allTbls.find(t=>t.table_name===tableName);
  if(!tbl)return;
  const col=(tbl.columns||[]).find(c=>c.name===colName);
  if(!col)return;
  // Build a small inline dialog
  const dlgId='dmColEditDlg_'+tableName+'_'+colName;
  let existing=document.getElementById(dlgId);if(existing)existing.remove();
  const types=['STRING','INT','BIGINT','DOUBLE','FLOAT','DECIMAL(18,2)','BOOLEAN','DATE','TIMESTAMP','ARRAY<STRING>','MAP<STRING,STRING>'];
  const typeOpts=types.map(t=>'<option value="'+t+'"'+(col.data_type.toUpperCase()===t?' selected':'')+'>'+t+'</option>').join('');
  const card=document.getElementById('dmCard_'+tableName);
  if(!card)return;
  const html='<div id="'+dlgId+'" style="background:var(--bg2);border:1px solid var(--border);border-radius:6px;padding:8px;margin-top:4px;">'+
    '<div style="font-size:10px;font-weight:700;margin-bottom:4px;">Edit Column: '+colName+'</div>'+
    '<div style="display:flex;gap:6px;flex-wrap:wrap;align-items:end;">'+
    '<div><label style="font-size:9px;">Name</label><input class="inp" id="'+dlgId+'_name" value="'+col.name+'" style="font-size:10px;width:120px;"></div>'+
    '<div><label style="font-size:9px;">Type</label><select class="inp" id="'+dlgId+'_type" style="font-size:10px;width:130px;">'+typeOpts+'</select></div>'+
    '<button class="btn btn-primary btn-xs" onclick="dmSaveColEdit(\''+tableName.replace(/'/g,"\\'")+'\',\''+colName.replace(/'/g,"\\'")+'\',\''+dlgId+'\')">Save</button>'+
    '<button class="btn btn-ghost btn-xs" onclick="document.getElementById(\''+dlgId+'\').remove()">Cancel</button>'+
    '</div></div>';
  card.insertAdjacentHTML('beforeend',html);
}

async function dmSaveColEdit(tableName,oldColName,dlgId){
  const newName=document.getElementById(dlgId+'_name').value.trim();
  const newType=document.getElementById(dlgId+'_type').value;
  if(!newName){toast('Column name required','terr');return;}
  const edits={column_edits:[]};
  if(newName!==oldColName)edits.column_edits.push({table_name:tableName,column_name:oldColName,field:'name',value:newName});
  // Get current type to compare
  const allTbls=[...(_dmModel.facts||[]),...(_dmModel.dimensions||[])];
  const tbl=allTbls.find(t=>t.table_name===tableName);
  const col=tbl?(tbl.columns||[]).find(c=>c.name===oldColName):null;
  if(col&&col.data_type.toUpperCase()!==newType)edits.column_edits.push({table_name:tableName,column_name:newName||oldColName,field:'data_type',value:newType});
  if(edits.column_edits.length===0){document.getElementById(dlgId).remove();return;}
  await _dmEdit(edits,'Column updated');
}

async function dmToggleColPK(tableName,colName,newVal){
  await _dmEdit({column_edits:[{table_name:tableName,column_name:colName,field:'is_pk',value:newVal}]},'PK toggled');
}

async function dmToggleColNull(tableName,colName,newVal){
  await _dmEdit({column_edits:[{table_name:tableName,column_name:colName,field:'is_nullable',value:newVal}]},'Nullable toggled');
}

async function dmRemoveCol(tableName,colName){
  if(!confirm('Remove column "'+colName+'" from '+tableName+'?'))return;
  await _dmEdit({column_removes:[{table_name:tableName,column_name:colName}]},'Column removed');
}

// ── Add Column Dialog ───────────────────────────────────────────────────────
function dmAddColDialog(tableName){
  const dlgId='dmAddColDlg_'+tableName;
  let existing=document.getElementById(dlgId);if(existing)existing.remove();
  const card=document.getElementById('dmCard_'+tableName);if(!card)return;
  const types=['STRING','INT','BIGINT','DOUBLE','FLOAT','DECIMAL(18,2)','BOOLEAN','DATE','TIMESTAMP'];
  const typeOpts=types.map(t=>'<option value="'+t+'">'+t+'</option>').join('');
  const html='<div id="'+dlgId+'" style="background:var(--bg2);border:1px solid var(--border);border-radius:6px;padding:8px;margin-top:4px;">'+
    '<div style="font-size:10px;font-weight:700;margin-bottom:4px;">Add Column to '+tableName+'</div>'+
    '<div style="display:flex;gap:6px;flex-wrap:wrap;align-items:end;">'+
    '<div><label style="font-size:9px;">Name</label><input class="inp" id="'+dlgId+'_name" placeholder="column_name" style="font-size:10px;width:120px;"></div>'+
    '<div><label style="font-size:9px;">Type</label><select class="inp" id="'+dlgId+'_type" style="font-size:10px;width:130px;">'+typeOpts+'</select></div>'+
    '<div><label style="font-size:9px;">PK</label><input type="checkbox" id="'+dlgId+'_pk"></div>'+
    '<div><label style="font-size:9px;">Nullable</label><input type="checkbox" id="'+dlgId+'_null" checked></div>'+
    '<button class="btn btn-primary btn-xs" onclick="dmSaveNewCol(\''+tableName.replace(/'/g,"\\'")+'\',\''+dlgId+'\')">Add</button>'+
    '<button class="btn btn-ghost btn-xs" onclick="document.getElementById(\''+dlgId+'\').remove()">Cancel</button>'+
    '</div></div>';
  card.insertAdjacentHTML('beforeend',html);
}

async function dmSaveNewCol(tableName,dlgId){
  const name=document.getElementById(dlgId+'_name').value.trim();
  const dtype=document.getElementById(dlgId+'_type').value;
  const pk=document.getElementById(dlgId+'_pk').checked;
  const nl=document.getElementById(dlgId+'_null').checked;
  if(!name){toast('Column name required','terr');return;}
  await _dmEdit({column_adds:[{table_name:tableName,column:{name:name,data_type:dtype,is_pk:pk,is_nullable:nl}}]},'Column "'+name+'" added');
}

// ── Add New Table Dialog ────────────────────────────────────────────────────
function dmShowAddTableDialog(){
  if(!_dmModel){toast('Generate a model first','terr');return;}
  let dlg=G('dmAddTableDlg');if(dlg){dlg.remove();}
  const html='<div id="dmAddTableDlg" style="position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);z-index:9999;background:var(--bg1);border:2px solid #F59E0B;border-radius:12px;padding:20px;width:420px;max-height:80vh;overflow-y:auto;box-shadow:0 20px 60px rgba(0,0,0,.3);">'+
    '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;">'+
    '<span style="font-weight:700;font-size:14px;color:#F59E0B;">\u2795 Add New Table</span>'+
    '<button class="btn btn-ghost btn-xs" onclick="G(\'dmAddTableDlg\').remove()" style="font-size:14px;">\u2715</button></div>'+
    '<div style="display:flex;gap:8px;margin-bottom:8px;">'+
    '<div style="flex:1;"><label style="font-size:10px;font-weight:600;">Table Name</label><input class="inp" id="dmNewTblName" placeholder="my_table" style="font-size:11px;width:100%;"></div>'+
    '<div><label style="font-size:10px;font-weight:600;">Role</label><select class="inp" id="dmNewTblRole" style="font-size:11px;"><option value="fact">Fact</option><option value="dimension">Dimension</option></select></div>'+
    '</div>'+
    '<div style="font-size:10px;font-weight:700;margin-bottom:4px;">Columns</div>'+
    '<div id="dmNewTblCols" style="margin-bottom:6px;"></div>'+
    '<button class="btn btn-ghost btn-xs" onclick="dmNewTblAddColRow()" style="margin-bottom:10px;font-size:9px;border:1px dashed #F59E0B44;color:#F59E0B;width:100%;">+ Add Column</button>'+
    '<div style="display:flex;gap:8px;">'+
    '<button class="btn btn-primary btn-sm" onclick="dmSaveNewTable()" style="background:#F59E0B;border-color:#F59E0B;flex:1;">Create Table</button>'+
    '<button class="btn btn-ghost btn-sm" onclick="G(\'dmAddTableDlg\').remove()">Cancel</button></div></div>';
  document.body.insertAdjacentHTML('beforeend',html);
  // Add initial column row
  dmNewTblAddColRow();
}

let _dmNewTblColIdx=0;
function dmNewTblAddColRow(){
  const idx=_dmNewTblColIdx++;
  const types=['STRING','INT','BIGINT','DOUBLE','FLOAT','DECIMAL(18,2)','BOOLEAN','DATE','TIMESTAMP'];
  const typeOpts=types.map(t=>'<option value="'+t+'">'+t+'</option>').join('');
  const html='<div style="display:flex;gap:4px;align-items:center;margin-bottom:4px;" id="dmNewTblColRow_'+idx+'">'+
    '<input class="inp dmNewTblColName" placeholder="col_name" style="font-size:10px;flex:1;">'+
    '<select class="inp dmNewTblColType" style="font-size:10px;width:120px;">'+typeOpts+'</select>'+
    '<label style="font-size:8px;white-space:nowrap;"><input type="checkbox" class="dmNewTblColPK"> PK</label>'+
    '<label style="font-size:8px;white-space:nowrap;"><input type="checkbox" class="dmNewTblColNull" checked> Null</label>'+
    '<button class="btn btn-ghost btn-xs" onclick="document.getElementById(\'dmNewTblColRow_'+idx+'\').remove()" style="color:#EF4444;font-size:10px;padding:0 3px;">\u2715</button>'+
    '</div>';
  G('dmNewTblCols').insertAdjacentHTML('beforeend',html);
}

async function dmSaveNewTable(){
  const name=G('dmNewTblName').value.trim();
  const role=G('dmNewTblRole').value;
  if(!name){toast('Table name required','terr');return;}
  const colRows=G('dmNewTblCols').children;
  const columns=[];
  for(let i=0;i<colRows.length;i++){
    const row=colRows[i];
    const cn=row.querySelector('.dmNewTblColName').value.trim();
    if(!cn)continue;
    columns.push({
      name:cn,
      data_type:row.querySelector('.dmNewTblColType').value,
      is_pk:row.querySelector('.dmNewTblColPK').checked,
      is_nullable:row.querySelector('.dmNewTblColNull').checked
    });
  }
  if(columns.length===0){toast('Add at least one column','terr');return;}
  await _dmEdit({table_adds:[{table_name:name,role:role,columns:columns}]},'Table "'+name+'" added');
  G('dmAddTableDlg').remove();
}

// ── Downloads ───────────────────────────────────────────────────────────────
function dmDownloadER(){
  const svg=G('dmErSvg');
  const clone=svg.cloneNode(true);
  clone.querySelector('#dmErGroup').setAttribute('transform','scale(1)');
  const xml=new XMLSerializer().serializeToString(clone);
  const svgBlob=new Blob(['<?xml version="1.0" encoding="UTF-8"?>'+xml],{type:'image/svg+xml'});
  const url=URL.createObjectURL(svgBlob);
  const img=new Image();
  img.onload=function(){
    const canvas=document.createElement('canvas');
    canvas.width=img.width*2;canvas.height=img.height*2;
    const ctx=canvas.getContext('2d');
    ctx.scale(2,2);ctx.fillStyle='#FFFFFF';ctx.fillRect(0,0,img.width,img.height);
    ctx.drawImage(img,0,0);
    canvas.toBlob(function(blob){
      const a=document.createElement('a');a.href=URL.createObjectURL(blob);
      a.download='data_model_er_diagram.png';a.click();URL.revokeObjectURL(a.href);
    },'image/png');
    URL.revokeObjectURL(url);
  };
  img.src=url;
}

function dmDownloadSVG(){
  const svg=G('dmErSvg');
  const clone=svg.cloneNode(true);
  clone.querySelector('#dmErGroup').setAttribute('transform','scale(1)');
  const xml=new XMLSerializer().serializeToString(clone);
  const blob=new Blob(['<?xml version="1.0" encoding="UTF-8"?>'+xml],{type:'image/svg+xml'});
  const a=document.createElement('a');a.href=URL.createObjectURL(blob);
  a.download='data_model_er_diagram.svg';a.click();URL.revokeObjectURL(a.href);
}

function dmDownloadDDL(){
  if(!_dmDdl)return;
  const blob=new Blob([_dmDdl],{type:'text/sql'});
  const a=document.createElement('a');a.href=URL.createObjectURL(blob);
  a.download='data_model_ddl.sql';a.click();URL.revokeObjectURL(a.href);
}

function dmCopyDDL(){
  if(!_dmDdl)return;
  navigator.clipboard.writeText(_dmDdl).then(()=>toast('DDL copied to clipboard','tok')).catch(()=>toast('Copy failed','terr'));
}

// Init on load
dmInit();

/* ═══════════ ADMIN — USER MANAGEMENT ═══════════ */
window.adminRefresh=async function(){
  const tbody=G('adminUserTbody');
  if(!tbody)return;
  tbody.innerHTML='<tr><td colspan="4" style="text-align:center;padding:40px;color:var(--t4)">Loading…</td></tr>';
  try{
    const r=await fetch('/api/v1/admin/users');
    if(r.status===403){tbody.innerHTML='<tr><td colspan="4" style="text-align:center;padding:40px;color:#ef4444;">Admin access required.</td></tr>';return;}
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Failed');
    const users=d.users||[];
    window._adminUsers=users;
    /* Stats */
    const sTotal=G('adminStatTotal'),sAdm=G('adminStatAdmins'),sDev=G('adminStatDevs'),sViw=G('adminStatViewers'),sCnt=G('adminUserCount');
    if(sTotal)sTotal.textContent=users.length;
    if(sAdm)sAdm.textContent=users.filter(u=>u.role==='Admin').length;
    if(sDev)sDev.textContent=users.filter(u=>u.role==='Developer').length;
    if(sViw)sViw.textContent=users.filter(u=>u.role==='Viewer').length;
    if(sCnt)sCnt.textContent=users.length;
    /* Table */
    if(!users.length){tbody.innerHTML='<tr><td colspan="4" style="text-align:center;padding:40px;color:var(--t4)">No users found.</td></tr>';return;}
    const roleBadge=r=>({Admin:'background:rgba(239,68,68,.1);color:#EF4444;border:1px solid rgba(239,68,68,.2)',Developer:'background:rgba(59,130,246,.1);color:#3B82F6;border:1px solid rgba(59,130,246,.2)',Viewer:'background:rgba(34,197,94,.1);color:#22C55E;border:1px solid rgba(34,197,94,.2)'}[r]||'background:var(--surface-2);color:var(--t3)');
    const avatar=u=>{const n=(u.display_name||u.username||'?');const i=n.split(' ').map(w=>w[0]).join('').slice(0,2).toUpperCase();const colors=['#3B82F6','#8B5CF6','#EC4899','#F59E0B','#10B981','#EF4444'];const c=colors[n.charCodeAt(0)%colors.length];return '<div style="width:32px;height:32px;border-radius:50%;background:'+c+';color:#fff;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;flex-shrink:0;">'+i+'</div>';};
    tbody.innerHTML=users.map(u=>`<tr style="border-bottom:1px solid var(--border);transition:background .15s;" onmouseenter="this.style.background='var(--surface-2)'" onmouseleave="this.style.background=''">
      <td style="padding:10px 18px;"><div style="display:flex;align-items:center;gap:10px;">${avatar(u)}<div><div style="font-weight:600;color:var(--t1);font-size:12px;">${_esc(u.username)}</div></div></div></td>
      <td style="padding:10px 18px;color:var(--t2);font-size:12px;">${_esc(u.display_name||'—')}</td>
      <td style="padding:10px 18px;"><span style="display:inline-block;padding:3px 12px;border-radius:9999px;font-size:11px;font-weight:600;${roleBadge(u.role)}">${_esc(u.role)}</span></td>
      <td style="padding:10px 18px;text-align:center;white-space:nowrap;">
        <button class="btn btn-ghost btn-xs" onclick="adminOpenEdit('${_esc(u.username)}','${_esc(u.display_name||'')}','${_esc(u.role)}')" title="Edit" style="padding:4px 8px;">
          <svg viewBox="0 0 24 24" style="width:13px;height:13px;"><path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>
          Edit
        </button>
        <button class="btn btn-ghost btn-xs" onclick="adminResetPw('${_esc(u.username)}')" title="Reset Password" style="padding:4px 8px;">
          <svg viewBox="0 0 24 24" style="width:13px;height:13px;"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0110 0v4"/></svg>
          Reset
        </button>
        <button class="btn btn-ghost btn-xs" style="color:#ef4444;padding:4px 8px;" onclick="adminDeleteUser('${_esc(u.username)}')" title="Delete">
          <svg viewBox="0 0 24 24" style="width:13px;height:13px;"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2"/></svg>
          Delete
        </button>
      </td>
    </tr>`).join('');
  }catch(e){toast(e.message,'terr');}
};
window.adminFilterTable=function(){
  const q=(G('adminSearchInput')||{}).value||'';
  const lc=q.toLowerCase();
  const rows=G('adminUserTbody')?.querySelectorAll('tr')||[];
  rows.forEach(r=>{r.style.display=r.textContent.toLowerCase().includes(lc)?'':'none';});
};
function _esc(s){const d=document.createElement('div');d.textContent=s;return d.innerHTML;}

window.adminCreateUser=async function(){
  const username=G('adminNewUser').value.trim();
  const display_name=G('adminNewDisplay').value.trim();
  const password=G('adminNewPass').value;
  const role=G('adminNewRole').value;
  if(!username||!password){toast('Username and password are required.','terr');return;}
  try{
    const r=await fetch('/api/v1/admin/users',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username,password,role,display_name})});
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Create failed');
    toast('User "'+username+'" created.','tok');
    G('adminNewUser').value='';G('adminNewDisplay').value='';G('adminNewPass').value='';G('adminNewRole').value='Viewer';
    adminRefresh();
  }catch(e){toast(e.message,'terr');}
};

window.adminOpenEdit=function(username,display_name,role){
  G('adminEditUsername').value=username;
  G('adminEditDisplay').value=display_name;
  G('adminEditRole').value=role;
  G('adminEditPass').value='';
  G('adminEditModal').style.display='flex';
};
window.adminCloseEditModal=function(){G('adminEditModal').style.display='none';};

window.adminSaveEdit=async function(){
  const username=G('adminEditUsername').value;
  const body={display_name:G('adminEditDisplay').value.trim(),role:G('adminEditRole').value};
  const pw=G('adminEditPass').value;
  if(pw)body.password=pw;
  try{
    const r=await fetch('/api/v1/admin/users/'+encodeURIComponent(username),{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Update failed');
    toast('User "'+username+'" updated.','tok');
    adminCloseEditModal();adminRefresh();
  }catch(e){toast(e.message,'terr');}
};

window.adminDeleteUser=async function(username){
  if(!confirm('Delete user "'+username+'"? This cannot be undone.'))return;
  try{
    const r=await fetch('/api/v1/admin/users/'+encodeURIComponent(username),{method:'DELETE'});
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Delete failed');
    toast('User "'+username+'" deleted.','tok');adminRefresh();
  }catch(e){toast(e.message,'terr');}
};

window.adminResetPw=async function(username){
  const pw=prompt('Enter new password for "'+username+'" (min 6 chars):');
  if(!pw)return;
  try{
    const r=await fetch('/api/v1/admin/users/'+encodeURIComponent(username)+'/reset-password',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({new_password:pw})});
    const d=await r.json();
    if(!d.success)throw new Error(d.error||'Reset failed');
    toast('Password reset for "'+username+'".','tok');
  }catch(e){toast(e.message,'terr');}
};

