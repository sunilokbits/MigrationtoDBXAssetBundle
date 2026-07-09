/* ═══════ Accelerator Video — Animated Slides + Voice ═══════ */
(function(){try{
var _chapters=[
  {t:0, title:'Welcome & Overview',
   voice:'Welcome to the SQL to Databricks Migration Accelerator. This tool automates your entire migration journey, from SQL Server stored procedures all the way to production Databricks workflows. Let us walk you through each module in this 5-minute overview.',
   bg:'linear-gradient(135deg,#4F46E5,#7C3AED)',
   icon:'<svg viewBox="0 0 120 120" fill="none"><rect x="10" y="20" width="100" height="70" rx="12" stroke="#fff" stroke-width="3"/><polygon points="50,40 50,75 80,57" fill="#A5B4FC"/><circle cx="95" cy="25" r="12" fill="#F59E0B" opacity=".8"/><rect x="15" y="95" width="90" height="6" rx="3" fill="rgba(255,255,255,.2)"/></svg>',
   bullets:['End-to-end SQL Server to Databricks migration','11+ integrated modules in a single UI','Automated conversion and self-healing','Zero-downtime production deployment']},

  {t:30, title:'Source Connection',
   voice:'The Source Connection module connects to your SQL Server or Azure SQL database. Enter your connection details, and the tool automatically discovers all stored procedures, views, and user-defined functions. You can select individual objects or use Select All for bulk migration.',
   bg:'linear-gradient(135deg,#0369A1,#0EA5E9)',
   icon:'<svg viewBox="0 0 120 120" fill="none"><rect x="8" y="30" width="44" height="55" rx="8" stroke="#fff" stroke-width="2.5"/><text x="30" y="62" text-anchor="middle" fill="#7DD3FC" font-size="18" font-weight="bold">SQL</text><rect x="68" y="30" width="44" height="55" rx="8" stroke="#fff" stroke-width="2.5"/><text x="90" y="62" text-anchor="middle" fill="#FDE68A" font-size="14" font-weight="bold">DBX</text><path d="M52 57 L68 57" stroke="#34D399" stroke-width="3" stroke-dasharray="4 3"><animate attributeName="stroke-dashoffset" from="14" to="0" dur="1s" repeatCount="indefinite"/></path><polygon points="65,52 65,62 72,57" fill="#34D399"/></svg>',
   bullets:['SQL Server & Azure SQL support','Auto-discover SPs, Views, UDFs','Checkbox selection for fine control','Secure token-based authentication']},

  {t:60, title:'Discovery',
   voice:'The Discovery module scans every SQL object selected for migration and performs a deep analysis. It scores complexity, identifies unsupported T-SQL patterns, builds a dependency graph, and generates a Bill of Materials with effort estimates and risk levels. This gives you full visibility before conversion begins.',
   bg:'linear-gradient(135deg,#047857,#10B981)',
   icon:'<svg viewBox="0 0 120 120" fill="none"><circle cx="52" cy="52" r="30" stroke="#6EE7B7" stroke-width="3"/><line x1="74" y1="74" x2="100" y2="100" stroke="#6EE7B7" stroke-width="4" stroke-linecap="round"/><circle cx="42" cy="44" r="4" fill="#FDE68A"/><circle cx="58" cy="44" r="4" fill="#FDE68A"/><path d="M40 58 Q52 70 64 58" stroke="#FDE68A" stroke-width="2.5" fill="none" stroke-linecap="round"/><rect x="35" y="28" width="34" height="3" rx="1.5" fill="rgba(255,255,255,.2)"/></svg>',
   bullets:['Complexity scoring for every SQL object','Bill of Materials with effort estimates','Interactive dependency graph','Root cause analysis & risk assessment']},

  {t:90, title:'Convert to PySpark',
   voice:'The Convert to PySpark module is the heart of the accelerator. With one click, it converts your SQL stored procedures, views, and UDFs into clean, production-ready PySpark notebooks. Each converted object gets its own Databricks notebook, complete with proper Spark SQL syntax and helper functions.',
   bg:'linear-gradient(135deg,#DC2626,#F97316)',
   icon:'<svg viewBox="0 0 120 120" fill="none"><rect x="15" y="12" width="50" height="40" rx="6" stroke="#FCA5A5" stroke-width="2"/><text x="40" y="38" text-anchor="middle" fill="#FCA5A5" font-size="11" font-weight="600">T-SQL</text><rect x="55" y="65" width="50" height="40" rx="6" stroke="#86EFAC" stroke-width="2"/><text x="80" y="91" text-anchor="middle" fill="#86EFAC" font-size="11" font-weight="600">PySpark</text><path d="M50 52 L70 65" stroke="#FBBF24" stroke-width="2.5"><animate attributeName="stroke-dasharray" values="0 40;40 0" dur="1.5s" repeatCount="indefinite"/></path><circle cx="60" cy="58" r="10" fill="#FBBF24" opacity=".9"/><text x="60" y="62" text-anchor="middle" fill="#fff" font-size="10" font-weight="bold">SP</text></svg>',
   bullets:['Automated T-SQL to PySpark conversion','One notebook per stored procedure','UDFs bundled into HelperFunction.py','Handles complex JOINs, CTEs, temp tables']},

  {t:130, title:'Deploy to Databricks',
   voice:'Once your code is converted, the Deploy module pushes all notebooks directly into your Databricks workspace using Asset Bundles. It organizes files into proper folder structures and validates the deployment. You can see the real-time status of each notebook being deployed.',
   bg:'linear-gradient(135deg,#059669,#10B981)',
   icon:'<svg viewBox="0 0 120 120" fill="none"><rect x="25" y="70" width="70" height="35" rx="8" stroke="#6EE7B7" stroke-width="2.5"/><text x="60" y="93" text-anchor="middle" fill="#6EE7B7" font-size="12" font-weight="600">Databricks</text><rect x="35" y="15" width="20" height="24" rx="4" fill="rgba(255,255,255,.15)" stroke="#fff" stroke-width="1.5"/><rect x="62" y="15" width="20" height="24" rx="4" fill="rgba(255,255,255,.15)" stroke="#fff" stroke-width="1.5"/><path d="M45 39 L45 70" stroke="#34D399" stroke-width="2" stroke-dasharray="4 3"><animate attributeName="stroke-dashoffset" from="14" to="0" dur=".8s" repeatCount="indefinite"/></path><path d="M72 39 L72 70" stroke="#34D399" stroke-width="2" stroke-dasharray="4 3"><animate attributeName="stroke-dashoffset" from="14" to="0" dur=".8s" repeatCount="indefinite"/></path></svg>',
   bullets:['Databricks Asset Bundle deployment','Auto folder structure creation','Real-time deploy status tracking','Workspace path configuration']},

  {t:160, title:'Databricks SQL Editor',
   voice:'The Databricks SQL Editor lets you browse your Databricks catalogs, schemas, and tables right from this UI. You can preview table data, run ad-hoc SQL queries, and verify that your migrated objects are correctly registered in Databricks.',
   bg:'linear-gradient(135deg,#7C3AED,#A855F7)',
   icon:'<svg viewBox="0 0 120 120" fill="none"><ellipse cx="60" cy="35" rx="40" ry="14" stroke="#C4B5FD" stroke-width="2.5"/><path d="M20 35 L20 55 Q20 69 60 69 Q100 69 100 55 L100 35" stroke="#C4B5FD" stroke-width="2.5" fill="none"/><path d="M20 55 L20 75 Q20 89 60 89 Q100 89 100 75 L100 55" stroke="#C4B5FD" stroke-width="2.5" fill="none"/><ellipse cx="60" cy="55" rx="40" ry="14" stroke="#C4B5FD" stroke-width="1" opacity=".4"/><circle cx="42" cy="45" r="4" fill="#FDE68A"/><circle cx="60" cy="45" r="4" fill="#86EFAC"/><circle cx="78" cy="45" r="4" fill="#93C5FD"/></svg>',
   bullets:['Browse catalogs, schemas & tables','Live data preview with row counts','Run SQL queries in the browser','Config-driven — no tokens in UI']},

  {t:190, title:'System Health Check',
  voice:'When errors happen, the System Health Check diagnoses them automatically. Paste any error message or stack trace, and it analyzes the root cause, provides a detailed explanation, and suggests specific fixes. It understands Databricks, Spark, and Python exceptions natively.',
   bg:'linear-gradient(135deg,#BE185D,#EC4899)',
   icon:'<svg viewBox="0 0 120 120" fill="none"><circle cx="60" cy="50" r="35" stroke="#F9A8D4" stroke-width="2.5"/><path d="M45 45 Q48 38 55 42" stroke="#F9A8D4" stroke-width="2.5" stroke-linecap="round"/><path d="M75 45 Q72 38 65 42" stroke="#F9A8D4" stroke-width="2.5" stroke-linecap="round"/><path d="M45 58 Q53 68 60 65 Q67 68 75 58" stroke="#F9A8D4" stroke-width="2.5" stroke-linecap="round" fill="none"/><path d="M35 90 L50 78" stroke="#86EFAC" stroke-width="2"/><circle cx="30" cy="94" r="8" stroke="#86EFAC" stroke-width="2"/><text x="30" y="98" text-anchor="middle" fill="#86EFAC" font-size="10">✓</text><rect x="15" y="18" width="12" height="3" rx="1.5" fill="#FCA5A5"/><rect x="93" y="18" width="12" height="3" rx="1.5" fill="#FCA5A5"/></svg>',
   bullets:['Automated error diagnosis','Paste any error or stack trace','Root cause analysis & fix suggestions','Spark, Python, Databricks aware']},

  {t:245, title:'Workflow Orchestration',
   voice:'Workflow Orchestration is your mission control. Create Databricks jobs, set CRON schedules, monitor pipeline runs, and track execution history — all from this dashboard. You get real-time stats, run history tables, and one-click Run Now buttons for each pipeline.',
   bg:'linear-gradient(135deg,#1E40AF,#3B82F6)',
   icon:'<svg viewBox="0 0 120 120" fill="none"><rect x="10" y="20" width="30" height="22" rx="5" stroke="#93C5FD" stroke-width="2"/><rect x="45" y="20" width="30" height="22" rx="5" stroke="#93C5FD" stroke-width="2"/><rect x="80" y="20" width="30" height="22" rx="5" stroke="#93C5FD" stroke-width="2"/><path d="M40 31 L45 31" stroke="#60A5FA" stroke-width="2"/><path d="M75 31 L80 31" stroke="#60A5FA" stroke-width="2"/><rect x="20" y="55" width="80" height="8" rx="4" fill="rgba(255,255,255,.1)"/><rect x="20" y="55" width="55" height="8" rx="4" fill="#3B82F6"><animate attributeName="width" values="20;55;20" dur="3s" repeatCount="indefinite"/></rect><rect x="20" y="70" width="80" height="8" rx="4" fill="rgba(255,255,255,.1)"/><rect x="20" y="70" width="70" height="8" rx="4" fill="#10B981"><animate attributeName="width" values="30;70;30" dur="4s" repeatCount="indefinite"/></rect><rect x="20" y="85" width="80" height="8" rx="4" fill="rgba(255,255,255,.1)"/><rect x="20" y="85" width="40" height="8" rx="4" fill="#F59E0B"><animate attributeName="width" values="10;40;10" dur="2.5s" repeatCount="indefinite"/></rect></svg>',
   bullets:['Create & manage Databricks jobs','CRON schedule configuration','Real-time pipeline monitoring','Run history & execution stats']},

  {t:280, title:'Data Modeling',
   voice:'The Data Modeling module classifies your tables into a Star or Snowflake schema automatically. It generates interactive ER diagrams, lets you edit table roles, add or remove columns, and produces ready-to-run DDL statements for your data warehouse.',
   bg:'linear-gradient(135deg,#0F766E,#14B8A6)',
   icon:'<svg viewBox="0 0 120 120" fill="none"><rect x="35" y="10" width="50" height="28" rx="6" stroke="#5EEAD4" stroke-width="2"/><text x="60" y="29" text-anchor="middle" fill="#5EEAD4" font-size="10" font-weight="600">FACT</text><rect x="5" y="65" width="40" height="24" rx="5" stroke="#A5B4FC" stroke-width="2"/><text x="25" y="81" text-anchor="middle" fill="#A5B4FC" font-size="9">DIM</text><rect x="75" y="65" width="40" height="24" rx="5" stroke="#A5B4FC" stroke-width="2"/><text x="95" y="81" text-anchor="middle" fill="#A5B4FC" font-size="9">DIM</text><rect x="40" y="75" width="40" height="24" rx="5" stroke="#FCA5A5" stroke-width="2"/><text x="60" y="91" text-anchor="middle" fill="#FCA5A5" font-size="9">DIM</text><line x1="45" y1="38" x2="25" y2="65" stroke="rgba(255,255,255,.3)" stroke-width="1.5"/><line x1="60" y1="38" x2="60" y2="75" stroke="rgba(255,255,255,.3)" stroke-width="1.5"/><line x1="75" y1="38" x2="95" y2="65" stroke="rgba(255,255,255,.3)" stroke-width="1.5"/></svg>',
   bullets:['Automated Star / Snowflake classification','Interactive ER diagram visualization','Inline table & column editing','Auto-generated DDL statements']},

  {t:310, title:'Azure DevOps Integration',
   voice:'The Azure DevOps Integration lets you push your Data Model artifacts directly to a Git repository. DDL scripts, ER diagram images, and model JSON are committed atomically to your Azure DevOps repo. Authentication uses a Personal Access Token stored securely in Azure Key Vault — no secrets are exposed in the UI or config files.',
   bg:'linear-gradient(135deg,#0078D4,#106EBE)',
   icon:'<svg viewBox="0 0 120 120" fill="none"><rect x="20" y="25" width="80" height="60" rx="10" stroke="#7EC8E3" stroke-width="2.5"/><text x="60" y="50" text-anchor="middle" fill="#7EC8E3" font-size="10" font-weight="600">Azure DevOps</text><path d="M40 65 L55 65 L55 75 L70 55 L55 55 L55 45 L40 65Z" fill="#4FC3F7" opacity=".8"/><circle cx="85" cy="35" r="8" stroke="#86EFAC" stroke-width="2"/><path d="M82 35 L84 37 L88 33" stroke="#86EFAC" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/><rect x="30" y="90" width="60" height="4" rx="2" fill="rgba(255,255,255,.2)"/></svg>',
   bullets:['Push DDL & ER diagrams to Git repo','Atomic commits with configurable branch','PAT stored securely in Azure Key Vault','Test Connection validates config instantly']},

  {t:345, title:'Summary & Next Steps',
   voice:'That completes our walkthrough of the SQL to Databricks Migration Accelerator. You have seen how each module works together to deliver an automated, end-to-end migration experience. Click on any sidebar tab to get started, and use the Help button for detailed documentation on each feature. Thank you for watching!',
   bg:'linear-gradient(135deg,#4F46E5,#7C3AED)',
   icon:'<svg viewBox="0 0 120 120" fill="none"><circle cx="60" cy="55" r="40" stroke="#C4B5FD" stroke-width="2.5"/><path d="M40 55 L55 70 L82 42" stroke="#86EFAC" stroke-width="4" stroke-linecap="round" stroke-linejoin="round"><animate attributeName="stroke-dasharray" values="0 60;60 0" dur="1.2s" fill="freeze"/></path><circle cx="60" cy="55" r="40" stroke="#A5B4FC" stroke-width="1" opacity=".3"><animate attributeName="r" values="40;44;40" dur="2s" repeatCount="indefinite"/></circle></svg>',
   bullets:['12 integrated migration modules','Automated conversion & healing','Production-ready deployment pipeline','Click any tab to get started!']}
];
var _total=365;
var _playing=false, _elapsed=0, _timer=null, _activeChIdx=-1, _spokenIdx=-1;

function _buildChapters(){
  var list=document.getElementById('vidChList'); if(!list)return;
  list.innerHTML=_chapters.map(function(ch,i){return '<div class="vid-ch-item'+(i===0?' active':'')+'" data-idx="'+i+'" onclick="seekToChapter('+i+')"><div class="vid-ch-num">'+(i+1)+'</div><div class="vid-ch-info"><div class="vid-ch-title">'+ch.title+'</div><div class="vid-ch-time">'+_fmtTime(ch.t)+'</div></div></div>';}).join('');
}
function _fmtTime(s){return Math.floor(s/60)+':'+String(Math.floor(s%60)).padStart(2,'0');}

function _showSlide(idx){
  var slide=document.getElementById('vidSlide'); if(!slide||idx<0||idx>=_chapters.length)return;
  var ch=_chapters[idx];
  slide.parentElement.style.background=ch.bg;
  var bhtml='';
  var colors=['#818CF8','#34D399','#FBBF24','#F472B6'];
  ch.bullets.forEach(function(b,i){bhtml+='<li style="animation-delay:'+((i+1)*0.1)+'s"><span class="vb-dot" style="background:'+colors[i%4]+'"></span>'+b+'</li>';});
  slide.innerHTML='<div class="vslide"><div class="vslide-ico">'+ch.icon+'</div><div class="vslide-title">'+ch.title+'</div><div class="vslide-sub">'+ch.voice.split('.')[0]+'.</div><ul class="vslide-bullets">'+bhtml+'</ul></div>';
}

function _speak(idx){
  if(!window.speechSynthesis)return;
  var tog=document.getElementById('vidVoiceToggle');
  if(tog&&!tog.checked)return;
  window.speechSynthesis.cancel();
  var ch=_chapters[idx]; if(!ch)return;
  var u=new SpeechSynthesisUtterance(ch.voice);
  u.rate=1.0; u.pitch=1.0; u.volume=1.0;
  var voices=window.speechSynthesis.getVoices();
  for(var v=0;v<voices.length;v++){
    if(voices[v].name.indexOf('Zira')>=0||voices[v].name.indexOf('David')>=0||voices[v].name.indexOf('Google')>=0||voices[v].name.indexOf('Samantha')>=0){u.voice=voices[v];break;}
  }
  window.speechSynthesis.speak(u);
}

function _updateUI(){
  var pct=Math.min(100,(_elapsed/_total)*100);
  var fill=document.getElementById('vidProgressFill'); if(fill)fill.style.width=pct+'%';
  var lbl=document.getElementById('vidTimeLabel'); if(lbl)lbl.textContent=_fmtTime(_elapsed)+' / '+_fmtTime(_total);
  var idx=0;
  for(var i=_chapters.length-1;i>=0;i--){if(_elapsed>=_chapters[i].t){idx=i;break;}}
  if(idx!==_activeChIdx){
    _activeChIdx=idx;
    _showSlide(idx);
    if(idx!==_spokenIdx){_spokenIdx=idx;_speak(idx);}
    document.querySelectorAll('.vid-ch-item').forEach(function(el,i){el.classList.toggle('active',i===idx);});
    var activeEl=document.querySelector('.vid-ch-item.active');
    if(activeEl)activeEl.scrollIntoView({behavior:'smooth',block:'nearest'});
  }
}
function _setPlayIcon(play){
  var ico=document.getElementById('vidPlayIco');
  if(ico)ico.innerHTML=play?'<rect x="6" y="4" width="4" height="16" rx="1"/><rect x="14" y="4" width="4" height="16" rx="1"/>':'<path d="M8 5v14l11-7z"/>';
}
window.openAcceleratorVideo=function(){
  var ov=document.getElementById('vidOverlay'); if(!ov)return;
  ov.classList.add('open');_buildChapters();_elapsed=0;_activeChIdx=-1;_spokenIdx=-1;_updateUI();
  if(window.speechSynthesis)window.speechSynthesis.getVoices();
};
window.closeAcceleratorVideo=function(){
  var ov=document.getElementById('vidOverlay'); if(!ov)return;
  ov.classList.remove('open');_playing=false;clearInterval(_timer);_setPlayIcon(false);
  if(window.speechSynthesis)window.speechSynthesis.cancel();
};
window.toggleVidPlay=function(){
  if(_playing){_playing=false;clearInterval(_timer);_setPlayIcon(false);if(window.speechSynthesis)window.speechSynthesis.pause();}
  else{_playing=true;_setPlayIcon(true);
    if(window.speechSynthesis&&window.speechSynthesis.paused)window.speechSynthesis.resume();
    else if(_spokenIdx!==_activeChIdx){_spokenIdx=_activeChIdx;_speak(_activeChIdx);}
    _timer=setInterval(function(){_elapsed++;if(_elapsed>=_total){_elapsed=_total;_playing=false;clearInterval(_timer);_setPlayIcon(false);}_updateUI();},1000);
  }
};
window.seekVid=function(e){
  var bar=document.getElementById('vidProgressBar'); if(!bar)return;
  var rect=bar.getBoundingClientRect();var pct=(e.clientX-rect.left)/rect.width;
  _elapsed=Math.max(0,Math.min(_total,Math.round(pct*_total)));_spokenIdx=-1;_updateUI();
};
window.seekToChapter=function(idx){
  if(idx>=0&&idx<_chapters.length){_elapsed=_chapters[idx].t;_spokenIdx=-1;_activeChIdx=-1;_updateUI();if(!_playing)toggleVidPlay();}
};
document.addEventListener('keydown',function(e){if(e.key==='Escape')closeAcceleratorVideo();});
if(window.speechSynthesis)window.speechSynthesis.onvoiceschanged=function(){window.speechSynthesis.getVoices();};
}catch(e){console.error('Supplementary video module error:',e);}
})();

(function(){try{
  /* Role → allowed nav-button ids */
  var ROLE_NAV = {
    Admin: null,                            /* null = everything visible */
    Developer: [
      'nav-wf-dashboard','nav-wf-metadata','nav-wf-pipelines','nav-wf-jobs','nav-wf-scheduler',
      'nav-convert','nav-deploy','nav-uc',
      'nav-wf-datamodel','nav-healer','nav-wf-settings'
    ],
    Viewer: [
      'nav-wf-dashboard',
      'nav-wf-reports','nav-wf-progress','nav-wf-schema','nav-wf-recon','nav-wf-dq',
      'nav-wf-audit'
    ]
  };
  /* Viewer: disable mutating buttons inside visible panes */
  var VIEWER_DISABLE_SELECTORS = '.btn-primary, .btn-sm, button[onclick*="convert"], button[onclick*="deploy"], button[onclick*="upload"], button[onclick*="execute"], button[onclick*="save"], button[onclick*="run"]';

  fetch('/api/v1/auth/me').then(function(r){return r.json()}).then(function(d){
    /* Update user chip */
    var initials = (d.display_name||'U').split(' ').map(function(w){return w[0]}).join('').substring(0,2).toUpperCase();
    var av = document.getElementById('userAvatar');
    var un = document.getElementById('userName');
    var rb = document.getElementById('userRoleBadge');
    if(av) av.textContent = initials;
    if(un) un.textContent = d.display_name || d.user;
    if(rb){
      rb.textContent = d.role;
      rb.className = 'user-role-badge role-' + d.role.toLowerCase();
    }

    /* Apply nav restrictions */
    var allowed = ROLE_NAV[d.role];
    if(allowed !== null && allowed !== undefined){
      var navBtns = document.querySelectorAll('.sb-nav .nav-btn');
      navBtns.forEach(function(btn){
        if(allowed.indexOf(btn.id)===-1){
          btn.style.display='none';
        }
      });
      /* Also hide section headers if all their children are hidden */
      var sections = document.querySelectorAll('.sb-nav .sb-sec');
      sections.forEach(function(sec){
        var next = sec.nextElementSibling;
        var anyVisible = false;
        while(next && !next.classList.contains('sb-sec')){
          if(next.classList.contains('nav-btn') && next.style.display !== 'none') anyVisible = true;
          next = next.nextElementSibling;
        }
        if(!anyVisible) sec.style.display = 'none';
      });
    }

    /* Viewer: disable action buttons in visible panes */
    if(d.role === 'Viewer'){
      setTimeout(function(){
        document.querySelectorAll(VIEWER_DISABLE_SELECTORS).forEach(function(b){
          b.disabled = true;
          b.title = 'Read-only access — contact Admin for edit permissions';
          b.style.opacity = '0.5';
          b.style.cursor = 'not-allowed';
        });
      }, 500);
    }

    /* Expose role globally for any other JS that needs it */
    window.__USER_ROLE = d.role;
    window.__USER_NAME = d.display_name || d.user;
  }).catch(function(){
    /* Auth failed — redirect to login */
    window.location.href = '/login';
  });
}catch(e){console.error('RBAC module error:',e);}
})();
