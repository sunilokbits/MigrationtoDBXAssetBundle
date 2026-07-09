"""Patch main.js to add interactive ER diagram editing features."""
import os

JS_PATH = os.path.join(os.path.dirname(__file__), 'migration_utility', 'static', 'main.js')

with open(JS_PATH, 'r', encoding='utf-8') as f:
    content = f.read()

print(f"File size: {len(content)} bytes")

# ═══════════════════════════════════════════════════════════════════════════════
# PATCH 1: Replace dmRenderER to add click-to-edit on table nodes + view nodes
# ═══════════════════════════════════════════════════════════════════════════════

OLD_ER = "function dmRenderER(er){"
if OLD_ER not in content:
    print("ERROR: Could not find dmRenderER marker")
    exit(1)

# Find the end of dmRenderER - it ends just before _dmDrawEdges
END_MARKER = "function _dmDrawEdges(er, g){"
pos_start = content.find(OLD_ER)
pos_end = content.find(END_MARKER)
if pos_end == -1:
    print("ERROR: Could not find _dmDrawEdges marker")
    exit(1)

NEW_RENDER_ER = '''function dmRenderER(er){
  const g=G('dmErGroup');
  g.innerHTML='';
  if(!er||!er.nodes) return;
  const svg=G('dmErSvg');
  _dmZoomLevel=1; _dmPanX=0; _dmPanY=0;
  g.setAttribute('transform','translate(0,0) scale(1)');

  const facts=er.nodes.filter(n=>n.type==='fact');
  const dims=er.nodes.filter(n=>n.type==='dimension');
  const views=er.nodes.filter(n=>n.type==='view');
  const W=Math.max(svg.clientWidth||1100,900);
  const H=Math.max(520, (facts.length+dims.length+views.length)*50);
  svg.setAttribute('height', H);

  // Position facts horizontally centered
  const factW=220, factGap=40;
  const totalFactW=facts.length*factW+(facts.length-1)*factGap;
  let fx=Math.max(20,(W-totalFactW)/2);
  facts.forEach((n,i)=>{if(n.x===undefined||n._autoLayout){n.x=fx+i*(factW+factGap);n.y=H/2-80;n._autoLayout=true;}});

  // Position dims in elliptical arc
  const cx=W/2, cy=H/2-30;
  const rx=Math.min(W/2.3,450), ry=Math.min(H/2.3,220);
  dims.forEach((n,i)=>{if(n.x===undefined||n._autoLayout){
    const angle=Math.PI + Math.PI*(i+1)/(dims.length+1);
    n.x=cx+Math.cos(angle)*rx-100;n.y=cy+Math.sin(angle)*ry-30;n._autoLayout=true;
  }});

  // Position views below
  views.forEach((n,i)=>{if(n.x===undefined||n._autoLayout){n.x=40+i*260;n.y=H-120;n._autoLayout=true;}});

  // Draw edges first (behind nodes)
  _dmDrawEdges(er, g);

  // Draw nodes
  er.nodes.forEach(n=>{
    const isFact=n.type==='fact';
    const isView=n.type==='view';
    const ng=document.createElementNS('http://www.w3.org/2000/svg','g');
    ng.setAttribute('transform','translate('+n.x+','+n.y+')');
    ng.setAttribute('data-node-id', n.id);
    ng.style.cursor='move';

    const cols=n.columns||[];
    const boxH=Math.max(72, 32+cols.length*14+14);
    const nodeW=220;

    // Drop shadow
    const shadow=document.createElementNS('http://www.w3.org/2000/svg','rect');
    shadow.setAttribute('x','3');shadow.setAttribute('y','3');
    shadow.setAttribute('width',nodeW);shadow.setAttribute('height',boxH);
    shadow.setAttribute('rx','10');shadow.setAttribute('fill','rgba(0,0,0,0.08)');
    ng.appendChild(shadow);

    // Main box
    const rect=document.createElementNS('http://www.w3.org/2000/svg','rect');
    rect.setAttribute('width',nodeW);rect.setAttribute('height',boxH);rect.setAttribute('rx','10');
    rect.setAttribute('fill',isView?'#FFFBEB':isFact?'#EFF6FF':'#F0FDF4');
    rect.setAttribute('stroke',isView?'#F59E0B':isFact?'#3B82F6':'#10B981');
    rect.setAttribute('stroke-width','2');
    ng.appendChild(rect);

    // Header bar
    const hdrColor=isView?'#F59E0B':isFact?'#3B82F6':'#10B981';
    const hdr=document.createElementNS('http://www.w3.org/2000/svg','rect');
    hdr.setAttribute('width',nodeW);hdr.setAttribute('height','28');hdr.setAttribute('rx','10');
    hdr.setAttribute('fill',hdrColor);ng.appendChild(hdr);
    const hdr2=document.createElementNS('http://www.w3.org/2000/svg','rect');
    hdr2.setAttribute('y','14');hdr2.setAttribute('width',nodeW);hdr2.setAttribute('height','14');
    hdr2.setAttribute('fill',hdrColor);ng.appendChild(hdr2);

    // Role badge
    const badge=document.createElementNS('http://www.w3.org/2000/svg','text');
    badge.setAttribute('x',nodeW-8);badge.setAttribute('y','17');badge.setAttribute('text-anchor','end');
    badge.setAttribute('fill','rgba(255,255,255,0.7)');badge.setAttribute('font-size','8');badge.setAttribute('font-family','system-ui');
    badge.textContent=isView?'VIEW':isFact?'FACT':'DIM';
    ng.appendChild(badge);

    // Edit icon (pencil) - top left next to name
    const editIcon=document.createElementNS('http://www.w3.org/2000/svg','text');
    editIcon.setAttribute('x',nodeW-24);editIcon.setAttribute('y','17');editIcon.setAttribute('text-anchor','end');
    editIcon.setAttribute('fill','rgba(255,255,255,0.85)');editIcon.setAttribute('font-size','10');
    editIcon.setAttribute('cursor','pointer');editIcon.textContent='\\u270E';
    editIcon.addEventListener('click',ev=>{ev.stopPropagation();dmOpenTableEditor(n);});
    ng.appendChild(editIcon);

    // Table name
    const title=document.createElementNS('http://www.w3.org/2000/svg','text');
    title.setAttribute('x','10');title.setAttribute('y','18');title.setAttribute('text-anchor','start');
    title.setAttribute('fill','white');title.setAttribute('font-size','11');title.setAttribute('font-weight','700');
    title.setAttribute('font-family','system-ui');
    title.textContent=n.label;ng.appendChild(title);

    // Separator
    const sep=document.createElementNS('http://www.w3.org/2000/svg','line');
    sep.setAttribute('x1','0');sep.setAttribute('y1','28');sep.setAttribute('x2',nodeW);sep.setAttribute('y2','28');
    sep.setAttribute('stroke',hdrColor+'30');sep.setAttribute('stroke-width','1');
    ng.appendChild(sep);

    // Columns with PK/FK/UQ icons
    cols.forEach((c,ci)=>{
      const cy2=42+ci*14;
      // Constraint icon
      const icon=document.createElementNS('http://www.w3.org/2000/svg','text');
      icon.setAttribute('x','6');icon.setAttribute('y',cy2);icon.setAttribute('font-size','9');
      if(c.is_pk){icon.textContent='\\ud83d\\udd11';icon.setAttribute('fill','#F59E0B');}
      else if(c.is_unique){icon.textContent='\\ud83c\\udfaf';icon.setAttribute('fill','#8B5CF6');}
      else if(c.fk_table){icon.textContent='\\ud83d\\udd17';icon.setAttribute('fill','#6366F1');}
      else{icon.textContent='\\u2022';icon.setAttribute('fill','#94A3B8');}
      ng.appendChild(icon);

      // Column name
      const ct=document.createElementNS('http://www.w3.org/2000/svg','text');
      ct.setAttribute('x','20');ct.setAttribute('y',cy2);ct.setAttribute('fill',c.is_pk?'#1E293B':'#475569');
      ct.setAttribute('font-size','10');ct.setAttribute('font-family','monospace');
      ct.setAttribute('font-weight',c.is_pk?'700':'400');
      ct.textContent=c.name;
      ng.appendChild(ct);

      // Type (right-aligned)
      const tt=document.createElementNS('http://www.w3.org/2000/svg','text');
      tt.setAttribute('x',nodeW-6);tt.setAttribute('y',cy2);tt.setAttribute('text-anchor','end');
      tt.setAttribute('fill','#94A3B8');tt.setAttribute('font-size','9');tt.setAttribute('font-family','monospace');
      tt.textContent=(c.data_type||'STRING').split('(')[0];
      ng.appendChild(tt);

      // Comment indicator
      if(c.comment){
        const ci2=document.createElementNS('http://www.w3.org/2000/svg','title');
        ci2.textContent=c.comment;
        ct.appendChild(ci2);
        // Small dot indicator for comment
        const cdot=document.createElementNS('http://www.w3.org/2000/svg','circle');
        cdot.setAttribute('cx',nodeW-42);cdot.setAttribute('cy',cy2-3);cdot.setAttribute('r','2');
        cdot.setAttribute('fill','#8B5CF6');
        ng.appendChild(cdot);
      }
    });

    // "+" button to add column at bottom of node
    const addBtnY=boxH-12;
    const addBtn=document.createElementNS('http://www.w3.org/2000/svg','text');
    addBtn.setAttribute('x',nodeW/2);addBtn.setAttribute('y',addBtnY);
    addBtn.setAttribute('text-anchor','middle');addBtn.setAttribute('fill','#94A3B8');
    addBtn.setAttribute('font-size','10');addBtn.setAttribute('cursor','pointer');
    addBtn.textContent='+ add column';
    addBtn.addEventListener('click',ev=>{ev.stopPropagation();dmOpenTableEditor(n,'addcol');});
    ng.appendChild(addBtn);

    // Drag support
    let dragging=false, dx=0, dy=0;
    ng.addEventListener('mousedown',ev=>{
      if(ev.button!==0)return;
      // Don't drag if clicking edit icon or add button
      if(ev.target===editIcon||ev.target===addBtn)return;
      dragging=true;dx=ev.clientX/_dmZoomLevel-n.x;dy=ev.clientY/_dmZoomLevel-n.y;
      ev.preventDefault();ev.stopPropagation();
      svg.style.cursor='grabbing';
      n._autoLayout=false; // user positioned
    });
    const onMove=ev=>{
      if(!dragging)return;
      n.x=ev.clientX/_dmZoomLevel-dx;
      n.y=ev.clientY/_dmZoomLevel-dy;
      ng.setAttribute('transform','translate('+n.x+','+n.y+')');
      _dmDrawEdges(er, g);
    };
    const onUp=()=>{if(dragging){dragging=false;svg.style.cursor='grab';}};
    svg.addEventListener('mousemove',onMove);
    svg.addEventListener('mouseup',onUp);
    svg.addEventListener('mouseleave',onUp);

    // Double-click to open editor
    ng.addEventListener('dblclick',ev=>{ev.stopPropagation();dmOpenTableEditor(n);});

    // Right-click context menu for relationship creation
    ng.addEventListener('contextmenu',ev=>{
      ev.preventDefault();ev.stopPropagation();
      _dmShowNodeContextMenu(n, ev);
    });

    g.appendChild(ng);
  });

  // Scroll-to-zoom on SVG
  svg.onwheel=ev=>{
    ev.preventDefault();
    const factor=ev.deltaY<0?1.1:0.9;
    _dmZoomLevel=Math.max(0.3,Math.min(3,_dmZoomLevel*factor));
    g.setAttribute('transform','translate('+_dmPanX+','+_dmPanY+') scale('+_dmZoomLevel+')');
  };

  // Pan on SVG background drag
  let _svgDragging=false, _sx=0, _sy=0;
  svg.addEventListener('mousedown',ev=>{
    if(ev.target===svg||ev.target.tagName==='svg'){
      _svgDragging=true;_sx=ev.clientX-_dmPanX;_sy=ev.clientY-_dmPanY;svg.style.cursor='grabbing';
    }
  });
  svg.addEventListener('mousemove',ev=>{
    if(!_svgDragging)return;
    _dmPanX=ev.clientX-_sx;_dmPanY=ev.clientY-_sy;
    g.setAttribute('transform','translate('+_dmPanX+','+_dmPanY+') scale('+_dmZoomLevel+')');
  });
  svg.addEventListener('mouseup',()=>{_svgDragging=false;svg.style.cursor='grab';});
}

// ═══════════════════════════════════════════════════════════════════════════════
// INTERACTIVE TABLE EDITOR — Opens when clicking pencil icon or double-click
// ═══════════════════════════════════════════════════════════════════════════════
function dmOpenTableEditor(node, mode){
  // Find table/view data in model
  const isView=node.type==='view';
  let tblData=null;
  if(isView){
    tblData=(_dmModel.views||[]).find(v=>v.view_name===node.id);
  } else {
    tblData=[...(_dmModel.facts||[]),...(_dmModel.dimensions||[])].find(t=>t.table_name===node.id);
  }
  if(!tblData&&!isView)return;

  // Remove existing editor if open
  const existing=document.getElementById('dmTableEditorPanel');
  if(existing)existing.remove();

  const panel=document.createElement('div');
  panel.id='dmTableEditorPanel';
  panel.style.cssText='position:fixed;top:60px;right:10px;width:460px;max-height:calc(100vh - 80px);overflow-y:auto;background:var(--bg1,#fff);border:2px solid '+(isView?'#F59E0B':node.type==='fact'?'#3B82F6':'#10B981')+';border-radius:14px;padding:18px;box-shadow:0 20px 60px rgba(0,0,0,.25);z-index:9999;';

  const hdrColor=isView?'#F59E0B':node.type==='fact'?'#3B82F6':'#10B981';
  const tableName=isView?tblData.view_name:tblData.table_name;
  const cols=isView?(tblData.columns||[]):(tblData.columns||[]);

  let html='<div style="display:flex;align-items:center;gap:8px;margin-bottom:12px;">';
  html+='<span style="font-size:16px;font-weight:800;color:'+hdrColor+';">'+(isView?'\\ud83d\\udc41 View':'\\ud83d\\udcdd Table')+': '+tableName+'</span>';
  html+='<div style="margin-left:auto;display:flex;gap:6px;">';
  if(!isView){
    html+='<button class="btn btn-ghost btn-xs" onclick="dmERAddRelFrom(\\''+tableName.replace(/'/g,"\\\\'")+'\\')">\\ud83d\\udd17 Add FK</button>';
  }
  html+='<button class="btn btn-ghost btn-xs" onclick="document.getElementById(\\'dmTableEditorPanel\\').remove()" style="font-size:14px;">\\u2715</button>';
  html+='</div></div>';

  // Table comment
  const tblComment=tblData.comment||'';
  html+='<div style="margin-bottom:10px;"><label style="font-size:10px;font-weight:600;color:var(--t3);">Table Comment</label>';
  html+='<input class="inp" id="dmTblEdComment" value="'+_escAttr(tblComment)+'" placeholder="Describe this table..." style="font-size:11px;width:100%;" onchange="dmSaveTableComment(\\''+_escJs(tableName)+'\\',this.value)"></div>';

  if(isView){
    // View definition editor
    html+='<div style="margin-bottom:10px;"><label style="font-size:10px;font-weight:600;color:var(--t3);">View SQL Definition</label>';
    html+='<textarea class="inp" id="dmViewDefEditor" style="font-size:11px;width:100%;height:120px;font-family:monospace;" onchange="dmSaveViewDef(\\''+_escJs(tableName)+'\\',this.value)">'+_escHtml(tblData.definition||'')+'</textarea></div>';
  }

  // Columns table
  html+='<div style="font-size:11px;font-weight:700;margin-bottom:6px;display:flex;align-items:center;gap:8px;">';
  html+='<span>Columns ('+cols.length+')</span>';
  html+='<button class="btn btn-ghost btn-xs" onclick="dmERAddColumn(\\''+_escJs(tableName)+'\\',\\''+node.type+'\\')">+ Add Column</button></div>';
  html+='<table style="width:100%;border-collapse:collapse;font-size:10px;">';
  html+='<thead><tr style="background:var(--bg2);"><th style="padding:4px;">Name</th><th style="padding:4px;">Type</th><th style="padding:4px;text-align:center;">PK</th><th style="padding:4px;text-align:center;">UQ</th><th style="padding:4px;text-align:center;">FK</th><th style="padding:4px;text-align:center;">Null</th><th style="padding:4px;">Comment</th><th style="padding:4px;"></th></tr></thead>';
  html+='<tbody>';
  cols.forEach((c,i)=>{
    const tn=_escJs(tableName), cn=_escJs(c.name);
    html+='<tr style="border-bottom:1px solid var(--border);">';
    html+='<td style="padding:3px;"><input class="inp" value="'+_escAttr(c.name)+'" style="font-size:10px;width:80px;font-family:monospace;" onchange="dmEREditCol(\\''+tn+'\\',\\''+cn+'\\',\\'name\\',this.value)"></td>';
    html+='<td style="padding:3px;"><select class="inp" style="font-size:10px;width:85px;" onchange="dmEREditCol(\\''+tn+'\\',\\''+cn+'\\',\\'data_type\\',this.value)">'+_dmTypeOptions(c.data_type)+'</select></td>';
    html+='<td style="padding:3px;text-align:center;"><input type="checkbox" '+(c.is_pk?'checked':'')+' onchange="dmEREditCol(\\''+tn+'\\',\\''+cn+'\\',\\'is_pk\\',this.checked)"></td>';
    html+='<td style="padding:3px;text-align:center;"><input type="checkbox" '+(c.is_unique?'checked':'')+' onchange="dmEREditCol(\\''+tn+'\\',\\''+cn+'\\',\\'is_unique\\',this.checked)"></td>';
    html+='<td style="padding:3px;text-align:center;"><button class="btn btn-ghost btn-xs" onclick="dmERSetFK(\\''+tn+'\\',\\''+cn+'\\',\\''+_escJs(c.fk_table||'')+'\\')">'+( c.fk_table? '\\ud83d\\udd17'+c.fk_table:'\\u2014')+'</button></td>';
    html+='<td style="padding:3px;text-align:center;"><input type="checkbox" '+(c.is_nullable?'checked':'')+' onchange="dmEREditCol(\\''+tn+'\\',\\''+cn+'\\',\\'is_nullable\\',this.checked)"></td>';
    html+='<td style="padding:3px;"><input class="inp" value="'+_escAttr(c.comment||'')+'" placeholder="..." style="font-size:9px;width:70px;" onchange="dmEREditCol(\\''+tn+'\\',\\''+cn+'\\',\\'comment\\',this.value)"></td>';
    html+='<td style="padding:3px;"><button class="btn btn-ghost btn-xs" onclick="dmERDelCol(\\''+tn+'\\',\\''+cn+'\\')" style="color:#EF4444;font-size:10px;">\\u2715</button></td>';
    html+='</tr>';
  });
  html+='</tbody></table>';

  panel.innerHTML=html;
  document.body.appendChild(panel);

  // Auto-focus add-column if mode requests it
  if(mode==='addcol'){setTimeout(()=>dmERAddColumn(tableName,node.type),100);}
}

// Helper for HTML entity escaping in editor
function _escAttr(s){return String(s||'').replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;');}
function _escHtml(s){return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
function _escJs(s){return String(s||'').replace(/\\\\/g,'\\\\\\\\').replace(/'/g,"\\\\'");}

function _dmTypeOptions(current){
  const types=['STRING','INT','BIGINT','SMALLINT','TINYINT','DOUBLE','FLOAT','DECIMAL(18,2)','DECIMAL(10,2)','BOOLEAN','DATE','TIMESTAMP','BINARY','ARRAY<STRING>','MAP<STRING,STRING>'];
  return types.map(t=>'<option value="'+t+'"'+(current&&current.toUpperCase()===t?' selected':'')+'>'+t+'</option>').join('');
}

// ═══════════════════════════════════════════════════════════════════════════════
// ER EDITOR API CALLS — Column edits from the editor panel
// ═══════════════════════════════════════════════════════════════════════════════
async function dmEREditCol(tableName, colName, field, value){
  await _dmEdit({column_edits:[{table_name:tableName, column_name:colName, field:field, value:value}]}, field+' updated');
  // Re-open editor on same node after refresh
  const node=(_dmErJson&&_dmErJson.nodes||[]).find(n=>n.id===tableName);
  if(node)setTimeout(()=>dmOpenTableEditor(node),200);
}

async function dmERDelCol(tableName, colName){
  if(!confirm('Delete column "'+colName+'" from '+tableName+'?'))return;
  await _dmEdit({column_removes:[{table_name:tableName, column_name:colName}]}, 'Column deleted');
  const node=(_dmErJson&&_dmErJson.nodes||[]).find(n=>n.id===tableName);
  if(node)setTimeout(()=>dmOpenTableEditor(node),200);
}

function dmERAddColumn(tableName, nodeType){
  const panel=document.getElementById('dmTableEditorPanel');
  if(!panel)return;
  // Inject add-column inline form
  const existingAdd=document.getElementById('dmERAddColForm');
  if(existingAdd)existingAdd.remove();
  const div=document.createElement('div');
  div.id='dmERAddColForm';
  div.style.cssText='background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:8px;margin-top:8px;';
  div.innerHTML='<div style="font-size:10px;font-weight:700;margin-bottom:4px;">New Column</div>'+
    '<div style="display:flex;gap:4px;flex-wrap:wrap;align-items:end;">'+
    '<div><label style="font-size:9px;">Name</label><input class="inp" id="dmERNewColName" placeholder="column_name" style="font-size:10px;width:100px;"></div>'+
    '<div><label style="font-size:9px;">Type</label><select class="inp" id="dmERNewColType" style="font-size:10px;width:100px;">'+_dmTypeOptions('')+'</select></div>'+
    '<label style="font-size:9px;"><input type="checkbox" id="dmERNewColPK"> PK</label>'+
    '<label style="font-size:9px;"><input type="checkbox" id="dmERNewColUQ"> UQ</label>'+
    '<label style="font-size:9px;"><input type="checkbox" id="dmERNewColNull" checked> Null</label>'+
    '<div><label style="font-size:9px;">Comment</label><input class="inp" id="dmERNewColComment" placeholder="" style="font-size:9px;width:80px;"></div>'+
    '<button class="btn btn-primary btn-xs" onclick="dmERSaveNewCol(\\''+_escJs(tableName)+'\\')">Add</button>'+
    '<button class="btn btn-ghost btn-xs" onclick="document.getElementById(\\'dmERAddColForm\\').remove()">Cancel</button>'+
    '</div>';
  panel.appendChild(div);
  document.getElementById('dmERNewColName').focus();
}

async function dmERSaveNewCol(tableName){
  const name=document.getElementById('dmERNewColName').value.trim();
  if(!name){toast('Column name required','terr');return;}
  const col={
    name:name,
    data_type:document.getElementById('dmERNewColType').value,
    is_pk:document.getElementById('dmERNewColPK').checked,
    is_unique:document.getElementById('dmERNewColUQ').checked,
    is_nullable:document.getElementById('dmERNewColNull').checked,
    comment:document.getElementById('dmERNewColComment').value.trim()
  };
  await _dmEdit({column_adds:[{table_name:tableName, column:col}]}, 'Column "'+name+'" added');
  const node=(_dmErJson&&_dmErJson.nodes||[]).find(n=>n.id===tableName);
  if(node)setTimeout(()=>dmOpenTableEditor(node),200);
}

async function dmSaveTableComment(tableName, comment){
  await _dmEdit({table_comments:[{table_name:tableName, comment:comment}]}, 'Comment saved');
}

async function dmSaveViewDef(viewName, definition){
  await _dmEdit({view_edits:[{view_name:viewName, definition:definition}]}, 'View definition saved');
}

// ═══════════════════════════════════════════════════════════════════════════════
// FK ASSIGNMENT — Set foreign key reference on a column
// ═══════════════════════════════════════════════════════════════════════════════
function dmERSetFK(tableName, colName, currentFK){
  if(!_dmModel)return;
  const allTables=[...(_dmModel.facts||[]),...(_dmModel.dimensions||[])].map(t=>t.table_name).filter(t=>t!==tableName);
  const opts=allTables.map(t=>'<option value="'+t+'"'+(t===currentFK?' selected':'')+'>'+t+'</option>').join('');
  const div=document.createElement('div');
  div.id='dmFKSetterDlg';
  div.style.cssText='position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);z-index:99999;background:var(--bg1,#fff);border:2px solid #6366F1;border-radius:12px;padding:16px;width:320px;box-shadow:0 20px 60px rgba(0,0,0,.3);';
  div.innerHTML='<div style="font-weight:700;font-size:13px;margin-bottom:10px;color:#6366F1;">\\ud83d\\udd17 Set Foreign Key Reference</div>'+
    '<div style="font-size:11px;margin-bottom:8px;color:var(--t2);">Column: <b>'+tableName+'.'+colName+'</b></div>'+
    '<div style="margin-bottom:8px;"><label style="font-size:10px;font-weight:600;">References Table:</label>'+
    '<select class="inp" id="dmFKTarget" style="width:100%;font-size:11px;"><option value="">\\u2014 None (remove FK) \\u2014</option>'+opts+'</select></div>'+
    '<div style="display:flex;gap:6px;"><button class="btn btn-primary btn-xs" onclick="dmERSaveFK(\\''+_escJs(tableName)+'\\',\\''+_escJs(colName)+'\\')">Save</button>'+
    '<button class="btn btn-ghost btn-xs" onclick="document.getElementById(\\'dmFKSetterDlg\\').remove()">Cancel</button></div>';
  const old=document.getElementById('dmFKSetterDlg');if(old)old.remove();
  document.body.appendChild(div);
}

async function dmERSaveFK(tableName, colName){
  const target=document.getElementById('dmFKTarget').value;
  document.getElementById('dmFKSetterDlg').remove();
  const edits={column_edits:[{table_name:tableName, column_name:colName, field:'fk_table', value:target}]};
  // Also add/update relationship if target is set
  if(target){
    edits.relationship_adds=[{from:tableName,to:target,type:'many-to-one',via_column:colName}];
  }
  await _dmEdit(edits, target?'FK set: '+colName+' -> '+target:'FK removed');
  const node=(_dmErJson&&_dmErJson.nodes||[]).find(n=>n.id===tableName);
  if(node)setTimeout(()=>dmOpenTableEditor(node),200);
}

// ═══════════════════════════════════════════════════════════════════════════════
// ADD RELATIONSHIP FROM ER (from a specific table)
// ═══════════════════════════════════════════════════════════════════════════════
function dmERAddRelFrom(tableName){
  if(!_dmModel)return;
  const allTables=[...(_dmModel.facts||[]),...(_dmModel.dimensions||[])].map(t=>t.table_name).filter(t=>t!==tableName);
  const fromCols=[...(_dmModel.facts||[]),...(_dmModel.dimensions||[])].find(t=>t.table_name===tableName);
  const colOpts=(fromCols?.columns||[]).map(c=>'<option value="'+c.name+'">'+c.name+'</option>').join('');
  const toOpts=allTables.map(t=>'<option value="'+t+'">'+t+'</option>').join('');

  const div=document.createElement('div');
  div.id='dmAddRelFromDlg';
  div.style.cssText='position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);z-index:99999;background:var(--bg1,#fff);border:2px solid #10B981;border-radius:12px;padding:16px;width:380px;box-shadow:0 20px 60px rgba(0,0,0,.3);';
  div.innerHTML='<div style="font-weight:700;font-size:13px;margin-bottom:10px;color:#10B981;">\\ud83d\\udd17 Create Relationship</div>'+
    '<div style="font-size:11px;margin-bottom:8px;color:var(--t2);">From: <b>'+tableName+'</b></div>'+
    '<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:8px;">'+
    '<div><label style="font-size:10px;font-weight:600;">FK Column</label><select class="inp" id="dmRelFKCol" style="width:100%;font-size:11px;">'+colOpts+'</select></div>'+
    '<div><label style="font-size:10px;font-weight:600;">To Table</label><select class="inp" id="dmRelToTbl" style="width:100%;font-size:11px;">'+toOpts+'</select></div>'+
    '</div>'+
    '<div style="margin-bottom:8px;"><label style="font-size:10px;font-weight:600;">Cardinality</label>'+
    '<select class="inp" id="dmRelType" style="width:100%;font-size:11px;"><option value="many-to-one">Many-to-One (*..1)</option><option value="one-to-many">One-to-Many (1..*)</option><option value="one-to-one">One-to-One (1..1)</option><option value="many-to-many">Many-to-Many (*..*)</option></select></div>'+
    '<div style="display:flex;gap:6px;"><button class="btn btn-primary btn-xs" onclick="dmERSaveRelFrom(\\''+_escJs(tableName)+'\\')">Create</button>'+
    '<button class="btn btn-ghost btn-xs" onclick="document.getElementById(\\'dmAddRelFromDlg\\').remove()">Cancel</button></div>';
  const old=document.getElementById('dmAddRelFromDlg');if(old)old.remove();
  document.body.appendChild(div);
}

async function dmERSaveRelFrom(tableName){
  const toTbl=document.getElementById('dmRelToTbl').value;
  const fkCol=document.getElementById('dmRelFKCol').value;
  const relType=document.getElementById('dmRelType').value;
  document.getElementById('dmAddRelFromDlg').remove();
  if(!toTbl){toast('Select a target table','terr');return;}
  await _dmEdit({
    relationship_adds:[{from:tableName,to:toTbl,type:relType,via_column:fkCol}],
    column_edits:[{table_name:tableName,column_name:fkCol,field:'fk_table',value:toTbl}]
  }, 'Relationship created: '+tableName+' -> '+toTbl);
}

// ═══════════════════════════════════════════════════════════════════════════════
// NODE CONTEXT MENU (right-click)
// ═══════════════════════════════════════════════════════════════════════════════
function _dmShowNodeContextMenu(node, ev){
  const old=document.getElementById('dmNodeCtxMenu');if(old)old.remove();
  const menu=document.createElement('div');
  menu.id='dmNodeCtxMenu';
  menu.style.cssText='position:fixed;left:'+ev.clientX+'px;top:'+ev.clientY+'px;z-index:99999;background:var(--bg1,#fff);border:1px solid var(--border);border-radius:10px;padding:6px 0;box-shadow:0 8px 30px rgba(0,0,0,.2);min-width:180px;';
  const items=[
    {icon:'\\u270E',label:'Edit Table',fn:()=>dmOpenTableEditor(node)},
    {icon:'\\ud83d\\udd17',label:'Add Relationship',fn:()=>dmERAddRelFrom(node.id)},
    {icon:'+',label:'Add Column',fn:()=>dmOpenTableEditor(node,'addcol')},
    {icon:'\\u2b50',label:'Toggle PK (first col)',fn:()=>{const c=(node.columns||[])[0];if(c)dmEREditCol(node.id,c.name,'is_pk',!c.is_pk);}},
    {icon:'\\ud83c\\udfaf',label:'Set Unique Key',fn:()=>dmOpenTableEditor(node)},
    {icon:'\\ud83d\\udcac',label:'Add Comment',fn:()=>dmOpenTableEditor(node)},
    {icon:'\\u2715',label:'Remove Table',fn:()=>dmRemoveTable(node.id),style:'color:#EF4444'},
  ];
  menu.innerHTML=items.map(it=>'<div style="padding:6px 14px;font-size:11px;cursor:pointer;display:flex;align-items:center;gap:8px;'+(it.style||'')+'" onmouseover="this.style.background=\\'var(--bg2)\\'" onmouseout="this.style.background=\\'\\'" onclick="this.parentElement.remove();('+_fnRef(it.fn)+')()"><span>'+it.icon+'</span><span>'+it.label+'</span></div>').join('');
  document.body.appendChild(menu);
  setTimeout(()=>{document.addEventListener('click',function _c(){menu.remove();document.removeEventListener('click',_c);},true);},50);
}

function _fnRef(fn){
  // Store fn reference and return a global callable
  if(!window._dmCtxFns)window._dmCtxFns=[];
  const idx=window._dmCtxFns.length;
  window._dmCtxFns.push(fn);
  return 'window._dmCtxFns['+idx+']';
}

// ═══════════════════════════════════════════════════════════════════════════════
// VIEWS — Load from Databricks / Create new
// ═══════════════════════════════════════════════════════════════════════════════
async function dmLoadViews(){
  const cat=G('dmCatalog').value, sch=G('dmSchema').value;
  if(!cat||!sch){toast('Select catalog & schema first','terr');return;}
  toast('Loading views...','tok');
  try{
    const r=await fetch('/api/v1/datamodel/views',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({catalog:cat,schema:sch})});
    const d=await r.json();
    if(d.success){
      const views=d.views||[];
      if(!views.length){toast('No views found in '+cat+'.'+sch,'tok');return;}
      // Add views to model
      const adds=views.map(v=>({view_name:v.view_name,definition:v.definition||'',comment:'',columns:[]}));
      await _dmEdit({view_adds:adds},'Loaded '+views.length+' view(s) from Databricks');
    }else{toast(d.error||'Failed to load views','terr');}
  }catch(e){toast('Error: '+e.message,'terr');}
}

function dmShowCreateViewDialog(){
  if(!_dmModel){toast('Generate a model first','terr');return;}
  const old=document.getElementById('dmCreateViewDlg');if(old)old.remove();
  const allTables=[...(_dmModel.facts||[]),...(_dmModel.dimensions||[])].map(t=>t.table_name);
  const div=document.createElement('div');
  div.id='dmCreateViewDlg';
  div.style.cssText='position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);z-index:99999;background:var(--bg1,#fff);border:2px solid #F59E0B;border-radius:12px;padding:20px;width:480px;max-height:80vh;overflow-y:auto;box-shadow:0 20px 60px rgba(0,0,0,.3);';
  div.innerHTML='<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;">'+
    '<span style="font-weight:700;font-size:14px;color:#F59E0B;">\\ud83d\\udc41 Create View</span>'+
    '<button class="btn btn-ghost btn-xs" onclick="document.getElementById(\\'dmCreateViewDlg\\').remove()" style="font-size:14px;">\\u2715</button></div>'+
    '<div style="margin-bottom:8px;"><label style="font-size:10px;font-weight:600;">View Name</label>'+
    '<input class="inp" id="dmNewViewName" placeholder="v_my_view" style="width:100%;font-size:11px;"></div>'+
    '<div style="margin-bottom:8px;"><label style="font-size:10px;font-weight:600;">SQL Definition</label>'+
    '<textarea class="inp" id="dmNewViewSQL" rows="6" style="width:100%;font-size:11px;font-family:monospace;" placeholder="SELECT col1, col2\\nFROM table1\\nJOIN table2 ON ..."></textarea></div>'+
    '<div style="margin-bottom:8px;"><label style="font-size:10px;font-weight:600;">Comment (optional)</label>'+
    '<input class="inp" id="dmNewViewComment" placeholder="Description..." style="width:100%;font-size:11px;"></div>'+
    '<div style="display:flex;gap:8px;">'+
    '<button class="btn btn-primary btn-sm" onclick="dmSaveNewView()" style="background:#F59E0B;border-color:#F59E0B;flex:1;">Create View</button>'+
    '<button class="btn btn-ghost btn-sm" onclick="document.getElementById(\\'dmCreateViewDlg\\').remove()">Cancel</button></div>';
  document.body.appendChild(div);
}

async function dmSaveNewView(){
  const name=document.getElementById('dmNewViewName').value.trim();
  const sql=document.getElementById('dmNewViewSQL').value.trim();
  const comment=document.getElementById('dmNewViewComment').value.trim();
  if(!name){toast('View name required','terr');return;}
  if(!sql){toast('SQL definition required','terr');return;}
  await _dmEdit({view_adds:[{view_name:name,definition:sql,comment:comment,columns:[]}]},'View "'+name+'" created');
  document.getElementById('dmCreateViewDlg').remove();
}

async function dmRemoveView(viewName){
  if(!confirm('Remove view "'+viewName+'" from model?'))return;
  await _dmEdit({view_removes:[{view_name:viewName}]},'View removed');
}

'''

content = content[:pos_start] + NEW_RENDER_ER + content[pos_end:]

print("PATCH 1 applied: dmRenderER + table editor + views + FK/UQ support")

# ═══════════════════════════════════════════════════════════════════════════════
# Save
# ═══════════════════════════════════════════════════════════════════════════════
with open(JS_PATH, 'w', encoding='utf-8') as f:
    f.write(content)

print(f"File saved: {len(content)} bytes")
print("SUCCESS")
