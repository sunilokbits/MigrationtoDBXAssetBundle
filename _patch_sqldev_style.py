"""Patch: Make ER diagram match Oracle SQL Developer Data Modeler look and feel."""
import os

JS_PATH = r'C:\Live_MigrationProject\Databrciks_Poc\Poc\MigrationtoDBXAssetBundle\migration_utility\static\main.js'

with open(JS_PATH, 'r', encoding='utf-8') as f:
    content = f.read()

print(f"File size: {len(content)} bytes")

# ═══════════════════════════════════════════════════════════════════════════════
# PATCH 1: Replace dmRenderER with SQL Developer style nodes
# ═══════════════════════════════════════════════════════════════════════════════
OLD_START = "function dmRenderER(er){"
OLD_END = "// \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\n// INTERACTIVE TABLE EDITOR"

pos_start = content.find(OLD_START)
pos_end = content.find(OLD_END)
if pos_start == -1 or pos_end == -1:
    print(f"ERROR: markers not found. start={pos_start}, end={pos_end}")
    exit(1)

NEW_CODE = r'''function dmRenderER(er){
  const g=G('dmErGroup');
  g.innerHTML='';
  if(!er||!er.nodes) return;
  const svg=G('dmErSvg');
  _dmZoomLevel=1; _dmPanX=0; _dmPanY=0;
  g.setAttribute('transform','translate(0,0) scale(1)');

  const facts=er.nodes.filter(n=>n.type==='fact');
  const dims=er.nodes.filter(n=>n.type==='dimension');
  const views=er.nodes.filter(n=>n.type==='view');
  const W=Math.max(svg.clientWidth||1200,1000);
  const H=Math.max(750, (facts.length+dims.length+views.length)*70);
  svg.setAttribute('height', H);

  // Position facts horizontally centered
  const factW=260, factGap=60;
  const totalFactW=facts.length*factW+(facts.length-1)*factGap;
  let fx=Math.max(40,(W-totalFactW)/2);
  facts.forEach((n,i)=>{if(n.x===undefined||n._autoLayout){n.x=fx+i*(factW+factGap);n.y=H/2-100;n._autoLayout=true;}});

  // Position dims spread around facts
  const cx=W/2, cy=H/2-20;
  const rx=Math.min(W/2.2,500), ry=Math.min(H/2.4,280);
  dims.forEach((n,i)=>{if(n.x===undefined||n._autoLayout){
    const angle=Math.PI + Math.PI*(i+1)/(dims.length+1);
    n.x=cx+Math.cos(angle)*rx-130;n.y=cy+Math.sin(angle)*ry-40;n._autoLayout=true;
  }});

  // Position views below
  views.forEach((n,i)=>{if(n.x===undefined||n._autoLayout){n.x=60+i*300;n.y=H-150;n._autoLayout=true;}});

  // Draw edges first (behind nodes)
  _dmDrawEdges(er, g);

  // Draw nodes — SQL Developer style
  er.nodes.forEach(n=>{
    const isFact=n.type==='fact';
    const isView=n.type==='view';
    const ng=document.createElementNS('http://www.w3.org/2000/svg','g');
    ng.setAttribute('transform','translate('+n.x+','+n.y+')');
    ng.setAttribute('data-node-id', n.id);
    ng.style.cursor='move';

    const cols=n.columns||[];
    // Compute FK constraints for bottom section
    const fkConstraints=cols.filter(c=>c.fk_table).map(c=>({col:c.name,ref:c.fk_table}));
    const pkCols=cols.filter(c=>c.is_pk);

    const nodeW=260;
    const rowH=16;
    const hdrH=24;
    const colSectionH=cols.length*rowH+4;
    // Constraints section (PK + FK listed at bottom)
    const constraintLines=[];
    if(pkCols.length)constraintLines.push({icon:'P>',text:n.label+'_PK ('+pkCols.map(c=>c.name).join(', ')+')',color:'#7C3AED'});
    fkConstraints.forEach(fk=>{constraintLines.push({icon:'F>',text:n.label+'_'+fk.ref+'_FK ('+fk.col+')',color:'#059669'});});
    const constraintSectionH=constraintLines.length?(constraintLines.length*14+8):0;
    const boxH=hdrH+colSectionH+constraintSectionH+6;

    // Drop shadow
    const shadow=document.createElementNS('http://www.w3.org/2000/svg','rect');
    shadow.setAttribute('x','2');shadow.setAttribute('y','2');
    shadow.setAttribute('width',nodeW);shadow.setAttribute('height',boxH);
    shadow.setAttribute('rx','3');shadow.setAttribute('fill','rgba(0,0,0,0.1)');
    ng.appendChild(shadow);

    // Main box — white background with colored border
    const borderColor=isView?'#D97706':isFact?'#1D4ED8':'#047857';
    const rect=document.createElementNS('http://www.w3.org/2000/svg','rect');
    rect.setAttribute('width',nodeW);rect.setAttribute('height',boxH);rect.setAttribute('rx','3');
    rect.setAttribute('fill','#FFFFFF');
    rect.setAttribute('stroke',borderColor);rect.setAttribute('stroke-width','1.5');
    ng.appendChild(rect);

    // Header — colored background (SQL Developer style: teal for dims, blue for facts, orange for views)
    const hdrBg=isView?'#FEF3C7':isFact?'#DBEAFE':'#D1FAE5';
    const hdr=document.createElementNS('http://www.w3.org/2000/svg','rect');
    hdr.setAttribute('width',nodeW);hdr.setAttribute('height',hdrH);hdr.setAttribute('rx','3');
    hdr.setAttribute('fill',hdrBg);
    ng.appendChild(hdr);
    // Bottom corners square
    const hdr2=document.createElementNS('http://www.w3.org/2000/svg','rect');
    hdr2.setAttribute('y',String(hdrH-3));hdr2.setAttribute('width',nodeW);hdr2.setAttribute('height','3');
    hdr2.setAttribute('fill',hdrBg);ng.appendChild(hdr2);

    // Header separator line
    const hdrLine=document.createElementNS('http://www.w3.org/2000/svg','line');
    hdrLine.setAttribute('x1','0');hdrLine.setAttribute('y1',hdrH);hdrLine.setAttribute('x2',nodeW);hdrLine.setAttribute('y2',hdrH);
    hdrLine.setAttribute('stroke',borderColor);hdrLine.setAttribute('stroke-width','1');
    ng.appendChild(hdrLine);

    // Role badge (D for Dimension, F for Fact, V for View)
    const roleBadge=document.createElementNS('http://www.w3.org/2000/svg','text');
    roleBadge.setAttribute('x','8');roleBadge.setAttribute('y',String(hdrH-7));
    roleBadge.setAttribute('fill',borderColor);roleBadge.setAttribute('font-size','11');
    roleBadge.setAttribute('font-weight','900');roleBadge.setAttribute('font-family','Consolas, monospace');
    roleBadge.textContent=isView?'V':isFact?'F':'D';
    ng.appendChild(roleBadge);

    // Table name
    const title=document.createElementNS('http://www.w3.org/2000/svg','text');
    title.setAttribute('x','24');title.setAttribute('y',String(hdrH-7));title.setAttribute('text-anchor','start');
    title.setAttribute('fill','#1E293B');title.setAttribute('font-size','11');title.setAttribute('font-weight','700');
    title.setAttribute('font-family','Segoe UI, system-ui, sans-serif');
    title.textContent=n.label;ng.appendChild(title);

    // Edit icon (pencil) — top right
    const editIcon=document.createElementNS('http://www.w3.org/2000/svg','text');
    editIcon.setAttribute('x',String(nodeW-10));editIcon.setAttribute('y',String(hdrH-7));editIcon.setAttribute('text-anchor','end');
    editIcon.setAttribute('fill',borderColor);editIcon.setAttribute('font-size','10');
    editIcon.setAttribute('cursor','pointer');editIcon.textContent='\u25bc';
    editIcon.addEventListener('click',ev=>{ev.stopPropagation();dmOpenTableEditor(n);});
    ng.appendChild(editIcon);

    // Columns section
    cols.forEach((c,ci)=>{
      const cy2=hdrH+6+ci*rowH;

      // Key indicators: P=PK, F=FK, *=NOT NULL, U=Unique
      let keyStr='';
      if(c.is_pk)keyStr+='P';
      if(c.fk_table)keyStr+='F';
      if(!c.is_nullable&&!c.is_pk)keyStr+='*';
      if(c.is_unique)keyStr+='U';

      // Key indicator text (fixed-width left column)
      const keyTxt=document.createElementNS('http://www.w3.org/2000/svg','text');
      keyTxt.setAttribute('x','6');keyTxt.setAttribute('y',String(cy2+11));
      keyTxt.setAttribute('fill',c.is_pk?'#7C3AED':c.fk_table?'#059669':'#64748B');
      keyTxt.setAttribute('font-size','9');keyTxt.setAttribute('font-family','Consolas, monospace');
      keyTxt.setAttribute('font-weight','700');
      keyTxt.textContent=keyStr;
      ng.appendChild(keyTxt);

      // Mandatory dot (asterisk indicator)
      if(!c.is_nullable){
        const mand=document.createElementNS('http://www.w3.org/2000/svg','text');
        mand.setAttribute('x','28');mand.setAttribute('y',String(cy2+11));
        mand.setAttribute('fill','#DC2626');mand.setAttribute('font-size','12');
        mand.setAttribute('font-family','Consolas, monospace');
        mand.textContent='*';
        ng.appendChild(mand);
      }

      // Column name
      const ct=document.createElementNS('http://www.w3.org/2000/svg','text');
      ct.setAttribute('x','36');ct.setAttribute('y',String(cy2+11));ct.setAttribute('fill','#1E293B');
      ct.setAttribute('font-size','10');ct.setAttribute('font-family','Consolas, monospace');
      ct.setAttribute('font-weight',c.is_pk?'700':'400');
      ct.textContent=c.name;
      ng.appendChild(ct);

      // Data type (right-aligned)
      const tt=document.createElementNS('http://www.w3.org/2000/svg','text');
      tt.setAttribute('x',String(nodeW-8));tt.setAttribute('y',String(cy2+11));tt.setAttribute('text-anchor','end');
      tt.setAttribute('fill','#6366F1');tt.setAttribute('font-size','9');tt.setAttribute('font-family','Consolas, monospace');
      tt.textContent=c.data_type||'VARCHAR';
      ng.appendChild(tt);

      // Comment tooltip
      if(c.comment){
        const ttip=document.createElementNS('http://www.w3.org/2000/svg','title');
        ttip.textContent=c.comment;
        ct.appendChild(ttip);
      }

      // Row separator
      if(ci<cols.length-1){
        const rowSep=document.createElementNS('http://www.w3.org/2000/svg','line');
        rowSep.setAttribute('x1','4');rowSep.setAttribute('y1',String(cy2+rowH-1));
        rowSep.setAttribute('x2',String(nodeW-4));rowSep.setAttribute('y2',String(cy2+rowH-1));
        rowSep.setAttribute('stroke','#E2E8F0');rowSep.setAttribute('stroke-width','0.5');
        ng.appendChild(rowSep);
      }
    });

    // Constraints section separator
    if(constraintLines.length){
      const cSepY=hdrH+colSectionH+2;
      const cSep=document.createElementNS('http://www.w3.org/2000/svg','line');
      cSep.setAttribute('x1','0');cSep.setAttribute('y1',String(cSepY));
      cSep.setAttribute('x2',nodeW);cSep.setAttribute('y2',String(cSepY));
      cSep.setAttribute('stroke',borderColor);cSep.setAttribute('stroke-width','0.8');
      cSep.setAttribute('stroke-dasharray','3,2');
      ng.appendChild(cSep);

      constraintLines.forEach((cl,cli)=>{
        const clY=cSepY+6+cli*14;
        const clIcon=document.createElementNS('http://www.w3.org/2000/svg','text');
        clIcon.setAttribute('x','6');clIcon.setAttribute('y',String(clY+10));
        clIcon.setAttribute('fill',cl.color);clIcon.setAttribute('font-size','9');
        clIcon.setAttribute('font-family','Consolas, monospace');clIcon.setAttribute('font-weight','700');
        clIcon.textContent=cl.icon;
        ng.appendChild(clIcon);

        const clTxt=document.createElementNS('http://www.w3.org/2000/svg','text');
        clTxt.setAttribute('x','24');clTxt.setAttribute('y',String(clY+10));
        clTxt.setAttribute('fill',cl.color);clTxt.setAttribute('font-size','9');
        clTxt.setAttribute('font-family','Consolas, monospace');
        clTxt.textContent=cl.text.length>34?cl.text.substring(0,34)+'...':cl.text;
        ng.appendChild(clTxt);
      });
    }

    // Drag support
    let dragging=false, ddx=0, ddy=0;
    ng.addEventListener('mousedown',ev=>{
      if(ev.button!==0)return;
      if(ev.target===editIcon)return;
      dragging=true;ddx=ev.clientX/_dmZoomLevel-n.x;ddy=ev.clientY/_dmZoomLevel-n.y;
      ev.preventDefault();ev.stopPropagation();
      svg.style.cursor='grabbing';
      n._autoLayout=false;
    });
    const onMove=ev=>{
      if(!dragging)return;
      n.x=ev.clientX/_dmZoomLevel-ddx;
      n.y=ev.clientY/_dmZoomLevel-ddy;
      ng.setAttribute('transform','translate('+n.x+','+n.y+')');
      _dmDrawEdges(er, g);
    };
    const onUp=()=>{if(dragging){dragging=false;svg.style.cursor='grab';}};
    svg.addEventListener('mousemove',onMove);
    svg.addEventListener('mouseup',onUp);
    svg.addEventListener('mouseleave',onUp);

    // Double-click to open editor
    ng.addEventListener('dblclick',ev=>{ev.stopPropagation();dmOpenTableEditor(n);});

    // Right-click context menu
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
    _dmZoomLevel=Math.max(0.2,Math.min(4,_dmZoomLevel*factor));
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

'''

content = content[:pos_start] + NEW_CODE + content[pos_end:]
print("PATCH 1 applied: SQL Developer-style node rendering")

# ═══════════════════════════════════════════════════════════════════════════════
# PATCH 2: Replace _dmDrawEdges with SQL Developer-style straight-line arrows
# ═══════════════════════════════════════════════════════════════════════════════
OLD_EDGES = "function _dmDrawEdges(er, g){"
OLD_EDGES_END = "function _dmCrowsFootLabel(type){"

pos_e_start = content.find(OLD_EDGES)
pos_e_end = content.find(OLD_EDGES_END)
if pos_e_start == -1 or pos_e_end == -1:
    print(f"ERROR: _dmDrawEdges markers not found. start={pos_e_start}, end={pos_e_end}")
    exit(1)

NEW_EDGES = r'''function _dmDrawEdges(er, g){
  // Remove old edge elements
  g.querySelectorAll('.dm-edge-el').forEach(el=>el.remove());

  const firstNodeG=g.querySelector('g[data-node-id]');
  const nodeW=260;

  er.edges.forEach((e,ei)=>{
    const from=er.nodes.find(n=>n.id===e.from);
    const to=er.nodes.find(n=>n.id===e.to);
    if(!from||!to)return;

    // Calculate connection points on nearest edges of boxes (SQL Developer style)
    const fromCols=from.columns||[];
    const toCols=to.columns||[];
    const fromH=24+fromCols.length*16+10;
    const toH=24+toCols.length*16+10;

    // Find best connection side (left/right/top/bottom of each box)
    const fromCx=from.x+nodeW/2, fromCy=from.y+fromH/2;
    const toCx=to.x+nodeW/2, toCy=to.y+toH/2;
    const dx=toCx-fromCx, dy=toCy-fromCy;

    let x1,y1,x2,y2;
    // Determine connection points based on relative position
    if(Math.abs(dx)>Math.abs(dy)){
      // Connect horizontally (left/right sides)
      if(dx>0){
        x1=from.x+nodeW; y1=fromCy;
        x2=to.x; y2=toCy;
      } else {
        x1=from.x; y1=fromCy;
        x2=to.x+nodeW; y2=toCy;
      }
    } else {
      // Connect vertically (top/bottom)
      if(dy>0){
        x1=fromCx; y1=from.y+fromH;
        x2=toCx; y2=to.y;
      } else {
        x1=fromCx; y1=from.y;
        x2=toCx; y2=to.y+toH;
      }
    }

    // Draw orthogonal line path (like SQL Developer — right-angle connectors)
    const mx=(x1+x2)/2, my=(y1+y2)/2;
    let pathD;
    if(Math.abs(dx)>Math.abs(dy)){
      // Horizontal dominant: go horizontal, then vertical, then horizontal
      pathD='M '+x1+' '+y1+' L '+mx+' '+y1+' L '+mx+' '+y2+' L '+x2+' '+y2;
    } else {
      // Vertical dominant: go vertical, then horizontal, then vertical
      pathD='M '+x1+' '+y1+' L '+x1+' '+my+' L '+x2+' '+my+' L '+x2+' '+y2;
    }

    const path=document.createElementNS('http://www.w3.org/2000/svg','path');
    path.classList.add('dm-edge-el');
    path.setAttribute('d', pathD);
    path.setAttribute('fill','none');
    path.setAttribute('stroke','#475569');
    path.setAttribute('stroke-width','1.5');
    path.setAttribute('stroke-dasharray', e.label==='many-to-many'?'6,3':'none');

    // Markers — crow's foot style (matching SQL Developer)
    const relType=e.label||'many-to-one';
    if(_dmNotation==='crowsfoot'){
      if(relType.includes('many-to-one')){
        path.setAttribute('marker-start','url(#dmCrowManyFill)');
        path.setAttribute('marker-end','url(#dmCrowOne)');
      } else if(relType.includes('one-to-many')){
        path.setAttribute('marker-start','url(#dmCrowOneFill)');
        path.setAttribute('marker-end','url(#dmCrowMany)');
      } else if(relType.includes('one-to-one')){
        path.setAttribute('marker-start','url(#dmCrowOneFill)');
        path.setAttribute('marker-end','url(#dmCrowOne)');
      } else if(relType.includes('many-to-many')){
        path.setAttribute('marker-start','url(#dmCrowManyFill)');
        path.setAttribute('marker-end','url(#dmCrowMany)');
      }
    } else {
      path.setAttribute('marker-end','url(#dmArrow)');
    }

    // Click to edit
    path.style.cursor='pointer';
    path.addEventListener('click',ev=>{
      ev.stopPropagation();
      _dmShowEdgeEditor(e, ei, mx, my);
    });

    // Hover effect
    path.addEventListener('mouseover',()=>{path.setAttribute('stroke','#2563EB');path.setAttribute('stroke-width','2.5');});
    path.addEventListener('mouseout',()=>{path.setAttribute('stroke','#475569');path.setAttribute('stroke-width','1.5');});

    if(firstNodeG) g.insertBefore(path,firstNodeG); else g.appendChild(path);

    // Relationship label near midpoint
    const lblX=(x1+x2)/2+10, lblY=(y1+y2)/2-8;
    const lbl=document.createElementNS('http://www.w3.org/2000/svg','text');
    lbl.classList.add('dm-edge-el');
    lbl.setAttribute('x',lblX);lbl.setAttribute('y',lblY);
    lbl.setAttribute('text-anchor','middle');
    lbl.setAttribute('fill','#6366F1');lbl.setAttribute('font-size','8');
    lbl.setAttribute('font-family','Consolas, monospace');lbl.setAttribute('font-weight','600');
    const relLabel=_dmNotation==='crowsfoot'?_dmCrowsFootLabel(e.label):e.label;
    lbl.textContent=relLabel||'';
    if(firstNodeG) g.insertBefore(lbl,firstNodeG); else g.appendChild(lbl);

    // FK column label (shows which column is the FK)
    if(e.via_column){
      const fkLbl=document.createElementNS('http://www.w3.org/2000/svg','text');
      fkLbl.classList.add('dm-edge-el');
      fkLbl.setAttribute('x',String(x1+(dx>0?15:-15)));fkLbl.setAttribute('y',String(y1-5));
      fkLbl.setAttribute('text-anchor',dx>0?'start':'end');
      fkLbl.setAttribute('fill','#059669');fkLbl.setAttribute('font-size','8');
      fkLbl.setAttribute('font-family','Consolas, monospace');
      fkLbl.textContent=e.via_column;
      if(firstNodeG) g.insertBefore(fkLbl,firstNodeG); else g.appendChild(fkLbl);
    }
  });
}

'''

content = content[:pos_e_start] + NEW_EDGES + content[pos_e_end:]
print("PATCH 2 applied: SQL Developer-style orthogonal connectors")

# ═══════════════════════════════════════════════════════════════════════════════
# Save
# ═══════════════════════════════════════════════════════════════════════════════
with open(JS_PATH, 'w', encoding='utf-8') as f:
    f.write(content)

print(f"Final file size: {len(content)} bytes")
print("SUCCESS")
