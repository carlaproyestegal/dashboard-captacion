#!/usr/bin/env python3
"""
generar_dashboard.py — Estudios Rafer
Dashboard Marketing Captación

Uso: python generar_dashboard.py
Requisitos: pip install pandas openpyxl
"""

import os, sys, json, base64, re, urllib.request
from datetime import datetime
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("❌ Falta pandas. Ejecuta: pip install pandas openpyxl")
    sys.exit(1)

# ── CONFIG ────────────────────────────────────────────────────────────────────
BASE        = Path(__file__).parent
DIR_SEG     = BASE / "datos" / "seguimiento"
DIR_INV     = BASE / "datos" / "inversion"
DIR_ASSETS  = BASE / "assets"
DIR_OUT     = BASE / "output"
LOGO_PATH   = DIR_ASSETS / "Logo-Rafer.png"

ETAPAS_ORDEN  = ['BD','Solicitud','Contactado','Interesado','Inscrito',
                 'No interesado','No cumple','Extrarradio']
ETAPAS_FUNNEL = ['BD','Solicitud','Contactado','Interesado','Inscrito']
ETAPAS_NEG    = ['No interesado','No cumple','Extrarradio']

# ── HELPERS ───────────────────────────────────────────────────────────────────
def fecha_de_nombre(nombre):
    m = re.search(r'(\d{2})_(\d{2})_(\d{4})', nombre)
    if m:
        d,mo,y = m.groups()
        try: return datetime(int(y),int(mo),int(d))
        except: return None
    return None

def nombre_corto(nombre):
    # Nombre completo tal como aparece en 'Equipo de ventas'
    return str(nombre).strip()

# ── CARGA SEGUIMIENTO ─────────────────────────────────────────────────────────
def cargar_seguimiento():
    archivos = sorted(DIR_SEG.glob("seguimiento_*.xlsx"))
    if not archivos:
        print(f"❌ No hay archivos en {DIR_SEG}")
        sys.exit(1)

    dfs = []
    for f in archivos:
        fecha = fecha_de_nombre(f.name)
        if not fecha:
            print(f"  ⚠️  Ignorado: {f.name} (nombre incorrecto)")
            continue
        try:
            df = pd.read_excel(f, sheet_name='Query actualización captación A')
        except Exception as e:
            try:
                df = pd.read_excel(f)
            except Exception as e2:
                print(f"  ⚠️  Error leyendo {f.name}: {e2}")
                continue

        rename = {
            'Fecha actualización':'fecha_act_raw','Source.Name.2':'campana',
            'Etapa':'etapa','Equipo de ventas':'curso','Comercial':'comercial',
            'Etapa equivalente':'etapa_equiv','Marketing':'es_mkt','Total':'total'
        }
        df = df.rename(columns={k:v for k,v in rename.items() if k in df.columns})

        # Eliminar PII
        pii = ['Alumno','Oportunidad','Teléfono','Correo electrónico','Notas','Creado el']
        df = df.drop(columns=[c for c in pii if c in df.columns], errors='ignore')

        df['fecha'] = fecha.strftime('%Y-%m-%d')
        df['es_mkt'] = pd.to_numeric(df.get('es_mkt', 0), errors='coerce').fillna(0).astype(int)
        # Siempre desde 'Etapa equivalente', nunca desde 'Etapa'
        if 'etapa_equiv' not in df.columns:
            print(f'  ⚠️  Columna Etapa equivalente no encontrada en {f.name}')
            df['etapa_equiv'] = ''
        df['etapa_equiv'] = df['etapa_equiv'].fillna('').astype(str).str.strip()
        df['etapa_equiv'] = df['etapa_equiv'].replace('INSCRITO PTE BONO', 'Inscrito')
        df['semana'] = pd.to_datetime(df['fecha']).dt.strftime('S%V-%G')
        df['mes']    = pd.to_datetime(df['fecha']).dt.strftime('%Y-%m')
        df['curso_short'] = df['curso'].apply(nombre_corto)
        df['campana'] = df.get('campana', pd.Series([''] * len(df))).fillna('').astype(str).str.strip()

        dfs.append(df)
        print(f"  ✓ {f.name}  →  {len(df)} registros  ({fecha.strftime('%d/%m/%Y')})")

    if not dfs:
        print("❌ Sin datos válidos de seguimiento")
        sys.exit(1)
    return pd.concat(dfs, ignore_index=True)

# ── CARGA INVERSIÓN ───────────────────────────────────────────────────────────
def cargar_inversion():
    archivos = sorted(DIR_INV.glob("inversion_*.xlsx"))
    validos  = [(f, fecha_de_nombre(f.name)) for f in archivos]
    validos  = [(f,d) for f,d in validos if d]
    if not validos:
        print("  ⚠️  Sin archivos de inversión")
        return pd.DataFrame(columns=['curso','importe'])

    ultimo = max(validos, key=lambda x: x[1])[0]
    try:
        df = pd.read_excel(ultimo)
        df.columns = [str(c).strip() for c in df.columns]
        df = df.rename(columns={'Equipo de ventas':'curso','Inversión marketing':'importe'})
        df = df[['curso','importe']].dropna(subset=['curso'])
        df['importe'] = pd.to_numeric(df['importe'], errors='coerce')
        print(f"  ✓ {ultimo.name}  →  inversión cargada")
        return df
    except Exception as e:
        print(f"  ⚠️  Error inversión: {e}")
        return pd.DataFrame(columns=['curso','importe'])

# ── CONSTRUIR JSON ────────────────────────────────────────────────────────────
def construir_json(df_all, df_inv):
    ultima_fecha = df_all['fecha'].max()
    df_cur = df_all[df_all['fecha'] == ultima_fecha]

    cursos      = sorted(df_cur['curso'].dropna().unique().tolist())
    comerciales = sorted(df_cur['comercial'].dropna().unique().tolist())
    semanas     = sorted(df_all['semana'].dropna().unique().tolist())
    meses       = sorted(df_all['mes'].dropna().unique().tolist())
    campanas    = sorted(df_all['campana'].dropna().unique().tolist())
    fechas      = sorted(df_all['fecha'].unique().tolist())
    curtos_map  = {c: nombre_corto(c) for c in cursos}

    # Inversión
    inv_map = {}
    if not df_inv.empty:
        for _, r in df_inv.iterrows():
            c = str(r['curso']).strip()
            v = r['importe']
            if pd.notna(v) and float(v) > 0:
                inv_map[c] = round(float(v), 2)

    cursos_sin_inv = [curtos_map.get(c,c) for c in cursos if c not in inv_map]

    inversion = [
        {'curso': c, 'short': curtos_map.get(c,c),
         'importe': inv_map.get(c, None),
         'disponible': c in inv_map}
        for c in cursos
    ]

    # Leads — solo campos necesarios, sin PII
    leads = []
    for _, r in df_all.iterrows():
        leads.append({
            'f':  str(r.get('fecha','')),
            'c':  str(r.get('curso','')),
            'cs': str(r.get('curso_short','')),
            'e':  str(r.get('etapa_equiv','')),
            'co': str(r.get('comercial','')),
            'm':  int(r.get('es_mkt', 0)),
            'se': str(r.get('semana','')),
            'me': str(r.get('mes','')),
            'ca': str(r.get('campana',''))
        })

    return {
        'meta': {
            'fecha_act':      ultima_fecha,
            'fecha_gen':      datetime.now().strftime('%d/%m/%Y %H:%M'),
            'n_leads':        int(len(df_cur)),
            'fechas':         fechas,
            'n_inv_ok':       len(cursos) - len(cursos_sin_inv),
            'n_cursos':       len(cursos),
            'sin_inv':        cursos_sin_inv
        },
        'filtros': {
            'cursos':     cursos,
            'shorts':     curtos_map,
            'comerciales': comerciales,
            'semanas':    semanas,
            'meses':      meses,
            'campanas':   campanas
        },
        'leads':     leads,
        'inversion': inversion
    }

# ── ACTIVOS ───────────────────────────────────────────────────────────────────
def logo_b64():
    if LOGO_PATH.exists():
        with open(LOGO_PATH,'rb') as f:
            return base64.b64encode(f.read()).decode()
    return ''

def fetch_chartjs():
    url = 'https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js'
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            js = r.read().decode('utf-8')
            print("  ✓ Chart.js descargado y embebido")
            return js
    except:
        print("  ⚠️  Sin internet — Chart.js cargará desde CDN al abrir el HTML")
        return None

def fetch_dmfont():
    url = 'https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&display=swap'
    try:
        req = urllib.request.Request(url, headers={'User-Agent':'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=10) as r:
            css = r.read().decode('utf-8')
            return f"<style>{css}</style>"
    except:
        return ""

# ── TEMPLATE HTML ─────────────────────────────────────────────────────────────
def html_template(data_json, logo, chartjs, fontcss):
    logo_src = f"data:image/png;base64,{logo}" if logo else ""
    chartjs_tag = (f"<script>{chartjs}</script>" if chartjs else
                   '<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>')

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Dashboard Captación — Estudios Rafer</title>
{fontcss}
{chartjs_tag}
<style>
/* ── RESET & BASE ── */
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
:root{{
  --blue:#29ABE2;--blue-light:#E8F6FD;--blue-mid:#B3DFF5;
  --gray:#6B7280;--gray-light:#F3F4F6;--gray-mid:#E5E7EB;--gray-dark:#111827;
  --green:#16A34A;--green-light:#DCFCE7;
  --orange:#EA580C;--orange-light:#FFF0E6;
  --red:#DC2626;--red-light:#FEE2E2;
  --white:#FFFFFF;
  --font:'DM Sans',-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif;
  --radius:10px;--radius-sm:6px;
  --shadow:0 1px 3px rgba(0,0,0,.08),0 1px 2px rgba(0,0,0,.04);
  --shadow-md:0 4px 12px rgba(0,0,0,.08);
  --sidebar-w:220px;--header-h:60px;--filter-h:52px;
  --top-offset:calc(var(--header-h) + var(--filter-h));
}}
html{{scroll-behavior:smooth;font-size:15px}}
body{{font-family:var(--font);color:var(--gray-dark);background:var(--gray-light);
  -webkit-font-smoothing:antialiased;}}

/* ── LAYOUT ── */
.layout{{display:flex;min-height:100vh}}
.sidebar{{
  width:var(--sidebar-w);min-width:var(--sidebar-w);
  background:var(--white);border-right:1px solid var(--gray-mid);
  position:fixed;top:var(--top-offset);bottom:0;overflow-y:auto;
  padding:12px 0;z-index:90;
}}
.main{{margin-left:var(--sidebar-w);flex:1;padding:28px;max-width:100%}}
@media(max-width:900px){{
  .sidebar{{transform:translateX(-100%);top:0;bottom:0;z-index:200;padding-top:72px}}
  .sidebar.open{{transform:translateX(0)}}
  .main{{margin-left:0;padding:16px}}
}}

/* ── HEADER ── */
.header{{
  position:fixed;top:0;left:0;right:0;height:var(--header-h);
  background:var(--white);border-bottom:1px solid var(--gray-mid);
  display:flex;align-items:center;padding:0 24px;gap:16px;
  z-index:100;box-shadow:var(--shadow);
}}
.header-logo{{height:32px;object-fit:contain}}
.header-logo-text{{font-family:var(--font);font-weight:700;font-size:1rem;color:var(--gray-dark)}}
.header-title{{font-size:.92rem;font-weight:600;color:var(--gray-dark);flex:1;white-space:nowrap}}
.header-meta{{display:flex;align-items:center;gap:8px;font-size:.78rem;color:var(--gray)}}
.badge{{padding:3px 10px;border-radius:20px;font-size:.72rem;font-weight:600;white-space:nowrap;font-family:var(--font)}}
.badge-ok{{background:var(--green-light);color:var(--green)}}
.badge-warn{{background:var(--orange-light);color:var(--orange)}}
.badge-info{{background:var(--blue-light);color:var(--blue)}}
.hamburger{{display:none;background:none;border:none;cursor:pointer;padding:6px;color:var(--gray-dark)}}
@media(max-width:900px){{
  .hamburger{{display:flex;align-items:center}}
  .header-title{{font-size:.82rem}}
  .header-meta .badge:not(:first-child){{display:none}}
}}
.overlay{{display:none;position:fixed;inset:0;background:rgba(0,0,0,.25);z-index:150}}
.overlay.active{{display:block}}

/* ── FILTER BAR ── */
.filterbar{{
  position:fixed;top:var(--header-h);left:0;right:0;height:var(--filter-h);
  background:var(--white);border-bottom:1px solid var(--gray-mid);
  display:flex;align-items:center;
  padding:0 24px 0 calc(var(--sidebar-w) + 24px);
  gap:12px;z-index:95;overflow-x:auto;
}}
@media(max-width:900px){{
  .filterbar{{padding:8px 16px;height:auto;flex-wrap:wrap;position:sticky;top:var(--header-h)}}
}}
.filter-group{{display:flex;flex-direction:column;gap:2px;min-width:110px}}
.filter-label{{font-size:.65rem;font-weight:700;color:var(--gray);text-transform:uppercase;
  letter-spacing:.6px;font-family:var(--font)}}
.filter-select{{
  border:1px solid var(--gray-mid);border-radius:var(--radius-sm);padding:4px 8px;
  font-family:var(--font);font-size:.78rem;color:var(--gray-dark);
  background:var(--white);cursor:pointer;outline:none;min-width:110px;
}}
.filter-select:focus{{border-color:var(--blue);box-shadow:0 0 0 3px var(--blue-light)}}
.btn-clear{{
  margin-left:auto;white-space:nowrap;border:1px solid var(--gray-mid);
  background:var(--white);border-radius:var(--radius-sm);padding:5px 14px;
  font-family:var(--font);font-size:.75rem;color:var(--gray);cursor:pointer;
  transition:all .15s;
}}
.btn-clear:hover{{border-color:var(--blue);color:var(--blue);background:var(--blue-light)}}

/* ── NAV SIDEBAR ── */
.nav-section{{padding:4px 0}}
.nav-sep{{
  font-size:.63rem;font-weight:700;color:var(--gray);text-transform:uppercase;
  letter-spacing:.9px;padding:12px 16px 4px;font-family:var(--font);
}}
.nav-item{{
  display:flex;align-items:center;gap:10px;padding:9px 16px;
  font-size:.84rem;font-family:var(--font);color:var(--gray);
  cursor:pointer;text-decoration:none;
  transition:background .12s,color .12s;
  border-radius:var(--radius-sm);margin:1px 8px;
}}
.nav-item:hover{{background:var(--gray-light);color:var(--gray-dark)}}
.nav-item.active{{
  background:var(--blue-light);color:var(--blue);font-weight:600;
}}
.nav-icon{{font-size:.95rem;width:18px;text-align:center}}

/* ── SECTIONS ── */
.section{{
  background:var(--white);border-radius:var(--radius);
  box-shadow:var(--shadow);padding:28px;margin-bottom:24px;
  animation:fadeIn .3s ease;
}}
@keyframes fadeIn{{from{{opacity:0;transform:translateY(6px)}}to{{opacity:1;transform:none}}}}
.section-title{{
  font-size:1rem;font-weight:700;color:var(--gray-dark);margin-bottom:20px;
  font-family:var(--font);
  display:flex;align-items:center;gap:8px;
}}
.section-title span.ico{{font-size:1rem}}
.section-subtitle{{
  font-size:.75rem;font-weight:700;color:var(--gray);font-family:var(--font);
  margin:24px 0 10px;text-transform:uppercase;letter-spacing:.6px;
}}

/* ── KPI CARDS (estilo HiringPlatform) ── */
.kpi-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:14px;margin-bottom:24px}}
.kpi-card{{
  border-radius:var(--radius);padding:18px 20px;
  transition:box-shadow .2s;cursor:default;
  position:relative;overflow:hidden;
}}
.kpi-card:hover{{box-shadow:var(--shadow-md)}}
.kpi-card.blue{{background:var(--blue-light);border:1px solid var(--blue-mid)}}
.kpi-card.green{{background:var(--green-light);border:1px solid #BBF7D0}}
.kpi-card.orange{{background:var(--orange-light);border:1px solid #FED7AA}}
.kpi-card.red{{background:var(--red-light);border:1px solid #FECACA}}
.kpi-card.gray{{background:var(--gray-light);border:1px solid var(--gray-mid)}}
.kpi-label{{font-size:.72rem;font-weight:600;color:var(--gray);font-family:var(--font);
  text-transform:uppercase;letter-spacing:.4px;margin-bottom:8px;
  display:flex;align-items:center;gap:6px;
}}
.kpi-value{{font-size:2.2rem;font-weight:700;color:var(--gray-dark);line-height:1;font-family:var(--font)}}
.kpi-value.small{{font-size:1.5rem}}
.kpi-sub{{font-size:.72rem;color:var(--gray);margin-top:5px;font-family:var(--font)}}

/* ── KPI CARD SMALL ── */
.kpi-grid-sm{{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:10px;margin-bottom:20px}}
.kpi-card-sm{{border-radius:var(--radius);padding:12px 16px;}}
.kpi-card-sm.blue{{background:var(--blue-light);border:1px solid var(--blue-mid)}}
.kpi-card-sm.green{{background:var(--green-light);border:1px solid #BBF7D0}}
.kpi-card-sm.orange{{background:var(--orange-light);border:1px solid #FED7AA}}
.kpi-card-sm.red{{background:var(--red-light);border:1px solid #FECACA}}
.kpi-card-sm.gray{{background:var(--gray-light);border:1px solid var(--gray-mid)}}
.kpi-card-sm .kpi-label{{font-size:.66rem;font-weight:700;color:var(--gray);text-transform:uppercase;
  letter-spacing:.4px;margin-bottom:5px;font-family:var(--font)}}
.kpi-card-sm .kpi-value{{font-size:1.4rem;font-weight:700;color:var(--gray-dark);line-height:1;font-family:var(--font)}}
.kpi-card-sm .kpi-sub{{font-size:.68rem;color:var(--gray);margin-top:3px;font-family:var(--font)}}

/* ── CHARTS ── */
.chart-wrap{{position:relative;width:100%;overflow:hidden}}
.chart-wrap.h240{{height:240px}}
.chart-wrap.h280{{height:280px}}
.chart-wrap.h320{{height:320px}}
.chart-wrap.h200{{height:200px}}
.chart-row{{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:20px}}
@media(max-width:700px){{.chart-row{{grid-template-columns:1fr}}}}
.chart-box{{background:var(--gray-light);border-radius:var(--radius);padding:16px;border:1px solid var(--gray-mid)}}
.chart-box-title{{font-size:.72rem;font-weight:700;color:var(--gray);margin-bottom:12px;
  text-transform:uppercase;letter-spacing:.5px;font-family:var(--font)}}

/* ── TABLES ── */
.table-wrap{{overflow-x:auto;border-radius:var(--radius);border:1px solid var(--gray-mid)}}
table{{width:100%;border-collapse:collapse;font-size:.83rem;font-family:var(--font)}}
th{{
  background:var(--gray-dark);color:var(--white);font-weight:600;font-family:var(--font);
  padding:10px 14px;text-align:left;white-space:nowrap;cursor:pointer;
  user-select:none;position:sticky;top:0;font-size:.78rem;
}}
th:hover{{background:#1f2937}}
th .sort-arrow{{font-size:.6rem;opacity:.4;margin-left:4px}}
th.asc .sort-arrow::after{{content:'▲'}}
th.desc .sort-arrow::after{{content:'▼'}}
th:not(.asc):not(.desc) .sort-arrow::after{{content:'⇅'}}
td{{padding:9px 14px;border-bottom:1px solid var(--gray-mid);vertical-align:middle;
  font-family:var(--font);font-size:.82rem;color:var(--gray-dark)}}
tr:last-child td{{border-bottom:none}}
tr:nth-child(even) td{{background:#FAFAFA}}
tr:hover td{{background:var(--blue-light)!important}}
.tfoot td{{background:var(--gray-dark)!important;color:var(--white)!important;font-weight:700;font-family:var(--font)}}
.badge-mkt{{background:var(--blue-light);color:var(--blue);padding:2px 8px;border-radius:20px;
  font-size:.72rem;font-weight:600;font-family:var(--font)}}
.badge-nomkt{{background:var(--gray-light);color:var(--gray);padding:2px 8px;border-radius:20px;
  font-size:.72rem;font-weight:600;font-family:var(--font)}}

/* ── COMPACT TABLE ── */
.tbl-compact table{{font-size:.76rem}}
.tbl-compact th,.tbl-compact td{{padding:6px 10px}}

/* ── DELTA ── */
.delta{{font-weight:700;font-size:.85rem;font-family:var(--font)}}
.delta.up{{color:var(--green)}}
.delta.down{{color:var(--red)}}
.delta.neutral{{color:var(--gray)}}

/* ── ALERTS ── */
.alert{{
  display:flex;align-items:center;gap:10px;padding:10px 14px;
  border-radius:var(--radius-sm);font-size:.82rem;margin-bottom:16px;
  font-family:var(--font);
}}
.alert.warn{{background:var(--orange-light);color:var(--orange);border-left:3px solid var(--orange)}}
.alert.info{{background:var(--blue-light);color:var(--blue);border-left:3px solid var(--blue)}}
.alert.success{{background:var(--green-light);color:var(--green);border-left:3px solid var(--green)}}

/* ── EMPTY STATE ── */
.empty{{text-align:center;padding:40px 20px;color:var(--gray);font-family:var(--font)}}
.empty .ico{{font-size:2rem;margin-bottom:8px}}
.empty .msg{{font-size:.9rem;margin-bottom:12px;font-family:var(--font)}}
.empty .btn{{
  background:var(--blue);color:var(--white);border:none;border-radius:var(--radius-sm);
  padding:7px 16px;font-family:var(--font);font-size:.82rem;cursor:pointer;
}}

/* ── PIVOT TABLE ── */
.pivot-wrap{{overflow-x:auto}}
.pivot-wrap table th{{font-size:.7rem;padding:7px 8px;font-family:var(--font)}}
.pivot-wrap table td{{font-size:.76rem;padding:7px 8px;font-family:var(--font)}}
.pivot-head-group{{background:var(--blue)!important}}
.pivot-mkt{{background:#EFF9FF!important;color:#0077aa!important;font-family:var(--font)}}

/* ── TOOLTIPS ── */
.th-tip{{cursor:help;white-space:nowrap;font-family:var(--font)}}
.th-tip::after{{
  content:attr(data-tip);
  position:absolute;
  top:110%;left:50%;transform:translateX(-50%);
  background:#1f2937;color:#fff;
  padding:6px 10px;border-radius:var(--radius-sm);
  font-size:.72rem;font-weight:400;white-space:nowrap;font-family:var(--font);
  opacity:0;pointer-events:none;
  transition:opacity .2s;z-index:999;
  box-shadow:var(--shadow-md);
}}
th{{position:relative}}
th:hover .th-tip::after{{opacity:1}}

/* ── FUNNEL CONTAINER ── */
#chart-funnel{{height:auto!important;min-height:200px;padding:4px 0}}

/* ── FOOTER ── */
.footer{{
  text-align:center;padding:24px;font-size:.75rem;color:var(--gray);
  border-top:1px solid var(--gray-mid);margin-top:8px;font-family:var(--font);
}}
.footer img{{height:26px;opacity:.5;margin-bottom:6px;display:block;margin:0 auto 6px}}

/* ── PRINT ── */
@media print{{
  .sidebar,.filterbar,.hamburger,.overlay{{display:none!important}}
  .main{{margin-left:0!important;padding:0!important}}
  .section{{box-shadow:none!important;break-inside:avoid}}
}}
</style>
</head>
<body>

<!-- OVERLAY MÓVIL -->
<div class="overlay" id="overlay" onclick="closeSidebar()"></div>

<!-- HEADER -->
<header class="header">
  <button class="hamburger" onclick="toggleSidebar()" aria-label="Menú">
    <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
      <line x1="3" y1="6" x2="21" y2="6"/><line x1="3" y1="12" x2="21" y2="12"/><line x1="3" y1="18" x2="21" y2="18"/>
    </svg>
  </button>
  {f'<img class="header-logo" src="{logo_src}" alt="Estudios Rafer">' if logo_src else '<span class="header-logo-text">Estudios Rafer</span>'}
  <span class="header-title">Dashboard Captación</span>
  <div class="header-meta">
    <span id="badge-fecha" class="badge badge-info"></span>
    <span id="badge-inv" class="badge"></span>
  </div>
</header>

<!-- FILTER BAR -->
<div class="filterbar" id="filterbar">
  <div class="filter-group">
    <span class="filter-label">Curso</span>
    <select class="filter-select" id="f-curso" onchange="applyFilters()">
      <option value="Todos">Todos</option>
    </select>
  </div>
  <div class="filter-group">
    <span class="filter-label">Comercial</span>
    <select class="filter-select" id="f-comercial" onchange="applyFilters()">
      <option value="Todos">Todos</option>
    </select>
  </div>
  <div class="filter-group">
    <span class="filter-label">Origen</span>
    <select class="filter-select" id="f-origen" onchange="applyFilters()">
      <option value="Todos">Todos</option>
      <option value="Marketing">Marketing</option>
      <option value="No Marketing">No Marketing</option>
    </select>
  </div>
  <div class="filter-group">
    <span class="filter-label">Período</span>
    <select class="filter-select" id="f-periodo-tipo" onchange="onPeriodoTipoChange()">
      <option value="Todos">Todos</option>
      <option value="Semana">Semana</option>
      <option value="Mes">Mes</option>
    </select>
  </div>
  <div class="filter-group" id="fg-periodo-valor" style="display:none">
    <span class="filter-label">Valor</span>
    <select class="filter-select" id="f-periodo-valor" onchange="applyFilters()"></select>
  </div>
  <div class="filter-group">
    <span class="filter-label">Campaña</span>
    <select class="filter-select" id="f-campana" onchange="applyFilters()">
      <option value="Todas">Todas</option>
    </select>
  </div>
  <button class="btn-clear" onclick="clearFilters()">✕ Limpiar filtros</button>
</div>

<!-- LAYOUT -->
<div class="layout" style="padding-top:var(--top-offset)">

  <!-- SIDEBAR -->
  <nav class="sidebar" id="sidebar">
    <div class="nav-section">
      <div class="nav-sep">Secciones</div>
      <a class="nav-item active" href="#s01" onclick="navClick(this)">
        <span class="nav-icon">📊</span>Resumen Principal
      </a>
      <a class="nav-item" href="#s02" onclick="navClick(this)">
        <span class="nav-icon">👥</span>Comercial
      </a>
      <a class="nav-item" href="#s03" onclick="navClick(this)">
        <span class="nav-icon">💰</span>ROI Marketing
      </a>
      <a class="nav-item" href="#s04" onclick="navClick(this)">
        <span class="nav-icon">📈</span>Evolución
      </a>
      <a class="nav-item" href="#s05" onclick="navClick(this)">
        <span class="nav-icon">🎯</span>Ajuste Campaña
      </a>
      <a class="nav-item" href="#s06" onclick="navClick(this)">
        <span class="nav-icon">🔎</span>Tabla Pivote
      </a>
    </div>
  </nav>

  <!-- MAIN -->
  <main class="main">

    <!-- S01 RESUMEN PRINCIPAL -->
    <section class="section" id="s01">
      <div class="section-title"><span class="ico">📊</span> Resumen Principal</div>
      <div class="kpi-grid" id="kpi-main"></div>
      <div class="chart-row">
        <div class="chart-box">
          <div class="chart-box-title">Embudo de captación</div>
          <div id="chart-funnel" style="padding:4px 0"></div>
        </div>
        <div class="chart-box">
          <div class="chart-box-title">Leads por curso</div>
          <div class="chart-wrap h280"><canvas id="chart-cursos"></canvas></div>
        </div>
      </div>
      <div class="chart-box">
        <div class="chart-box-title">Marketing vs Sin Marketing por etapa</div>
        <div class="chart-wrap h200"><canvas id="chart-origen"></canvas></div>
      </div>
    </section>

    <!-- S02 COMERCIAL -->
    <section class="section" id="s02">
      <div class="section-title"><span class="ico">👥</span> Rendimiento Comercial</div>
      <div class="chart-box" style="margin-bottom:20px">
        <div class="chart-box-title">Total leads vs Inscritos por comercial</div>
        <div class="chart-wrap h240"><canvas id="chart-comercial"></canvas></div>
      </div>
      <div class="section-subtitle">Efectividad de cierre</div>
      <div class="table-wrap" id="tbl-comercial-wrap"></div>
      <div class="section-subtitle" style="margin-top:20px">Desglose Marketing vs Sin Marketing</div>
      <div class="table-wrap" id="tbl-comercial-mkt-wrap"></div>
    </section>

    <!-- S03 ROI -->
    <section class="section" id="s03">
      <div class="section-title"><span class="ico">💰</span> ROI & Inversión Marketing</div>
      <div id="inv-alert"></div>
      <div class="kpi-grid-sm" id="kpi-roi"></div>
      <div class="chart-row">
        <div class="chart-box">
          <div class="chart-box-title">CPL por curso</div>
          <div class="chart-wrap h240"><canvas id="chart-cpl"></canvas></div>
        </div>
        <div class="chart-box">
          <div class="chart-box-title">Distribución inversión</div>
          <div class="chart-wrap h240"><canvas id="chart-inv-dist"></canvas></div>
        </div>
      </div>
      <div class="section-subtitle">Detalle por curso</div>
      <div class="table-wrap" id="tbl-roi-wrap"></div>
    </section>

    <!-- S04 EVOLUCIÓN -->
    <section class="section" id="s04">
      <div class="section-title"><span class="ico">📈</span> Evolución Temporal</div>
      <div id="evo-alert"></div>
      <div style="display:flex;gap:16px;align-items:flex-end;flex-wrap:wrap;margin-bottom:16px">
        <div class="filter-group">
          <span class="filter-label">Fecha 1 (base)</span>
          <select class="filter-select" id="f-fecha1" onchange="renderEvolucion()"></select>
        </div>
        <div class="filter-group">
          <span class="filter-label">Fecha 2 (comparar)</span>
          <select class="filter-select" id="f-fecha2" onchange="renderEvolucion()"></select>
        </div>
      </div>
      <div class="chart-box" style="margin-bottom:20px">
        <div class="chart-box-title">Evolución de etapas en el tiempo</div>
        <div class="chart-wrap h280"><canvas id="chart-evo"></canvas></div>
      </div>
      <div class="section-subtitle">Comparativa entre fechas seleccionadas</div>
      <div class="table-wrap" id="tbl-evo-wrap"></div>
    </section>

    <!-- S05 AJUSTE -->
    <section class="section" id="s05">
      <div class="section-title"><span class="ico">🎯</span> Ajuste de Campaña</div>
      <div class="kpi-grid" id="kpi-ajuste"></div>
      <div class="chart-row">
        <div class="chart-box">
          <div class="chart-box-title">Calidad de leads marketing (global)</div>
          <div class="chart-wrap h200"><canvas id="chart-ajuste-donut"></canvas></div>
        </div>
        <div class="chart-box">
          <div class="chart-box-title">Tasa de ajuste por curso</div>
          <div class="chart-wrap h200"><canvas id="chart-ajuste-bar"></canvas></div>
        </div>
      </div>
      <div class="section-subtitle">Por curso</div>
      <div class="table-wrap" id="tbl-ajuste-curso-wrap"></div>
      <div class="section-subtitle" style="margin-top:20px">Por comercial</div>
      <div class="table-wrap" id="tbl-ajuste-com-wrap"></div>
    </section>

    <!-- S06 PIVOTE -->
    <section class="section" id="s06">
      <div class="section-title"><span class="ico">🔎</span> Tabla Pivote — Desglose Completo</div>
      <div class="pivot-wrap" id="tbl-pivote-wrap"></div>
    </section>

    <!-- FOOTER -->
    <footer class="footer">
      {f'<img src="{logo_src}" alt="Estudios Rafer">' if logo_src else ''}
      <div id="footer-text"></div>
    </footer>

  </main>
</div>

<!-- DATA + SCRIPTS -->
<script>
const DATA = {data_json};

// ── UTILS ──────────────────────────────────────────────────────────────────
const fmt  = n => n == null ? '—' : Number(n).toLocaleString('es-ES');
const fmtE = n => n == null ? '—' : Number(n).toLocaleString('es-ES',{{minimumFractionDigits:2,maximumFractionDigits:2}})+'€';
const fmtP = n => n == null || isNaN(n) ? '—' : (n*100).toFixed(1)+'%';
const round2 = n => Math.round(n*100)/100;
const ETAPAS = ['BD','Solicitud','Contactado','Interesado','Inscrito','No interesado','No cumple','Extrarradio'];
const ETAPAS_F = ['BD','Solicitud','Contactado','Interesado','Inscrito'];
const COLORS_ETAPA = ['#7fb3d3','#5dade2','#29ABE2','#1a7faa','#27AE60','#95a5a6','#e74c3c','#e67e22'];

// Chart palette
const C_BLUE   = '#29ABE2';
const C_GRAY   = '#95a5a6';
const C_GREEN  = '#27AE60';
const C_ORANGE = '#E67E22';
const C_RED    = '#E74C3C';
const C_LBLUE  = 'rgba(41,171,226,.15)';

// Destroy and recreate chart safely
const charts = {{}};
function mkChart(id, config) {{
  if (charts[id]) charts[id].destroy();
  const ctx = document.getElementById(id);
  if (!ctx) return;
  charts[id] = new Chart(ctx, config);
}}

// ── FILTER STATE ──────────────────────────────────────────────────────────
let S = {{ curso:'Todos', comercial:'Todos', origen:'Todos',
           periodo_tipo:'Todos', periodo_valor:'', campana:'Todas' }};

// Filter leads for CURRENT snapshot
function filterLeads(leads, useLatest=true) {{
  const latest = DATA.meta.fecha_act;
  return leads.filter(l => {{
    if (useLatest && l.f !== latest) return false;
    if (S.curso !== 'Todos' && l.c !== S.curso) return false;
    if (S.comercial !== 'Todos' && l.co !== S.comercial) return false;
    if (S.origen === 'Marketing' && l.m !== 1) return false;
    if (S.origen === 'No Marketing' && l.m !== 0) return false;
    if (S.periodo_tipo === 'Semana' && l.se !== S.periodo_valor) return false;
    if (S.periodo_tipo === 'Mes' && l.me !== S.periodo_valor) return false;
    if (S.campana !== 'Todas' && l.ca !== S.campana) return false;
    return true;
  }});
}}

// Count leads by field value
function cnt(leads, field, val) {{
  return leads.filter(l => l[field] === val).length;
}}
function cntEtapa(leads, etapa, mkt=null) {{
  return leads.filter(l => l.e === etapa && (mkt===null || l.m===mkt)).length;
}}

// ── INIT FILTERS ─────────────────────────────────────────────────────────
function initFilters() {{
  const F = DATA.filtros;

  F.cursos.forEach(c => {{
    const o = new Option(F.shorts[c] || c, c);
    document.getElementById('f-curso').add(o);
  }});
  F.comerciales.forEach(c => {{
    document.getElementById('f-comercial').add(new Option(c,c));
  }});
  F.campanas.forEach(c => {{
    document.getElementById('f-campana').add(new Option(c,c));
  }});

  // Evolution date selectors
  const fechas = DATA.meta.fechas;
  const fmtF = f => f.split('-').reverse().join('/');
  ['f-fecha1','f-fecha2'].forEach((id,i) => {{
    const sel = document.getElementById(id);
    fechas.forEach(f => sel.add(new Option(fmtF(f), f)));
    sel.value = fechas[i===0 ? 0 : fechas.length-1] || fechas[0];
  }});
}}

function onPeriodoTipoChange() {{
  const tipo = document.getElementById('f-periodo-tipo').value;
  const fg = document.getElementById('fg-periodo-valor');
  const sel = document.getElementById('f-periodo-valor');
  S.periodo_tipo = tipo;
  sel.innerHTML = '';
  if (tipo === 'Semana') {{
    fg.style.display = '';
    DATA.filtros.semanas.forEach(s => sel.add(new Option(s,s)));
    S.periodo_valor = DATA.filtros.semanas[0]||'';
  }} else if (tipo === 'Mes') {{
    fg.style.display = '';
    DATA.filtros.meses.forEach(m => sel.add(new Option(m,m)));
    S.periodo_valor = DATA.filtros.meses[0]||'';
  }} else {{
    fg.style.display = 'none';
    S.periodo_valor = '';
  }}
  applyFilters();
}}

function applyFilters() {{
  S.curso      = document.getElementById('f-curso').value;
  S.comercial  = document.getElementById('f-comercial').value;
  S.origen     = document.getElementById('f-origen').value;
  S.periodo_tipo = document.getElementById('f-periodo-tipo').value;
  if (S.periodo_tipo !== 'Todos')
    S.periodo_valor = document.getElementById('f-periodo-valor').value;
  S.campana    = document.getElementById('f-campana').value;
  renderAll();
}}

function clearFilters() {{
  S = {{ curso:'Todos', comercial:'Todos', origen:'Todos',
         periodo_tipo:'Todos', periodo_valor:'', campana:'Todas' }};
  document.getElementById('f-curso').value = 'Todos';
  document.getElementById('f-comercial').value = 'Todos';
  document.getElementById('f-origen').value = 'Todos';
  document.getElementById('f-periodo-tipo').value = 'Todos';
  document.getElementById('fg-periodo-valor').style.display = 'none';
  document.getElementById('f-campana').value = 'Todas';
  renderAll();
}}

// ── RENDER ALL ───────────────────────────────────────────────────────────
function renderAll() {{
  const leads = filterLeads(DATA.leads);
  renderHeader();
  renderS01(leads);
  renderS02(leads);
  renderS03(leads);
  renderEvolucion();
  renderS05(leads);
  renderS06();
}}

// ── HEADER ───────────────────────────────────────────────────────────────
function renderHeader() {{
  const m = DATA.meta;
  const fmtF = f => f.split('-').reverse().join('/');
  document.getElementById('badge-fecha').textContent = '📅 ' + fmtF(m.fecha_act);
  const inv = document.getElementById('badge-inv');
  if (m.n_inv_ok < m.n_cursos) {{
    inv.textContent = '⚠️ Inversión ' + m.n_inv_ok + '/' + m.n_cursos + ' cursos';
    inv.className = 'badge badge-warn';
  }} else {{
    inv.textContent = '✓ Inversión completa';
    inv.className = 'badge badge-ok';
  }}
  document.getElementById('footer-text').innerHTML =
    `Generado el ${{m.fecha_gen}} &nbsp;·&nbsp; Datos actualizados a ${{fmtF(m.fecha_act)}} &nbsp;·&nbsp; Estudios Rafer`;
}}

// ── S01 RESUMEN ───────────────────────────────────────────────────────────
function renderS01(leads) {{
  const total  = leads.length;
  const mkt    = leads.filter(l=>l.m===1).length;
  const nomkt  = leads.filter(l=>l.m===0).length;
  const ins    = cntEtapa(leads,'Inscrito');
  const insMkt = cntEtapa(leads,'Inscrito',1);
  const convG  = total  > 0 ? ins/total   : 0;
  const convM  = mkt    > 0 ? insMkt/mkt  : 0;

  document.getElementById('kpi-main').innerHTML = [
    kpiCard('Total Leads',      fmt(total),  '','blue'),
    kpiCard('Leads Marketing',  fmt(mkt),    (mkt&&total?fmtP(mkt/total):'—')+' del total','blue'),
    kpiCard('Inscritos',        fmt(ins),    '','green'),
    kpiCard('Conv. Global',     fmtP(convG), '','green'),
    kpiCard('Conv. Marketing',  fmtP(convM), '','blue'),
  ].join('');

  if (!total) {{ renderEmpty('kpi-main'); return; }}

  // HTML Funnel (Solicitud→BD→Contactado→Interesado→Inscrito)
  const funnelOrden = ['Solicitud','BD','Contactado','Interesado','Inscrito'];
  const funnelTotal = funnelOrden.map(e=>cntEtapa(leads,e));
  const funnelMktD  = funnelOrden.map(e=>cntEtapa(leads,e,1));
  const funnelMax   = Math.max(...funnelTotal,1);
  const funnelColors= ['#7fb3d3','#5dade2','#29ABE2','#1a7faa','#27AE60'];
  const funnelEl = document.getElementById('chart-funnel');
  if(funnelEl){{
    funnelEl.innerHTML = funnelOrden.map((e,i)=>{{
      const tot = funnelTotal[i];
      const mkt = funnelMktD[i];
      const nom = tot - mkt;
      const pct = funnelMax>0 ? tot/funnelMax : 0;
      const minW = 40, maxW = 100;
      const w = minW + (maxW-minW)*pct;
      const conv = i>0 && funnelTotal[i-1]>0 ? (tot/funnelTotal[i-1]*100).toFixed(0)+'%' : '';
      return `<div style="display:flex;align-items:center;gap:10px;margin-bottom:6px">
        <div style="width:90px;text-align:right;font-size:.78rem;color:#555;font-weight:600">${{e}}</div>
        <div style="flex:1;display:flex;flex-direction:column;align-items:center">
          <div style="width:${{w}}%;background:${{funnelColors[i]}};border-radius:4px;height:32px;
            display:flex;align-items:center;justify-content:center;gap:6px;transition:width .4s">
            <span style="color:#fff;font-weight:700;font-size:.85rem">${{tot}}</span>
            ${{mkt>0?`<span style="font-size:.7rem;color:rgba(255,255,255,.8)">(Mkt: ${{mkt}})</span>`:''}}
          </div>
        </div>
        <div style="width:36px;font-size:.72rem;color:#aaa;text-align:left">${{conv}}</div>
      </div>`;
    }}).join('') +
    `<div style="display:flex;gap:12px;justify-content:center;margin-top:8px;font-size:.75rem;color:#555">
      <span><span style="display:inline-block;width:10px;height:10px;background:#29ABE2;border-radius:2px;margin-right:4px"></span>Marketing</span>
      <span><span style="display:inline-block;width:10px;height:10px;background:#95a5a6;border-radius:2px;margin-right:4px"></span>Sin Mkt</span>
    </div>`;
  }}

  // Cursos chart
  const cursos = [...new Set(leads.map(l=>l.cs))].filter(Boolean);
  const cLeads = cursos.map(cs=>leads.filter(l=>l.cs===cs).length);
  const cIns   = cursos.map(cs=>leads.filter(l=>l.cs===cs&&l.e==='Inscrito').length);
  const sortI  = cLeads.map((_,i)=>i).sort((a,b)=>cLeads[b]-cLeads[a]);
  mkChart('chart-cursos',{{
    type:'bar',
    data:{{
      labels: sortI.map(i=>cursos[i]),
      datasets:[
        {{label:'Total Leads', data:sortI.map(i=>cLeads[i]), backgroundColor:C_LBLUE,
          borderColor:C_BLUE,borderWidth:1.5,borderRadius:4}},
        {{label:'Inscritos',   data:sortI.map(i=>cIns[i]),   backgroundColor:C_GREEN,
          borderRadius:4}},
      ]
    }},
    options:{{
      indexAxis:'y',responsive:true,maintainAspectRatio:false,
      plugins:{{
        legend:{{position:'top',labels:{{font:{{family:'DM Sans',size:11}}}}}},
        datalabels:{{
          anchor:'end',align:'end',
          font:{{family:'DM Sans',size:10,weight:'600'}},
          color:'#555',
          formatter:(v)=>v>0?v:''
        }}
      }},
      scales:{{
        x:{{grid:{{color:'rgba(0,0,0,.05)'}},ticks:{{font:{{size:10}}}}}},
        y:{{grid:{{display:false}},ticks:{{font:{{size:10}}}}}}
      }}
    }}
  }});

  // Origen por etapa
  const allEtapas = ETAPAS.filter(e=>leads.some(l=>l.e===e));
  mkChart('chart-origen',{{
    type:'bar',
    data:{{
      labels: allEtapas,
      datasets:[
        {{label:'Marketing',  data:allEtapas.map(e=>cntEtapa(leads,e,1)), backgroundColor:C_BLUE,  borderRadius:3}},
        {{label:'Sin Mkt',    data:allEtapas.map(e=>cntEtapa(leads,e,0)), backgroundColor:C_GRAY,  borderRadius:3}},
      ]
    }},
    options:{{
      responsive:true,maintainAspectRatio:false,
      plugins:{{legend:{{position:'top',labels:{{font:{{family:'DM Sans',size:11}}}}}}}},
      scales:{{
        x:{{stacked:true,grid:{{display:false}}}},
        y:{{stacked:true,grid:{{color:'rgba(0,0,0,.05)'}}}}
      }}
    }}
  }});
}}

// ── S02 COMERCIAL ─────────────────────────────────────────────────────────
function renderS02(leads) {{
  const coms = [...new Set(leads.map(l=>l.co))].filter(Boolean).sort();

  // Chart
  mkChart('chart-comercial',{{
    type:'bar',
    data:{{
      labels: coms,
      datasets:[
        {{label:'Total Leads', data:coms.map(c=>leads.filter(l=>l.co===c).length),
          backgroundColor:C_LBLUE,borderColor:C_BLUE,borderWidth:1.5,borderRadius:4}},
        {{label:'Inscritos',   data:coms.map(c=>leads.filter(l=>l.co===c&&l.e==='Inscrito').length),
          backgroundColor:C_GREEN,borderRadius:4}},
      ]
    }},
    options:{{
      responsive:true,maintainAspectRatio:false,
      plugins:{{legend:{{position:'top',labels:{{font:{{family:'DM Sans',size:11}}}}}}}},
      scales:{{x:{{grid:{{display:false}}}},y:{{grid:{{color:'rgba(0,0,0,.05)'}}}}}}
    }}
  }});

  // Tabla efectividad
  const rows = coms.map(c => {{
    const cl  = leads.filter(l=>l.co===c);
    const tot = cl.length;
    const con = cntEtapa(cl,'Contactado');
    const int = cntEtapa(cl,'Interesado');
    const sol = cntEtapa(cl,'Solicitud');
    const ins = cntEtapa(cl,'Inscrito');
    return {{c,tot,con,int,sol,ins,
      tCon:tot?con/tot:0, tInt:tot?int/tot:0, tCie:tot?ins/tot:0}};
  }});

  document.getElementById('tbl-comercial-wrap').innerHTML = buildTable(
    ['Comercial',
     '<span class="th-tip" data-tip="Inscritos / Total Leads">T.Cierre ⓘ</span>',
     'Total','Inscritos','Interesados','Contactados','Solicitudes'],
    rows.map(r=>[
      r.c,
      `<b style="color:var(--green)">${{fmtP(r.tCie)}}</b>`,
      fmt(r.tot),
      `<b style="color:var(--green)">${{fmt(r.ins)}}</b>`,
      fmt(r.int), fmt(r.con), fmt(r.sol),
    ]),
    [[`<b style="color:var(--green)">${{fmtP(rows.reduce((a,r)=>a+r.ins,0)/Math.max(rows.reduce((a,r)=>a+r.tot,0),1))}}</b>`,
      fmt(rows.reduce((a,r)=>a+r.tot,0)),
      fmt(rows.reduce((a,r)=>a+r.ins,0)),
      fmt(rows.reduce((a,r)=>a+r.int,0)),
      fmt(rows.reduce((a,r)=>a+r.con,0)),
      fmt(rows.reduce((a,r)=>a+r.sol,0))]]
  );

  // Tabla Mkt vs No Mkt
  const rowsMkt = coms.map(c => {{
    const cl    = leads.filter(l=>l.co===c);
    const lMkt  = cl.filter(l=>l.m===1).length;
    const iMkt  = cl.filter(l=>l.m===1&&l.e==='Inscrito').length;
    const lNom  = cl.filter(l=>l.m===0).length;
    const iNom  = cl.filter(l=>l.m===0&&l.e==='Inscrito').length;
    return {{c, lMkt,iMkt,cMkt:lMkt?iMkt/lMkt:0,
               lNom,iNom,cNom:lNom?iNom/lNom:0,
               pMkt:cl.length?lMkt/cl.length:0}};
  }});

  document.getElementById('tbl-comercial-mkt-wrap').innerHTML =
    '<div class="tbl-compact">' +
    buildTable(
      ['Comercial','Leads Mkt','Ins. Mkt','Conv% Mkt',
       'Leads Sin Mkt','Ins. Sin Mkt','Conv% Sin Mkt','% Leads Mkt'],
      rowsMkt.map(r=>[
        r.c,
        `<span class="badge-mkt">${{fmt(r.lMkt)}}</span>`,
        fmt(r.iMkt), fmtP(r.cMkt),
        `<span class="badge-nomkt">${{fmt(r.lNom)}}</span>`,
        fmt(r.iNom), fmtP(r.cNom), fmtP(r.pMkt)
      ])
    ) + '</div>';
}}

// ── S03 ROI ───────────────────────────────────────────────────────────────
function renderS03(leads) {{
  const mktLeads = leads.filter(l=>l.m===1);
  const mktIns   = mktLeads.filter(l=>l.e==='Inscrito').length;
  const invData  = DATA.inversion.filter(i=>i.disponible);
  const invTotal = invData.reduce((a,i)=>a+(i.importe||0),0);
  const cpl      = mktLeads.length > 0 ? round2(invTotal/mktLeads.length) : null;
  const cpi      = mktIns > 0 ? round2(invTotal/mktIns) : null;
  const convMkt  = mktLeads.length > 0 ? mktIns/mktLeads.length : 0;

  // Alert inversión
  const alertEl = document.getElementById('inv-alert');
  if (DATA.meta.sin_inv.length > 0) {{
    alertEl.innerHTML = `<div class="alert warn">⚠️ Sin dato de inversión: ${{DATA.meta.sin_inv.join(', ')}}. Las métricas de ROI de esos cursos no están disponibles.</div>`;
  }} else alertEl.innerHTML = '<div class="alert success">✓ Todos los cursos tienen dato de inversión.</div>';

  document.getElementById('kpi-roi').innerHTML = [
    kpiCardSm('Total Inversión', fmtE(invTotal), DATA.meta.n_inv_ok+'/'+DATA.meta.n_cursos+' cursos', 'blue'),
    kpiCardSm('Leads Mkt',       fmt(mktLeads.length), '', 'blue'),
    kpiCardSm('CPL',             fmtE(cpl),    'Coste por Lead', 'orange'),
    kpiCardSm('Inscritos Mkt',   fmt(mktIns),  '', 'green'),
    kpiCardSm('CPI',             fmtE(cpi),    'Coste por Inscrito', 'orange'),
    kpiCardSm('Conv. Mkt',       fmtP(convMkt),'', 'green'),
  ].join('');

  // Build per-course data
  const cursoRows = DATA.inversion.map(inv => {{
    const cls   = leads.filter(l=>l.c===inv.curso&&l.m===1);
    const lMkt  = cls.length;
    const iMkt  = cls.filter(l=>l.e==='Inscrito').length;
    const imp   = inv.disponible ? inv.importe : null;
    const cplC  = imp && lMkt  ? round2(imp/lMkt)  : null;
    const cpiC  = imp && iMkt  ? round2(imp/iMkt)  : null;
    const conv  = lMkt ? iMkt/lMkt : 0;
    const pInv  = imp && invTotal ? imp/invTotal : null;
    return {{
      short: inv.short, curso: inv.curso,
      imp, lMkt, iMkt, cplC, cpiC, conv, pInv, disponible: inv.disponible
    }};
  }});

  // CPL chart
  const withCpl = cursoRows.filter(r=>r.cplC!=null).sort((a,b)=>a.cplC-b.cplC);
  mkChart('chart-cpl',{{
    type:'bar',
    data:{{
      labels: withCpl.map(r=>r.short),
      datasets:[{{
        label:'CPL (€)',
        data: withCpl.map(r=>r.cplC),
        backgroundColor: C_ORANGE, borderRadius:4
      }}]
    }},
    options:{{
      indexAxis:'y',responsive:true,maintainAspectRatio:false,
      plugins:{{legend:{{display:false}}}},
      scales:{{x:{{grid:{{color:'rgba(0,0,0,.05)'}},ticks:{{callback:v=>v+'€'}}}},y:{{grid:{{display:false}}}}}}
    }}
  }});

  // Inv distribution donut
  const withInv = cursoRows.filter(r=>r.disponible);
  mkChart('chart-inv-dist',{{
    type:'doughnut',
    data:{{
      labels: withInv.map(r=>r.short),
      datasets:[{{
        data: withInv.map(r=>r.imp),
        backgroundColor:['#29ABE2','#27AE60','#E67E22','#9B59B6','#E74C3C','#1ABC9C'],
        borderWidth:2
      }}]
    }},
    options:{{
      responsive:true,maintainAspectRatio:false,
      plugins:{{
        legend:{{position:'right',labels:{{font:{{family:'DM Sans',size:10}},boxWidth:12}}}},
        tooltip:{{callbacks:{{label:ctx=>` ${{fmtE(ctx.raw)}}`}}}}
      }}
    }}
  }});

  // Table
  document.getElementById('tbl-roi-wrap').innerHTML = buildTable(
    [
      'Curso','Inversión','Leads Mkt','Inscritos Mkt',
      '<span class="th-tip" data-tip="Coste Por Lead = Inversión / Leads Mkt">CPL ⓘ</span>',
      '<span class="th-tip" data-tip="Coste Por Inscrito = Inversión / Inscritos Mkt">CPI ⓘ</span>',
      '<span class="th-tip" data-tip="Conv% Mkt = Inscritos Mkt / Leads Mkt × 100">Conv% Mkt ⓘ</span>',
      '<span class="th-tip" data-tip="% Inversión = Inversión curso / Inversión total × 100">% Inversión ⓘ</span>'
    ],
    cursoRows.map(r=>[
      r.short,
      r.disponible ? fmtE(r.imp) : '<span style="color:var(--gray)">—</span>',
      fmt(r.lMkt), fmt(r.iMkt),
      r.cplC!=null ? fmtE(r.cplC) : '—',
      r.cpiC!=null ? fmtE(r.cpiC) : '—',
      fmtP(r.conv),
      r.pInv!=null ? fmtP(r.pInv) : '—'
    ]),
    [[fmtE(invTotal),
      fmt(cursoRows.reduce((a,r)=>a+r.lMkt,0)),
      fmt(cursoRows.reduce((a,r)=>a+r.iMkt,0)),
      fmtE(cpl)+'',fmtE(cpi)+'',fmtP(convMkt),'']]
  );
}}

// ── S04 EVOLUCIÓN ─────────────────────────────────────────────────────────
function renderEvolucion() {{
  const fechas = DATA.meta.fechas;
  const alertEl = document.getElementById('evo-alert');

  if (fechas.length < 2) {{
    alertEl.innerHTML = '<div class="alert info">ℹ️ Añade más Excels al histórico para activar la comparativa. Actualmente solo hay una fecha de datos.</div>';
  }} else {{
    alertEl.innerHTML = '';
  }}

  // Line chart (all dates, all funnel stages)
  const COLORS_LINE = [C_BLUE,'#27AE60','#E67E22','#9B59B6','#E74C3C'];
  const datasets = ETAPAS_F.map((etapa,i)=>{{
    const data = fechas.map(f => {{
      const fl = DATA.leads.filter(l=>l.f===f && l.e===etapa);
      // Apply curso/comercial/origen filters but not period
      return fl.filter(l=>{{
        if (S.curso!=='Todos' && l.c!==S.curso) return false;
        if (S.comercial!=='Todos' && l.co!==S.comercial) return false;
        if (S.origen==='Marketing' && l.m!==1) return false;
        if (S.origen==='No Marketing' && l.m!==0) return false;
        return true;
      }}).length;
    }});
    return {{
      label:etapa, data,
      borderColor:COLORS_LINE[i],backgroundColor:COLORS_LINE[i]+'22',
      tension:.3, pointRadius:4, fill:false
    }};
  }});

  const fmtF = f=>f.split('-').reverse().join('/');
  mkChart('chart-evo',{{
    type:'line',
    data:{{
      labels: fechas.map(fmtF),
      datasets
    }},
    options:{{
      responsive:true,maintainAspectRatio:false,
      plugins:{{legend:{{position:'top',labels:{{font:{{family:'DM Sans',size:11}}}}}}}},
      scales:{{
        x:{{grid:{{color:'rgba(0,0,0,.05)'}}}},
        y:{{grid:{{color:'rgba(0,0,0,.05)'}}}}
      }}
    }}
  }});

  // Comparison table between two selected dates
  const f1 = document.getElementById('f-fecha1')?.value;
  const f2 = document.getElementById('f-fecha2')?.value;
  if (!f1 || !f2) {{ document.getElementById('tbl-evo-wrap').innerHTML=''; return; }}

  const leadsF1 = DATA.leads.filter(l=>l.f===f1);
  const leadsF2 = DATA.leads.filter(l=>l.f===f2);

  const rows = ETAPAS_F.concat(['No interesado','No cumple','Extrarradio']).map(etapa => {{
    const t1  = cntEtapa(leadsF1,etapa);
    const m1  = cntEtapa(leadsF1,etapa,1);
    const t2  = cntEtapa(leadsF2,etapa);
    const m2  = cntEtapa(leadsF2,etapa,1);
    const dt  = t2-t1;
    const dm  = m2-m1;
    const dtp = t1>0 ? dt/t1 : null;
    return {{etapa,t1,m1,t2,m2,dt,dm,dtp}};
  }});

  document.getElementById('tbl-evo-wrap').innerHTML = buildTable(
    ['Etapa',
     `F1 Total (${{fmtF(f1)}})`,`F1 Mkt`,
     `F2 Total (${{fmtF(f2)}})`,`F2 Mkt`,
     'Δ Total','Δ Mkt','Δ%'],
    rows.map(r=>[
      r.etapa,
      fmt(r.t1), fmt(r.m1),
      fmt(r.t2), fmt(r.m2),
      deltaCell(r.dt), deltaCell(r.dm),
      r.dtp!=null ? `<span class="delta ${{r.dt>0?'up':r.dt<0?'down':'neutral'}}">${{(r.dtp*100).toFixed(1)}}%</span>` : '—'
    ])
  );
}}

function deltaCell(d) {{
  const cls = d>0?'up':d<0?'down':'neutral';
  const ico = d>0?'↑':d<0?'↓':'→';
  return `<span class="delta ${{cls}}">${{ico}} ${{Math.abs(d)}}</span>`;
}}

// ── S05 AJUSTE ────────────────────────────────────────────────────────────
function renderS05(leads) {{
  const mkt    = leads.filter(l=>l.m===1);
  const noCum  = mkt.filter(l=>l.e==='No cumple').length;
  const extra  = mkt.filter(l=>l.e==='Extrarradio').length;
  const inval  = noCum+extra;
  const tAdj   = mkt.length>0 ? inval/mkt.length : 0;
  const validos = 1-tAdj;

  document.getElementById('kpi-ajuste').innerHTML = [
    kpiCard('No Cumple (Mkt)', fmt(noCum),  'Leads descartados','red'),
    kpiCard('Extrarradio',     fmt(extra),  'Fuera de zona','red'),
    kpiCard('Total Inválidos', fmt(inval),  '','red'),
    kpiCard('Tasa de Ajuste',  fmtP(tAdj), '(No Cumple+Extra)/Leads Mkt','orange'),
    kpiCard('Leads Válidos',   fmtP(validos),'','green'),
  ].join('');

  mkChart('chart-ajuste-donut',{{
    type:'doughnut',
    data:{{
      labels:['Leads Válidos','No Cumple','Extrarradio'],
      datasets:[{{
        data:[mkt.length-inval, noCum, extra],
        backgroundColor:[C_GREEN, C_RED, C_ORANGE],
        borderWidth:2
      }}]
    }},
    options:{{
      responsive:true,maintainAspectRatio:false,
      plugins:{{legend:{{position:'right',labels:{{font:{{family:'DM Sans',size:11}}}}}}}}
    }}
  }});

  // Bar chart ajuste por curso
  const cursos = [...new Set(leads.map(l=>l.cs))].filter(Boolean);
  const tasas  = cursos.map(cs=>{{
    const m = leads.filter(l=>l.cs===cs&&l.m===1);
    const inv = m.filter(l=>l.e==='No cumple'||l.e==='Extrarradio').length;
    return m.length>0 ? round2(inv/m.length*100) : 0;
  }});
  mkChart('chart-ajuste-bar',{{
    type:'bar',
    data:{{
      labels: cursos,
      datasets:[{{label:'Tasa Ajuste %',data:tasas,
        backgroundColor:tasas.map(t=>t>20?C_RED:t>10?C_ORANGE:C_GREEN),borderRadius:4}}]
    }},
    options:{{
      indexAxis:'y',responsive:true,maintainAspectRatio:false,
      plugins:{{legend:{{display:false}}}},
      scales:{{x:{{max:100,ticks:{{callback:v=>v+'%'}},grid:{{color:'rgba(0,0,0,.05)'}}}},y:{{grid:{{display:false}}}}}}
    }}
  }});

  // Table por curso
  const rowsCurso = cursos.map(cs=>{{
    const m  = leads.filter(l=>l.cs===cs&&l.m===1);
    const nc = m.filter(l=>l.e==='No cumple').length;
    const ex = m.filter(l=>l.e==='Extrarradio').length;
    const ta = m.length>0 ? (nc+ex)/m.length : 0;
    return [cs, fmt(m.length), fmt(nc), fmt(ex), fmt(nc+ex), fmtP(ta), fmtP(1-ta)];
  }});
  document.getElementById('tbl-ajuste-curso-wrap').innerHTML = buildTable(
    ['Curso','Leads Mkt','No Cumple','Extrarradio','Total Inválidos','Tasa Ajuste','Leads Válidos %'],
    rowsCurso
  );

  // Table por comercial
  const coms = [...new Set(leads.map(l=>l.co))].filter(Boolean).sort();
  const rowsCom = coms.map(co=>{{
    const m  = leads.filter(l=>l.co===co&&l.m===1);
    const nc = m.filter(l=>l.e==='No cumple').length;
    const ex = m.filter(l=>l.e==='Extrarradio').length;
    const ta = m.length>0 ? (nc+ex)/m.length : 0;
    return [co, fmt(m.length), fmt(nc), fmt(ex), fmt(nc+ex), fmtP(ta), fmtP(1-ta)];
  }});
  document.getElementById('tbl-ajuste-com-wrap').innerHTML = buildTable(
    ['Comercial','Leads Mkt','No Cumple','Extrarradio','Total Inválidos','Tasa Ajuste','Leads Válidos %'],
    rowsCom
  );
}}

// ── S06 TABLA PIVOTE ──────────────────────────────────────────────────────
function renderS06() {{
  const leads = filterLeads(DATA.leads);
  const cursos = DATA.filtros.cursos;
  const shorts = DATA.filtros.shorts;
  const etapas = ['BD','Solicitud','Contactado','Interesado','Inscrito','No interesado','No cumple','Extrarradio'];

  let html = '<div class="pivot-wrap"><table><thead><tr>';
  html += '<th rowspan="2" style="min-width:160px">Curso</th>';
  etapas.forEach(e=>{{
    html+=`<th colspan="2" class="pivot-head-group">${{e}}</th>`;
  }});
  html+='<th rowspan="2">Total</th><th rowspan="2">Inscritos</th>';
  html+='<th rowspan="2">Conv%</th><th rowspan="2">Inv.€</th><th rowspan="2">CPL€</th>';
  html+='</tr><tr>';
  etapas.forEach(()=>{{
    html+='<th>T</th><th class="pivot-mkt">M</th>';
  }});
  html+='</tr></thead><tbody>';

  // Total row
  const allLeads = filterLeads(DATA.leads);
  html += pivotRow('TOTAL (todos)', null, allLeads, etapas, true);

  // Per course
  cursos.forEach(c=>{{
    const cl = allLeads.filter(l=>l.c===c);
    html += pivotRow(shorts[c]||c, c, cl, etapas, false);
  }});

  html+='</tbody></table></div>';
  document.getElementById('tbl-pivote-wrap').innerHTML=html;
}}

function pivotRow(label, curso, leads, etapas, isTotals) {{
  const inv  = DATA.inversion.find(i=>i.curso===curso);
  const imp  = inv?.disponible ? inv.importe : null;
  const mkt  = leads.filter(l=>l.m===1).length;
  const ins  = cntEtapa(leads,'Inscrito');
  const conv = leads.length>0 ? ins/leads.length : 0;
  const cpl  = imp && mkt>0 ? round2(imp/mkt) : null;
  const style= isTotals ? ' class="tfoot"':'';

  let row=`<tr${{style}}>`;
  row+=`<td><b>${{label}}</b></td>`;
  etapas.forEach(e=>{{
    row+=`<td>${{cntEtapa(leads,e)}}</td>`;
    row+=`<td class="pivot-mkt" style="color:#0077aa;font-size:.75rem">${{cntEtapa(leads,e,1)}}</td>`;
  }});
  row+=`<td><b>${{fmt(leads.length)}}</b></td>`;
  row+=`<td style="color:var(--green)"><b>${{fmt(ins)}}</b></td>`;
  row+=`<td>${{fmtP(conv)}}</td>`;
  row+=`<td>${{imp!=null?fmtE(imp):'—'}}</td>`;
  row+=`<td>${{cpl!=null?fmtE(cpl):'—'}}</td>`;
  row+='</tr>';
  return row;
}}

// ── TABLE BUILDER ─────────────────────────────────────────────────────────
function buildTable(headers, rows, footerRows=[]) {{
  if (!rows.length) return emptyState();
  let s = sortState[headers[0]] || {{}};
  let html = '<table><thead><tr>';
  headers.forEach((h,i)=>{{
    const cls = s.col===i?(s.dir?'asc':'desc'):'';
    html+=`<th class="${{cls}}" onclick="sortTable(this,${{i}})">
      ${{h}}<span class="sort-arrow"></span></th>`;
  }});
  html+='</tr></thead><tbody>';
  rows.forEach(r=>{{
    html+='<tr>'+r.map(c=>`<td>${{c}}</td>`).join('')+'</tr>';
  }});
  if (footerRows.length) {{
    footerRows.forEach(fr=>{{
      html+='<tr class="tfoot"><td></td>'+fr.map(c=>`<td>${{c}}</td>`).join('')+'</tr>';
    }});
  }}
  html+='</tbody></table>';
  return html;
}}

const sortState = {{}};
function sortTable(th, colIndex) {{
  const table = th.closest('table');
  const key   = th.closest('table').parentElement.id||'t';
  if (!sortState[key]) sortState[key]={{col:-1,dir:true}};
  const st = sortState[key];
  const asc = st.col===colIndex ? !st.dir : true;
  st.col=colIndex; st.dir=asc;

  const tbody = table.querySelector('tbody');
  const rows  = Array.from(tbody.querySelectorAll('tr:not(.tfoot)'));
  rows.sort((a,b)=>{{
    const av = a.cells[colIndex]?.textContent.trim()||'';
    const bv = b.cells[colIndex]?.textContent.trim()||'';
    const an = parseFloat(av.replace(/[^0-9.-]/g,''));
    const bn = parseFloat(bv.replace(/[^0-9.-]/g,''));
    if (!isNaN(an)&&!isNaN(bn)) return asc?an-bn:bn-an;
    return asc?av.localeCompare(bv,'es'):bv.localeCompare(av,'es');
  }});
  rows.forEach(r=>tbody.appendChild(r));

  table.querySelectorAll('th').forEach((t,i)=>{{
    t.className=i===colIndex?(asc?'asc':'desc'):'';
  }});
}}

// ── KPI CARD BUILDER ──────────────────────────────────────────────────────
function kpiCard(label, value, sub='', color='blue') {{
  return `<div class="kpi-card ${{color}}">
    <div class="kpi-label">${{label}}</div>
    <div class="kpi-value ${{value.length>6?'small':''}}">${{value}}</div>
    ${{sub?`<div class="kpi-sub">${{sub}}</div>`:''}}
  </div>`;
}}

function kpiCardSm(label, value, sub='', color='blue') {{
  return `<div class="kpi-card-sm ${{color}}">
    <div class="kpi-label">${{label}}</div>
    <div class="kpi-value">${{value}}</div>
    ${{sub?`<div class="kpi-sub">${{sub}}</div>`:''}}
  </div>`;
}}

function emptyState() {{
  return `<div class="empty">
    <div class="ico">🔍</div>
    <div class="msg">Sin datos con los filtros aplicados</div>
    <button class="btn" onclick="clearFilters()">Limpiar filtros</button>
  </div>`;
}}

// ── NAVIGATION ────────────────────────────────────────────────────────────
function navClick(el) {{
  document.querySelectorAll('.nav-item').forEach(n=>n.classList.remove('active'));
  el.classList.add('active');
  const targetId = el.getAttribute('href').replace('#','');
  document.querySelectorAll('.section').forEach(s=>{{
    s.style.display = s.id===targetId ? 'block' : 'none';
  }});
  window.scrollTo({{top:0,behavior:'smooth'}});
  closeSidebar();
}}

function toggleSidebar() {{
  document.getElementById('sidebar').classList.toggle('open');
  document.getElementById('overlay').classList.toggle('active');
}}
function closeSidebar() {{
  document.getElementById('sidebar').classList.remove('open');
  document.getElementById('overlay').classList.remove('active');
}}

// Intersection observer for active nav
const observer = new IntersectionObserver(entries=>{{
  entries.forEach(e=>{{
    if(e.isIntersecting){{
      const id=e.target.id;
      document.querySelectorAll('.nav-item').forEach(n=>{{
        n.classList.toggle('active', n.getAttribute('href')==='#'+id);
      }});
    }}
  }});
}}, {{threshold:.3,rootMargin:'-80px 0px -60% 0px'}});

// ── BOOT ──────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', ()=>{{
  initFilters();
  renderAll();
  // Panel mode: show only first section on load
  const sections = document.querySelectorAll('.section');
  sections.forEach((s,i)=>{{ s.style.display = i===0?'block':'none'; }});
  // Observer not needed in panel mode
}});
</script>
</body>
</html>"""

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    DIR_OUT.mkdir(parents=True, exist_ok=True)

    print("\n🚀 Generando dashboard Estudios Rafer...\n")
    print("📂 Cargando seguimiento:")
    df_seg = cargar_seguimiento()

    print("\n📂 Cargando inversión:")
    df_inv = cargar_inversion()

    print("\n⚙️  Procesando datos...")
    data = construir_json(df_seg, df_inv)

    print("🖼️  Cargando assets...")
    logo   = logo_b64()
    chartjs = fetch_chartjs()
    fontcss = fetch_dmfont()

    data_json = json.dumps(data, ensure_ascii=False, separators=(',',':'))

    print("📄 Construyendo HTML...")
    html = html_template(data_json, logo, chartjs, fontcss)

    # Output filename
    fecha_str = data['meta']['fecha_act'].replace('-','_')
    # Convert YYYY_MM_DD → DD_MM_YYYY for filename
    partes = fecha_str.split('_')
    if len(partes)==3:
        fecha_fn = f"{partes[2]}_{partes[1]}_{partes[0]}"
    else:
        fecha_fn = fecha_str

    out = DIR_OUT / f"dashboard_marketing_{fecha_fn}.html"
    out.write_text(html, encoding='utf-8')

    size_kb = round(out.stat().st_size/1024)
    print(f"\n✅ Dashboard generado: {out}")
    print(f"   Tamaño: {size_kb} KB")
    print(f"   Leads cargados: {data['meta']['n_leads']}")
    print(f"   Fechas en histórico: {len(data['meta']['fechas'])}")
    print(f"   Inversión: {data['meta']['n_inv_ok']}/{data['meta']['n_cursos']} cursos\n")

if __name__ == '__main__':
    main()