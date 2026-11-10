"""Веб-дашборд бота: HTTP + SSE поверх aiohttp (без новых зависимостей).

Модуль — чистый транспорт и представление: он не принимает торговых
решений и не лезет в сеть Solana. Один снимок состояния собирает
:class:`~core.pipeline.LivePipeline.dashboard_snapshot`, сервер раз в
``DASHBOARD_INTERVAL_SECONDS`` пушит его подписчикам через Server-Sent
Events, страница рисует панели: сессия, позиции с нереализованным PnL,
покупки/продажи, журнал сделок, отбор, латентность и хвост лога.

Приватность по умолчанию: сервер слушает ``127.0.0.1`` — наружу не
торчит; на VPS смотреть через ``ssh -L 8787:127.0.0.1:8787`` либо
осознанно выставить ``DASHBOARD_HOST=0.0.0.0`` (тогда закрыть порт
фаерволом — аутентификации на странице нет).

Занятый порт не валит бота: предупреждение и продолжение без дашборда.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import secrets
import time
from collections import deque
from collections.abc import Awaitable, Callable
from typing import Any

from aiohttp import web
from loguru import logger as _root_logger

from utils.logger import get_logger

logger = get_logger()

#: Провайдер снимка состояния (пайплайн).
SnapshotProvider = Callable[[], dict[str, Any]]

#: Обработчик ручного действия из дашборда (sell / average / sell_all).
ActionHandler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]

#: Обработчик запуска бэктеста (sample / csv / mint).
BacktestRunner = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]

#: Обработчик полной остановки бота (запрошенной со страницы).
ShutdownHandler = Callable[[], Awaitable[None]]

#: Время жизни сессионного токена, c (12 часов).
SESSION_TTL_SECONDS: float = 12 * 3600

#: HTML-страница дашборда: без сборки, без CDN — всё в одном файле.
_PAGE: str = """<!doctype html>
<html lang="ru"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Pump Scalper — дашборд</title>
<style>
:root{--bg:#0d1117;--card:#161b22;--line:#30363d;--fg:#c9d1d9;--dim:#8b949e;
--green:#3fb950;--red:#f85149;--blue:#58a6ff;--yellow:#d29922}
*{box-sizing:border-box}
body{background:var(--bg);color:var(--fg);font:14px/1.45 "Segoe UI",system-ui,sans-serif;margin:0;padding:14px}
header{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:12px}
h1{font-size:18px;margin:0}
.badge{border:1px solid var(--line);border-radius:12px;padding:2px 10px;font-size:12px;color:var(--dim)}
.badge.live{color:var(--red);border-color:var(--red)}
.badge.dry{color:var(--blue);border-color:var(--blue)}
.badge.ok{color:var(--green)}
#conn{margin-left:auto;font-size:12px;color:var(--dim)}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(330px,1fr));gap:12px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px 14px}
.card h2{font-size:12px;text-transform:uppercase;letter-spacing:.08em;color:var(--dim);margin:0 0 10px}
.wide{grid-column:1/-1}
.big{font-size:30px;font-weight:600;margin:4px 0}
.row{display:flex;justify-content:space-between;gap:10px;padding:2px 0}
.row .k{color:var(--dim)}
table{width:100%;border-collapse:collapse;font-size:13px}
td,th{padding:4px 6px;border-bottom:1px solid #21262d;text-align:left}
th{color:var(--dim);font-weight:500}
.pos{color:var(--green)}.neg{color:var(--red)}.warn{color:var(--yellow)}
.bar{height:8px;background:#21262d;border-radius:4px;overflow:hidden;margin:2px 0 8px}
.bar>i{display:block;height:100%}
pre{margin:0;max-height:460px;overflow:auto;font:12px/1.5 Consolas,monospace;white-space:pre-wrap}
code{color:var(--blue)}
a.mintlink{text-decoration:none}
a.mintlink:hover{text-decoration:underline;text-decoration-color:var(--blue)}
svg{width:100%;height:90px;display:block}
ul{margin:6px 0 0;padding-left:18px}
li{margin:2px 0}
.btn{border:1px solid var(--line);border-radius:6px;background:#21262d;color:var(--fg);
padding:4px 10px;font-size:12px;cursor:pointer}
.btn:hover{background:#2d333b}
.btn.danger{border-color:var(--red);color:var(--red);font-weight:600;padding:8px 18px;font-size:14px}
.btn.mini{padding:2px 8px;font-size:11px;margin-right:4px}
.btn.mini.warn{color:var(--yellow);border-color:var(--yellow)}
.btn:disabled{opacity:.45;cursor:default}
#login{position:fixed;inset:0;background:rgba(1,4,9,.92);display:flex;align-items:center;justify-content:center;z-index:10}
#login .box{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:24px 28px;width:280px}
#login h2{margin:0 0 14px;font-size:16px}
#login input{width:100%;margin-bottom:10px;padding:8px 10px;background:#0d1117;border:1px solid var(--line);
border-radius:6px;color:var(--fg);font-size:13px}
#loginerr{color:var(--red);font-size:12px;min-height:16px}
#action_status{font-size:12px;color:var(--dim);margin-left:10px}
#log.frozen{outline:1px solid var(--blue)}
.badge.plain{border:none;padding:2px 0;border-radius:0}
#network{cursor:pointer}
#network.live{border-color:var(--blue);color:var(--blue)}
#network:hover{border-color:#fff;color:#fff}
.btn.mini.run{color:var(--green);border-color:var(--green)}
.btn.bt-run,.btn.bt-stop{padding:8px 18px;font-size:14px;font-weight:700}
.btn.bt-run{color:var(--green);border-color:var(--green)}
.btn.bt-stop{color:var(--red);border-color:var(--red)}
#backtest{display:none}
.btrow{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin:6px 0}
.btrow label{color:var(--dim);font-size:12px}
.btrow input,.btrow textarea{background:#0d1117;border:1px solid var(--line);
border-radius:6px;color:var(--fg);padding:6px 8px;font-size:13px}
.btrow textarea{width:100%;font:12px/1.4 Consolas,monospace}
.modebadge{cursor:pointer;border:1px solid var(--line);border-radius:12px;
padding:3px 12px;font-size:12px;color:var(--dim)}
.modebadge.active{color:#fff;border-color:var(--blue);background:#1f6feb33}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:8px;margin:10px 0}
.minicard{background:#0d1117;border:1px solid var(--line);border-radius:8px;padding:8px 10px}
.minicard .k{font-size:11px;color:var(--dim)}
.minicard .v{font-size:18px;font-weight:600}
.reasonbar{display:flex;align-items:center;gap:8px;margin:3px 0;font-size:12px}
.reasonbar .bar{flex:1;margin:0}
.poshead{display:flex;align-items:center;gap:10px;margin-bottom:10px}
.poshead h2{margin:0}
.poshead .spacer{flex:1}
</style></head><body>
<div id="login" style="display:none">
  <div class="box">
    <h2>Вход в дашборд</h2>
    <input id="lu" placeholder="логин" autocomplete="username">
    <input id="lp" type="password" placeholder="пароль" autocomplete="current-password">
    <div id="loginerr"></div>
    <button class="btn" style="width:100%" id="loginbtn">Войти</button>
  </div>
</div>
<header>
  <h1>PUMP SCALPER</h1>
  <span class="badge" id="mode">…</span>
  <span class="badge" id="network">…</span>
  <span class="badge plain" id="venue">…</span>
  <span class="badge plain" id="uptime">…</span>
  <span class="badge plain" id="poscount">…</span>
  <span id="conn">подключение…</span>
  <span style="margin-left:auto"><button class="btn mini run" id="botstart" disabled title="Возобновить торговлю после остановки">Старт бота</button> <button class="btn mini danger" id="botstoptrading" title="Остановить торговлю (дашборд продолжает работать; открытые позиции — без управления)">Остановить торговлю</button> <button class="btn mini danger" id="botkill" title="Полностью остановить процесс бота (дашборд тоже выключится)">ОСТАНОВИТЬ БОТА</button> <button class="btn mini" id="logout">выйти</button></span>
</header>
<main class="grid">
  <section class="card"><h2>Сессия</h2><div id="session"></div></section>
  <section class="card"><h2>PnL (реализованный)</h2>
    <div class="big" id="pnl">—</div>
    <svg id="chart" viewBox="0 0 300 80" preserveAspectRatio="none"></svg>
    <div class="row"><span class="k">нереализованный</span><span id="unreal">—</span></div>
  </section>
  <section class="card wide"><div class="poshead">
      <h2>Открытые позиции</h2>
      <span class="spacer"></span>
      <span id="action_status"></span>
      <button class="btn danger" id="sellall">ПРОДАТЬ ВСЁ</button>
    </div>
    <div id="positions">—</div>
  </section>
  <section class="card"><h2>Поток сделок</h2><div id="stream"></div></section>
  <section class="card"><h2>Отбор токенов</h2><div id="screen"></div></section>
  <section class="card"><h2>Латентность (мс, p50 / p95)</h2><div id="latency">—</div></section>
    <section class="card wide">
    <div class="poshead">
      <h2>Бэктест стратегии на исторических сделках</h2>
      <span class="spacer"></span>
      <button class="btn" id="backtab">развернуть ▸</button>
    </div>
    <div id="backtest" style="display:none">
      <div class="btrow">
        <span class="modebadge active" id="bt_mode_mint" data-mode="mint">Ссылка pump.fun</span>
        <span class="modebadge" id="bt_mode_csv" data-mode="csv">CSV-текст</span>
        <span style="color:var(--dim);font-size:12px">движок: живая стратегия + dry-run исполнитель на исторических сделках</span>
      </div>
      <div class="btrow" id="bt_url_row">
        <label>ссылка / mint:</label>
        <input id="bt_url" style="flex:1;min-width:280px" placeholder="https://pump.fun/coin/<MINT> или адрес mint">
        <label>транзакций:</label>
        <input id="bt_maxtx" type="number" value="500" min="50" max="1000" style="width:90px">
      </div>
      <div id="bt_csv_row" style="display:none">
        <div class="btrow">
          <label>начальный резерв SOL:</label>
          <input id="bt_initsol" type="number" value="90" style="width:90px">
          <label>резерв токенов:</label>
          <input id="bt_inittoken" type="number" value="600000000" style="width:150px">
        </div>
        <textarea id="bt_csv" rows="6" placeholder="signature,pool_address,base_mint,dex,side,sol_amount,token_amount,wallet,timestamp"></textarea>
      </div>
      <div class="btrow">
        <label>размер входа, SOL:</label>
        <input id="bt_amount" type="number" value="0.5" step="0.1" style="width:80px">
        <label>окно входа, c:</label>
        <input id="bt_entryw" type="number" value="5" style="width:60px">
        <label>окно выхода, c:</label>
        <input id="bt_exitw" type="number" value="2" style="width:60px">
        <label>макс. позиций:</label>
        <input id="bt_maxpos" type="number" value="1" style="width:50px">
        <span class="spacer"></span>
        <span id="bt_status" style="color:var(--dim);font-size:12px"></span>
        <button class="btn mini" id="bt_sample" title="Заполнить форму демонстрационным CSV">демо-CSV</button>
        <button class="btn bt-run" id="bt_run">ЗАПУСТИТЬ</button>
        <button class="btn bt-stop" id="bt_stop" disabled title="Остановить прогон и показать статистику по пройденной части">ОСТАНОВИТЬ</button>
      </div>
      <div id="bt_results"></div>
    </div>
  </section>

<section class="card wide"><div class="poshead"><h2>Лог (хвост)</h2><span class="spacer"></span><button class="btn mini" id="logclear">Очистить лог</button></div><pre id="log">—</pre></section>

</main>
<script>
const $=id=>document.getElementById(id);
const fmt=(n,d=2)=>Number(n).toLocaleString('ru-RU',{minimumFractionDigits:d,maximumFractionDigits:d});
const price=p=>p>0?p.toExponential(2):'—';
const cls=v=>v>0?'pos':v<0?'neg':'';
const sgn=(v,d=2)=>(v>0?'+':'')+fmt(v,d);
const esc=s=>{const d=document.createElement('div');d.textContent=String(s);return d.innerHTML};
const dur=s=>{s=Math.max(0,Math.round(s));const m=Math.floor(s/60),h=Math.floor(m/60);
 return h?`${h}ч ${m%60}м`:m?`${m}м ${s%60}с`:`${s}с`};
const short=m=>m&&m.length>8?m.slice(0,8)+'…':m;
const pnlTxt=v=>`<span class="${cls(v)}">${sgn(v)} SOL</span>`;
const pctTxt=v=>`<span class="${cls(v)}">${sgn(v,1)}%</span>`;

function render(d){
  const s=d.session||{};
  const trading=(s.trading==='running');
  if($('botstart'))$('botstart').disabled=trading;
  if($('botstoptrading'))$('botstoptrading').disabled=!trading;
  currentMode=(s.mode==='live')?'live':'dry';
  const mode=$('mode');
  mode.textContent=s.mode||'—';
  mode.className='badge '+(s.mode==='live'?'live':'dry');
  mode.title='Текущий режим исполнения';
  const net=$('network');
  net.textContent=s.network||'—';
  net.className='badge'+((currentMode==='live')?' live':'');
  net.title=(currentMode==='live')
    ?'Активная торговля на mainnet — нажмите, чтобы вернуться в DRY-RUN'
    :'Нажмите — переключить в LIVE (реальный SOL)';
  $('venue').textContent='venue: '+(s.venue||'—');
  $('uptime').textContent='uptime '+dur(s.uptime_s||0);
  $('poscount').textContent=`позиции ${s.positions_open??0}/${s.positions_limit??0}`;
  $('poscount').className='badge plain'+((s.positions_open||0)>0?' ok':'');

  const st=d.stats||{};
  const wr=st.win_rate_pct??0;
  $('session').innerHTML=`
   <div class="row"><span class="k">закрыто сделок</span><span>${st.closed??0} (win ${st.wins??0} / loss ${st.losses??0})</span></div>
   <div class="row"><span class="k">win-rate</span><span>${fmt(wr,1)}%</span></div>
   <div class="bar"><i style="width:${Math.min(100,wr)}%;background:${wr>=50?'var(--green)':'var(--yellow)'}"></i></div>
   <div class="row"><span class="k">лучший / худший</span><span><span class="pos">${sgn(st.best_pct??0,1)}%</span> / <span class="neg">${sgn(st.worst_pct??0,1)}%</span></span></div>
   <div class="row"><span class="k">ср. холд</span><span>${dur(st.avg_hold_seconds||0)}</span></div>
   <div class="row"><span class="k">ордера</span><span>${st.orders_ok??0} ок / <span class="${(st.orders_fail||0)>0?'neg':''}">${st.orders_fail??0} сбой</span></span></div>
   <div class="row"><span class="k">входы / выходы</span><span>${st.entries??0} / ${st.exits??0}</span></div>
   <div class="row"><span class="k">резерв</span><span>${fmt(s.reserved_sol||0,3)}${s.capital_sol?` / ${fmt(s.capital_sol,2)}`:''} SOL</span></div>`;

  $('pnl').innerHTML=pnlTxt(st.pnl_sol||0)+` <small class="${cls(st.pnl_pct||0)}">${sgn(st.pnl_pct||0,1)}%</small>`;
  const un=d.unrealized_sol||0;
  $('unreal').innerHTML=`<span class="${cls(un)}">${sgn(un)} SOL</span>`;
  drawChart(d.pnl_curve||[]);

  const rows=(d.positions||[]).map(p=>`<tr>
    <td><a class="mintlink" href="https://pump.fun/coin/${esc(p.base_mint)}" target="_blank" rel="noopener noreferrer" title="Открыть график на pump.fun"><code title="${esc(p.base_mint)}">${esc(short(p.base_mint))}</code></a></td>
    <td>${p.stage}</td><td>${fmt(p.pool_sol||0,1)}</td><td>${fmt(p.volume_sol||0,1)}</td>
    <td>${fmt(p.entry_sol,3)}</td><td>${fmt(p.token_amount,0)}</td>
    <td>${price(p.entry_price)}</td><td>${price(p.current_price)}</td>
    <td class="${cls(p.pnl_pct)}">${sgn(p.pnl_pct,1)}%</td>
    <td>${price(p.peak_price)}</td><td>${dur(p.hold_seconds)}</td>
    <td>${p.tp1_taken?'✔':'—'}</td><td>${p.stop_loss_pct>0?p.stop_loss_pct+'%':'30%'}</td>
    <td><button class="btn mini" data-act="sell" data-mint="${esc(p.base_mint)}">✕ закрыть</button><button class="btn mini warn" data-act="average" data-mint="${esc(p.base_mint)}">＋ усреднить</button></td></tr>`).join('');
  $('positions').innerHTML=rows
    ?`<table><tr><th>mint</th><th>стадия</th><th>ликв. SOL</th><th>объём SOL</th><th>вход</th><th>токены</th><th>вход цена</th><th>тек.</th><th>PnL</th><th>пик</th><th>холд</th><th>TP1</th><th>стоп</th><th>ручное</th></tr>${rows}</table>`
    :'—';

  const fl=d.stream||{};
  const total=(fl.buys||0)+(fl.sells||0)||1;
  const bar=(v,color)=>`<div class="bar"><i style="width:${Math.min(100,100*v/total)}%;background:${color}"></i></div>`;
  $('stream').innerHTML=`
   <div class="row"><span class="k">события</span><span>${fmt(fl.events||0,0)} (~${fmt(fl.events_per_sec||0,1)}/с)</span></div>
   <div class="row"><span class="k">покупки</span><span>${fmt(fl.buys||0,0)}</span></div>${bar(fl.buys||0,'var(--green)')}
   <div class="row"><span class="k">продажи</span><span>${fmt(fl.sells||0,0)}</span></div>${bar(fl.sells||0,'var(--red)')}
   <div class="row"><span class="k">пулов в работе</span><span>${fl.pools??0}</span></div>
   <div class="row"><span class="k">очередь / потерь</span><span>${fl.queue_size??0}/${fl.dropped??0}</span></div>
   <div class="row"><span class="k">ошибки разбора</span><span class="${(fl.parse_errors||0)>0?'warn':''}">${fl.parse_errors??0}</span></div>`;

  const sc=d.screening||{};
  const hr=Object.entries(sc.hold_reasons||{});
  const hrHtml=hr.length
    ?`<div class="k" style="margin-top:8px">почему нет входа (воронка фильтров):</div>`+
     hr.map(([k,v])=>`<div class="row"><span class="k">${esc(k)}</span><span>${v}</span></div>`).join('')
    :'';
  $('screen').innerHTML=`
   <div class="row"><span class="k">ждут проверки</span><span>${sc.pending??0}</span></div>
   <div class="row"><span class="k">одобрено</span><span class="pos">${sc.approved??0}</span></div>
   <div class="row"><span class="k">отклонено</span><span class="neg">${sc.rejected??0}</span></div>
   ${hrHtml}`;

  const lat=(d.stream&&d.stream.latency)||{};
  $('latency').innerHTML=Object.keys(lat).length
    ?`<table><tr><th>этап</th><th>p50</th><th>p95</th><th>n</th></tr>${Object.entries(lat).map(([k,v])=>
      `<tr><td>${esc(k)}</td><td>${fmt(v.p50_ms||0,1)}</td><td>${fmt(v.p95_ms||0,1)}</td><td>${v.count??0}</td></tr>`).join('')}</table>`
    :'—';

  updateLog(d.logs||[]);
}

function drawChart(curve){
  const svg=document.getElementById('chart');
  if(!curve||curve.length<2){svg.innerHTML='<text x="8" y="45" fill="#8b949e" font-size="11">мало данных</text>';return}
  const vals=curve.map(p=>p[1]);
  const min=Math.min(...vals,0),max=Math.max(...vals,0);
  const span=(max-min)||1;
  const pts=curve.map((p,i)=>{
    const x=300*i/(curve.length-1);
    const y=76-(p[1]-min)/span*68-4;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(' ');
  const zero=76-(0-min)/span*68-4;
  svg.innerHTML=`<line x1="0" y1="${zero}" x2="300" y2="${zero}" stroke="#30363d" stroke-dasharray="3 3"/>
   <polyline points="${pts}" fill="none" stroke="${vals[vals.length-1]>=0?'#3fb950':'#f85149'}" stroke-width="1.6"/>`;
}

let TOKEN=localStorage.getItem('dash_token')||'';
let es=null,busy=false;

function authHeaders(extra={}){return Object.assign({},extra,TOKEN?{'Authorization':'Bearer '+TOKEN}:{})}

async function api(path,opts={}){return fetch(path,Object.assign({},opts,{headers:authHeaders(opts.headers||{})}))}

async function refreshOnce(){try{const r=await api('/api/snapshot');if(r.status===401){showLogin();return}const d=await r.json();render(d)}catch(_){}}

function connect(){
  if(es)es.close();
  es=new EventSource('/api/stream'+(TOKEN?('?token='+encodeURIComponent(TOKEN)):''));
  es.onmessage=e=>{try{render(JSON.parse(e.data))}catch(_){}}
  es.onopen=()=>{$('conn').textContent=''};
  es.onerror=()=>{$('conn').textContent='переподключение…';refreshOnce()};
}

function showLogin(){$('login').style.display='flex'}
function hideLogin(){$('login').style.display='none';$('loginerr').textContent=''}

$('loginbtn').onclick=async()=>{
  const r=await fetch('/api/login',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({user:$('lu').value.trim(),password:$('lp').value})});
  const d=await r.json().catch(()=>({}));
  if(d.ok&&d.token){TOKEN=d.token;localStorage.setItem('dash_token',TOKEN);hideLogin();connect();refreshOnce()}
  else{$('loginerr').textContent=d.error||'неверный логин или пароль'}
};
$('network').onclick=async()=>{
  const target=(currentMode==='live')?'dry':'live';
  const msg=(target==='live')
   ?'ПЕРЕХОД В LIVE: ордера будут исполняться РЕАЛЬНЫМ SOL с вашего кошелька; нужны настроенный кошелёк и закрытые открытые позиции. Перевести?'
   :'ПЕРЕХОД В DRY-RUN: ордера перестанут исполняться реально — бумажная торговля. Перевести?';
  if(!confirm(msg))return;
  const net=$('network');
  net.style.opacity='0.5';
  try{
    const r=await api('/api/action',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({action:'bot_mode',mode:target})});
    if(r.status===401){showLogin();return}
    const d=await r.json();
    if(!d.ok){$('conn').textContent='ошибка: '+(d.error||'?')}
  }catch(e){}
  net.style.opacity='1';
  refreshOnce();
};
$('botstart').onclick=async()=>{
  if(!confirm('Возобновить торговлю?'))return;
  $('botstart').disabled=true;
  try{
    const r=await api('/api/action',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({action:'bot_start'})});
    if(r.status===401){showLogin();return}
    const d=await r.json();
    if(!d.ok){$('conn').textContent='ошибка старта: '+(d.error||'?')}
  }catch(e){}
};
$('botstoptrading').onclick=async()=>{
  if(!confirm('Остановить торговлю? Открытые позиции останутся БЕЗ управления (TP/SL не сработают), пока не нажмёте «Старт бота». Дашборд продолжит работать.'))return;
  $('botstoptrading').disabled=true;
  try{
    const r=await api('/api/action',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({action:'bot_stop'})});
    if(r.status===401){showLogin();return}
  }catch(e){}
  refreshOnce();
};
$('botkill').onclick=async()=>{
  if(!confirm('ПОЛНОСТЬЮ остановить бота? Дашборд выключится вместе с ним. Перезапуск: systemctl start pumpbot (или вручную).'))return;
  $('botkill').disabled=true;
  try{
    const r=await api('/api/shutdown',{method:'POST'});
    if(r.status===401){showLogin();return}
  }catch(e){}
  if(es)es.close();
  $('conn').textContent='⏹ бот остановлен — процесс завершён. Перезапуск: systemctl start pumpbot (VPS) или python main.py (локально)';
  $('conn').style.color='var(--red)';
  $('botstart').disabled=true;$('botstoptrading').disabled=true;
  $('botkill').disabled=true;$('logout').disabled=true;
};
$('logout').onclick=()=>{TOKEN='';localStorage.removeItem('dash_token');if(es)es.close();showLogin()};

const logEl=$('log');
function selectionInLog(){
  const s=window.getSelection();
  return !!(s && s.rangeCount>0 && !s.isCollapsed && logEl.contains(s.anchorNode));
}
function updateLog(lines){
  if(selectionInLog()){
    logEl.classList.add('frozen');
    return;
  }
  logEl.classList.remove('frozen');
  const atBottom=logEl.scrollHeight-logEl.scrollTop-logEl.clientHeight<8;
  const top=logEl.scrollTop;
  logEl.textContent=lines.join(String.fromCharCode(10))||'—';
  logEl.scrollTop=atBottom?logEl.scrollHeight:top;
}
$('logclear').onclick=async()=>{
  try{
    const r=await api('/api/log/clear',{method:'POST'});
    if(r.status===401){showLogin();return}
    logEl.textContent='—';
  }catch(e){}
};
$('sellall').onclick=async()=>{
  if(busy)return;
  if(!confirm('Продать ВСЕ открытые позиции прямо сейчас?'))return;
  busy=true;$('sellall').disabled=true;$('action_status').textContent='отправка…';
  try{
    const r=await api('/api/action',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({action:'sell_all'})});
    if(r.status===401){showLogin();return}
    const d=await r.json();
    const failed=(d.results||[]).filter(x=>!x.ok);
    $('action_status').textContent=d.ok
      ?'все позиции закрыты'
      :`готово с ошибками: ${failed.map(x=>short(x.mint||'')+' — '+(x.error||'?')).join('; ')}`;
    if(d.ok)refreshOnce();
  }catch(e){$('action_status').textContent='ошибка сети'}
  busy=false;$('sellall').disabled=false;
};

$('positions').onclick=async e=>{
  const btn=e.target.closest('button[data-act]');
  if(!btn||busy)return;
  const act=btn.dataset.act,mint=btn.dataset.mint;
  const ask=act==='sell'
    ?`Закрыть позицию ${short(mint)} (продать все токены по рынку)?`
    :`Усредниться в ${short(mint)} (докупить объём TRADE_AMOUNT_SOL)?`;
  if(!confirm(ask))return;
  busy=true;document.querySelectorAll('button[data-act]').forEach(b=>b.disabled=true);
  $('action_status').textContent='отправка…';
  try{
    const r=await api('/api/action',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({action:act,mint})});
    if(r.status===401){showLogin();return}
    const d=await r.json();
    $('action_status').textContent=d.ok
      ?`${act==='sell'?'позиция закрыта':'позиция усреднена'}: ${short(mint)}`
      :`ошибка: ${d.error||'неизвестно'}`;
    if(d.ok)refreshOnce();
  }catch(err){$('action_status').textContent='ошибка сети'}
  busy=false;document.querySelectorAll('button[data-act]').forEach(b=>b.disabled=false);
};

// --- Backtest ---
let btMode='mint',btBusy=false,btRunId='',currentMode='dry';
function setBtMode(m){btMode=m;
 $('bt_mode_mint').classList.toggle('active',m==='mint');
 $('bt_mode_csv').classList.toggle('active',m==='csv');
 $('bt_url_row').style.display=m==='mint'?'flex':'none';
 $('bt_csv_row').style.display=m==='csv'?'block':'none';}
$('bt_mode_mint').onclick=()=>setBtMode('mint');
$('bt_mode_csv').onclick=()=>setBtMode('csv');
$('backtab').onclick=()=>{const b=$('backtest');const show=b.style.display==='none';
 b.style.display=show?'block':'none';
 $('backtab').textContent=show?'свернуть ▴':'развернуть ▸';};
$('bt_sample').onclick=async()=>{try{
  const r=await api('/api/backtest/sample');if(r.status===401){showLogin();return}
  setBtMode('csv');$('bt_csv').value=await r.text();
  $('bt_status').textContent='демо-CSV загружен — жмите ЗАПУСТИТЬ';
 }catch(e){$('bt_status').textContent='ошибка загрузки демо'}};

function btCollect(){const p={mode:btMode,
 trade_amount_sol:+$('bt_amount').value||0.5,
 entry_window_seconds:+$('bt_entryw').value||5,
 exit_window_seconds:+$('bt_exitw').value||2,
 max_positions:+$('bt_maxpos').value||1};
 if(btMode==='mint'){p.url=$('bt_url').value.trim();p.max_tx=+$('bt_maxtx').value||500}
 else{p.csv=$('bt_csv').value;p.initial_sol=+$('bt_initsol').value||90;
  p.initial_token=+$('bt_inittoken').value||600000000}
 return p}

function renderBacktest(r){
 const cards=[['сделок',r.trades],['win-rate',fmt(r.win_rate_pct,1)+'%'],
  ['PnL',sgn(r.pnl_sol,3)+' SOL'],['PnL %',sgn(r.pnl_pct,1)+'%'],
  ['макс. просадка',fmt(r.max_drawdown_sol,3)+' SOL'],
  ['ср. холд',dur(r.avg_hold_seconds)],['лучший',sgn(r.best_trade_pct,1)+'%'],
  ['худший',sgn(r.worst_trade_pct,1)+'%']];
 const total=Object.values(r.exit_reasons||{}).reduce((a,b)=>a+b,0)||1;
 const reasons=Object.entries(r.exit_reasons||{}).map(([k,v])=>
  `<div class="reasonbar"><span style="width:160px">${esc(k)}</span>
   <span class="bar"><i style="width:${100*v/total}%;background:var(--blue)"></i></span><span>${v}</span></div>`).join('')||'—';
 const rows=(r.closed_trades||[]).map(tr=>`<tr>
  <td><code title="${esc(tr.base_mint)}">${esc(short(tr.base_mint))}</code></td>
  <td>${esc(tr.exit_reason)}</td>
  <td class="${cls(tr.pnl_pct)}">${sgn(tr.pnl_pct,1)}%</td>
  <td class="${cls(tr.pnl_sol)}">${sgn(tr.pnl_sol,3)}</td>
  <td>${dur(tr.hold_seconds)}</td>
  <td>${price(tr.entry_price)} → ${price(tr.exit_price)}</td></tr>`).join('');
 $('bt_results').innerHTML=`
  <div class="cards">${cards.map(([k,v])=>`<div class="minicard"><div class="k">${k}</div><div class="v">${v}</div></div>`).join('')}</div>
  <h2 style="margin:8px 0">Причины выхода</h2>${reasons}
  ${rows?`<h2 style="margin:8px 0">Сделки</h2><table><tr><th>токен</th><th>причина</th><th>PnL%</th><th>PnL SOL</th><th>холд</th><th>вход → выход</th></tr>${rows}</table>`:'<div style="color:var(--dim)">сделок не было — пороги входа не сработали на этих данных</div>'}`;
}

$('bt_run').onclick=async()=>{
 if(btBusy||busy)return;
 busy=true;btBusy=true;$('bt_run').disabled=true;$('bt_stop').disabled=false;
 const p=btCollect();
 btRunId=Date.now()+'-'+Math.random().toString(36).slice(2,7);
 p.run_id=btRunId;
 $('bt_status').textContent='гоняю бэктест… (большие истории могут занять время — есть кнопка ОСТАНОВИТЬ)';
 $('bt_results').innerHTML='';
 try{
  const r=await api('/api/backtest',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(p)});
  if(r.status===401){showLogin();return}
  const d=await r.json();
  if(!d.ok){$('bt_status').innerHTML=`<span class="neg">${esc(d.error||'ошибка')}</span>`}
  else{
   const rep=d.report;
   $('bt_status').textContent=(rep.stopped?'⏹ остановлено — статистика по пройденной части: ':'готово: ')
    +`событий ${rep.events}, сделок ${rep.trades}, PnL ${sgn(rep.pnl_sol,3)} SOL`;
   renderBacktest(rep);
  }
 }catch(e){$('bt_status').textContent='ошибка сети'}
 $('bt_stop').disabled=true;
 btBusy=false;busy=false;$('bt_run').disabled=false;
};
$('bt_stop').onclick=async()=>{
 if(!btRunId||$('bt_stop').disabled)return;
 try{
  await api('/api/backtest',{method:'POST',headers:{'Content-Type':'application/json'},
   body:JSON.stringify({mode:'stop',run_id:btRunId})});
  $('bt_status').textContent='остановка…';
 }catch(e){}
};

async function boot(){
  if(!TOKEN){showLogin();return}
  const r=await api('/api/snapshot');
  if(r.status===401){TOKEN='';localStorage.removeItem('dash_token');showLogin();return}
  connect();refreshOnce();
}
boot();
</script></body></html>
"""


class DashboardServer:
    """HTTP-сервер дашборда: страница, снимок и SSE-поток.

    Не знает о пайплайне — получает провайдер снимка (функцию без
    аргументов, возвращающую JSON-совместимый dict). Живёт в том же
    event-loop, что и бот; обработчики не блокируют торговлю.
    """

    def __init__(
        self,
        provider: SnapshotProvider,
        *,
        host: str = "127.0.0.1",
        port: int = 8787,
        interval_seconds: float = 2.0,
        log_tail_size: int = 200,
        actions: ActionHandler | None = None,
        backtest_runner: BacktestRunner | None = None,
        on_shutdown: ShutdownHandler | None = None,
        auth_user: str = "",
        auth_password: str = "",
    ) -> None:
        """Инициализирует сервер (без сетевых вызовов).

        Args:
            provider: Функция сборки снимка состояния.
            host: Адрес привязки (``127.0.0.1`` — только локально).
            port: Порт (``0`` — свободный, для тестов).
            interval_seconds: Интервал SSE-пуша, c.
            log_tail_size: Ёмкость кольцевого буфера лога.
            actions: Обработчик ручных действий (``POST /api/action``);
                без него действие вернёт 503.
            backtest_runner: Обработчик запуска бэктеста
                (``POST /api/backtest``); без него — 503.
            on_shutdown: Обработчик полной остановки бота
                (``POST /api/shutdown``); без него — 503.
            auth_user: Логин дашборда; аутентификация включена, только
                если задан и ``auth_user``, и ``auth_password``.
            auth_password: Пароль дашборда (из ``DASHBOARD_PASSWORD``).
        """
        self._provider = provider
        self._host = host
        self._port = port
        self._interval = max(0.5, interval_seconds)
        self._log_tail: deque[str] = deque(maxlen=log_tail_size)
        self._runner: web.AppRunner | None = None
        self._sink_id: int | None = None
        self._shutdown_task: asyncio.Task[None] | None = None
        self._actions = actions
        self._backtest_runner = backtest_runner
        self._on_shutdown = on_shutdown
        self._auth_user = auth_user
        self._auth_password = auth_password
        self._tokens: dict[str, float] = {}  # token -> expiry (unix ts)

    # --------------------------------------------------------------- Свойства
    @property
    def url(self) -> str:
        """Адрес страницы (после старта)."""
        port = self._port
        if self._runner is not None:
            sockets = getattr(self._runner, "addresses", [])
            if sockets:
                port = sockets[0][1]
        return f"http://{self._host}:{port}"

    def log_tail(self, limit: int = 200) -> list[str]:
        """Возвращает последние строки лога для страницы.

        Args:
            limit: Сколько строк отдать.

        Returns:
            Список отформатированных строк (старые → новые).
        """
        return list(self._log_tail)[-limit:]

    # ---------------------------------------------------------- Жизненный цикл
    def attach_logger(self) -> None:
        """Подключает loguru-sink: хвост лога дублируется в дашборд."""
        if self._sink_id is not None:
            return

        def _sink(message: Any) -> None:
            # Хвост дашборда — обзорный: трейсбеки и длинные записи
            # уплотняем до первой строки (полная версия в файлах логов).
            first = str(message).rstrip("\n").split("\n", 1)[0]
            if len(first) > 300:
                first = first[:297] + "…"
            self._log_tail.append(first)

        self._sink_id = _root_logger.add(
            _sink, level="INFO", format="{time:HH:mm:ss} {level} {message}"
        )

    async def start(self) -> None:
        """Поднимает HTTP-сервер. Порт занятия поднимает OSError."""
        app = web.Application()
        app.router.add_get("/", self._page)
        app.router.add_post("/api/login", self._login)
        app.router.add_get("/api/snapshot", self._snapshot)
        app.router.add_get("/api/stream", self._stream)
        app.router.add_post("/api/action", self._action)
        app.router.add_post("/api/backtest", self._backtest)
        app.router.add_post("/api/log/clear", self._log_clear)
        app.router.add_post("/api/shutdown", self._shutdown)
        app.router.add_get("/api/backtest/sample", self._backtest_sample)
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._host, self._port)
        await site.start()
        auth_state = "включена" if self._auth_enabled() else "ВЫКЛЮЧЕНА (пароль не задан)"
        logger.info("Веб-дашборд: {} (аутентификация {})", self.url, auth_state)

    async def close(self) -> None:
        """Останавливает сервер и отсоединяет loguru-sink."""
        if self._sink_id is not None:
            with contextlib.suppress(Exception):
                _root_logger.remove(self._sink_id)
            self._sink_id = None
        if self._runner is not None:
            await self._runner.cleanup()
        self._runner = None

    # ------------------------------------------------------------- Обработчики
    def _auth_enabled(self) -> bool:
        """Включена ли аутентификация (оба секрета непустые).

        Returns:
            ``True``, если логин и пароль заданы в настройках.
        """
        return bool(self._auth_user) and bool(self._auth_password)

    def _token_valid(self, token: str) -> bool:
        """Проверяет сессионный токен на валидность и свежесть.

        Args:
            token: Токен из заголовка/ query-параметра.

        Returns:
            ``True``, если токен известен и не истёк.
        """
        if not token:
            return False
        expiry = self._tokens.get(token)
        if expiry is None or expiry < time.time():
            self._tokens.pop(token, None)
            return False
        return True

    def _request_token(self, request: web.Request) -> str:
        """Извлекает токен из заголовка Authorization или query.

        Args:
            request: HTTP-запрос.

        Returns:
            Токен (пустая строка, если нет).
        """
        header = request.headers.get("Authorization", "")
        if header.startswith("Bearer "):
            return header[len("Bearer "):].strip()
        return str(request.query.get("token", ""))

    def _authorized(self, request: web.Request) -> bool:
        """Проверяет доступ к защищённому эндпоинту.

        Args:
            request: HTTP-запрос.

        Returns:
            ``True``, если аутентификация выключена или токен валиден.
        """
        if not self._auth_enabled():
            return True
        return self._token_valid(self._request_token(request))

    def _unauthorized(self) -> web.Response:
        """Стандартный ответ 401.

        Returns:
            JSON-ответ с ошибкой аутентификации.
        """
        return web.json_response(
            {"ok": False, "error": "требуется вход"}, status=401
        )

    async def _login(self, request: web.Request) -> web.Response:
        """``POST /api/login``: выдаёт сессионный токен по логину/паролю.

        Args:
            request: HTTP-запрос с JSON ``{user, password}``.

        Returns:
            ``{ok, token}`` или 401.
        """
        if not self._auth_enabled():
            return web.json_response({"ok": True, "token": ""})
        try:
            payload = await request.json()
        except Exception:
            return self._unauthorized()
        user = str(payload.get("user", ""))
        password = str(payload.get("password", ""))
        if user != self._auth_user or password != self._auth_password:
            return web.json_response(
                {"ok": False, "error": "неверный логин или пароль"}, status=401
            )
        token = secrets.token_hex(32)
        self._tokens[token] = time.time() + SESSION_TTL_SECONDS
        return web.json_response({"ok": True, "token": token})

    async def _log_clear(self, request: web.Request) -> web.Response:
        """``POST /api/log/clear``: чистит кольцевой буфер хвоста лога.

        Файловые логи бота не затрагиваются — очищается только панель
        дашборда (буфер наполняется заново новыми сообщениями).

        Args:
            request: HTTP-запрос.

        Returns:
            JSON ``{ok: True}`` или 401.
        """
        if not self._authorized(request):
            return self._unauthorized()
        self._log_tail.clear()
        return web.json_response({"ok": True})

    async def _shutdown(self, request: web.Request) -> web.Response:
        """``POST /api/shutdown``: полная остановка бота (и дашборда).

        Ответ отправляется сразу, затем запускается graceful-остановка
        пайплайна. Процесс завершится с кодом 0 — при systemd-юните с
        ``Restart=on-failure`` автоперезапуска не будет.

        Args:
            request: HTTP-запрос.

        Returns:
            JSON ``{ok: True}`` или 401/503.
        """
        if not self._authorized(request):
            return self._unauthorized()
        if self._on_shutdown is None:
            return web.json_response(
                {"ok": False, "error": "остановка недоступна"}, status=503
            )
        logger.warning("Остановка бота запрошена с веб-дашборда")

        handler = self._on_shutdown

        async def _delayed() -> None:
            # даём ответу уйти в сеть до закрытия сервера
            await asyncio.sleep(0.5)
            try:
                await handler()
            except Exception:
                logger.exception("Обработчик остановки упал")

        self._shutdown_task = asyncio.create_task(_delayed())
        return web.json_response({"ok": True})

    async def _page(self, request: web.Request) -> web.Response:
        """Отдаёт HTML-страницу дашборда.

        Args:
            request: HTTP-запрос.

        Returns:
            Ответ с HTML.
        """
        return web.Response(
            text=_PAGE,
            content_type="text/html",
            headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
        )

    async def _snapshot(self, request: web.Request) -> web.Response:
        """Отдаёт разовый JSON-снимок состояния (требует токен).

        Args:
            request: HTTP-запрос.

        Returns:
            JSON-ответ или 401.
        """
        if not self._authorized(request):
            return self._unauthorized()
        return web.json_response(
            self._provider(), dumps=lambda data: json.dumps(data, ensure_ascii=False)
        )

    async def _action(self, request: web.Request) -> web.Response:
        """``POST /api/action``: ручное действие (sell/average/sell_all).

        Args:
            request: HTTP-запрос с JSON ``{action, mint?}``.

        Returns:
            JSON-результат действия или 401/503.
        """
        if not self._authorized(request):
            return self._unauthorized()
        if self._actions is None:
            return web.json_response(
                {"ok": False, "error": "ручные действия недоступны"}, status=503
            )
        try:
            payload = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "битый JSON"})
        if not isinstance(payload, dict):
            return web.json_response({"ok": False, "error": "битый JSON"})
        result = await self._actions(payload)
        return web.json_response(
            result, dumps=lambda data: json.dumps(data, ensure_ascii=False)
        )

    async def _backtest(self, request: web.Request) -> web.Response:
        """``POST /api/backtest``: запуск бэктеста (sample/csv/mint).

        Args:
            request: HTTP-запрос с JSON-параметрами режима.

        Returns:
            JSON с отчётом/CSV или 401/503.
        """
        if not self._authorized(request):
            return self._unauthorized()
        if self._backtest_runner is None:
            return web.json_response(
                {"ok": False, "error": "бэктест недоступен"}, status=503
            )
        try:
            payload = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "битый JSON"})
        if not isinstance(payload, dict):
            return web.json_response({"ok": False, "error": "битый JSON"})
        result = await self._backtest_runner(payload)
        return web.json_response(
            result, dumps=lambda data: json.dumps(data, ensure_ascii=False)
        )

    async def _backtest_sample(self, request: web.Request) -> web.Response:
        """``GET /api/backtest/sample``: демо-CSV для формы бэктеста.

        Args:
            request: HTTP-запрос.

        Returns:
            CSV-текст или 401/503.
        """
        if not self._authorized(request):
            return self._unauthorized()
        if self._backtest_runner is None:
            return web.json_response(
                {"ok": False, "error": "бэктест недоступен"}, status=503
            )
        result = await self._backtest_runner({"mode": "sample"})
        return web.Response(
            text=str(result.get("csv", "")), content_type="text/csv"
        )

    async def _stream(self, request: web.Request) -> web.StreamResponse:
        """SSE-поток: пушит снимок каждые ``interval`` секунд (требует токен).

        Args:
            request: HTTP-запрос.

        Returns:
            Потоковый ответ (держится, пока подключён клиент).
        """
        if not self._authorized(request):
            return self._unauthorized()
        response = web.StreamResponse(
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            }
        )
        await response.prepare(request)
        try:
            while True:
                payload = json.dumps(self._provider(), ensure_ascii=False)
                await response.write(f"data: {payload}\n\n".encode())
                await asyncio.sleep(self._interval)
        except (asyncio.CancelledError, ConnectionResetError, RuntimeError):
            pass
        finally:
            with contextlib.suppress(Exception):
                await response.write_eof()
        return response
