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
  <span class="badge" id="venue">…</span>
  <span class="badge" id="uptime">…</span>
  <span class="badge" id="poscount">…</span>
  <span id="conn">подключение…</span>
  <span style="margin-left:auto"><button class="btn mini" id="logout">выйти</button></span>
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
  <section class="card wide"><h2>Лог (хвост)</h2><pre id="log">—</pre></section>
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
  const mode=$('mode');
  mode.textContent=s.mode||'—';
  mode.className='badge '+(s.mode==='live'?'live':'dry');
  $('network').textContent=s.network||'—';
  $('venue').textContent='venue: '+(s.venue||'—');
  $('uptime').textContent='uptime '+dur(s.uptime_s||0);
  $('poscount').textContent=`позиции ${s.positions_open??0}/${s.positions_limit??0}`;
  $('poscount').className='badge '+((s.positions_open||0)>0?'ok':'');

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
    <td><code title="${esc(p.base_mint)}">${esc(short(p.base_mint))}</code></td>
    <td>${p.stage}</td><td>${fmt(p.entry_sol,3)}</td><td>${fmt(p.token_amount,0)}</td>
    <td>${price(p.entry_price)}</td><td>${price(p.current_price)}</td>
    <td class="${cls(p.pnl_pct)}">${sgn(p.pnl_pct,1)}%</td>
    <td>${price(p.peak_price)}</td><td>${dur(p.hold_seconds)}</td>
    <td>${p.tp1_taken?'✔':'—'}</td>
    <td><button class="btn mini" data-act="sell" data-mint="${esc(p.base_mint)}">✕ закрыть</button><button class="btn mini warn" data-act="average" data-mint="${esc(p.base_mint)}">＋ усреднить</button></td></tr>`).join('');
  $('positions').innerHTML=rows
    ?`<table><tr><th>mint</th><th>стадия</th><th>вход</th><th>токены</th><th>вход цена</th><th>тек.</th><th>PnL</th><th>пик</th><th>холд</th><th>TP1</th><th>ручное</th></tr>${rows}</table>`
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
  const rj=(sc.recent_rejections||[]).map(r=>`<li><code>${esc(short(r.base_mint))}</code> — ${esc(r.reason)}</li>`).join('');
  $('screen').innerHTML=`
   <div class="row"><span class="k">ждут проверки</span><span>${sc.pending??0}</span></div>
   <div class="row"><span class="k">одобрено</span><span class="pos">${sc.approved??0}</span></div>
   <div class="row"><span class="k">отклонено</span><span class="neg">${sc.rejected??0}</span></div>
   ${rj?`<ul>${rj}</ul>`:''}`;

  const lat=d.latency||{};
  $('latency').innerHTML=Object.keys(lat).length
    ?`<table><tr><th>этап</th><th>p50</th><th>p95</th><th>n</th></tr>${Object.entries(lat).map(([k,v])=>
      `<tr><td>${esc(k)}</td><td>${fmt(v.p50_ms||0,1)}</td><td>${fmt(v.p95_ms||0,1)}</td><td>${v.count??0}</td></tr>`).join('')}</table>`
    :'—';

  $('log').textContent=(d.logs||[]).join('\\n')||'—';
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
$('logout').onclick=()=>{TOKEN='';localStorage.removeItem('dash_token');if(es)es.close();showLogin()};

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
        self._actions = actions
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
            self._log_tail.append(str(message).rstrip("\n"))

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

    async def _page(self, request: web.Request) -> web.Response:
        """Отдаёт HTML-страницу дашборда.

        Args:
            request: HTTP-запрос.

        Returns:
            Ответ с HTML.
        """
        return web.Response(text=_PAGE, content_type="text/html")

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
