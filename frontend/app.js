// O navegador não entra em grupo multicast: o membro do grupo é o DashboardServer.
// Este script consome o SSE de /api/events e chama a API do painel.

const API = new URLSearchParams(location.search).get("api") ?? "";

const state = {
  status: null,
  instances: new Map(), // instanceId -> instance
  clockOffset: 0,       // serverTime - Date.now(): o TTL é calculado no relógio do servidor
  leaving: new Set(),
};

const $ = (id) => document.getElementById(id);
const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const now = () => Date.now() + state.clockOffset;
const hhmmss = (ms) => new Date(ms).toLocaleTimeString("pt-BR", { hour12: false });


function connect() {
  const events = new EventSource(`${API}/api/events`);
  events.onopen = () => setConn(true);
  events.onerror = () => setConn(false); // EventSource reconecta sozinho
  events.onmessage = (msg) => handle(JSON.parse(msg.data));
}

function handle(event) {
  switch (event.type) {
    case "snapshot":
      state.clockOffset = event.serverTime - Date.now();
      state.instances = new Map(event.instances.map((i) => [i.instanceId, i]));
      $("log").innerHTML = "";
      [...event.packets].reverse().forEach(addPacket);
      setStatus(event.status);
      renderServices();
      break;
    case "status":
      setStatus(event.status);
      if (event.note) addNote(event.note);
      renderServices();
      break;
    case "registry":
      onRegistry(event.event, event.instance);
      break;
    case "packet":
      addPacket(event);
      bumpReceived();
      break;
  }
}

function onRegistry(type, instance) {
  if (type === "REGISTERED" || type === "RENEWED") {
    state.instances.set(instance.instanceId, instance);
    state.leaving.delete(instance.instanceId);
    renderServices();
    flash(instance.instanceId);
    return;
  }
  // DEREGISTERED ou EXPIRED: a linha fica esmaecida por 900 ms antes de sair
  if (type === "EXPIRED") addNote(`TTL venceu: ${instance.instanceId} expirou sem ALIVE`);
  state.leaving.add(instance.instanceId);
  renderServices();
  setTimeout(() => {
    if (!state.leaving.has(instance.instanceId)) return;
    state.leaving.delete(instance.instanceId);
    state.instances.delete(instance.instanceId);
    renderServices();
  }, 900);
}


function setConn(live) {
  $("conn").className = `conn ${live ? "live" : "down"}`;
  $("conn-text").textContent = live ? "stream ao vivo" : "reconectando…";
}

function setStatus(status) {
  state.status = status;
  $("group-addr").textContent = `${status.group}:${status.port}`;
  $("group-iface").textContent =
    `interface ${status.interfaceName} · ${status.interfaceDisplayName} · ${status.localAddress}`;

  const pill = $("membership-pill");
  pill.className = `pill ${status.joined ? "on" : "off"}`;
  pill.textContent = status.joined ? "MembershipKey válida · inscrito" : "sem MembershipKey · fora do grupo";
  $("btn-join").disabled = status.joined;
  $("btn-leave").disabled = !status.joined;

  $("stat-received").textContent = status.stats.received;
  $("stat-malformed").textContent = status.stats.malformed;

  $("blocked-list").innerHTML = status.blockedSources.map((ip) =>
    `<span class="chip">${esc(ip)}<button data-unblock="${esc(ip)}" title="unblock()">×</button></span>`).join("");

  $("announcers").innerHTML = status.announcers.map((a) => `
    <div class="announcer">
      <div>
        <div class="mono">${esc(a.service)} · ${esc(a.host)}:${a.port}</div>
        <div class="meta">${esc(a.instanceId)} · a cada ${a.intervalSeconds}s · ttl ${a.ttlSeconds}s</div>
      </div>
      <button class="btn small danger" data-stop="${esc(a.instanceId)}" title="close(): envia BYE">parar</button>
    </div>`).join("");
}

function renderServices() {
  const byService = new Map();
  for (const instance of state.instances.values()) {
    if (!byService.has(instance.service)) byService.set(instance.service, []);
    byService.get(instance.service).push(instance);
  }
  const names = [...byService.keys()].sort();
  $("stat-services").textContent = names.length;
  $("stat-instances").textContent = state.instances.size - state.leaving.size;
  $("services-empty").style.display = names.length ? "none" : "block";

  $("services").innerHTML = names.map((name) => {
    const list = byService.get(name).sort((a, b) => a.firstSeen - b.firstSeen);
    return `
      <div class="service">
        <div class="service-head">
          <span class="service-name">${esc(name)}</span>
          <span class="badge">${list.length} ${list.length === 1 ? "instância" : "instâncias"}</span>
        </div>
        ${list.map(instanceRow).join("")}
      </div>`;
  }).join("");
  tickTtl();
}

function instanceRow(i) {
  const leaving = state.leaving.has(i.instanceId) ? " leaving" : "";
  const local = i.local ? `<span class="tag">deste painel</span>` : "";
  return `
    <div class="instance${leaving}" data-id="${esc(i.instanceId)}">
      <div>
        <div class="addr">${esc(i.host)}:${i.port}${local}</div>
        <div class="instance-id">${esc(i.instanceId)}</div>
      </div>
      <div class="meta">
        seq ${i.seq} · origem ${esc(i.source)}<br>
        visto desde ${hhmmss(i.firstSeen)}
      </div>
      <div>
        <div class="ttl-bar"><div class="ttl-fill" data-expires="${i.expiresAt}" data-ttl="${i.ttlSeconds}"></div></div>
        <div class="ttl-text" data-expires-text="${i.expiresAt}"></div>
      </div>
    </div>`;
}

function tickTtl() {
  const t = now();
  document.querySelectorAll(".ttl-fill").forEach((bar) => {
    const remaining = Math.max(0, Number(bar.dataset.expires) - t);
    const ratio = remaining / (Number(bar.dataset.ttl) * 1000);
    bar.style.width = `${Math.min(100, ratio * 100)}%`;
    bar.className = `ttl-fill${ratio < 0.34 ? " crit" : ratio < 0.67 ? " warn" : ""}`;
    const text = bar.parentElement.nextElementSibling;
    text.textContent = `expira em ${(remaining / 1000).toFixed(1)}s`;
  });
}

function flash(instanceId) {
  const row = document.querySelector(`.instance[data-id="${CSS.escape(instanceId)}"]`);
  if (!row) return;
  row.classList.add("flash");
  setTimeout(() => row.classList.remove("flash"), 350);
}


const lastSeqShown = new Map();

function addPacket(p) {
  if (p.kind === "ALIVE" && $("hide-renewals").checked && lastSeqShown.has(p.instanceId)) return;
  if (p.kind === "ALIVE") lastSeqShown.set(p.instanceId, p.seq);

  const detail = {
    ALIVE: `${esc(p.service)} → ${esc(p.host)}:${p.port} <span class="src">seq ${p.seq}</span>`,
    BYE: `${esc(p.instanceId)}`,
    SEARCH: `quem é ${esc(p.service)}? <span class="src">${esc(p.searcherId)}</span>`,
  }[p.kind] + ` <span class="src">de ${esc(p.sourceAddress)}</span>`;

  const li = document.createElement("li");
  li.innerHTML = `
    <span class="time">${hhmmss(p.at)}</span>
    <span class="kind ${p.kind}">${p.kind}</span>
    <span class="detail" title="origem ${esc(p.sourceAddress)}:${p.sourcePort}">${detail}</span>
    <button data-block="${esc(p.sourceAddress)}" title="memkey.block(${esc(p.sourceAddress)})">block</button>`;
  prependLog(li);
}

function addNote(text) {
  const li = document.createElement("li");
  li.className = "note";
  li.innerHTML = `<span class="time">${hhmmss(now())}</span><span>${esc(text)}</span>`;
  prependLog(li);
}

function prependLog(li) {
  const log = $("log");
  log.prepend(li);
  while (log.children.length > 200) log.lastElementChild.remove();
}

function bumpReceived() {
  if (state.status) $("stat-received").textContent = ++state.status.stats.received;
}


async function call(method, path, params = {}) {
  const query = new URLSearchParams(params).toString();
  const response = await fetch(`${API}${path}${query ? `?${query}` : ""}`, { method });
  const body = response.status === 204 ? null : await response.json();
  if (!response.ok) throw new Error(body?.error ?? `HTTP ${response.status}`);
  return body;
}

async function run(action, okMessage) {
  try {
    await action();
    if (okMessage) toast(okMessage);
  } catch (e) {
    toast(e.message, true);
  }
}

function toast(text, isError = false) {
  const el = $("toast");
  el.textContent = text;
  el.className = `toast show${isError ? " err" : ""}`;
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => (el.className = "toast"), 2600);
}

$("btn-join").onclick = () => run(() => call("POST", "/api/membership/join"), "join(): inscrito no grupo");
$("btn-leave").onclick = () => run(() => call("POST", "/api/membership/leave"), "drop(): fora do grupo");
$("btn-search").onclick = () => run(() => call("POST", "/api/search", { service: "*" }), "SEARCH * enviado ao grupo");

$("block-form").onsubmit = (e) => {
  e.preventDefault();
  const address = $("block-input").value.trim();
  if (!address) return;
  run(() => call("POST", "/api/sources/block", { address }), `block(${address})`);
  $("block-input").value = "";
};

$("announce-form").onsubmit = (e) => {
  e.preventDefault();
  const params = Object.fromEntries(new FormData(e.target));
  $("announce-error").textContent = "";
  call("POST", "/api/announcers", params)
    .then((a) => toast(`anunciando ${a.instanceId}`))
    .catch((err) => ($("announce-error").textContent = err.message));
};

document.addEventListener("click", (e) => {
  const target = e.target.closest("button[data-block], button[data-unblock], button[data-stop]");
  if (!target) return;
  const { block, unblock, stop } = target.dataset;
  if (block) {
    run(() => call("POST", "/api/sources/block", { address: block }),
      `block(${block}): tudo que sai desse IP é descartado, inclusive instâncias da mesma máquina`);
  } else if (unblock) {
    run(() => call("POST", "/api/sources/unblock", { address: unblock }), `unblock(${unblock})`);
  } else if (stop) {
    run(() => call("DELETE", `/api/announcers/${encodeURIComponent(stop)}`), "announcer parado: BYE enviado");
  }
});

setInterval(tickTtl, 250);
connect();
