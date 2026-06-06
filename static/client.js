/* ──────────────────────────────────────────────
   PFO 3 — Cliente web
   Lógica de envío de tareas, polling de resultados
   y refresco del historial.
   ────────────────────────────────────────────── */

const POLL_INTERVAL_MS = 500;
const MAX_POLL_ATTEMPTS = 60;   // 60 × 500ms = 30s por tarea

const $  = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

const statusEl   = $("#status");
const statusText = $("#status-text");
const form       = $("#task-form");
const msgInput   = $("#msg");
const tasksList  = $("#tasks");
const histList   = $("#history");
const quickBtns  = $$(".quick-commands button");

// Mapa en memoria: task_id → { cmd, attempts, li }
const inflight = new Map();

/* ── Status indicator ─────────────────────── */
function setStatus(state, text) {
  statusEl.classList.remove("ok", "warn", "err");
  if (state) statusEl.classList.add(state);
  statusText.textContent = text;
}

async function checkHealth() {
  try {
    const r = await fetch("/api/health");
    if (!r.ok) throw new Error();
    const data = await r.json();
    setStatus("ok", `conectado · ${data.workers} workers · ${data.queue}`);
  } catch {
    setStatus("err", "servidor no disponible");
  }
}

/* ── Submit task ──────────────────────────── */
form.addEventListener("submit", async (e) => {
  e.preventDefault();
  const msg = msgInput.value.trim();
  if (!msg) return;
  await sendTask(msg);
  msgInput.value = "";
  msgInput.focus();
});

quickBtns.forEach((b) => {
  b.addEventListener("click", () => sendTask(b.dataset.cmd));
});

async function sendTask(msg) {
  const li = createTaskLi(msg);
  tasksList.prepend(li);
  // Si era el "empty state", limpiarlo
  const empty = tasksList.querySelector(".empty");
  if (empty) empty.remove();

  try {
    const r = await fetch("/api/tasks", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ msg }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    inflight.set(data.task_id, { msg, attempts: 0, li });
    updateTaskStatus(li, "pending", `task_id ${data.task_id.slice(0, 8)}…`);
    pollTask(data.task_id);
  } catch (err) {
    updateTaskStatus(li, "errored", "error al enviar");
    li.querySelector(".task-result").textContent = String(err);
  }
}

function createTaskLi(msg) {
  const li = document.createElement("li");
  li.className = "task";
  li.innerHTML = `
    <div class="task-header">
      <span class="task-cmd"></span>
      <span class="task-meta"><span class="task-status">…</span></span>
    </div>
    <div class="task-result">
      <span class="label">Resultado</span>
      <span class="content"><span class="spinner"></span> en cola…</span>
    </div>
  `;
  li.querySelector(".task-cmd").textContent = msg;
  return li;
}

function updateTaskStatus(li, status, meta) {
  li.classList.remove("done", "errored");
  if (status === "done")    li.classList.add("done");
  if (status === "errored") li.classList.add("errored");
  const statusEl = li.querySelector(".task-status");
  statusEl.textContent = status;
  const metaEl = li.querySelector(".task-meta");
  metaEl.innerHTML = `<span class="task-status">${status}</span> · ${meta}`;
}

/* ── Polling ──────────────────────────────── */
async function pollTask(taskId) {
  const entry = inflight.get(taskId);
  if (!entry) return;
  entry.attempts += 1;

  try {
    const r = await fetch(`/api/tasks/${encodeURIComponent(taskId)}`);
    // 404 = aún no procesada (sigue en RabbitMQ o pendiente de INSERT)
    if (r.status === 404) {
      if (entry.attempts < MAX_POLL_ATTEMPTS) {
        setTimeout(() => pollTask(taskId), POLL_INTERVAL_MS);
      } else {
        updateTaskStatus(entry.li, "errored", "timeout");
      }
      return;
    }
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();

    if (data.status === "done") {
      const resultEl = entry.li.querySelector(".task-result .content");
      resultEl.textContent = "";
      resultEl.style.whiteSpace = "pre-wrap";
      resultEl.textContent = data.resultado || "(sin resultado)";
      const meta = `Worker-${data.worker} · ${data.fecha || ""}`;
      updateTaskStatus(entry.li, "done", meta);
      inflight.delete(taskId);
      // Refrescar el historial (la nueva fila ya está en PostgreSQL)
      loadHistory();
    } else if (entry.attempts < MAX_POLL_ATTEMPTS) {
      setTimeout(() => pollTask(taskId), POLL_INTERVAL_MS);
    } else {
      updateTaskStatus(entry.li, "errored", "timeout");
      inflight.delete(taskId);
    }
  } catch (err) {
    if (entry.attempts < MAX_POLL_ATTEMPTS) {
      setTimeout(() => pollTask(taskId), POLL_INTERVAL_MS);
    } else {
      updateTaskStatus(entry.li, "errored", String(err));
      inflight.delete(taskId);
    }
  }
}

/* ── History ──────────────────────────────── */
$("#refresh-history").addEventListener("click", loadHistory);
$("#clear-tasks").addEventListener("click", () => {
  // Cancelar polling de las tareas que aún estén pendientes
  inflight.forEach((_entry, id) => inflight.delete(id));
  tasksList.innerHTML = '<li class="empty">Lista vacía.</li>';
});

async function loadHistory() {
  try {
    const r = await fetch("/api/historial");
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const rows = await r.json();
    renderHistory(rows);
  } catch (err) {
    histList.innerHTML = `<li class="empty">Error al cargar: ${err}</li>`;
  }
}

function renderHistory(rows) {
  if (!rows || rows.length === 0) {
    histList.innerHTML = '<li class="empty">Sin mensajes aún.</li>';
    return;
  }
  histList.innerHTML = "";
  rows.forEach((row) => {
    const li = document.createElement("li");
    li.className = "task done";
    const time = new Date(row.fecha + "Z").toLocaleTimeString();
    li.innerHTML = `
      <div class="task-header">
        <span class="task-cmd"></span>
        <span class="task-meta">#${row.id} · w${row.worker} · ${time} · ${row.ip}</span>
      </div>
    `;
    li.querySelector(".task-cmd").textContent = row.msg;
    histList.appendChild(li);
  });
}

/* ── Bootstrap ────────────────────────────── */
checkHealth();
loadHistory();
setInterval(checkHealth, 5000);
