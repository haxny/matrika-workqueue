/**
 * Matriky MZA Helper — Service Worker
 *
 * Polls the workqueue coordinator for MZA download tasks,
 * delegates DZI tile stitching to an OffscreenDocument,
 * and uploads the result JPEG back to the coordinator.
 */

const COORDINATOR_URL = "https://g.book.cz/wq";
const POLL_INTERVAL_MIN = 2; // Poll every 2 minutes (human-like pacing)
const ALARM_NAME = "mza-poll";

// --- Worker identity ---

async function getWorkerId() {
  const { workerId } = await chrome.storage.local.get("workerId");
  if (workerId) return workerId;
  const id = "ext-" + crypto.randomUUID().slice(0, 8);
  await chrome.storage.local.set({ workerId: id });
  return id;
}

const DEFAULT_API_KEY = "s_uM8iZvL3A1F0lAdcgfWxdcpWau12RD";

async function getApiKey() {
  const { apiKey } = await chrome.storage.local.get("apiKey");
  return apiKey || DEFAULT_API_KEY;
}

async function isEnabled() {
  const { enabled } = await chrome.storage.local.get("enabled");
  return enabled === true;
}

// --- Badge helpers ---

function setBadge(text, color) {
  chrome.action.setBadgeText({ text });
  chrome.action.setBadgeBackgroundColor({ color });
}

function setBadgeIdle() {
  setBadge("", "#888");
}

function setBadgeActive() {
  setBadge("⬇", "#4CAF50");
}

function setBadgeOff() {
  setBadge("OFF", "#999");
}

function setBadgeError() {
  setBadge("ERR", "#F44336");
}

// --- Stats ---

async function incrementStats(bytesDownloaded) {
  const { stats } = await chrome.storage.local.get("stats");
  const s = stats || { pagesCompleted: 0, bytesDownloaded: 0, errors: 0 };
  s.pagesCompleted += 1;
  s.bytesDownloaded += bytesDownloaded || 0;
  await chrome.storage.local.set({ stats: s });
}

async function incrementErrors() {
  const { stats } = await chrome.storage.local.get("stats");
  const s = stats || { pagesCompleted: 0, bytesDownloaded: 0, errors: 0 };
  s.errors += 1;
  await chrome.storage.local.set({ stats: s });
}

// --- Offscreen document management ---

let offscreenCreating = null;

async function ensureOffscreen() {
  const contexts = await chrome.runtime.getContexts({
    contextTypes: ["OFFSCREEN_DOCUMENT"],
  });
  if (contexts.length > 0) return;

  if (offscreenCreating) {
    await offscreenCreating;
    return;
  }

  offscreenCreating = chrome.offscreen.createDocument({
    url: "offscreen.html",
    reasons: ["DOM_PARSER"],
    justification: "DZI tile stitching requires DOMParser and OffscreenCanvas",
  });
  await offscreenCreating;
  offscreenCreating = null;
}

// --- Coordinator API ---

async function fetchTask() {
  const workerId = await getWorkerId();
  const apiKey = await getApiKey();
  if (!apiKey) return null;

  const resp = await fetch(`${COORDINATOR_URL}/api/task?capabilities=mza`, {
    headers: {
      "X-Worker-Id": workerId,
      "X-API-Key": apiKey,
    },
  });
  if (!resp.ok) {
    console.error("fetchTask failed:", resp.status, await resp.text());
    return null;
  }
  const data = await resp.json();
  if (data.status === "no_task" || data.status === "daily_limit") return null;
  return data;
}

async function uploadResult(taskId, blob, resultJson) {
  const workerId = await getWorkerId();
  const apiKey = await getApiKey();

  const form = new FormData();
  form.append("task_id", String(taskId));
  form.append("success", "true");
  form.append("result_json", JSON.stringify(resultJson));
  form.append("file", blob, "page.jpg");

  const resp = await fetch(`${COORDINATOR_URL}/api/result`, {
    method: "POST",
    headers: {
      "X-Worker-Id": workerId,
      "X-API-Key": apiKey,
    },
    body: form,
  });
  if (!resp.ok) {
    throw new Error(`Upload failed: ${resp.status} ${await resp.text()}`);
  }
  return resp.json();
}

async function reportError(taskId, errorMsg) {
  const workerId = await getWorkerId();
  const apiKey = await getApiKey();

  const form = new FormData();
  form.append("task_id", String(taskId));
  form.append("success", "false");
  form.append("error", errorMsg);

  await fetch(`${COORDINATOR_URL}/api/result`, {
    method: "POST",
    headers: {
      "X-Worker-Id": workerId,
      "X-API-Key": apiKey,
    },
    body: form,
  }).catch(() => {});
}

// --- Main poll loop ---

let working = false;

async function pollAndExecute() {
  if (working) return;
  if (!(await isEnabled())) {
    setBadgeOff();
    return;
  }

  working = true;
  await chrome.storage.local.set({ lastStatus: "polling" });

  try {
    const task = await fetchTask();
    if (!task) {
      setBadgeIdle();
      await chrome.storage.local.set({ lastStatus: "idle" });
      working = false;
      return;
    }

    setBadgeActive();
    await chrome.storage.local.set({
      lastStatus: "working",
      currentTask: { id: task.task_id, type: task.task_type, payload: task.payload },
    });

    console.log("Got task:", task.task_id, task.task_type, task.payload);

    if (task.task_type !== "mza") {
      await reportError(task.task_id, "Extension only supports mza tasks");
      working = false;
      return;
    }

    // Delegate to offscreen document
    await ensureOffscreen();
    const result = await chrome.runtime.sendMessage({
      target: "offscreen",
      action: "stitch_dzi",
      payload: task.payload,
    });

    if (!result || !result.success) {
      const err = result ? result.error : "No response from offscreen";
      console.error("Stitch failed:", err);
      await reportError(task.task_id, err);
      await incrementErrors();
      setBadgeError();
      await chrome.storage.local.set({ lastStatus: "error", lastError: err });
      working = false;
      return;
    }

    // Convert base64 back to blob for upload
    const binaryStr = atob(result.base64);
    const bytes = new Uint8Array(binaryStr.length);
    for (let i = 0; i < binaryStr.length; i++) {
      bytes[i] = binaryStr.charCodeAt(i);
    }
    const blob = new Blob([bytes], { type: "image/jpeg" });

    await uploadResult(task.task_id, blob, {
      file_size: blob.size,
      width: result.width,
      height: result.height,
      source: "extension",
    });

    await incrementStats(blob.size);
    setBadgeIdle();
    await chrome.storage.local.set({
      lastStatus: "idle",
      currentTask: null,
    });

    console.log("Task completed:", task.task_id, `${(blob.size / 1024 / 1024).toFixed(1)} MB`);
  } catch (e) {
    console.error("Poll error:", e);
    setBadgeError();
    await chrome.storage.local.set({ lastStatus: "error", lastError: e.message });
  } finally {
    working = false;
  }
}

// --- Alarm handler ---
// NOTE: No setTimeout — MV3 kills service workers ~30s after alarm handler returns,
// so any pending setTimeout would be lost. Poll directly; alarm interval provides spacing.

chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm.name === ALARM_NAME) {
    pollAndExecute();
  }
});

// --- Start/stop ---

async function start() {
  await chrome.alarms.create(ALARM_NAME, { periodInMinutes: POLL_INTERVAL_MIN });
  setBadgeIdle();
  // Immediate first poll
  pollAndExecute();
}

async function stop() {
  await chrome.alarms.clear(ALARM_NAME);
  setBadgeOff();
  await chrome.storage.local.set({ lastStatus: "off", currentTask: null });
}

// --- Message handler (from popup) ---

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg.target === "offscreen") return false; // Let offscreen handle it

  if (msg.action === "toggle") {
    chrome.storage.local.get("enabled", ({ enabled }) => {
      const newState = !enabled;
      chrome.storage.local.set({ enabled: newState }, () => {
        if (newState) start();
        else stop();
        sendResponse({ enabled: newState });
      });
    });
    return true; // Async response
  }

  if (msg.action === "getStatus") {
    chrome.storage.local.get(
      ["enabled", "lastStatus", "lastError", "currentTask", "stats", "workerId", "apiKey"],
      (data) => sendResponse(data)
    );
    return true;
  }

  if (msg.action === "setApiKey") {
    chrome.storage.local.set({ apiKey: msg.apiKey }, () => {
      sendResponse({ ok: true });
    });
    return true;
  }

  if (msg.action === "pollNow") {
    pollAndExecute();
    sendResponse({ ok: true });
    return false;
  }
});

// --- Init on install/startup ---

chrome.runtime.onInstalled.addListener(async () => {
  await getWorkerId(); // Generate ID on first install
  // Auto-enable on install — no user action needed
  await chrome.storage.local.set({ enabled: true, apiKey: DEFAULT_API_KEY });
  start();
});

chrome.runtime.onStartup.addListener(async () => {
  if (await isEnabled()) {
    start();
  } else {
    setBadgeOff();
  }
});
