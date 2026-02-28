/**
 * Popup logic — toggle worker, display stats, manage API key.
 */

const toggleEl = document.getElementById("toggle");
const toggleLabel = document.getElementById("toggle-label");
const statusEl = document.getElementById("status");
const pagesEl = document.getElementById("pages");
const bytesEl = document.getElementById("bytes");
const workerIdEl = document.getElementById("worker-id");
const pollNowEl = document.getElementById("poll-now");

function formatBytes(b) {
  if (b < 1024) return b + " B";
  if (b < 1024 * 1024) return (b / 1024).toFixed(1) + " KB";
  if (b < 1024 * 1024 * 1024) return (b / (1024 * 1024)).toFixed(1) + " MB";
  return (b / (1024 * 1024 * 1024)).toFixed(2) + " GB";
}

function updateUI(data) {
  const enabled = data.enabled === true;
  toggleEl.checked = enabled;
  toggleLabel.textContent = enabled ? "Zapnuto" : "Vypnuto";

  const status = data.lastStatus || "off";
  const statusMap = {
    off: "Vypnuto",
    idle: "Čeká na úkol...",
    polling: "Hledám úkol...",
    working: "Stahuji stránku...",
    error: "Chyba: " + (data.lastError || "neznámá"),
  };
  statusEl.textContent = statusMap[status] || status;
  statusEl.className = "status " + (status === "working" ? "working" : status === "error" ? "error" : "");

  const stats = data.stats || { pagesCompleted: 0, bytesDownloaded: 0 };
  pagesEl.textContent = stats.pagesCompleted;
  bytesEl.textContent = formatBytes(stats.bytesDownloaded);

  if (data.workerId) {
    workerIdEl.textContent = data.workerId;
  }
}

// Load current state
chrome.runtime.sendMessage({ action: "getStatus" }, updateUI);

// Toggle on/off
toggleEl.addEventListener("change", () => {
  chrome.runtime.sendMessage({ action: "toggle" }, (resp) => {
    if (resp) {
      toggleEl.checked = resp.enabled;
      toggleLabel.textContent = resp.enabled ? "Zapnuto" : "Vypnuto";
      // Refresh full status after a moment
      setTimeout(() => {
        chrome.runtime.sendMessage({ action: "getStatus" }, updateUI);
      }, 500);
    }
  });
});

// Poll now
pollNowEl.addEventListener("click", () => {
  chrome.runtime.sendMessage({ action: "pollNow" });
  statusEl.textContent = "Hledám úkol...";
  setTimeout(() => {
    chrome.runtime.sendMessage({ action: "getStatus" }, updateUI);
  }, 2000);
});
