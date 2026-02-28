# Matriky MZA Helper — Browser Extension

Browser extension that volunteers your computer to help download historical parish register pages from the Moravian Provincial Archive (MZA).

## How It Works

1. The extension polls the workqueue coordinator for pending MZA page download tasks
2. When a task is assigned, it fetches DZI tiles directly from www.mza.cz using your browser
3. Tiles are stitched into a full JPEG image using an OffscreenCanvas
4. The result is uploaded back to the coordinator at g.book.cz/wq/

## Installation (Developer Mode)

1. Open `chrome://extensions/` in Chrome, Edge, or Brave
2. Enable **Developer mode** (toggle in the top right)
3. Click **Load unpacked** and select this `extension/` folder
4. Click the extension icon in the toolbar
5. Paste the API key: `s_uM8iZvL3A1F0lAdcgfWxdcpWau12RD`
6. Toggle ON

## Supported Browsers

- Chrome (desktop)
- Edge (desktop)
- Brave (desktop)
- Firefox: not supported yet (no OffscreenDocument API)
- Mobile: not supported yet

## Rate Limiting

- The coordinator limits each worker to 488 MB/day
- The extension adds 45-90 second random delays between tasks
- Domain-level rate limits are enforced server-side

## Files

| File | Purpose |
|------|---------|
| `manifest.json` | MV3 extension manifest |
| `service_worker.js` | Background poll loop, task dispatch, result upload |
| `offscreen.html` | Minimal HTML for OffscreenDocument context |
| `offscreen.js` | DZI tile fetching + Canvas stitching |
| `popup.html` | Extension popup UI |
| `popup.js` | Popup controls and stats display |
| `icons/` | Extension icons (16, 48, 128px) |
