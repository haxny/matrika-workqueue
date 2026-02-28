/**
 * Offscreen document — DZI tile fetching + canvas stitching.
 *
 * Receives a message with MZA task payload, fetches the DZI descriptor,
 * downloads all tiles at max zoom, stitches them on a Canvas, and returns
 * the result as a base64-encoded JPEG.
 */

const IIP_BASE = "https://www.mza.cz/iipsrv/iipsrv.fcgi";
const REFERER = "https://www.mza.cz/actapublica/";
const MAX_CONCURRENT = 6;

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg.target !== "offscreen") return;
  if (msg.action !== "stitch_dzi") return;

  stitchDzi(msg.payload)
    .then((result) => sendResponse(result))
    .catch((e) => sendResponse({ success: false, error: e.message }));

  return true; // Async response
});

async function stitchDzi(payload) {
  const jp2Path = payload.jp2_path;
  const dziUrl = payload.dzi_url || `${IIP_BASE}?Deepzoom=${jp2Path}.dzi`;

  // 1. Fetch and parse DZI XML
  const dziResp = await fetch(dziUrl, {
    headers: { Referer: REFERER },
  });
  if (!dziResp.ok) {
    throw new Error(`DZI fetch failed: ${dziResp.status} ${dziResp.statusText}`);
  }
  const dziXml = await dziResp.text();

  const parser = new DOMParser();
  const doc = parser.parseFromString(dziXml, "text/xml");
  const imageEl = doc.querySelector("Image");
  if (!imageEl) throw new Error("Invalid DZI: no <Image> element");

  const sizeEl = imageEl.querySelector("Size");
  if (!sizeEl) throw new Error("Invalid DZI: no <Size> element");

  const tileSize = parseInt(imageEl.getAttribute("TileSize") || "256", 10);
  const overlap = parseInt(imageEl.getAttribute("Overlap") || "0", 10);
  const format = imageEl.getAttribute("Format") || "jpg";
  const width = parseInt(sizeEl.getAttribute("Width"), 10);
  const height = parseInt(sizeEl.getAttribute("Height"), 10);

  if (!width || !height) throw new Error(`Invalid DZI dimensions: ${width}x${height}`);

  // 2. Calculate max zoom level and tile grid
  const maxLevel = Math.ceil(Math.log2(Math.max(width, height)));
  const cols = Math.ceil(width / tileSize);
  const rows = Math.ceil(height / tileSize);

  // 3. Build base URL for tiles
  // DZI URL: .../iipsrv.fcgi?Deepzoom=path.dzi
  // Tile URL: .../iipsrv.fcgi?Deepzoom=path_files/level/col_row.format
  const baseUrl = dziUrl.replace(".dzi", "");
  const tileBaseUrl = `${baseUrl}_files/${maxLevel}`;

  console.log(`DZI: ${width}x${height}, tiles ${cols}x${rows}, level ${maxLevel}, tileSize ${tileSize}, overlap ${overlap}`);

  // 4. Fetch all tiles with concurrency limit
  const tiles = [];
  for (let row = 0; row < rows; row++) {
    for (let col = 0; col < cols; col++) {
      tiles.push({ col, row, url: `${tileBaseUrl}/${col}_${row}.${format}` });
    }
  }

  const tileImages = new Map();
  let idx = 0;

  async function fetchNext() {
    while (idx < tiles.length) {
      const tile = tiles[idx++];
      const resp = await fetch(tile.url, {
        headers: { Referer: REFERER },
      });
      if (!resp.ok) {
        throw new Error(`Tile fetch failed: ${tile.col}_${tile.row} → ${resp.status}`);
      }
      const blob = await resp.blob();
      const bmp = await createImageBitmap(blob);
      tileImages.set(`${tile.col}_${tile.row}`, { bmp, col: tile.col, row: tile.row });
    }
  }

  const workers = [];
  for (let i = 0; i < Math.min(MAX_CONCURRENT, tiles.length); i++) {
    workers.push(fetchNext());
  }
  await Promise.all(workers);

  // 5. Stitch tiles on canvas
  const canvas = new OffscreenCanvas(width, height);
  const ctx = canvas.getContext("2d");

  for (const [, tile] of tileImages) {
    const x = tile.col * tileSize - (tile.col > 0 ? overlap : 0);
    const y = tile.row * tileSize - (tile.row > 0 ? overlap : 0);

    // For non-edge tiles, skip the overlap pixels from top/left
    let sx = tile.col > 0 ? overlap : 0;
    let sy = tile.row > 0 ? overlap : 0;
    let sw = tile.bmp.width - sx;
    let sh = tile.bmp.height - sy;

    // Destination position
    let dx = tile.col * tileSize;
    let dy = tile.row * tileSize;

    ctx.drawImage(tile.bmp, sx, sy, sw, sh, dx, dy, sw, sh);
    tile.bmp.close();
  }

  // 6. Export as JPEG
  const jpegBlob = await canvas.convertToBlob({ type: "image/jpeg", quality: 1.0 });

  // Convert to base64 for message passing (blobs can't cross contexts)
  const buffer = await jpegBlob.arrayBuffer();
  const binary = Array.from(new Uint8Array(buffer))
    .map((b) => String.fromCharCode(b))
    .join("");
  const base64 = btoa(binary);

  return {
    success: true,
    base64,
    width,
    height,
    fileSize: jpegBlob.size,
    tilesCount: tiles.length,
  };
}
