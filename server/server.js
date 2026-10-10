const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 55555;
const PUBLIC_DIR = path.resolve(__dirname, '../street-fighter-2-javascript');

const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif': 'image/gif',
  '.ico': 'image/x-icon',
  '.svg': 'image/svg+xml',
  '.ogg': 'audio/ogg',
  '.mp3': 'audio/mpeg',
  '.wav': 'audio/wav',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2'
};

// HTTP Static Server
const server = http.createServer((req, res) => {
  let reqPath = decodeURI(req.url.split('?')[0]);
  if (reqPath === '/' || reqPath === '') {
    reqPath = '/index.html';
  }

  const filePath = path.join(PUBLIC_DIR, reqPath);

  // Prevent directory traversal
  if (!filePath.startsWith(PUBLIC_DIR)) {
    res.writeHead(403);
    res.end('Access Denied');
    return;
  }

  fs.stat(filePath, (err, stats) => {
    if (err || !stats.isFile()) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('404 Not Found');
      return;
    }

    const ext = path.extname(filePath).toLowerCase();
    const contentType = MIME_TYPES[ext] || 'application/octet-stream';

    res.writeHead(200, {
      'Content-Type': contentType,
      'Cache-Control': 'no-cache',
      'Access-Control-Allow-Origin': '*'
    });

    const stream = fs.createReadStream(filePath);
    stream.pipe(res);
  });
});

// WebSocket Server for WebRTC Signaling & Room Slot Coordination
const wss = new WebSocketServer({ server });

/**
 * Room structure:
 * {
 *   id: string,
 *   clients: Set<WebSocket>,
 *   slots: { 0: WebSocket | null, 1: WebSocket | null }
 * }
 */
const rooms = new Map();

function getOrCreateRoom(roomId = 'default') {
  if (!rooms.has(roomId)) {
    rooms.set(roomId, {
      id: roomId,
      clients: new Set(),
      slots: { 0: null, 1: null }
    });
  }
  return rooms.get(roomId);
}

function broadcastRoomSlots(room) {
  const payload = JSON.stringify({
    type: 'room-slots',
    roomId: room.id,
    p1Taken: room.slots[0] !== null,
    p2Taken: room.slots[1] !== null
  });

  for (const client of room.clients) {
    if (client.readyState === 1 /* OPEN */) {
      client.send(payload);
    }
  }
}

wss.on('connection', (ws) => {
  let currentRoom = null;
  let playerSlot = null; // 0 (P1), 1 (P2), or null

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }

    switch (msg.type) {
      case 'join-room': {
        const roomId = msg.roomId || 'default';
        currentRoom = getOrCreateRoom(roomId);
        currentRoom.clients.add(ws);

        ws.send(JSON.stringify({
          type: 'room-joined',
          roomId: currentRoom.id,
          p1Taken: currentRoom.slots[0] !== null,
          p2Taken: currentRoom.slots[1] !== null
        }));
        break;
      }

      case 'select-slot': {
        if (!currentRoom) currentRoom = getOrCreateRoom('default');
        const desiredSlot = Number(msg.slot); // 0 or 1

        if (desiredSlot !== 0 && desiredSlot !== 1) {
          ws.send(JSON.stringify({ type: 'slot-error', message: 'Invalid slot' }));
          return;
        }

        // If slot is occupied by another active client
        if (currentRoom.slots[desiredSlot] !== null && currentRoom.slots[desiredSlot] !== ws) {
          ws.send(JSON.stringify({
            type: 'slot-error',
            message: `Player ${desiredSlot + 1} is already taken!`,
            p1Taken: currentRoom.slots[0] !== null,
            p2Taken: currentRoom.slots[1] !== null
          }));
          return;
        }

        // Free previous slot if switching
        if (playerSlot !== null && playerSlot !== desiredSlot) {
          currentRoom.slots[playerSlot] = null;
        }

        playerSlot = desiredSlot;
        currentRoom.slots[playerSlot] = ws;

        ws.send(JSON.stringify({
          type: 'slot-confirmed',
          slot: playerSlot,
          p1Taken: currentRoom.slots[0] !== null,
          p2Taken: currentRoom.slots[1] !== null
        }));

        // Broadcast slot change to all players in the room
        broadcastRoomSlots(currentRoom);

        // Check if both players are now ready
        const p1Socket = currentRoom.slots[0];
        const p2Socket = currentRoom.slots[1];
        if (p1Socket && p2Socket) {
          const matchReadyMsg = JSON.stringify({
            type: 'both-players-ready',
            roomId: currentRoom.id
          });
          p1Socket.send(matchReadyMsg);
          p2Socket.send(matchReadyMsg);
        }
        break;
      }

      case 'reset-slots': {
        if (!currentRoom) return;
        currentRoom.slots[0] = null;
        currentRoom.slots[1] = null;
        playerSlot = null;
        broadcastRoomSlots(currentRoom);
        break;
      }

      // WebRTC Signaling Forwarding (Offer, Answer, ICE Candidates)
      case 'webrtc-offer':
      case 'webrtc-answer':
      case 'webrtc-ice-candidate':
      // Low-Latency Relay Fallback (if direct P2P data channel fails)
      case 'relay-input':
      case 'relay-state':
      case 'relay-round-event':
      case 'relay-ping': {
        if (!currentRoom || playerSlot === null) return;
        const opponentSlot = playerSlot === 0 ? 1 : 0;
        const opponentSocket = currentRoom.slots[opponentSlot];

        if (opponentSocket && opponentSocket.readyState === 1) {
          // Forward directly to opponent
          opponentSocket.send(raw);
        }
        break;
      }

      default:
        break;
    }
  });

  ws.on('close', () => {
    if (currentRoom) {
      currentRoom.clients.delete(ws);
      if (playerSlot !== null && currentRoom.slots[playerSlot] === ws) {
        currentRoom.slots[playerSlot] = null;
        const opponentSlot = playerSlot === 0 ? 1 : 0;
        const opponentSocket = currentRoom.slots[opponentSlot];
        if (opponentSocket && opponentSocket.readyState === 1) {
          opponentSocket.send(JSON.stringify({
            type: 'peer-disconnected',
            disconnectedSlot: playerSlot
          }));
        }
        broadcastRoomSlots(currentRoom);
      }
      if (currentRoom.clients.size === 0) {
        rooms.delete(currentRoom.id);
      }
    }
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n======================================================`);
  console.log(`  Street Fighter 2 Online Multiplayer Server Started!  `);
  console.log(`======================================================`);
  console.log(`- Local Access:   http://localhost:${PORT}`);

  const networkInterfaces = os.networkInterfaces();
  for (const name of Object.keys(networkInterfaces)) {
    for (const net of networkInterfaces[name]) {
      if (net.family === 'IPv4' && !net.internal) {
        console.log(`- Network/Mobile: http://${net.address}:${PORT}`);
      }
    }
  }
  console.log(`======================================================\n`);
});
