const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 10000;
const MAX_PLAYERS = 4;
const rooms = new Map();

function makeCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code;
  do {
    code = Array.from({ length: 6 }, () =>
      chars[Math.floor(Math.random() * chars.length)]
    ).join('');
  } while (rooms.has(code));
  return code;
}

function send(ws, data) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function cleanup(ws) {
  const roomCode = ws.roomCode;
  if (!roomCode || !rooms.has(roomCode)) return;

  const room = rooms.get(roomCode);
  const leavingId = ws.peerId;
  room.peers.delete(leavingId);

  if (leavingId === 1) {
    for (const peer of room.peers.values()) {
      send(peer, { type: 'room_error', reason: 'host_left' });
      peer.roomCode = null;
      peer.peerId = -1;
    }
    rooms.delete(roomCode);
  } else {
    send(room.peers.get(1), { type: 'peer_left', peer_id: leavingId });
    if (room.peers.size === 0) rooms.delete(roomCode);
  }

  ws.roomCode = null;
  ws.peerId = -1;
}

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end(`Machine Party signaling server running. Active rooms: ${rooms.size}\n`);
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  ws.roomCode = null;
  ws.peerId = -1;

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (msg.type === 'ping') {
      send(ws, { type: 'pong' });
      return;
    }

    if (msg.type === 'create_room') {
      cleanup(ws);
      const code = makeCode();
      const room = {
        peers: new Map(),
        nextPeerId: 2,
        joinable: true,
      };

      ws.roomCode = code;
      ws.peerId = 1;
      room.peers.set(1, ws);
      rooms.set(code, room);

      send(ws, { type: 'room_created', room: code, peer_id: 1 });
      return;
    }

    if (msg.type === 'join_room') {
      cleanup(ws);
      const code = String(msg.room || '').trim().toUpperCase();
      const room = rooms.get(code);

      if (!room) {
        send(ws, { type: 'room_error', reason: 'not_found' });
        return;
      }
      if (!room.joinable) {
        send(ws, { type: 'room_error', reason: 'closed' });
        return;
      }
      if (room.peers.size >= MAX_PLAYERS) {
        send(ws, { type: 'room_error', reason: 'full' });
        return;
      }

      const id = room.nextPeerId++;
      ws.roomCode = code;
      ws.peerId = id;
      room.peers.set(id, ws);

      send(ws, { type: 'joined_room', room: code, peer_id: id });
      send(room.peers.get(1), { type: 'peer_joined', peer_id: id });
      return;
    }

    if (msg.type === 'set_joinable') {
      if (ws.peerId === 1 && rooms.has(ws.roomCode)) {
        rooms.get(ws.roomCode).joinable = Boolean(msg.joinable);
      }
      return;
    }

    if (msg.type === 'leave_room') {
      cleanup(ws);
      return;
    }

    if (['offer', 'answer', 'ice'].includes(msg.type)) {
      const room = rooms.get(ws.roomCode);
      if (!room) return;

      const targetId = Number(msg.to);
      const target = room.peers.get(targetId);
      if (!target) return;

      send(target, { ...msg, from: ws.peerId });
    }
  });

  ws.on('close', () => cleanup(ws));
  ws.on('error', () => cleanup(ws));
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Machine Party signaling server listening on ${PORT}`);
});
