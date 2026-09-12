import { WebSocketServer } from 'ws';
import * as Y from 'yjs';
import { createServer } from 'http';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';
dotenv.config();

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = parseInt(process.env.PORT || '8080');
const PERSIST_DIR = path.resolve(__dirname, '..', process.env.PERSIST_DIR || './data');
const MAX_USERS = parseInt(process.env.MAX_USERS_PER_ROOM || '12');
const NODE_ENV = process.env.NODE_ENV || 'development';

if (!fs.existsSync(PERSIST_DIR)) fs.mkdirSync(PERSIST_DIR, {recursive:true});

console.log(`[CONFIG] PORT=${PORT} ENV=${NODE_ENV} MAX_USERS=${MAX_USERS} PERSIST=${PERSIST_DIR}`);

// ROOM REGISTRY
const rooms = new Map(); // code -> {roomId, ydoc, yMap, users Map, version, createdAt, updatedAt, clients Set}

function generateCode(){
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  const arr = new Uint32Array(6);
  crypto.getRandomValues(arr);
  let c=''; for(let i=0;i<6;i++) c+=chars[arr[i]%chars.length];
  return c;
}
const getRoom = (code)=> rooms.get((code||'').toUpperCase().trim());
function isValidCode(code){ return /^[A-Z0-9]{6}$/.test((code||'').toUpperCase().trim()); }

function createRoom(code=null){
  const roomId = (code||generateCode()).toUpperCase();
  if(!isValidCode(roomId)) throw new Error('INVALID_CODE');
  if(rooms.has(roomId)) return rooms.get(roomId);
  const ydoc = new Y.Doc();
  const yMap = ydoc.getMap('room');
  yMap.set('presentation', null);
  const room = { roomId, ydoc, yMap, users:new Map(), version:0, createdAt:new Date().toISOString(), updatedAt:new Date().toISOString(), clients:new Set() };
  rooms.set(roomId, room);
  persistRoom(roomId);
  console.log(`[CREATE] ${roomId} total=${rooms.size}`);
  return room;
}

function persistRoom(roomId){
  try{
    const room = rooms.get(roomId);
    if(!room) return;
    const pres = room.yMap.get('presentation');
    const data = { roomId, version:room.version, createdAt:room.createdAt, updatedAt:new Date().toISOString(), presentation: pres||null };
    fs.writeFileSync(path.join(PERSIST_DIR, `${roomId}.json`), JSON.stringify(data));
  }catch(e){ console.error('persist error', e.message); }
}

function loadRooms(){
  try{
    const files = fs.readdirSync(PERSIST_DIR).filter(f=>f.endsWith('.json'));
    for(const f of files){
      const data = JSON.parse(fs.readFileSync(path.join(PERSIST_DIR,f),'utf8'));
      if(!isValidCode(data.roomId)) continue;
      const ydoc = new Y.Doc();
      const yMap = ydoc.getMap('room');
      if(data.presentation) yMap.set('presentation', data.presentation);
      rooms.set(data.roomId, { roomId:data.roomId, ydoc, yMap, users:new Map(), version:data.version||0, createdAt:data.createdAt, updatedAt:data.updatedAt, clients:new Set() });
      console.log(`[RESTORED] ${data.roomId} v${data.version}`);
    }
  }catch(e){ console.log('No persisted rooms', e.message); }
}
loadRooms();

// Rate limiting simple
const rateLimit = new Map(); // ip -> {count, resetAt}
function checkRateLimit(ip){
  const now = Date.now();
  const entry = rateLimit.get(ip) || {count:0, resetAt: now+60000};
  if(now > entry.resetAt){ entry.count=0; entry.resetAt=now+60000; }
  entry.count++;
  rateLimit.set(ip, entry);
  return entry.count < 100; // 100 req/min per IP
}

// HTTP SERVER with health endpoint
const httpServer = createServer((req,res)=>{
  const ip = req.socket.remoteAddress;
  if(!checkRateLimit(ip)){ res.writeHead(429,{'Content-Type':'application/json'}); return res.end(JSON.stringify({error:'RATE_LIMITED'})); }
  res.setHeader('Access-Control-Allow-Origin','*');
  res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers','Content-Type');
  if(req.method==='OPTIONS'){ res.writeHead(200); return res.end(); }

  if(req.url==='/health'){
    res.writeHead(200,{'Content-Type':'application/json'});
    return res.end(JSON.stringify({status:'ok', service:'CollabSlides', environment: NODE_ENV, rooms: rooms.size, maxUsersPerRoom: MAX_USERS, uptime: Math.floor(process.uptime()), timestamp: new Date().toISOString()}));
  }
  if(req.url==='/rooms'){
    res.writeHead(200,{'Content-Type':'application/json'});
    return res.end(JSON.stringify({rooms: Array.from(rooms.keys()).map(k=>{const r=rooms.get(k); return {roomId:k, version:r.version, users:r.users.size, maxUsers:MAX_USERS, createdAt:r.createdAt}}), count: rooms.size}));
  }
  if(req.url.startsWith('/room/')){
    const code = req.url.split('/')[2]?.split('?')[0]?.toUpperCase();
    if(!isValidCode(code)){ res.writeHead(400,{'Content-Type':'application/json'}); return res.end(JSON.stringify({error:'INVALID_CODE'})); }
    const room = getRoom(code);
    if(!room){ res.writeHead(404,{'Content-Type':'application/json'}); return res.end(JSON.stringify({error:'ROOM_NOT_FOUND', roomId:code})); }
    res.writeHead(200,{'Content-Type':'application/json'});
    return res.end(JSON.stringify({roomId:code, version:room.version, userCount:room.users.size, maxUsers:MAX_USERS, users:Array.from(room.users.values())}));
  }
  res.writeHead(200,{'Content-Type':'application/json'});
  res.end(JSON.stringify({name:'CollabSlides Cloud Server', version:'3.0.0', health:'/health', maxUsersPerRoom:MAX_USERS}));
});

const wss = new WebSocketServer({ server: httpServer });

wss.on('connection', (ws, req)=>{
  ws._id = Math.random().toString(36).slice(2,10);
  ws._roomId=null; ws._userId=null;
  ws._ip = req.socket.remoteAddress;
  console.log(`[CONNECT] ${ws._id} ip=${ws._ip}`);

  ws.on('message', (raw)=>{
    try{
      if(raw.length > 500000){ ws.send(JSON.stringify({type:'ERROR', message:'PAYLOAD_TOO_LARGE'})); return; } // 500KB limit
      const msg = JSON.parse(raw.toString());
      if(!msg.type || typeof msg.type!=='string'){ ws.send(JSON.stringify({type:'ERROR', message:'INVALID_MESSAGE'})); return; }

      if(msg.type==='CREATE_ROOM'){
        const code = (msg.code||generateCode()).toUpperCase();
        if(!isValidCode(code)){ ws.send(JSON.stringify({type:'ERROR', message:'INVALID_CODE'})); return; }
        const room = createRoom(code);
        if(room.users.size >= MAX_USERS){ ws.send(JSON.stringify({type:'ROOM_FULL', roomId:code, maxUsers:MAX_USERS})); return; }
        if(msg.presentation){
          room.ydoc.transact(()=> room.yMap.set('presentation', msg.presentation));
          room.version++; room.updatedAt=new Date().toISOString(); persistRoom(code);
        }
        ws._roomId=code; ws._userId=msg.userId||`user-${ws._id}`;
        room.clients.add(ws);
        room.users.set(ws._userId, {userId:ws._userId, name:(msg.userName||'User').slice(0,30), color:msg.color||'#7C3AED', joinedAt:new Date().toISOString()});
        ws.send(JSON.stringify({type:'ROOM_CREATED', roomId:code, version:room.version, presentation:room.yMap.get('presentation')}));
        broadcast(code, {type:'USER_JOINED', userId:ws._userId, userName:msg.userName, color:msg.color, userCount:room.users.size, maxUsers:MAX_USERS}, ws);
        return;
      }

      if(msg.type==='JOIN_ROOM'){
        const code = (msg.code||'').toUpperCase().trim();
        if(!isValidCode(code)){ ws.send(JSON.stringify({type:'ERROR', message:'INVALID_CODE'})); return; }
        const room = getRoom(code);
        if(!room){ ws.send(JSON.stringify({type:'ROOM_NOT_FOUND', roomId:code})); return; }
        if(room.users.size >= MAX_USERS){ ws.send(JSON.stringify({type:'ROOM_FULL', roomId:code, maxUsers:MAX_USERS, userCount:room.users.size})); return; }
        ws._roomId=code; ws._userId=msg.userId||`user-${ws._id}`;
        room.clients.add(ws);
        room.users.set(ws._userId, {userId:ws._userId, name:(msg.userName||'User').slice(0,30), color:msg.color||'#7C3AED', joinedAt:new Date().toISOString()});
        ws.send(JSON.stringify({type:'FULL_STATE', roomId:code, version:room.version, presentation:room.yMap.get('presentation'), users:Array.from(room.users.values()), maxUsers:MAX_USERS}));
        broadcast(code, {type:'USER_JOINED', userId:ws._userId, userName:msg.userName, color:msg.color, userCount:room.users.size, maxUsers:MAX_USERS}, ws);
        console.log(`[JOIN] ${ws._userId} -> ${code} ${room.users.size}/${MAX_USERS} v${room.version}`);
        return;
      }

      if(msg.type==='OP'){
        const code = (msg.code||ws._roomId||'').toUpperCase().trim();
        if(!isValidCode(code)) return;
        const room = getRoom(code);
        if(!room) return;
        // Validate op
        if(!msg.op || !msg.op.type){ return; }
        const allowedOps = ['SLIDE_ADD','SLIDE_DELETE','SLIDE_REORDER','ELEMENT_ADD','ELEMENT_UPDATE','ELEMENT_DELETE','PAGE_SETUP','FULL_REPLACE','BATCH'];
        if(!allowedOps.includes(msg.op.type)){ ws.send(JSON.stringify({type:'ERROR', message:'INVALID_OP'})); return; }
        room.ydoc.transact(()=>{
          const current = room.yMap.get('presentation');
          if(!current && msg.presentation){ room.yMap.set('presentation', msg.presentation); }
          else if(msg.op){ const updated = applyOp(current, msg.op); room.yMap.set('presentation', updated); }
          else if(msg.presentation){ room.yMap.set('presentation', msg.presentation); }
        });
        room.version++; room.updatedAt=new Date().toISOString();
        if(room.version%3===0) persistRoom(code);
        broadcast(code, {type:'OP', op:msg.op, presentation:msg.presentation, version:room.version, userId:ws._userId}, ws);
        return;
      }

      if(msg.type==='PRESENCE'){
        const code = (msg.code||ws._roomId||'').toUpperCase().trim();
        const room = getRoom(code);
        if(!room) return;
        const u = room.users.get(ws._userId);
        if(u){ u.cursor=msg.cursor; u.selection=msg.selection; u.lastSeen=new Date().toISOString(); }
        broadcast(code, {type:'PRESENCE', userId:ws._userId, cursor:msg.cursor, selection:msg.selection, name:msg.userName, color:msg.color}, ws);
        return;
      }

      if(msg.type==='PING'){ ws.send(JSON.stringify({type:'PONG'})); return; }
      if(msg.type==='LEAVE_ROOM'){ handleLeave(ws); return; }

    }catch(e){ console.error('msg error', e.message); ws.send(JSON.stringify({type:'ERROR', message:'INVALID_JSON'})); }
  });

  ws.on('close',()=> handleLeave(ws));
  ws.on('error',()=> handleLeave(ws));
});

function handleLeave(ws){
  const code = ws._roomId;
  if(!code) return;
  const room = getRoom(code);
  if(!room) return;
  room.clients.delete(ws);
  if(ws._userId) room.users.delete(ws._userId);
  broadcast(code, {type:'USER_LEFT', userId:ws._userId, userCount:room.users.size, maxUsers:MAX_USERS});
  console.log(`[LEAVE] ${ws._userId} left ${code} ${room.users.size}/${MAX_USERS}`);
}

function broadcast(code, payload, exclude=null){
  const room = getRoom(code);
  if(!room) return;
  const data = JSON.stringify(payload);
  for(const c of room.clients){ if(c!==exclude && c.readyState===1){ try{c.send(data);}catch{} } }
}

function applyOp(current, op){
  if(!current) return null;
  let p = JSON.parse(JSON.stringify(current));
  try{
    if(op.type==='SLIDE_ADD'){ p.slides.splice(op.index ?? p.slides.length, 0, op.slide); }
    else if(op.type==='SLIDE_DELETE'){ p.slides = p.slides.filter(s=>s.id!==op.slideId); }
    else if(op.type==='SLIDE_REORDER'){ const [m]=p.slides.splice(op.from,1); p.slides.splice(op.to,0,m); }
    else if(op.type==='ELEMENT_ADD'){ const s=p.slides.find(s=>s.id===op.slideId); if(s) s.elements.push(op.element); }
    else if(op.type==='ELEMENT_UPDATE'){ const s=p.slides.find(s=>s.id===op.slideId); if(s){ const e=s.elements.find(e=>e.id===op.elementId); if(e) Object.assign(e, op.patch); } }
    else if(op.type==='ELEMENT_DELETE'){ const s=p.slides.find(s=>s.id===op.slideId); if(s) s.elements=s.elements.filter(e=>e.id!==op.elementId); }
    else if(op.type==='PAGE_SETUP'){ p.pageSize=op.pageSize; }
    else if(op.type==='FULL_REPLACE'){ p=op.presentation; }
    else if(op.type==='BATCH'){ for(const sub of op.ops) p=applyOp(p, sub); }
  }catch(e){ console.error('applyOp', e.message); }
  return p;
}

httpServer.listen(PORT, '0.0.0.0', ()=>{
  console.log(`\n🚀 CollabSlides Cloud v3.0 - 12 users max`);
  console.log(`   HTTP: http://localhost:${PORT}/health`);
  console.log(`   WS: ws://localhost:${PORT}`);
  console.log(`   Rooms: ${rooms.size} loaded`);
  console.log(`   Max per room: ${MAX_USERS}`);
  console.log(`   Persistence: ${PERSIST_DIR}\n`);
});
