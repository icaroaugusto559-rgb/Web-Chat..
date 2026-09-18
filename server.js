/**
 * Servidor de chat.
 *
 *   1. Express    -> estaticos + /uploads (avatars) + /health
 *   2. Socket.io  -> canal de tempo real
 *   3. SQLite     -> salas, usuarios, membros, pedidos e mensagens
 *
 * Fluxo de entrada:
 *   tela inicial (nome) -> criar sala | entrar com codigo
 *   criar sala  -> codigo unico garantido pelo banco; criador vira admin
 *   entrar      -> sala publica: entra direto | sala privada: cria pedido e aguarda o admin
 *
 * Conceitos que separam um prototipo de um chat de verdade:
 *   - seq gerada no servidor (nunca o relogio do cliente)
 *   - clientId + dedup (reenvio nao duplica mensagem)
 *   - ack em toda escrita
 *   - token de sessao para manter a identidade entre reconexoes
 */

const path = require('path');
const fs = require('fs');
const http = require('http');
const crypto = require('crypto');
const express = require('express');
const cors = require('cors');
const sharp = require('sharp');
const webpush = require('web-push');
const { Server } = require('socket.io');
const db = require('./db');

const PORT = process.env.PORT || 3000;
const DB_FILE = process.env.DB_FILE || path.join(__dirname, 'data', 'chat.db');
const UPLOAD_DIR = process.env.UPLOAD_DIR || path.join(__dirname, 'uploads');
const VAPID_FILE = process.env.VAPID_FILE || path.join(__dirname, 'data', 'vapid.json');
const VAPID_SUBJECT = process.env.VAPID_SUBJECT || 'mailto:dev@example.com';
// Origens (Netlify etc.) autorizadas a chamar a API de outro dominio.
// Vazio = aceita qualquer origem (modo local/demo); preenchido = lista estrita.
const ALLOWED_ORIGINS = (process.env.WEB_CHAT_ALLOWED_ORIGINS || '')
  .split(',').map((x) => x.trim()).filter(Boolean);

const MAX_MESSAGE_LENGTH = 1000;
const MAX_NAME_LENGTH = 24;
const MAX_ROOM_NAME_LENGTH = 40;
const MAX_DESCRIPTION_LENGTH = 200;
const MAX_AVATAR_BYTES = 6 * 1024 * 1024;   // 6 MB de base64 vindo do cliente
const MAX_FILE_BYTES = 8 * 1024 * 1024;     // 8 MB por anexo
const AVATAR_SIZE = 512;                     // lado do quadrado final
const THUMB_WIDTH = 1024;                    // imagens no chat sao redimensionadas
const ALLOWED_MIME = new Set(['image/png', 'image/jpeg', 'image/webp', 'image/gif']);
const PAGE_SIZE = 50;
const RATE_LIMIT_WINDOW_MS = 5000;
const RATE_LIMIT_MAX = 12;

/* ------------------------------------------------------------------ *
 *  Bootstrap
 * ------------------------------------------------------------------ */

fs.mkdirSync(UPLOAD_DIR, { recursive: true });
fs.mkdirSync(path.dirname(VAPID_FILE), { recursive: true });
const sql = db.openDb(DB_FILE);
console.log(`Banco: ${DB_FILE}`);

/* ------------------------------------------------------------------ *
 *  Web Push (VAPID)
 * ------------------------------------------------------------------ *
 * O Web Push e um padrao do navegador: nao precisa de conta no Google nem
 * de FCM. O servidor assina a notificacao com um par de chaves VAPID e o
 * navegador entrega mesmo com a aba fechada.
 *
 * As chaves sao geradas uma vez e guardadas em data/vapid.json — se elas
 * mudarem a cada restart, as assinaturas antigas param de funcionar.
 */
function loadVapidKeys() {
  // Em hospedagem com disco efemero (Render Free) as chaves vem por ENV,
  // senao cada deploy regeneraria o par e quebraria as assinaturas antigas.
  if (process.env.VAPID_PUBLIC_KEY && process.env.VAPID_PRIVATE_KEY) {
    console.log('Chaves VAPID vindas do ambiente (ENV).');
    return { publicKey: process.env.VAPID_PUBLIC_KEY, privateKey: process.env.VAPID_PRIVATE_KEY };
  }
  try {
    const saved = JSON.parse(fs.readFileSync(VAPID_FILE, 'utf8'));
    if (saved.publicKey && saved.privateKey) return saved;
  } catch (_) { /* primeira execucao */ }
  const keys = webpush.generateVAPIDKeys();
  fs.writeFileSync(VAPID_FILE, JSON.stringify(keys, null, 2), { mode: 0o600 });
  console.log(`Chaves VAPID geradas em ${VAPID_FILE}`);
  return keys;
}

const VAPID_KEYS = loadVapidKeys();
webpush.setVapidDetails(VAPID_SUBJECT, VAPID_KEYS.publicKey, VAPID_KEYS.privateKey);

/** Envia push para os dispositivos de uma pessoa. Erros nao derrubam nada. */
async function pushTo(userId, { title, body, tag, data }) {
  const subs = db.listPushSubscriptions(sql, userId);
  if (subs.length === 0) return 0;

  const payload = JSON.stringify({ title, body, tag, data: data || {} });
  let sent = 0;
  for (const s of subs) {
    try {
      await webpush.sendNotification(
        { endpoint: s.endpoint, keys: { p256dh: s.p256dh, auth: s.auth } },
        payload,
        { TTL: 3600 },
      );
      sent += 1;
    } catch (err) {
      // 404/410 = o navegador descartou a assinatura. Limpamos e seguimos.
      if (err.statusCode === 404 || err.statusCode === 410) {
        db.prunePushSubscription(sql, s.endpoint);
      } else {
        console.warn(`push falhou (${err.statusCode || err.code || 'erro'}): ${err.message}`);
      }
    }
  }
  return sent;
}

/**
 * Notifica quem esta FORA do ar: se a pessoa tem um socket conectado nesta
 * sala, ela ja esta vendo a tela — push seria ruido.
 */
function notifyOffline(userId, roomId, payload) {
  const here = [...online.values()].some((u) => u.userId === userId && u.roomId === roomId);
  if (here) return Promise.resolve(0);
  return pushTo(userId, payload);
}

// Transacoes preparadas uma unica vez (better-sqlite3 recomenda reuso).
const txCreateRoom = db.createRoom(sql);
const txJoinRoom = db.joinRoom(sql);
const txDecideRequest = db.decideRequest(sql);

const app = express();
app.use(express.json({ limit: '64kb' }));

// CORS: chamadas sem Origin (curl, testes) sempre passam; com Origin, so as
// listadas em WEB_CHAT_ALLOWED_ORIGINS (ou todas, se a variavel estiver vazia).
app.use(cors({
  origin: (origin, cb) => {
    if (!origin || ALLOWED_ORIGINS.length === 0 || ALLOWED_ORIGINS.includes(origin)) return cb(null, true);
    return cb(new Error('Origem nao permitida pelo CORS: ' + origin));
  },
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type'],
}));

/**
 * O service worker precisa controlar a raiz do site. Como ele e servido de
 * /sw.js, o escopo padrao ja seria "/", mas o header deixa isso explicito e
 * evita problema quando ha proxy na frente.
 *
 * IMPORTANTE: esta rota vem ANTES do express.static. Se viesse depois, o
 * estatico serviria o arquivo primeiro e o header nunca seria aplicado.
 */
app.get('/sw.js', (_req, res) => {
  res.setHeader('Content-Type', 'application/javascript; charset=utf-8');
  res.setHeader('Service-Worker-Allowed', '/');
  res.setHeader('Cache-Control', 'no-cache');
  res.sendFile(path.join(__dirname, 'public', 'sw.js'));
});

app.use(express.static(path.join(__dirname, 'public')));
app.use('/uploads', express.static(UPLOAD_DIR, { maxAge: '7d', immutable: true }));

/** Chave publica VAPID: o navegador precisa dela para se inscrever. */
app.get('/push/key', (_req, res) => {
  res.json({ publicKey: VAPID_KEYS.publicKey });
});

/** Registra (ou atualiza) a assinatura de push de uma pessoa. */
app.post('/push/subscribe', (req, res) => {
  const { endpoint, keys, token, name } = req.body || {};
  if (!endpoint || !keys?.p256dh || !keys?.auth) {
    return res.status(400).json({ ok: false, error: 'Assinatura incompleta.' });
  }
  try {
    const user = db.resolveUser(sql, { name: name || 'Convidado', token });
    db.addPushSubscription(sql, {
      endpoint,
      userId: user.id,
      keys: { p256dh: keys.p256dh, auth: keys.auth },
      userAgent: String(req.get('user-agent') || '').slice(0, 300),
    });
    return res.json({ ok: true, token: user.token });
  } catch (err) {
    console.error('push/subscribe', err);
    return res.status(500).json({ ok: false, error: 'Nao deu para registrar.' });
  }
});

app.post('/push/unsubscribe', (req, res) => {
  const { endpoint } = req.body || {};
  if (endpoint) db.removePushSubscription(sql, endpoint);
  res.json({ ok: true });
});

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: ALLOWED_ORIGINS.length ? ALLOWED_ORIGINS : true,
    methods: ['GET', 'POST'],
  },
  cors: { origin: '*' },
  // Anexos viajam como base64 no payload do socket. Base64 infla ~33%, entao
  // 8 MB de arquivo viram ~10,7 MB: o limite do engine.io tem que folgar isso.
  maxHttpBufferSize: 14 * 1024 * 1024,
});

/* ------------------------------------------------------------------ *
 *  Estado de conexao (volatil: quem esta conectado agora)
 * ------------------------------------------------------------------ */

/** @type {Map<string, {userId:number, name:string, roomCode:string, roomId:number, role:string, joinedAt:number}>} */
const online = new Map();
/** @type {Map<string, Set<string>>} roomCode -> socketIds */
const roomSockets = new Map();
/** @type {Map<number, Set<string>>} userId -> socketIds (para avisar pedidos) */
const userSockets = new Map();

function track(userId, socketId, add) {
  if (!userSockets.has(userId)) userSockets.set(userId, new Set());
  const set = userSockets.get(userId);
  if (add) set.add(socketId); else set.delete(socketId);
  if (set.size === 0) userSockets.delete(userId);
}

function socketsOf(userId) {
  return [...(userSockets.get(userId) || [])]
    .map((id) => io.sockets.sockets.get(id))
    .filter(Boolean);
}

function emitToUser(userId, event, payload) {
  for (const s of socketsOf(userId)) s.emit(event, payload);
}

/** Sockets conectados de uma sala. */
function socketsInRoom(roomCode) {
  return [...(roomSockets.get(roomCode) || [])]
    .map((id) => io.sockets.sockets.get(id))
    .filter(Boolean);
}

function presenceFor(roomId) {
  const rows = db.listMembers(sql, roomId);
  const here = new Set([...online.values()].filter((u) => u.roomId === roomId).map((u) => u.userId));
  return rows.map((r) => ({
    userId: r.id,
    name: r.name,
    role: r.role,
    online: here.has(r.id),
  }));
}

function systemMessage(roomId, text) {
  const room = db.getRoomById(sql, roomId);
  const { message } = db.appendMessage(sql, { roomId, author: 'sistema', authorId: null, text, kind: 'system' });
  if (room) io.to(room.code).emit('message', message);
  return message;
}

function broadcastPresence(roomId) {
  const room = db.getRoomById(sql, roomId);
  if (!room) return;
  io.to(room.code).emit('presence', presenceFor(roomId));
}

/** Avisa os admins online sobre pedidos pendentes. */
function notifyAdmins(roomId) {
  const room = db.getRoomById(sql, roomId);
  if (!room) return;
  const admins = db.listMembers(sql, roomId).filter((m) => m.role === 'admin');
  const pending = db.countPending(sql, roomId);
  for (const a of admins) {
    emitToUser(a.id, 'requests:update', { roomCode: room.code, pending, requests: db.listPendingRequests(sql, roomId) });
  }
}

/* ------------------------------------------------------------------ *
 *  Avatar (upload de imagem)
 * ------------------------------------------------------------------ */

async function saveAvatar(dataUrl, roomCode) {
  const decoded = decodeDataUrl(dataUrl, { allowMime: ALLOWED_MIME, maxBytes: MAX_AVATAR_BYTES, what: 'Imagem' });
  if (!decoded.ok) return decoded;
  const { mime, buffer } = decoded;

  // sharp valida de verdade: se o conteudo nao for uma imagem, ele lanca erro.
  // Isso e um caso esperado (alguem mandou lixo), entao devolvemos um erro
  // amigavel em vez de deixar a excecao subir e sujar o log com stack trace.
  let meta;
  try {
    meta = await sharp(buffer).metadata();
  } catch (err) {
    return { ok: false, error: 'Esse arquivo nao e uma imagem valida.' };
  }
  if (!meta.width || !meta.height) return { ok: false, error: 'Nao consegui ler essa imagem.' };

  const ext = meta.format === 'jpeg' ? 'jpg' : (meta.format || 'png');
  const filename = `${db.normalizeCode(roomCode)}-${crypto.randomBytes(8).toString('hex')}.${ext}`;
  const full = path.join(UPLOAD_DIR, filename);

  try {
    await sharp(buffer)
      .rotate()                                      // aplica EXIF orientation
      .resize(AVATAR_SIZE, AVATAR_SIZE, { fit: 'cover', position: 'centre' })
      .toFormat(ext === 'jpg' ? 'jpeg' : ext, { quality: 85 })
      .toFile(full);
  } catch (err) {
    console.warn(`saveAvatar: falha ao gravar ${filename}: ${err.message}`);
    return { ok: false, error: 'Nao consegui salvar a imagem.' };
  }

  return { ok: true, filename, url: `/uploads/${filename}`, bytes: fs.statSync(full).size };
}

/* ------------------------------------------------------------------ *
 *  Anexos (foto e arquivo no chat)
 * ------------------------------------------------------------------ */

function decodeDataUrl(dataUrl, { allowMime, maxBytes, what }) {
  if (typeof dataUrl !== 'string' || !dataUrl.startsWith('data:')) {
    return { ok: false, error: `${what} invalido.` };
  }
  const match = dataUrl.match(/^data:([^;]+);base64,([\s\S]*)$/);
  if (!match) return { ok: false, error: `${what} invalido.` };

  const mime = match[1].toLowerCase();
  if (allowMime && !allowMime.has(mime)) {
    return { ok: false, error: `Tipo ${mime} nao e aceito aqui.` };
  }
  const buffer = Buffer.from(match[2], 'base64');
  if (buffer.length === 0) return { ok: false, error: `${what} vazio.` };
  if (buffer.length > maxBytes) {
    return { ok: false, error: `${what} grande demais (max ${Math.round(maxBytes / 1024 / 1024)} MB).` };
  }
  return { ok: true, mime, buffer };
}

/**
 * Salva um anexo e devolve o objeto que vai junto na mensagem.
 *  - imagem  -> redimensionada e marcada kind:'image' (o cliente mostra preview)
 *  - arquivo -> guardado como veio, kind:'file' (o cliente mostra cartao de download)
 */
async function saveAttachment(dataUrl, originalName) {
  const decoded = decodeDataUrl(dataUrl, { allowMime: null, maxBytes: MAX_FILE_BYTES, what: 'Arquivo' });
  if (!decoded.ok) return decoded;

  const { mime, buffer } = decoded;
  const safeName = String(originalName || 'arquivo').replace(/[\\/\u0000-\u001f]/g, '_').slice(0, 120) || 'arquivo';
  const isImage = mime.startsWith('image/');
  const filename = `${Date.now().toString(36)}-${crypto.randomBytes(8).toString('hex')}`;
  const full = path.join(UPLOAD_DIR, filename);

  if (isImage) {
    let meta;
    try {
      meta = await sharp(buffer).metadata();
    } catch (err) {
      return { ok: false, error: 'Essa imagem esta corrompida.' };
    }
    const ext = meta.format === 'jpeg' ? 'jpg' : (meta.format || 'png');
    const target = path.join(UPLOAD_DIR, `${filename}.${ext}`);
    try {
      const out = await sharp(buffer)
        .rotate()
        // So encolhe: nunca aumenta uma imagem pequena.
        .resize({ width: THUMB_WIDTH, height: THUMB_WIDTH, fit: 'inside', withoutEnlargement: true })
        .toFormat(ext === 'jpg' ? 'jpeg' : ext, { quality: 82 })
        .toFile(target);
      const finalMeta = await sharp(target).metadata();
      return {
        ok: true,
        attachment: {
          kind: 'image',
          url: `/uploads/${path.basename(target)}`,
          name: safeName,
          mime,
          bytes: out.size,
          width: finalMeta.width,
          height: finalMeta.height,
        },
      };
    } catch (err) {
      console.warn(`saveAttachment: imagem falhou: ${err.message}`);
      return { ok: false, error: 'Nao consegui processar essa imagem.' };
    }
  }

  // Nao-imagem: guarda como veio.
  const ext = path.extname(safeName).replace(/[^.\w-]/g, '').slice(0, 12);
  const target = path.join(UPLOAD_DIR, `${filename}${ext}`);
  fs.writeFileSync(target, buffer);
  return {
    ok: true,
    attachment: {
      kind: 'file',
      url: `/uploads/${path.basename(target)}`,
      name: safeName,
      mime,
      bytes: buffer.length,
    },
  };
}

/* ------------------------------------------------------------------ *
 *  Socket.io
 * ------------------------------------------------------------------ */

io.on('connection', (socket) => {
  const bucket = { count: 0, resetAt: Date.now() + RATE_LIMIT_WINDOW_MS };
  const ack = (fn) => (typeof fn === 'function' ? fn : () => {});

  /** Gera um codigo unico de verdade (usado pelo botao "gerar"). */
  socket.on('room:code', (_payload, cb) => {
    try {
      ack(cb)({ ok: true, code: db.generateUniqueCode(sql, 5) });
    } catch (err) {
      ack(cb)({ ok: false, error: err.message });
    }
  });

  /** Informacoes publicas de uma sala, para a tela de banner. */
  socket.on('room:profile', (payload, cb) => {
    try {
      const code = db.normalizeCode(payload?.code);
      if (code.length < db.CODE_MIN) {
        return ack(cb)({ ok: false, error: `O codigo precisa ter pelo menos ${db.CODE_MIN} caracteres.` });
      }
      const profile = db.roomProfile(sql, code);
      if (!profile) return ack(cb)({ ok: false, error: 'not_found', detail: 'Nenhuma sala com esse codigo.' });
      return ack(cb)({ ok: true, profile });
    } catch (err) {
      return ack(cb)({ ok: false, error: err.message });
    }
  });

  /**
   * Cria a sala. O codigo e verificado pelo banco: duplicado nunca passa.
   * payload: { name, code, roomName, description, isPrivate, avatar, token }
   */
  socket.on('room:create', async (payload, cb) => {
    const reply = ack(cb);
    try {
      const name = String(payload?.name || '').trim().slice(0, MAX_NAME_LENGTH);
      if (name.length < 2) return reply({ ok: false, error: 'Escolha um nome com pelo menos 2 caracteres.' });

      const roomName = String(payload?.roomName || '').trim().slice(0, MAX_ROOM_NAME_LENGTH) || 'Sala sem nome';
      const description = String(payload?.description || '').trim().slice(0, MAX_DESCRIPTION_LENGTH);
      const isPrivate = Boolean(payload?.isPrivate);
      const code = db.normalizeCode(payload?.code);

      const user = db.resolveUser(sql, { name, token: payload?.token });

      const created = txCreateRoom({ code, name: roomName, description, isPrivate, avatarPath: null, user });
      if (!created.ok) return reply({ ok: false, error: created.error, detail: created.detail });

      const room = db.getRoomById(sql, created.roomId);

      let avatarUrl = null;
      if (payload?.avatar) {
        const saved = await saveAvatar(payload.avatar, room.code);
        if (saved.ok) {
          db.setAvatar(sql, room.id, saved.filename);
          avatarUrl = saved.url;
        }
        // Se a imagem for invalida a sala ainda assim e criada: nao e motivo
        // para derrubar o fluxo inteiro.
      }

      reply({
        ok: true,
        code: room.code,
        token: user.token,
        profile: db.roomProfile(sql, room.code),
        avatarUrl,
      });
    } catch (err) {
      console.error('room:create', err);
      return reply({ ok: false, error: 'Erro ao criar a sala.' });
    }
  });

  /** Troca o avatar de uma sala (so o admin). */
  socket.on('room:avatar', async (payload, cb) => {
    const reply = ack(cb);
    const sess = online.get(socket.id);
    if (!sess) return reply({ ok: false, error: 'Entre em uma sala primeiro.' });

    const room = db.getRoomById(sql, sess.roomId);
    if (!room) return reply({ ok: false, error: 'Sala nao encontrada.' });
    const me = db.getMembership(sql, room.id, sess.userId);
    if (!me || me.role !== 'admin') return reply({ ok: false, error: 'forbidden', detail: 'So o admin troca a foto.' });

    try {
      const saved = await saveAvatar(payload?.avatar, room.code);
      if (!saved.ok) return reply(saved);
      db.setAvatar(sql, room.id, saved.filename);
      io.to(room.code).emit('room:profile', db.roomProfile(sql, room.code));
      return reply({ ok: true, url: saved.url, profile: db.roomProfile(sql, room.code) });
    } catch (err) {
      console.error('room:avatar', err);
      return reply({ ok: false, error: 'Erro ao salvar a imagem.' });
    }
  });

  /** Monta o pacote que o cliente precisa para abrir o chat. */
  function joinedPayload(room, user, role) {
    return {
      ok: true,
      state: 'joined',
      token: user.token,
      code: room.code,
      role,
      name: user.name,
      history: db.listMessages(sql, room.id, { limit: PAGE_SIZE }),
      reactions: db.listReactions(sql, room.id),
      reads: db.listReads(sql, room.id),
      presence: presenceFor(room.id),
      profile: db.roomProfile(sql, room.code),
      pending: db.countPending(sql, room.id),
      requests: role === 'admin' ? db.listPendingRequests(sql, room.id) : [],
    };
  }

  /** Registra o socket na sala e no mapa de online (idempotente). */
  function attachToRoom(socket, room, user, role) {
    socket.join(room.code);
    online.set(socket.id, { userId: user.id, name: user.name, roomCode: room.code, roomId: room.id, role, joinedAt: Date.now() });
    if (!roomSockets.has(room.code)) roomSockets.set(room.code, new Set());
    roomSockets.get(room.code).add(socket.id);
    track(user.id, socket.id, true);
  }

  /**
   * Entra na sala.
   *  - publica  -> vira membro e recebe historico
   *  - privada  -> cria pedido e fica aguardando (nao entra no canal)
   *  - ja estava nesta sala com o mesmo token -> reentrada (ex.: a pessoa
   *    recarregou a pagina ou acabou de ser aprovada). Nao anuncia de novo.
   */
  socket.on('join', (payload, cb) => {
    const reply = ack(cb);
    try {
      const name = String(payload?.name || '').trim().slice(0, MAX_NAME_LENGTH);
      if (name.length < 2) return reply({ ok: false, error: 'Escolha um nome com pelo menos 2 caracteres.' });

      const code = db.normalizeCode(payload?.code);
      const room = db.getRoomByCode(sql, code);
      if (!room) return reply({ ok: false, error: 'not_found', detail: 'Nenhuma sala com esse codigo.' });

      const user = db.resolveUser(sql, { name, token: payload?.token });

      // Reentrada: mesmo socket, mesma pessoa, mesma sala.
      const sess = online.get(socket.id);
      if (sess) {
        if (sess.userId !== user.id) {
          return reply({ ok: false, error: 'Esta conexao ja esta em uso por outra pessoa.' });
        }
        if (sess.roomId === room.id) {
          const membership = db.getMembership(sql, room.id, user.id);
          if (!membership) return reply({ ok: false, error: 'Voce nao esta mais nesta sala.' });
          attachToRoom(socket, room, user, membership.role);
          return reply(joinedPayload(room, user, membership.role));
        }
        return reply({ ok: false, error: `Voce ja esta na sala #${sess.roomCode}. Saia dela antes de entrar em outra.` });
      }

      const joined = txJoinRoom({ roomId: room.id, userId: user.id });

      if (!joined.ok) {
        return reply({ ok: false, error: joined.error, detail: joined.detail, profile: db.roomProfile(sql, room.code) });
      }

      // Sala privada: cria/atualiza o pedido e para aqui.
      if (!joined.joined) {
        const isNewRequest = !joined.duplicate;
        track(user.id, socket.id, true);
        notifyAdmins(room.id);
        if (isNewRequest) {
          for (const a of db.listMembers(sql, room.id).filter((m) => m.role === 'admin')) {
            pushTo(a.id, {
              title: 'Novo pedido de entrada',
              body: `${user.name} quer entrar em #${room.code} (${room.name}).`,
              tag: `pending-${room.code}`,
              data: { roomCode: room.code, pending: true },
            });
          }
        }
        return reply({
          ok: true,
          state: 'pending',
          token: user.token,
          code: room.code,
          duplicate: Boolean(joined.duplicate),
          profile: db.roomProfile(sql, room.code),
          pendingSince: db.getRequest(sql, room.id, user.id)?.created_at,
        });
      }

      // Entrou de verdade.
      const role = joined.role || 'member';
      attachToRoom(socket, room, user, role);

      const announcement = db.appendMessage(sql, {
        roomId: room.id, author: 'sistema', authorId: null,
        text: `${user.name} entrou na sala`, kind: 'system',
      }).message;
      io.to(room.code).emit('message', announcement);

      // A presenca vai ANTES do ack: quem ja esta na sala tem que ver a lista
      // atualizada junto com o anuncio de "entrou", e nao depois.
      broadcastPresence(room.id);
      reply(joinedPayload(room, user, role));
      return undefined;
    } catch (err) {
      console.error('join', err);
      return reply({ ok: false, error: 'Erro ao entrar na sala.' });
    }
  });

  /** Lista de pedidos pendentes (so admin). */
  socket.on('requests:list', (_payload, cb) => {
    const reply = ack(cb);
    const sess = online.get(socket.id);
    if (!sess) return reply({ ok: false, error: 'Entre em uma sala primeiro.' });
    const me = db.getMembership(sql, sess.roomId, sess.userId);
    if (!me || me.role !== 'admin') return reply({ ok: false, error: 'forbidden', detail: 'So o admin ve os pedidos.' });
    return reply({ ok: true, requests: db.listPendingRequests(sql, sess.roomId), pending: db.countPending(sql, sess.roomId) });
  });

  /** Aprova ou recusa um pedido (so admin). */
  socket.on('request:decide', (payload, cb) => {
    const reply = ack(cb);
    const sess = online.get(socket.id);
    if (!sess) return reply({ ok: false, error: 'Entre em uma sala primeiro.' });

    const requestId = Number(payload?.requestId);
    if (!Number.isFinite(requestId)) return reply({ ok: false, error: 'Pedido invalido.' });

    const decided = txDecideRequest({ requestId, adminUserId: sess.userId, approve: Boolean(payload?.approve) });
    if (!decided.ok) {
      return reply({ ok: false, error: decided.error, detail: decided.detail });
    }

    const room = db.getRoomById(sql, decided.roomId);

    // Avisa quem pediu.
    if (decided.approved) {
      const welcome = db.appendMessage(sql, {
        roomId: room.id, author: 'sistema', authorId: null,
        text: `${decided.userName} foi aprovado(a) e entrou na sala`, kind: 'system',
      }).message;
      io.to(room.code).emit('message', welcome);
      emitToUser(decided.userId, 'request:approved', { roomCode: room.code, profile: db.roomProfile(sql, room.code) });
      pushTo(decided.userId, {
        title: 'Pedido aprovado',
        body: `Voce pode entrar em #${room.code} (${room.name}).`,
        tag: `request-${room.code}`,
        data: { roomCode: room.code, approved: true },
      });
    } else {
      emitToUser(decided.userId, 'request:rejected', { roomCode: room.code });
      pushTo(decided.userId, {
        title: 'Pedido recusado',
        body: `Seu pedido para entrar em #${room.code} foi recusado.`,
        tag: `request-${room.code}`,
        data: { roomCode: room.code, approved: false },
      });
    }

    notifyAdmins(room.id);
    broadcastPresence(room.id);
    return reply({ ok: true, approved: decided.approved, pending: db.countPending(sql, room.id) });
  });

  /** Envia mensagem. ack devolve a seq definitiva. */
  socket.on('message', (payload, cb) => {
    const reply = ack(cb);
    const sess = online.get(socket.id);
    if (!sess) return reply({ ok: false, error: 'Entre em uma sala primeiro.' });
    const now = Date.now();
    if (now > bucket.resetAt) { bucket.count = 0; bucket.resetAt = now + RATE_LIMIT_WINDOW_MS; }
    if (bucket.count >= RATE_LIMIT_MAX) return reply({ ok: false, error: 'Devagar: muitas mensagens em sequencia.' });

    const text = String(payload?.text || '').trim().slice(0, MAX_MESSAGE_LENGTH);

    // O anexo vem ja enviado (attachment:upload). Validamos o minimo aqui:
    // precisa ter URL do nosso proprio /uploads, senao alguem poderia enfiar
    // uma URL qualquer dentro da bolha.
    let attachment = null;
    if (payload?.attachment) {
      const a = payload.attachment;
      if (typeof a.url === 'string' && a.url.startsWith('/uploads/') && (a.kind === 'image' || a.kind === 'file')) {
        attachment = {
          kind: a.kind,
          url: a.url.slice(0, 300),
          name: String(a.name || 'arquivo').slice(0, 120),
          mime: String(a.mime || '').slice(0, 100),
          bytes: Number(a.bytes) || 0,
          width: Number(a.width) || undefined,
          height: Number(a.height) || undefined,
        };
      } else {
        return reply({ ok: false, error: 'Anexo invalido.' });
      }
    }
    if (!text && !attachment) return reply({ ok: false, error: 'Mensagem vazia.' });

    bucket.count += 1;
    const { message, duplicate } = db.appendMessage(sql, {
      roomId: sess.roomId,
      author: sess.name,
      authorId: sess.userId,
      clientId: payload?.clientId ? String(payload.clientId).slice(0, 80) : undefined,
      text,
      replyTo: payload?.replyTo,
      attachment,
    });

    // Todos recebem, inclusive o autor (troca "enviando" por "entregue").
    if (!duplicate) io.to(sess.roomCode).emit('message', message);

    // Push para quem esta com a aba fechada / fora do ar.
    if (!duplicate) {
      const room = db.getRoomById(sql, sess.roomId);
      const base = text || (attachment.kind === 'image' ? '📷 Foto' : `📎 ${attachment.name}`);
      const excerpt = base.length > 90 ? `${base.slice(0, 90)}…` : base;
      for (const m of db.listMembers(sql, sess.roomId)) {
        if (m.id === sess.userId) continue;
        notifyOffline(m.id, sess.roomId, {
          title: `${sess.name} · #${sess.roomCode}`,
          body: excerpt,
          tag: `msg-${sess.roomCode}`,
          data: { roomCode: sess.roomCode, roomName: room?.name || sess.roomCode, messageId: message.id },
        });
      }
    }
    return reply({ ok: true, message, duplicate });
  });

  /** Historico paginado por cursor. */
  socket.on('history', (payload, cb) => {
    const reply = ack(cb);
    const sess = online.get(socket.id);
    if (!sess) return reply({ ok: false, error: 'Entre em uma sala primeiro.' });
    const beforeSeq = Number(payload?.beforeSeq);
    const messages = db.listMessages(sql, sess.roomId, {
      beforeSeq: Number.isFinite(beforeSeq) ? beforeSeq : undefined,
      limit: PAGE_SIZE,
    });
    return reply({ ok: true, messages, oldestSeq: messages[0]?.seq ?? 0 });
  });

  socket.on('typing', (payload) => {
    const sess = online.get(socket.id);
    if (!sess) return;
    socket.to(sess.roomCode).emit('typing', { name: sess.name, isTyping: Boolean(payload?.isTyping) });
  });

  /* ------------------------- anexo: foto e arquivo ------------------------- */

  /**
   * Sobe o arquivo e devolve o descritor. O cliente entao envia a mensagem com
   * esse descritor — assim o upload nao fica preso ao ack da mensagem e a
   * pessoa ve o progresso antes de enviar.
   */
  socket.on('attachment:upload', async (payload, cb) => {
    const reply = ack(cb);
    const sess = online.get(socket.id);
    if (!sess) return reply({ ok: false, error: 'Entre em uma sala primeiro.' });

    try {
      const saved = await saveAttachment(payload?.dataUrl, payload?.name);
      if (!saved.ok) return reply(saved);
      return reply({ ok: true, attachment: saved.attachment });
    } catch (err) {
      console.error('attachment:upload', err);
      return reply({ ok: false, error: 'Erro ao enviar o arquivo.' });
    }
  });

  /* ------------------------------ visto por ------------------------------ */

  /**
   * A pessoa leu tudo ate `upToSeq`. Marcamos de uma vez e avisamos a sala,
   * para o autor ver o "visto por" aparecer.
   */
  socket.on('message:read', (payload, cb) => {
    const reply = ack(cb);
    const sess = online.get(socket.id);
    if (!sess) return reply({ ok: false, error: 'Entre em uma sala primeiro.' });

    const upToSeq = Number(payload?.upToSeq);
    const res = db.markRead(sql)({ roomId: sess.roomId, userId: sess.userId, upToSeq });
    if (!res.ok) return reply({ ok: false, error: res.error, detail: res.detail });

    if (res.newlyRead > 0) {
      socket.to(sess.roomCode).emit('reads', {
        userId: sess.userId,
        name: sess.name,
        upToSeq,
      });
    }
    return reply({ ok: true, newlyRead: res.newlyRead });
  });

  /* ------------------------- reacoes e apagar ------------------------- */

  /** Alterna uma reacao numa mensagem. */
  socket.on('message:react', (payload, cb) => {
    const reply = ack(cb);
    const sess = online.get(socket.id);
    if (!sess) return reply({ ok: false, error: 'Entre em uma sala primeiro.' });

    const messageId = Number(String(payload?.messageId || '').replace(/^m/, ''));
    if (!Number.isFinite(messageId)) return reply({ ok: false, error: 'Mensagem invalida.' });

    const res = db.toggleReaction(sql)({
      roomId: sess.roomId, messageId, userId: sess.userId, emoji: payload?.emoji,
    });
    if (!res.ok) return reply({ ok: false, error: res.error, detail: res.detail });

    io.to(sess.roomCode).emit('reactions', {
      messageId: payload.messageId,
      emoji: res.emoji,
      reactions: res.reactions,
    });
    return reply({ ok: true, added: res.added });
  });

  /** Apaga uma mensagem (o autor, ou um admin). */
  socket.on('message:delete', (payload, cb) => {
    const reply = ack(cb);
    const sess = online.get(socket.id);
    if (!sess) return reply({ ok: false, error: 'Entre em uma sala primeiro.' });

    const rawId = String(payload?.messageId || '');
    const messageId = Number(rawId.replace(/^m/, ''));
    if (!Number.isFinite(messageId)) return reply({ ok: false, error: 'Mensagem invalida.' });

    const res = db.deleteMessage(sql)({ roomId: sess.roomId, messageId, actorUserId: sess.userId });
    if (!res.ok) return reply({ ok: false, error: res.error, detail: res.detail });

    io.to(sess.roomCode).emit('message:deleted', { messageId: rawId, seq: res.seq, byAdmin: res.byAdmin });
    return reply({ ok: true });
  });

  /* ---------------------- administracao da sala ---------------------- */

  /** Edita nome, descricao ou privacidade (so admin). */
  socket.on('room:update', (payload, cb) => {
    const reply = ack(cb);
    const sess = online.get(socket.id);
    if (!sess) return reply({ ok: false, error: 'Entre em uma sala primeiro.' });

    const res = db.updateRoom(sql)({
      roomId: sess.roomId,
      actorUserId: sess.userId,
      name: payload?.name,
      description: payload?.description,
      isPrivate: payload?.isPrivate,
    });
    if (!res.ok) return reply({ ok: false, error: res.error, detail: res.detail });

    const profile = db.roomProfile(sql, sess.roomCode);
    io.to(sess.roomCode).emit('room:profile', profile);
    const note = db.appendMessage(sql, {
      roomId: sess.roomId, author: 'sistema', authorId: null,
      text: `${sess.name} atualizou as informacoes da sala`, kind: 'system',
    }).message;
    io.to(sess.roomCode).emit('message', note);
    return reply({ ok: true, profile });
  });

  /** Promove a admin ou rebaixa a member (so admin). */
  socket.on('member:role', (payload, cb) => {
    const reply = ack(cb);
    const sess = online.get(socket.id);
    if (!sess) return reply({ ok: false, error: 'Entre em uma sala primeiro.' });

    const res = db.setRole(sql)({
      roomId: sess.roomId,
      targetUserId: Number(payload?.userId),
      actorUserId: sess.userId,
      role: payload?.role,
    });
    if (!res.ok) return reply({ ok: false, error: res.error, detail: res.detail });

    const note = db.appendMessage(sql, {
      roomId: sess.roomId, author: 'sistema', authorId: null,
      text: res.role === 'admin' ? `${res.name} agora e administrador(a)` : `${res.name} voltou a ser membro`,
      kind: 'system',
    }).message;
    io.to(sess.roomCode).emit('message', note);
    broadcastPresence(sess.roomId);
    notifyAdmins(sess.roomId);
    return reply({ ok: true, role: res.role });
  });

  /** Remove um membro (admin) ou sai da sala (o proprio). */
  socket.on('member:remove', (payload, cb) => {
    const reply = ack(cb);
    const sess = online.get(socket.id);
    if (!sess) return reply({ ok: false, error: 'Entre em uma sala primeiro.' });

    const targetUserId = Number(payload?.userId) || sess.userId;
    const res = db.removeMember(sql)({ roomId: sess.roomId, targetUserId, actorUserId: sess.userId });
    if (!res.ok) return reply({ ok: false, error: res.error, detail: res.detail });

    const note = db.appendMessage(sql, {
      roomId: sess.roomId, author: 'sistema', authorId: null,
      text: res.self ? `${res.name} saiu da sala` : `${res.name} foi removido(a) da sala`,
      kind: 'system',
    }).message;
    io.to(sess.roomCode).emit('message', note);

    // Desconecta os sockets daquela pessoa nesta sala.
    for (const s of socketsOf(targetUserId)) {
      const os = online.get(s.id);
      if (os && os.roomId === sess.roomId) {
        online.delete(s.id);
        roomSockets.get(sess.roomCode)?.delete(s.id);
        s.leave(sess.roomCode);
        s.emit('removed', { roomCode: sess.roomCode, self: res.self });
      }
    }
    track(targetUserId, socket.id, false);

    if (!res.self) {
      pushTo(targetUserId, {
        title: 'Voce foi removido(a)',
        body: `${sess.name} removeu voce de #${sess.roomCode}.`,
        tag: `removed-${sess.roomCode}`,
        data: { roomCode: sess.roomCode, removed: true },
      });
    }

    broadcastPresence(sess.roomId);
    notifyAdmins(sess.roomId);
    return reply({ ok: true, self: res.self });
  });

  /** Apaga a sala inteira (so o criador). */
  socket.on('room:delete', (_payload, cb) => {
    const reply = ack(cb);
    const sess = online.get(socket.id);
    if (!sess) return reply({ ok: false, error: 'Entre em uma sala primeiro.' });

    const roomCode = sess.roomCode;
    const roomId = sess.roomId;
    const membersBefore = db.listMembers(sql, roomId);
    const res = db.deleteRoom(sql)({ roomId, actorUserId: sess.userId });
    if (!res.ok) return reply({ ok: false, error: res.error, detail: res.detail });

    for (const s of socketsInRoom(roomCode)) {
      online.delete(s.id);
      s.emit('room:deleted', { roomCode });
      s.leave(roomCode);
    }
    roomSockets.delete(roomCode);

    for (const m of membersBefore) {
      if (m.id === sess.userId) continue;
      pushTo(m.id, {
        title: 'Sala apagada',
        body: `#${roomCode} foi apagada por ${sess.name}.`,
        tag: `deleted-${roomCode}`,
        data: { roomCode, deleted: true },
      });
    }
    return reply({ ok: true, code: res.code });
  });

  socket.on('disconnect', () => {
    const sess = online.get(socket.id);
    if (!sess) return;
    online.delete(socket.id);
    roomSockets.get(sess.roomCode)?.delete(socket.id);
    track(sess.userId, socket.id, false);

    const room = db.getRoomById(sql, sess.roomId);
    if (!room) return;
    const stillHere = [...online.values()].some((u) => u.userId === sess.userId && u.roomId === sess.roomId);
    if (!stillHere) {
      const announcement = db.appendMessage(sql, {
        roomId: room.id, author: 'sistema', authorId: null,
        text: `${sess.name} saiu da sala`, kind: 'system',
      }).message;
      io.to(room.code).emit('message', announcement);
    }
    broadcastPresence(room.id);
  });
});

app.get('/health', (_req, res) => {
  const rooms = sql.prepare('SELECT code, name, is_private FROM rooms ORDER BY created_at DESC LIMIT 50').all();
  res.json({
    ok: true,
    sockets: io.engine.clientsCount,
    rooms: rooms.length,
    roomList: rooms,
    db: DB_FILE,
    uptime: process.uptime(),
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Chat rodando em http://0.0.0.0:${PORT}`);
});
