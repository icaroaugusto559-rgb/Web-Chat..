/**
 * Camada de persistencia (SQLite via better-sqlite3).
 *
 * Por que SQLite aqui: o requisito "o codigo da sala nao pode se repetir de
 * jeito nenhum" so e garantido de verdade se a unicidade for aplicada pelo
 * banco, e nao por uma verificacao em memoria. A coluna `code` tem PRIMARY
 * KEY, entao o proprio banco recusa duplicatas — mesmo com duas requisicoes
 * simultaneas, e mesmo depois de reiniciar o servidor.
 *
 * A tabela `retired_codes` guarda todo codigo ja usado, inclusive de salas
 * apagadas, para que um codigo nunca seja reciclado.
 */

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const Database = require('better-sqlite3');

const ALPHABET = 'abcdefghijklmnopqrstuvwxyz0123456789';
const CODE_MIN = 3;
const CODE_MAX = 24;

function openDb(file) {
  const dir = path.dirname(path.resolve(file));
  fs.mkdirSync(dir, { recursive: true });

  const db = new Database(file);
  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');

  const hasColumn = (table, col) => db.prepare(`PRAGMA table_info(${table})`).all().some((c) => c.name === col);

  db.exec(`
    CREATE TABLE IF NOT EXISTS rooms (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      code          TEXT    NOT NULL UNIQUE,
      name          TEXT    NOT NULL,
      description   TEXT    NOT NULL DEFAULT '',
      is_private    INTEGER NOT NULL DEFAULT 0,
      avatar_path   TEXT,
      created_by    INTEGER NOT NULL,
      created_at    INTEGER NOT NULL
    );

    -- Todo codigo ja usado algum dia. Existe para que um codigo nunca seja
    -- reciclado, nem depois que a sala for apagada.
    --   used_by_room = 0 -> apenas reservado (botao "gerar codigo"); ainda pode
    --                       virar uma sala.
    --   used_by_room = 1 -> ja pertenceu a uma sala de verdade; fica bloqueado
    --                       para sempre, mesmo que a sala seja apagada.
    CREATE TABLE IF NOT EXISTS retired_codes (
      code         TEXT PRIMARY KEY,
      retired_at   INTEGER NOT NULL,
      used_by_room INTEGER NOT NULL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS users (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      name       TEXT    NOT NULL,
      token      TEXT    NOT NULL UNIQUE,
      created_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS memberships (
      room_id   INTEGER NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
      user_id   INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      role      TEXT    NOT NULL DEFAULT 'member',
      joined_at INTEGER NOT NULL,
      PRIMARY KEY (room_id, user_id)
    );

    CREATE TABLE IF NOT EXISTS requests (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      room_id    INTEGER NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
      user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      status     TEXT    NOT NULL DEFAULT 'pending',
      decided_by INTEGER,
      created_at INTEGER NOT NULL,
      decided_at INTEGER,
      UNIQUE (room_id, user_id)
    );

    CREATE TABLE IF NOT EXISTS messages (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      room_id    INTEGER NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
      seq        INTEGER NOT NULL,
      author_id  INTEGER,
      author     TEXT    NOT NULL,
      client_id  TEXT,
      kind       TEXT    NOT NULL DEFAULT 'chat',
      text       TEXT    NOT NULL,
      created_at INTEGER NOT NULL,
      UNIQUE (room_id, seq)
    );

    CREATE INDEX IF NOT EXISTS idx_rooms_code        ON rooms (code);
    CREATE INDEX IF NOT EXISTS idx_messages_room_seq ON messages (room_id, seq);
    CREATE INDEX IF NOT EXISTS idx_memberships_user  ON memberships (user_id);
    CREATE INDEX IF NOT EXISTS idx_requests_room     ON requests (room_id, status);
  `);

  // ------------------ Migracoes leves (bancos antigos) ------------------
  if (!hasColumn('retired_codes', 'used_by_room')) {
    db.exec('ALTER TABLE retired_codes ADD COLUMN used_by_room INTEGER NOT NULL DEFAULT 0');
    db.exec('UPDATE retired_codes SET used_by_room = 1 WHERE code IN (SELECT code FROM rooms)');
  }
  if (!hasColumn('messages', 'deleted_at')) {
    db.exec('ALTER TABLE messages ADD COLUMN deleted_at INTEGER');
  }
  if (!hasColumn('messages', 'reply_to')) {
    db.exec('ALTER TABLE messages ADD COLUMN reply_to INTEGER REFERENCES messages(id) ON DELETE SET NULL');
  }
  if (!hasColumn('messages', 'attachment')) {
    db.exec('ALTER TABLE messages ADD COLUMN attachment TEXT');
  }
  db.exec(`
    CREATE TABLE IF NOT EXISTS reactions (
      room_id    INTEGER NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
      message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
      user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      emoji      TEXT    NOT NULL,
      created_at INTEGER NOT NULL,
      PRIMARY KEY (room_id, message_id, user_id)
    );
    CREATE INDEX IF NOT EXISTS idx_reactions_msg ON reactions (room_id, message_id);

    -- Assinaturas de push (Web Push / VAPID). O endpoint e a chave publica
    -- da assinatura juntos identificam o dispositivo, entao sao a chave.
    CREATE TABLE IF NOT EXISTS push_subscriptions (
      endpoint   TEXT PRIMARY KEY,
      user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      p256dh     TEXT    NOT NULL,
      auth       TEXT    NOT NULL,
      user_agent TEXT,
      created_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_push_user ON push_subscriptions (user_id);

    -- "Visto por": marca ate onde cada pessoa leu. Guardamos uma linha por
    -- (mensagem, pessoa) para conseguir responder "quem ja viu esta?".
    CREATE TABLE IF NOT EXISTS message_reads (
      room_id    INTEGER NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
      message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
      user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      read_at    INTEGER NOT NULL,
      PRIMARY KEY (message_id, user_id)
    );
    CREATE INDEX IF NOT EXISTS idx_reads_room ON message_reads (room_id, message_id);
    CREATE INDEX IF NOT EXISTS idx_reads_user ON message_reads (user_id);
  `);

  return db;
}

/* ------------------------------------------------------------------ *
 *  Codigos de sala
 * ------------------------------------------------------------------ */

function normalizeCode(raw) {
  return String(raw || '').trim().toLowerCase().replace(/[^a-z0-9]/g, '').slice(0, CODE_MAX);
}

function randomCode(len = 5) {
  let out = '';
  for (let i = 0; i < len; i += 1) {
    out += ALPHABET[crypto.randomInt(0, ALPHABET.length)];
  }
  return out;
}

/**
 * Gera um codigo garantidamente inedito.
 * Tenta inserir em retired_codes; se o banco reclamar de PRIMARY KEY,
 * o codigo ja existiu um dia e tentamos outro.
 */
function generateUniqueCode(db, len = 5, maxTries = 60) {
  const insert = db.prepare('INSERT INTO retired_codes (code, retired_at) VALUES (?, ?)');
  for (let i = 0; i < maxTries; i += 1) {
    const code = randomCode(len);
    try {
      insert.run(code, Date.now());
      return code;
    } catch (err) {
      if (String(err.code || '').includes('SQLITE_CONSTRAINT')) continue;
      throw err;
    }
  }
  throw new Error(`Nao consegui gerar um codigo unico em ${maxTries} tentativas`);
}

/**
 * Reserva/valida um codigo escolhido pela pessoa.
 *
 * Tres casos:
 *   1. ja existe uma SALA com esse codigo          -> recusa (duplicata)
 *   2. codigo aposentado que JA pertenceu a uma sala -> recusa (nao recicla)
 *   3. codigo novo, ou apenas reservado pelo botao "gerar" -> aceita
 *
 * O caso 3 existe porque o fluxo real e: a pessoa clica em "gerar codigo"
 * (que reserva) e so depois confirma a criacao da sala com aquele codigo.
 */
function reserveCode(db, code) {
  const clean = normalizeCode(code);
  if (clean.length < CODE_MIN) {
    return { ok: false, error: `O codigo precisa ter pelo menos ${CODE_MIN} caracteres.` };
  }
  if (db.prepare('SELECT 1 FROM rooms WHERE code = ?').get(clean)) {
    return { ok: false, error: 'code_taken', detail: 'Esse codigo ja esta em uso por outra sala.' };
  }
  const retired = db.prepare('SELECT used_by_room FROM retired_codes WHERE code = ?').get(clean);
  if (retired && retired.used_by_room) {
    return { ok: false, error: 'code_taken', detail: 'Esse codigo ja pertenceu a outra sala e nao pode ser reaproveitado.' };
  }
  db.prepare('INSERT INTO retired_codes (code, retired_at, used_by_room) VALUES (?, ?, 0) ON CONFLICT(code) DO NOTHING')
    .run(clean, Date.now());
  return { ok: true, code: clean };
}

/** Marca que o codigo passou a pertencer a uma sala de verdade. */
function markCodeUsed(db, code) {
  db.prepare('UPDATE retired_codes SET used_by_room = 1 WHERE code = ?').run(code);
}

/* ------------------------------------------------------------------ *
 *  Usuarios / sessao
 * ------------------------------------------------------------------ */

function resolveUser(db, { name, token }) {
  if (token) {
    const found = db.prepare('SELECT * FROM users WHERE token = ?').get(token);
    if (found) {
      // Atualiza o nome se a pessoa mudou, mantendo a mesma identidade.
      const cleanName = String(name || '').trim().slice(0, 24);
      if (cleanName.length >= 2 && cleanName !== found.name) {
        db.prepare('UPDATE users SET name = ? WHERE id = ?').run(cleanName, found.id);
        found.name = cleanName;
      }
      return found;
    }
  }
  const cleanName = String(name || '').trim().slice(0, 24);
  const newToken = crypto.randomBytes(24).toString('hex');
  const info = db.prepare('INSERT INTO users (name, token, created_at) VALUES (?, ?, ?)')
    .run(cleanName, newToken, Date.now());
  return db.prepare('SELECT * FROM users WHERE id = ?').get(info.lastInsertRowid);
}

/* ------------------------------------------------------------------ *
 *  Salas
 * ------------------------------------------------------------------ */

const createRoomTx = (db) => db.transaction(({ code, name, description, isPrivate, avatarPath, user }) => {
  const reserved = reserveCode(db, code);
  if (!reserved.ok) return { ok: false, error: reserved.error, detail: reserved.detail };

  const now = Date.now();
  const roomInfo = db.prepare(`
    INSERT INTO rooms (code, name, description, is_private, avatar_path, created_by, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run(reserved.code, name, description, isPrivate ? 1 : 0, avatarPath || null, user.id, now);

  // A partir daqui o codigo fica bloqueado para sempre, mesmo que a sala sumir.
  markCodeUsed(db, reserved.code);

  db.prepare(`
    INSERT INTO memberships (room_id, user_id, role, joined_at) VALUES (?, ?, 'admin', ?)
  `).run(roomInfo.lastInsertRowid, user.id, now);

  return { ok: true, roomId: roomInfo.lastInsertRowid, code: reserved.code };
});

function getRoomByCode(db, code) {
  return db.prepare('SELECT * FROM rooms WHERE code = ?').get(normalizeCode(code));
}

function getRoomById(db, id) {
  return db.prepare('SELECT * FROM rooms WHERE id = ?').get(id);
}

/** Informacao publica da sala (usada na tela de banner). */
function roomProfile(db, code) {
  const room = getRoomByCode(db, code);
  if (!room) return null;
  const creator = db.prepare('SELECT name FROM users WHERE id = ?').get(room.created_by);
  const members = db.prepare('SELECT COUNT(*) c FROM memberships WHERE room_id = ?').get(room.id).c;
  return {
    code: room.code,
    name: room.name,
    description: room.description,
    isPrivate: Boolean(room.is_private),
    avatarUrl: room.avatar_path ? `/uploads/${path.basename(room.avatar_path)}` : null,
    creatorName: creator?.name || 'desconhecido',
    members,
    createdAt: room.created_at,
  };
}

function setAvatar(db, roomId, avatarPath) {
  db.prepare('UPDATE rooms SET avatar_path = ? WHERE id = ?').run(avatarPath, roomId);
}

/* ------------------------------------------------------------------ *
 *  Pertencimento e pedidos de entrada
 * ------------------------------------------------------------------ */

function getMembership(db, roomId, userId) {
  return db.prepare('SELECT * FROM memberships WHERE room_id = ? AND user_id = ?').get(roomId, userId);
}

function getRequest(db, roomId, userId) {
  return db.prepare('SELECT * FROM requests WHERE room_id = ? AND user_id = ?').get(roomId, userId);
}

function listMembers(db, roomId) {
  return db.prepare(`
    SELECT u.id, u.name, m.role, m.joined_at
    FROM memberships m JOIN users u ON u.id = m.user_id
    WHERE m.room_id = ?
    ORDER BY u.name COLLATE NOCASE
  `).all(roomId);
}

function countMembers(db, roomId) {
  return db.prepare('SELECT COUNT(*) c FROM memberships WHERE room_id = ?').get(roomId).c;
}

function isAdmin(db, roomId, userId) {
  const m = getMembership(db, roomId, userId);
  return Boolean(m && m.role === 'admin');
}

/**
 * Promove a admin ou rebaixa a member.
 * Regras: o criador nunca perde o posto, ninguem rebaixa a si mesmo,
 * e so um admin pode mexer em papel.
 */
const setRoleTx = (db) => db.transaction(({ roomId, targetUserId, actorUserId, role }) => {
  if (role !== 'admin' && role !== 'member') return { ok: false, error: 'Papel invalido.' };
  if (!isAdmin(db, roomId, actorUserId)) {
    return { ok: false, error: 'forbidden', detail: 'So um administrador pode mudar papeis.' };
  }
  const room = getRoomById(db, roomId);
  if (!room) return { ok: false, error: 'Sala nao encontrada.' };
  const target = getMembership(db, roomId, targetUserId);
  if (!target) return { ok: false, error: 'Essa pessoa nao esta na sala.' };
  if (targetUserId === room.created_by) {
    return { ok: false, error: 'creator', detail: 'Quem criou a sala e sempre administrador.' };
  }
  if (targetUserId === actorUserId) {
    return { ok: false, error: 'self', detail: 'Voce nao pode mudar o seu proprio papel.' };
  }
  db.prepare('UPDATE memberships SET role = ? WHERE room_id = ? AND user_id = ?').run(role, roomId, targetUserId);
  const name = db.prepare('SELECT name FROM users WHERE id = ?').get(targetUserId)?.name;
  return { ok: true, role, userId: targetUserId, name };
});

/** Remove um membro. Admins podem remover; qualquer um pode sair de si mesmo. */
const removeMemberTx = (db) => db.transaction(({ roomId, targetUserId, actorUserId }) => {
  if (!isAdmin(db, roomId, actorUserId) && actorUserId !== targetUserId) {
    return { ok: false, error: 'forbidden', detail: 'So um administrador pode remover alguem.' };
  }
  const room = getRoomById(db, roomId);
  if (!room) return { ok: false, error: 'Sala nao encontrada.' };
  const target = getMembership(db, roomId, targetUserId);
  if (!target) return { ok: false, error: 'Essa pessoa nao esta na sala.' };
  if (targetUserId === room.created_by) {
    return { ok: false, error: 'creator', detail: 'Quem criou a sala nao pode ser removido. Apague a sala para encerrar.' };
  }
  db.prepare('DELETE FROM memberships WHERE room_id = ? AND user_id = ?').run(roomId, targetUserId);
  db.prepare('DELETE FROM requests WHERE room_id = ? AND user_id = ?').run(roomId, targetUserId);
  const name = db.prepare('SELECT name FROM users WHERE id = ?').get(targetUserId)?.name;
  return { ok: true, userId: targetUserId, name, self: actorUserId === targetUserId };
});

/**
 * Apaga a sala inteira (mensagens, membros, pedidos e reacoes vao junto pelo
 * ON DELETE CASCADE). O codigo continua em retired_codes com used_by_room = 1,
 * entao nunca volta a circular.
 */
const deleteRoomTx = (db) => db.transaction(({ roomId, actorUserId }) => {
  const room = getRoomById(db, roomId);
  if (!room) return { ok: false, error: 'Sala nao encontrada.' };
  if (room.created_by !== actorUserId) {
    return { ok: false, error: 'forbidden', detail: 'So quem criou a sala pode apaga-la.' };
  }
  db.prepare('DELETE FROM rooms WHERE id = ?').run(roomId);
  return { ok: true, code: room.code };
});

const updateRoomTx = (db) => db.transaction(({ roomId, actorUserId, name, description, isPrivate }) => {
  if (!isAdmin(db, roomId, actorUserId)) {
    return { ok: false, error: 'forbidden', detail: 'So um administrador pode editar a sala.' };
  }
  const room = getRoomById(db, roomId);
  if (!room) return { ok: false, error: 'Sala nao encontrada.' };
  const cleanName = name === undefined ? room.name : String(name).trim().slice(0, 40);
  if (!cleanName) return { ok: false, error: 'O nome da sala nao pode ficar vazio.' };
  const cleanDesc = description === undefined ? room.description : String(description).trim().slice(0, 200);
  const priv = isPrivate === undefined ? room.is_private : (isPrivate ? 1 : 0);
  db.prepare('UPDATE rooms SET name = ?, description = ?, is_private = ? WHERE id = ?')
    .run(cleanName, cleanDesc, priv, roomId);
  return { ok: true };
});

function listPendingRequests(db, roomId) {
  return db.prepare(`
    SELECT r.id, r.created_at, u.id AS user_id, u.name
    FROM requests r JOIN users u ON u.id = r.user_id
    WHERE r.room_id = ? AND r.status = 'pending'
    ORDER BY r.created_at ASC
  `).all(roomId);
}

function countPending(db, roomId) {
  return db.prepare("SELECT COUNT(*) c FROM requests WHERE room_id = ? AND status = 'pending'").get(roomId).c;
}

/** Cria um pedido de entrada. Retorna { ok, requestId } ou { ok:false, error }. */
function createRequest(db, roomId, userId) {
  const existing = getRequest(db, roomId, userId);
  if (existing) {
    if (existing.status === 'pending') return { ok: true, requestId: existing.id, duplicate: true };
    if (existing.status === 'approved') return { ok: false, error: 'already_member' };
    if (existing.status === 'rejected') return { ok: false, error: 'rejected_before', detail: 'Seu pedido ja foi recusado nesta sala.' };
  }
  const info = db.prepare("INSERT INTO requests (room_id, user_id, status, created_at) VALUES (?, ?, 'pending', ?)")
    .run(roomId, userId, Date.now());
  return { ok: true, requestId: info.lastInsertRowid };
}

/**
 * Decide um pedido. So o admin da sala pode (role = 'admin').
 * Retorna o pedido decidido, ou { ok:false, error }.
 */
const decideRequestTx = (db) => db.transaction(({ requestId, adminUserId, approve }) => {
  const req = db.prepare('SELECT * FROM requests WHERE id = ?').get(requestId);
  if (!req) return { ok: false, error: 'Pedido nao encontrado.' };
  if (req.status !== 'pending') return { ok: false, error: 'Esse pedido ja foi decidido.' };

  const admin = getMembership(db, req.room_id, adminUserId);
  if (!admin || admin.role !== 'admin') return { ok: false, error: 'forbidden', detail: 'So o administrador da sala pode aprovar.' };

  const now = Date.now();
  if (approve) {
    db.prepare("INSERT INTO memberships (room_id, user_id, role, joined_at) VALUES (?, ?, 'member', ?) ON CONFLICT(room_id, user_id) DO NOTHING")
      .run(req.room_id, req.user_id, now);
    db.prepare("UPDATE requests SET status = 'approved', decided_by = ?, decided_at = ? WHERE id = ?")
      .run(adminUserId, now, requestId);
  } else {
    db.prepare("UPDATE requests SET status = 'rejected', decided_by = ?, decided_at = ? WHERE id = ?")
      .run(adminUserId, now, requestId);
  }

  const user = db.prepare('SELECT name FROM users WHERE id = ?').get(req.user_id);
  return { ok: true, roomId: req.room_id, userId: req.user_id, userName: user?.name, approved: Boolean(approve) };
});

/**
 * Entra numa sala ja existente.
 *  - sala publica  -> vira membro na hora
 *  - sala privada  -> cria pedido e fica aguardando o admin
 */
const joinRoomTx = (db) => db.transaction(({ roomId, userId }) => {
  const room = getRoomById(db, roomId);
  if (!room) return { ok: false, error: 'Sala nao encontrada.' };

  const membership = getMembership(db, roomId, userId);
  if (membership) return { ok: true, joined: true, role: membership.role };

  if (!room.is_private) {
    db.prepare("INSERT INTO memberships (room_id, user_id, role, joined_at) VALUES (?, ?, 'member', ?) ON CONFLICT(room_id, user_id) DO NOTHING")
      .run(roomId, userId, Date.now());
    return { ok: true, joined: true, role: 'member' };
  }

  const requested = createRequest(db, roomId, userId);
  if (!requested.ok) return requested;
  return { ok: true, joined: false, pending: true, requestId: requested.requestId, duplicate: Boolean(requested.duplicate) };
});

/* ------------------------------------------------------------------ *
 *  Mensagens
 * ------------------------------------------------------------------ */

function nextSeq(db, roomId) {
  const row = db.prepare('SELECT COALESCE(MAX(seq), 0) m FROM messages WHERE room_id = ?').get(roomId);
  return row.m + 1;
}

function findByClientId(db, roomId, clientId) {
  if (!clientId) return undefined;
  return db.prepare('SELECT * FROM messages WHERE room_id = ? AND client_id = ?').get(roomId, clientId);
}

/**
 * Trecho da mensagem original, para mostrar na citacao.
 * Se a original foi apagada, devolve null e o cliente mostra "mensagem apagada".
 */
function replySnippet(db, messageId) {
  const row = db.prepare(`
    SELECT m.id, m.author, m.text, m.deleted_at, m.kind, u.name AS author_name
    FROM messages m LEFT JOIN users u ON u.id = m.author_id
    WHERE m.id = ?
  `).get(messageId);
  if (!row) return null;
  if (row.deleted_at) return { id: `m${row.id}`, author: row.author_name || row.author, text: '', deleted: true };
  return {
    id: `m${row.id}`,
    author: row.author_name || row.author,
    text: String(row.text || '').slice(0, 160),
    deleted: false,
  };
}

function appendMessage(db, { roomId, author, authorId, clientId, text, kind = 'chat', replyTo, attachment, at = Date.now() }) {
  const existing = findByClientId(db, roomId, clientId);
  if (existing) return { message: toWire(db, existing), duplicate: true };

  // Validacao da citacao: tem que existir e ser da mesma sala.
  let replyId = null;
  if (replyTo) {
    const numeric = Number(String(replyTo).replace(/^m/, ''));
    const target = Number.isFinite(numeric)
      ? db.prepare('SELECT id FROM messages WHERE id = ? AND room_id = ?').get(numeric, roomId)
      : null;
    if (target) replyId = target.id;
  }

  const seq = nextSeq(db, roomId);
  const info = db.prepare(`
    INSERT INTO messages (room_id, seq, author_id, author, client_id, kind, text, reply_to, attachment, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(roomId, seq, authorId ?? null, author, clientId || null, kind, text, replyId,
    attachment ? JSON.stringify(attachment) : null, at);

  const row = db.prepare('SELECT * FROM messages WHERE id = ?').get(info.lastInsertRowid);
  return { message: toWire(db, row), duplicate: false };
}

function toWire(db, row) {
  const deleted = Boolean(row.deleted_at);
  let attachment = null;
  if (row.attachment && !deleted) {
    try { attachment = JSON.parse(row.attachment); } catch (_) { attachment = null; }
  }
  return {
    id: `m${row.id}`,
    clientId: row.client_id || undefined,
    seq: row.seq,
    type: row.kind,
    author: row.author,
    // Mensagem apagada viaja sem texto: o cliente mostra "mensagem apagada".
    text: deleted ? '' : row.text,
    deleted,
    reply: row.reply_to && db ? replySnippet(db, row.reply_to) : null,
    attachment,
    at: row.created_at,
  };
}

function listMessages(db, roomId, { beforeSeq, limit = 50 } = {}) {
  const rows = Number.isFinite(beforeSeq)
    ? db.prepare('SELECT * FROM messages WHERE room_id = ? AND seq < ? ORDER BY seq DESC LIMIT ?').all(roomId, beforeSeq, limit)
    : db.prepare('SELECT * FROM messages WHERE room_id = ? ORDER BY seq DESC LIMIT ?').all(roomId, limit);
  return rows.reverse().map((r) => toWire(db, r));
}

/* ------------------------------------------------------------------ *
 *  Reacoes
 * ------------------------------------------------------------------ */

/** Todas as reacoes da sala, agrupadas por mensagem: { msgId: [{emoji, userId, name}] }. */
function listReactions(db, roomId) {
  const rows = db.prepare(`
    SELECT r.message_id, r.emoji, r.user_id, u.name
    FROM reactions r JOIN users u ON u.id = r.user_id
    WHERE r.room_id = ?
    ORDER BY r.created_at ASC
  `).all(roomId);
  const byMessage = new Map();
  for (const r of rows) {
    const key = `m${r.message_id}`;
    if (!byMessage.has(key)) byMessage.set(key, []);
    byMessage.get(key).push({ emoji: r.emoji, userId: r.user_id, name: r.name });
  }
  return Object.fromEntries(byMessage);
}

function listReactionsFor(db, roomId, messageId) {
  // Devolve em camelCase: e o formato que o cliente desenha na bolha.
  return db.prepare(`
    SELECT r.emoji, r.user_id, u.name
    FROM reactions r JOIN users u ON u.id = r.user_id
    WHERE r.room_id = ? AND r.message_id = ?
    ORDER BY r.created_at ASC
  `).all(roomId, messageId).map((r) => ({ emoji: r.emoji, userId: r.user_id, name: r.name }));
}

/**
 * Alterna a reacao de uma pessoa numa mensagem.
 *  - sem reacao        -> adiciona
 *  - mesmo emoji       -> remove (desfazer)
 *  - emoji diferente   -> troca (uma reacao por pessoa por mensagem)
 */
const toggleReactionTx = (db) => db.transaction(({ roomId, messageId, userId, emoji }) => {
  const clean = String(emoji || '').trim().slice(0, 16);
  if (!clean) return { ok: false, error: 'Reacao vazia.' };
  const msg = db.prepare('SELECT * FROM messages WHERE id = ? AND room_id = ?').get(messageId, roomId);
  if (!msg) return { ok: false, error: 'Mensagem nao encontrada.' };
  if (!getMembership(db, roomId, userId)) return { ok: false, error: 'forbidden', detail: 'Voce nao esta nesta sala.' };

  const existing = db.prepare('SELECT emoji FROM reactions WHERE room_id = ? AND message_id = ? AND user_id = ?')
    .get(roomId, messageId, userId);

  if (existing) {
    if (existing.emoji === clean) {
      db.prepare('DELETE FROM reactions WHERE room_id = ? AND message_id = ? AND user_id = ?')
        .run(roomId, messageId, userId);
      return { ok: true, added: false, emoji: clean, reactions: listReactionsFor(db, roomId, messageId) };
    }
    db.prepare('UPDATE reactions SET emoji = ?, created_at = ? WHERE room_id = ? AND message_id = ? AND user_id = ?')
      .run(clean, Date.now(), roomId, messageId, userId);
    return { ok: true, added: true, changed: true, emoji: clean, reactions: listReactionsFor(db, roomId, messageId) };
  }

  db.prepare('INSERT INTO reactions (room_id, message_id, user_id, emoji, created_at) VALUES (?, ?, ?, ?, ?)')
    .run(roomId, messageId, userId, clean, Date.now());
  return { ok: true, added: true, emoji: clean, reactions: listReactionsFor(db, roomId, messageId) };
});

/* ------------------------------------------------------------------ *
 *  Apagar mensagem (soft delete: a seq nao muda, o texto some)
 * ------------------------------------------------------------------ */

const deleteMessageTx = (db) => db.transaction(({ roomId, messageId, actorUserId }) => {
  const msg = db.prepare('SELECT * FROM messages WHERE id = ? AND room_id = ?').get(messageId, roomId);
  if (!msg) return { ok: false, error: 'Mensagem nao encontrada.' };
  if (msg.deleted_at) return { ok: false, error: 'Essa mensagem ja foi apagada.' };
  if (msg.kind !== 'chat') return { ok: false, error: 'Avisos do sistema nao podem ser apagados.' };

  const isAuthor = msg.author_id === actorUserId;
  if (!isAuthor && !isAdmin(db, roomId, actorUserId)) {
    return { ok: false, error: 'forbidden', detail: 'Voce so pode apagar as suas mensagens.' };
  }
  db.prepare('UPDATE messages SET deleted_at = ? WHERE id = ?').run(Date.now(), messageId);
  db.prepare('DELETE FROM reactions WHERE room_id = ? AND message_id = ?').run(roomId, messageId);
  return { ok: true, seq: msg.seq, byAdmin: !isAuthor };
});

/* ------------------------------------------------------------------ *
 *  Assinaturas de push
 * ------------------------------------------------------------------ */

function addPushSubscription(db, { endpoint, userId, keys, userAgent }) {
  db.prepare(`
    INSERT INTO push_subscriptions (endpoint, user_id, p256dh, auth, user_agent, created_at)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(endpoint) DO UPDATE SET
      user_id = excluded.user_id,
      p256dh  = excluded.p256dh,
      auth    = excluded.auth,
      user_agent = excluded.user_agent
  `).run(endpoint, userId, keys.p256dh, keys.auth, userAgent || null, Date.now());
  return { ok: true };
}

function removePushSubscription(db, endpoint) {
  db.prepare('DELETE FROM push_subscriptions WHERE endpoint = ?').run(endpoint);
  return { ok: true };
}

function listPushSubscriptions(db, userId) {
  return db.prepare('SELECT * FROM push_subscriptions WHERE user_id = ?').all(userId);
}

/** Remove assinaturas que o servidor de push recusou (endpoint expirado). */
function prunePushSubscription(db, endpoint) {
  db.prepare('DELETE FROM push_subscriptions WHERE endpoint = ?').run(endpoint);
}

/* ------------------------------------------------------------------ *
 *  Visto por
 * ------------------------------------------------------------------ */

/**
 * Marca como lidas todas as mensagens da sala ate `upToSeq` para aquela pessoa.
 * Uma escrita so, em vez de uma por mensagem.
 */
const markReadTx = (db) => db.transaction(({ roomId, userId, upToSeq }) => {
  const limit = Number(upToSeq);
  if (!Number.isFinite(limit) || limit <= 0) return { ok: false, error: 'Seq invalida.' };
  if (!getMembership(db, roomId, userId)) {
    return { ok: false, error: 'forbidden', detail: 'Voce nao esta nesta sala.' };
  }
  const now = Date.now();
  const info = db.prepare(`
    INSERT OR IGNORE INTO message_reads (room_id, message_id, user_id, read_at)
    SELECT m.room_id, m.id, ?, ?
    FROM messages m
    WHERE m.room_id = ? AND m.seq <= ? AND m.author_id IS NOT ?
  `).run(userId, now, roomId, limit, userId);
  return { ok: true, newlyRead: info.changes };
});

/** Mapa messageId -> [nome de quem leu], para a sala toda. */
function listReads(db, roomId) {
  const rows = db.prepare(`
    SELECT r.message_id, u.name
    FROM message_reads r JOIN users u ON u.id = r.user_id
    WHERE r.room_id = ?
  `).all(roomId);
  const byMessage = new Map();
  for (const r of rows) {
    const key = `m${r.message_id}`;
    if (!byMessage.has(key)) byMessage.set(key, []);
    byMessage.get(key).push(r.name);
  }
  return Object.fromEntries(byMessage);
}

function listReadsFor(db, roomId, messageId) {
  return db.prepare(`
    SELECT u.name FROM message_reads r JOIN users u ON u.id = r.user_id
    WHERE r.room_id = ? AND r.message_id = ?
  `).all(roomId, messageId).map((r) => r.name);
}

module.exports = {
  ALPHABET,
  CODE_MIN,
  CODE_MAX,
  openDb,
  normalizeCode,
  randomCode,
  generateUniqueCode,
  reserveCode,
  markCodeUsed,
  resolveUser,
  createRoom: createRoomTx,
  getRoomByCode,
  getRoomById,
  roomProfile,
  setAvatar,
  updateRoom: updateRoomTx,
  deleteRoom: deleteRoomTx,
  getMembership,
  getRequest,
  listMembers,
  countMembers,
  isAdmin,
  setRole: setRoleTx,
  removeMember: removeMemberTx,
  listPendingRequests,
  countPending,
  createRequest,
  decideRequest: decideRequestTx,
  joinRoom: joinRoomTx,
  nextSeq,
  appendMessage,
  listMessages,
  listReactions,
  listReactionsFor,
  toggleReaction: toggleReactionTx,
  deleteMessage: deleteMessageTx,
  replySnippet,
  markRead: markReadTx,
  listReads,
  listReadsFor,
  addPushSubscription,
  removePushSubscription,
  listPushSubscriptions,
  prunePushSubscription,
  toWire,
};
