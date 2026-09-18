/* Service worker do Web Chat.
 *
 * Faz duas coisas:
 *   1. recebe o evento `push` do navegador e mostra a notificacao
 *   2. ao clicar, abre (ou foca) a aba e manda o cliente ir para a sala
 *
 * Nao ha cache offline de proposito: um chat mostrando dado velho e pior do
 * que nao abrir.
 */

self.addEventListener('install', (event) => {
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(self.clients.claim());
});

self.addEventListener('push', (event) => {
  let data = {};
  try {
    data = event.data ? event.data.json() : {};
  } catch (_) {
    data = { title: 'Web Chat', body: event.data ? event.data.text() : '' };
  }

  const title = data.title || 'Web Chat';
  const options = {
    body: data.body || '',
    tag: data.tag || 'web-chat',
    // Agrupa por sala: a nova substituia a anterior em vez de empilhar.
    renotify: Boolean(data.tag),
    icon: '/icon-192.png',
    badge: '/icon-192.png',
    data: data.data || {},
    actions: data.data?.roomCode
      ? [{ action: 'open', title: 'Abrir conversa' }]
      : [],
  };

  event.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();

  const roomCode = event.notification.data?.roomCode;
  const target = `/${roomCode ? `?sala=${encodeURIComponent(roomCode)}` : ''}`;

  event.waitUntil((async () => {
    const all = await self.clients.matchAll({ type: 'window', includeUncontrolled: true });

    // Ja existe uma aba aberta? Traz para a frente e avisa qual sala abrir.
    for (const client of all) {
      if ('focus' in client) {
        client.postMessage({ type: 'push-open', roomCode });
        return client.focus();
      }
    }
    return self.clients.openWindow(target);
  })());
});
