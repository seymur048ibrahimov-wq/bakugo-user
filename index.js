/* BakuGo — push bildiriş serveri (Firebase Cloud Functions, 2-ci nəsil)
   1) sendPushQueue   : Admin paneldə "Real mobil push də göndər" seçilən kampaniyalar (pushQueue kolleksiyası)
   2) notifyNewOrder  : Sifariş "searching_courier" statusuna keçəndə ONLINE kuryerlərə push
   Tokenlər: couriers/{uid}.fcmToken, users/{uid}.fcmToken, restaurants/{id}.fcmToken */

const { onDocumentCreated, onDocumentWritten } = require("firebase-functions/v2/firestore");
const { setGlobalOptions } = require("firebase-functions/v2");
const logger = require("firebase-functions/logger");
const { initializeApp } = require("firebase-admin/app");
const { getFirestore, FieldValue } = require("firebase-admin/firestore");
const { getMessaging } = require("firebase-admin/messaging");

initializeApp();
const db = getFirestore();
setGlobalOptions({ maxInstances: 5 });

const COLLECTION = { courier: "couriers", customer: "users", restaurant: "restaurants" };
const APP_LINK = {
  courier: "https://seymur048ibrahimov-wq.github.io/bakugo-kuryer/",
  customer: "https://seymur048ibrahimov-wq.github.io/bakugo-musteri/"
  // restaurant: "https://seymur048ibrahimov-wq.github.io/bakugo-restoran/"   ← restoran tətbiqi yerləşəndə aç
};

/* Bir kolleksiyadakı (istəyə görə filtrli) bütün tokenləri yığır: [{token, ref}] */
async function collectTokens(coll, filterFn) {
  const snap = await db.collection(coll).get();
  const out = [];
  snap.forEach((d) => {
    const v = d.data();
    if (v && typeof v.fcmToken === "string" && v.fcmToken.length > 20 && (!filterFn || filterFn(v))) {
      out.push({ token: v.fcmToken, ref: d.ref });
    }
  });
  return out;
}

/* 500-lük partiyalarla göndərir, etibarsız tokenləri bazadan silir. */
async function sendToTokens(list, { title, body, link, tag }) {
  let success = 0, failure = 0;
  for (let i = 0; i < list.length; i += 500) {
    const chunk = list.slice(i, i + 500);
    const res = await getMessaging().sendEachForMulticast({
      tokens: chunk.map((x) => x.token),
      notification: { title, body },
      webpush: {
        notification: {
          icon: "https://seymur048ibrahimov-wq.github.io/bakugo-kuryer/icon-192.png",
          badge: "https://seymur048ibrahimov-wq.github.io/bakugo-kuryer/icon-96.png",
          tag: tag || "bakugo",
          renotify: true,
          requireInteraction: true,
          vibrate: [400, 150, 400]
        },
        headers: { Urgency: "high", TTL: "3600" },
        ...(link ? { fcmOptions: { link } } : {})
      }
    });
    success += res.successCount;
    failure += res.failureCount;
    const cleanups = [];
    res.responses.forEach((r, idx) => {
      const code = r.error && r.error.code;
      if (code === "messaging/registration-token-not-registered" || code === "messaging/invalid-registration-token") {
        cleanups.push(chunk[idx].ref.update({ fcmToken: FieldValue.delete() }).catch(() => null));
      }
    });
    await Promise.all(cleanups);
  }
  return { success, failure };
}

/* 1) Admin kampaniyası → push */
exports.sendPushQueue = onDocumentCreated("pushQueue/{id}", async (event) => {
  const snap = event.data;
  if (!snap) return;
  const job = snap.data();
  const audience = job.audience;
  const coll = COLLECTION[audience];
  if (!coll) {
    await snap.ref.update({ status: "failed", error: "naməlum audience: " + audience });
    return;
  }
  try {
    const tokens = await collectTokens(coll);
    const r = tokens.length
      ? await sendToTokens(tokens, { title: job.title || "BakuGo", body: job.body || "", link: APP_LINK[audience], tag: "camp-" + (job.campaignId || event.params.id) })
      : { success: 0, failure: 0 };
    const status = r.success === 0 ? "failed" : (r.failure > 0 ? "partial" : "sent");
    await snap.ref.update({ status, successCount: r.success, failureCount: r.failure, tokenCount: tokens.length, processedAt: FieldValue.serverTimestamp() });
    if (job.campaignId) {
      await db.collection("campaigns").doc(job.campaignId).set({ pushStatus: status }, { merge: true });
    }
    logger.info("pushQueue", event.params.id, audience, r);
  } catch (e) {
    logger.error("pushQueue xətası", e);
    await snap.ref.update({ status: "failed", error: String(e && e.message || e) });
    if (job.campaignId) await db.collection("campaigns").doc(job.campaignId).set({ pushStatus: "failed" }, { merge: true });
  }
});

/* 2) Yeni sifariş (və ya təkrar axtarış) → ONLINE kuryerlərə push */
exports.notifyNewOrder = onDocumentWritten("orders/{id}", async (event) => {
  const before = event.data && event.data.before && event.data.before.exists ? event.data.before.data() : null;
  const after = event.data && event.data.after && event.data.after.exists ? event.data.after.data() : null;
  if (!after || after.status !== "searching_courier" || after.courierId) return;
  const isNewSearch = !before || before.status !== "searching_courier" || before.searchStartedAt !== after.searchStartedAt;
  if (!isNewSearch) return;

  const tokens = await collectTokens("couriers", (c) => c.status === "online" && c.isAvailable !== false && c.isBlocked !== true);
  if (!tokens.length) { logger.info("ONLINE kuryer tokeni yoxdur", event.params.id); return; }

  const where = after.restaurantName ? ("📍 " + after.restaurantName) : "Yaxınlıqda yeni çatdırılma var";
  const r = await sendToTokens(tokens, {
    title: "🔔 Yeni sifariş!",
    body: where + " — qəbul etmək üçün toxunun",
    link: APP_LINK.courier,
    tag: "order-" + event.params.id
  });
  logger.info("yeni sifariş push", event.params.id, r);
});
