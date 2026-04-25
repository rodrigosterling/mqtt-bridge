const mqtt = require("mqtt");

const {
  MQTT_HOST,
  MQTT_PORT = "8883",
  MQTT_USER,
  MQTT_PASS,
  MQTT_TOPIC = "campo/+/+/data",
  INGEST_URL,
  INGEST_API_KEY
} = process.env;

if (!MQTT_HOST || !MQTT_USER || !MQTT_PASS || !INGEST_URL) {
  console.error("Faltan variables de entorno requeridas.");
  process.exit(1);
}

const client = mqtt.connect({
  host: MQTT_HOST,
  port: Number(MQTT_PORT),
  protocol: "mqtts",
  username: MQTT_USER,
  password: MQTT_PASS,
  rejectUnauthorized: false, // tu HUB usa setInsecure(); mantener coherencia
  reconnectPeriod: 5000
});

client.on("connect", () => {
  console.log("[BRIDGE] MQTT conectado");
  client.subscribe(MQTT_TOPIC, { qos: 0 }, (err) => {
    if (err) console.error("[BRIDGE] Error subscribe:", err.message);
    else console.log("[BRIDGE] Suscrito a", MQTT_TOPIC);
  });
});

client.on("reconnect", () => console.log("[BRIDGE] Reintentando MQTT..."));
client.on("error", (err) => console.error("[BRIDGE] MQTT error:", err.message));

client.on("message", async (topic, payloadBuf) => {
  try {
    const raw = payloadBuf.toString("utf8");
    const body = JSON.parse(raw);

    // Normalización mínima para /api/ingest
    const msg = {
      nodeId: Number(body.nodeId),
      type: String(body.type || "unknown"),
      zona: String(body.zona || body.zone || "zona1"),
      battery: Number(body.battery || 0),
      timestamp: Number(body.timestamp || Math.floor(Date.now() / 1000)),
      values: Array.isArray(body.values) ? body.values.map(Number) : []
    };

    if (!Number.isFinite(msg.nodeId) || msg.nodeId < 1 || msg.values.length === 0) {
      console.warn("[BRIDGE] Mensaje ignorado (incompleto):", topic, raw);
      return;
    }

    const res = await fetch(INGEST_URL, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...(INGEST_API_KEY ? { "x-api-key": INGEST_API_KEY } : {})
      },
      body: JSON.stringify(msg)
    });

    if (!res.ok) {
      const text = await res.text();
      console.error(`[BRIDGE] ingest fail ${res.status}: ${text}`);
      return;
    }

    console.log(`[BRIDGE] OK ${topic} -> ingest`);
  } catch (e) {
    console.error("[BRIDGE] Error procesando mensaje:", e.message);
  }
});
