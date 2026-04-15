"""
app.py — Proxy HTTPS para recibir webhooks de Slack y actualizar BigQuery directamente.
Deployado en Render.com con HTTPS gratuito.

Variables de entorno requeridas:
  GOOGLE_APPLICATION_CREDENTIALS_JSON  — contenido del JSON de ADC (authorized_user)
  SLACK_BOT_TOKEN                      — xoxb-... para abrir modales
"""

import json
import os
import threading
import urllib.parse
import urllib.request

from flask import Flask, request, Response

app = Flask(__name__)

POSTS_TABLE = "`meli-sbox.COMMSCROSS.POSTS_CX_NEWS_ALARMAS`"
SLACK_IDS   = "`meli-sbox.COMMSCROSS.SLACK_USER_IDS`"
LK_PEOPLE   = "`meli-bi-data.WHOWNER.LK_MELI_PEOPLE`"
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "")


def get_bq_client():
    print("[bq] iniciando get_bq_client")
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from google.cloud import bigquery

    raw = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON", "")
    if not raw:
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS_JSON no configurado")

    print("[bq] credenciales encontradas, largo=" + str(len(raw)))
    d = json.loads(raw)
    creds = Credentials(
        token=None,
        refresh_token=d["refresh_token"],
        client_id=d["client_id"],
        client_secret=d["client_secret"],
        token_uri="https://oauth2.googleapis.com/token",
    )
    creds.refresh(Request())
    print("[bq] credenciales refrescadas OK")
    return bigquery.Client(project="meli-bi-data", credentials=creds)


def slack_post(url, payload):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    urllib.request.urlopen(req, timeout=5)


def slack_api(method, payload):
    url = f"https://slack.com/api/{method}"
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        },
        method="POST",
    )
    urllib.request.urlopen(req, timeout=5)


# ── Handlers ──────────────────────────────────────────────────────────────────

def handle_keep_active(post_id, slack_user_id, response_url):
    bq = get_bq_client()
    bq.query(f"""
        UPDATE {POSTS_TABLE}
        SET
          VALIDITY_VALIDATION_DATE = CURRENT_DATE(),
          VALIDITY_VALIDATION_BY = (
            SELECT EMAIL FROM {SLACK_IDS}
            WHERE SLACK_USER_ID = '{slack_user_id}' LIMIT 1
          ),
          VALIDITY_ALERT1_DATE = NULL
        WHERE POST_ID = {post_id}
    """).result()
    slack_post(response_url, {
        "response_type": "ephemeral",
        "text": "\u2705 Post marcado como activo. El contador fue reseteado.",
    })
    print(f"[keep_active] POST {post_id} actualizado")


def handle_deactivate(post_id, response_url):
    slack_post(response_url, {
        "response_type": "ephemeral",
        "text": "\u2705 Registrado. Record\u00e1 desactivar el post en Beedoo.",
    })
    print(f"[deactivate] POST {post_id} \u2014 confirmaci\u00f3n enviada")


def handle_indicate_ldap(post_id, trigger_id):
    slack_api("views.open", {
        "trigger_id": trigger_id,
        "view": {
            "type": "modal",
            "callback_id": "ldap_input_a1",
            "private_metadata": str(post_id),
            "title": {"type": "plain_text", "text": "Indicar LDAP"},
            "submit": {"type": "plain_text", "text": "Confirmar"},
            "close": {"type": "plain_text", "text": "Cancelar"},
            "blocks": [
                {
                    "type": "input",
                    "block_id": "ldap_block",
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "ldap_input",
                        "placeholder": {"type": "plain_text", "text": "ej: jperez"},
                    },
                    "label": {"type": "plain_text", "text": "LDAP del nuevo responsable"},
                }
            ],
        },
    })
    print(f"[indicate_ldap] Modal abierto para POST {post_id}")


def handle_ldap_submission(post_id, ldap, slack_user_id):
    bq = get_bq_client()
    bq.query(f"""
        UPDATE {POSTS_TABLE}
        SET
          LAST_EDITOR = (
            SELECT EMAIL FROM {LK_PEOPLE}
            WHERE USERNAME = '{ldap}' LIMIT 1
          ),
          VALIDITY_ALERT1_DATE = NULL,
          VALIDITY_VALIDATION_DATE = NULL
        WHERE POST_ID = {post_id}
    """).result()
    slack_api("chat.postMessage", {
        "channel": slack_user_id,
        "text": "\u2705 LDAP registrado. El post fue reasignado.",
    })
    print(f"[ldap_submission] POST {post_id} reasignado a {ldap}")


# ── Dispatcher ────────────────────────────────────────────────────────────────

def process_payload(payload_str):
    try:
        print(f"[process] payload recibido, largo={len(payload_str)}")
        payload = json.loads(payload_str)
        interaction_type = payload.get("type")
        print(f"[process] interaction_type={interaction_type}")

        if interaction_type == "block_actions":
            action = payload["actions"][0]
            action_id = action["action_id"]
            post_id = int(action["value"])
            slack_user_id = payload["user"]["id"]
            response_url = payload.get("response_url", "")
            trigger_id = payload.get("trigger_id", "")
            print(f"[process] action_id={action_id} post_id={post_id}")

            if action_id == "keep_active_a1":
                handle_keep_active(post_id, slack_user_id, response_url)
            elif action_id == "deactivate_a1":
                handle_deactivate(post_id, response_url)
            elif action_id == "indicate_ldap_a1":
                handle_indicate_ldap(post_id, trigger_id)

        elif interaction_type == "view_submission":
            view = payload.get("view", {})
            if view.get("callback_id") == "ldap_input_a1":
                post_id = int(view.get("private_metadata", "0"))
                ldap = view["state"]["values"]["ldap_block"]["ldap_input"]["value"]
                slack_user_id = payload["user"]["id"]
                handle_ldap_submission(post_id, ldap, slack_user_id)

    except Exception as e:
        import traceback
        print(f"[error] process_payload: {e}")
        print(traceback.format_exc())


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/webhook/a1-handler", methods=["GET", "POST"])
def handler():
    if request.method == "GET":
        return Response("OK", status=200)

    body = request.get_data(as_text=True)

    # Slack envía payload=<url-encoded JSON>
    parsed = urllib.parse.parse_qs(body)
    payload_str = parsed.get("payload", [None])[0]

    if not payload_str:
        print("[warn] No payload found in body")
        return Response("", status=200)

    # Responder a Slack inmediatamente (3s timeout) y procesar en background
    t = threading.Thread(target=process_payload, args=(payload_str,))
    t.daemon = True
    t.start()

    return Response("", status=200)


@app.route("/", methods=["GET"])
def health():
    return "OK", 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
