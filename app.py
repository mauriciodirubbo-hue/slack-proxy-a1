from flask import Flask, request, Response
import urllib.request
import os
import threading

app = Flask(__name__)

VERDI_URL = os.getenv("VERDI_URL", "http://verdi-flows.melisystems.com/webhook/a1-handler")


def forward_to_verdi(body, content_type):
    try:
        req = urllib.request.Request(
            VERDI_URL,
            data=body,
            headers={"Content-Type": content_type},
            method="POST"
        )
        urllib.request.urlopen(req, timeout=10)
        print("[proxy] Forwarded to Verdi OK")
    except Exception as e:
        print(f"[proxy] Error forwarding: {e}")


@app.route("/webhook/a1-handler", methods=["GET", "POST"])
def handler():
    if request.method == "GET":
        return Response("", status=200)
    body = request.get_data()
    content_type = request.content_type or "application/x-www-form-urlencoded"
    t = threading.Thread(target=forward_to_verdi, args=(body, content_type))
    t.daemon = True
    t.start()
    return Response("", status=200)


@app.route("/", methods=["GET"])
def health():
    return "OK", 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
