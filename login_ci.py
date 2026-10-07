import base64
import json
import os
import urllib.request

from telethon.errors import SessionPasswordNeededError
from telethon.sessions import StringSession
from telethon.sync import TelegramClient

from nacl import encoding, public

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
STEP = os.environ["STEP"]
STATE = "login_state.json"
REPO = os.environ.get("REPO", "Mazemc1/Brand_rnd")
GH_PAT = os.environ.get("GH_PAT", "")


def gh(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        "https://api.github.com" + path,
        data=data,
        method=method,
        headers={
            "Authorization": "Bearer " + GH_PAT,
            "Accept": "application/vnd.github+json",
            "User-Agent": "login-ci",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read().decode())


def set_secret(name, value):
    pk = gh("GET", "/repos/%s/actions/secrets/public-key" % REPO)
    box = public.SealedBox(public.PublicKey(pk["key"].encode(), encoding.Base64Encoder()))
    enc = base64.b64encode(box.encrypt(value.encode())).decode()
    gh("PUT", "/repos/%s/actions/secrets/%s" % (REPO, name), {"encrypted_value": enc, "key_id": pk["key_id"]})


state = json.load(open(STATE)) if os.path.exists(STATE) else {}

if STEP == "send":
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    client.connect()
    sent = client.send_code_request(os.environ["PHONE"].strip())
    state.update(phone=os.environ["PHONE"].strip(), phone_code_hash=sent.phone_code_hash, session=client.session.save())
    json.dump(state, open(STATE, "w"))
    client.disconnect()
    print("CODE_SENT")
elif STEP == "sign":
    client = TelegramClient(StringSession(state["session"]), API_ID, API_HASH)
    client.connect()
    try:
        client.sign_in(phone=state["phone"], code=os.environ["CODE"].strip(), phone_code_hash=state["phone_code_hash"])
    except SessionPasswordNeededError:
        client.sign_in(password=os.environ["PASSWORD"])
    session_str = client.session.save()
    client.disconnect()
    set_secret("TELEGRAM_SESSION", session_str)
    print("SESSION_SET_OK")
else:
    raise SystemExit("unknown step")
