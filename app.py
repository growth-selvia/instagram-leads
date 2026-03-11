"""
Selvia — Instagram Lead Scraper API (v4 — Login por Sessão)
=============================================================
Usa arquivo de sessão do Instaloader para autenticação.
Resolve problemas de 2FA, challenge e rate limit.

SETUP:
    1. No seu Mac, rode:
         pip3 install instaloader
         instaloader --login SEU_USUARIO
    2. Copie o arquivo de sessão:
         cat ~/.config/instaloader/session-SEU_USUARIO | base64
    3. No Render, crie a variável de ambiente:
         INSTAGRAM_SESSION = (cole o base64 aqui)
         INSTAGRAM_USERNAME = SEU_USUARIO
    4. Deploy!

Requisitos:
    pip install flask instaloader gunicorn requests

Execução:
    gunicorn app:app --bind 0.0.0.0:5000 --workers 2 --timeout 60 --access-logfile -
"""

import instaloader
import re
import os
import json
import sys
import time
import random
import base64
import logging
import threading
import traceback
import requests as http_requests
from datetime import datetime
from pathlib import Path
from flask import Flask, request, jsonify

# ============================================================
# CONFIGURAÇÃO
# ============================================================

app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

import functools
print = functools.partial(print, flush=True)

# Variáveis de ambiente
API_TOKEN = os.environ.get("API_TOKEN", "SELVIA_API_TOKEN_AQUI")
INSTAGRAM_USERNAME = os.environ.get("INSTAGRAM_USERNAME", "")
INSTAGRAM_SESSION_B64 = os.environ.get("INSTAGRAM_SESSION", "")

# Diretório para salvar a sessão no container
SESSION_DIR = Path("/tmp/instaloader-session")
SESSION_DIR.mkdir(parents=True, exist_ok=True)

KEYWORDS_MEDICINA = [
    "medicina", "med", "medica", "médica", "médico", "medico",
    "medicine", "medical", "med student", "estudante de medicina",
    "acadêmico de medicina", "academico de medicina",
    "futuro médico", "futura médica", "futuro medico", "futura medica",
    "interno", "interna", "internato",
    "residência", "residencia",
    "⚕️", "🩺", "🏥", "💉", "🥼",
]

REGEX_MEDICINA = [
    r"\bmed\s?\d{2,4}\b",
    r"\bmedicina\s?\d{2,4}\b",
    r"\bturma\s+med",
    r"\b\d{2,4}\s*med\b",
]


# ============================================================
# SESSÃO DO INSTAGRAM
# ============================================================

def setup_session_file():
    """
    Decodifica a sessão base64 da variável de ambiente
    e salva como arquivo no container.
    """
    if not INSTAGRAM_SESSION_B64 or not INSTAGRAM_USERNAME:
        logger.warning("[SESSION] ⚠️ INSTAGRAM_SESSION ou INSTAGRAM_USERNAME não configurados!")
        logger.warning("[SESSION] O scraper vai rodar SEM login (rate limit agressivo)")
        sys.stdout.flush()
        return None

    session_path = SESSION_DIR / f"session-{INSTAGRAM_USERNAME}"

    try:
        session_data = base64.b64decode(INSTAGRAM_SESSION_B64)
        session_path.write_bytes(session_data)
        logger.info(f"[SESSION] ✅ Arquivo de sessão salvo: {session_path}")
        logger.info(f"[SESSION] ✅ Usuário: @{INSTAGRAM_USERNAME}")
        sys.stdout.flush()
        return str(session_path)
    except Exception as e:
        logger.error(f"[SESSION] ❌ Erro ao decodificar sessão: {e}")
        sys.stdout.flush()
        return None


def create_loader_with_session():
    """
    Cria uma instância do Instaloader autenticada via sessão.
    Retorna (loader, logged_in: bool)
    """
    loader = instaloader.Instaloader(
        download_pictures=False,
        download_videos=False,
        download_video_thumbnails=False,
        download_geotags=False,
        download_comments=False,
        save_metadata=False,
        compress_json=False,
        quiet=True,
    )

    if not INSTAGRAM_USERNAME or not INSTAGRAM_SESSION_B64:
        logger.info("[LOADER] Sem credenciais — rodando sem login")
        sys.stdout.flush()
        return loader, False

    try:
        # Carrega a sessão do arquivo
        session_path = SESSION_DIR / f"session-{INSTAGRAM_USERNAME}"

        if not session_path.exists():
            setup_session_file()

        loader.load_session_from_file(INSTAGRAM_USERNAME, str(session_path))

        # Testa se a sessão é válida
        test_profile = instaloader.Profile.from_username(loader.context, INSTAGRAM_USERNAME)
        logger.info(f"[LOADER] ✅ Login OK via sessão! Logado como @{test_profile.username}")
        sys.stdout.flush()
        return loader, True

    except instaloader.exceptions.LoginException as e:
        logger.error(f"[LOADER] ❌ Sessão expirada ou inválida: {e}")
        logger.error(f"[LOADER] Gere uma nova sessão no seu Mac com: instaloader --login {INSTAGRAM_USERNAME}")
        sys.stdout.flush()
        return loader, False

    except Exception as e:
        logger.error(f"[LOADER] ❌ Erro ao carregar sessão: {e}")
        logger.error(f"[LOADER] Traceback:\n{traceback.format_exc()}")
        sys.stdout.flush()
        return loader, False


# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def check_auth(req):
    token = req.headers.get("Authorization", "").replace("Bearer ", "")
    return token == API_TOKEN


def bio_matches_medicina(bio: str) -> bool:
    if not bio:
        return False
    bio_lower = bio.lower()
    for kw in KEYWORDS_MEDICINA:
        if kw.lower() in bio_lower:
            return True
    for pattern in REGEX_MEDICINA:
        if re.search(pattern, bio_lower):
            return True
    return False


def get_profile_info(loader, username):
    try:
        profile = instaloader.Profile.from_username(loader.context, username)
        return {
            "username": profile.username,
            "nome": profile.full_name or profile.username,
            "bio": profile.biography or "",
            "is_private": profile.is_private,
            "followers": profile.followers,
        }
    except Exception as e:
        logger.warning(f"Perfil @{username} inacessível: {e}")
        sys.stdout.flush()
        return None


def scrape_commenters(loader, target_username, max_posts=0):
    profile = instaloader.Profile.from_username(loader.context, target_username)
    if profile.is_private:
        raise ValueError(f"Perfil @{target_username} é privado.")

    commenters = {}
    posts_analyzed = 0
    total_comments = 0

    logger.info(f"Coletando @{target_username} ({profile.mediacount} posts)")
    sys.stdout.flush()

    for post in profile.get_posts():
        if max_posts > 0 and posts_analyzed >= max_posts:
            break
        posts_analyzed += 1
        try:
            for comment in post.get_comments():
                commenter = comment.owner.username
                total_comments += 1
                if commenter not in commenters:
                    commenters[commenter] = {"count": 0}
                commenters[commenter]["count"] += 1
        except Exception as e:
            logger.warning(f"  Erro no post {post.shortcode}: {e}")

        logger.info(f"  Post {posts_analyzed}/{profile.mediacount}: {post.shortcode} — {total_comments} comentários")
        sys.stdout.flush()
        time.sleep(random.uniform(3, 8))

    logger.info(f"Coleta OK: {posts_analyzed} posts, {total_comments} comentários, {len(commenters)} únicos")
    sys.stdout.flush()
    return {
        "commenters": commenters,
        "posts_analyzed": posts_analyzed,
        "total_comments": total_comments,
    }


def enrich_and_filter_leads(loader, commenters, min_comments=1, check_bio=True):
    leads = []
    sorted_commenters = sorted(commenters.items(), key=lambda x: x[1]["count"], reverse=True)

    for username, data in sorted_commenters:
        if data["count"] < min_comments:
            continue
        lead = {
            "username": username,
            "nome": username,
            "bio": "",
            "is_private": None,
            "followers": None,
            "total_comentarios": data["count"],
            "bio_match_medicina": False,
            "status": "pendente",
            "versao_msg": chr(65 + (len(leads) % 6)),
            "data_envio": "",
            "coletado_em": datetime.utcnow().isoformat(),
        }
        if check_bio:
            info = get_profile_info(loader, username)
            if info:
                lead["nome"] = info["nome"]
                lead["bio"] = info["bio"]
                lead["is_private"] = info["is_private"]
                lead["followers"] = info["followers"]
                lead["bio_match_medicina"] = bio_matches_medicina(info["bio"])
            time.sleep(random.uniform(2, 5))
        leads.append(lead)

    logger.info(f"Leads: {len(leads)} total, {sum(1 for l in leads if l['bio_match_medicina'])} medicina")
    sys.stdout.flush()
    return leads


# ============================================================
# ENVIO PARA WEBHOOK
# ============================================================

def send_to_webhook(webhook_url, payload):
    logger.info(f"[WEBHOOK] ==========================================")
    logger.info(f"[WEBHOOK] URL: {webhook_url}")
    logger.info(f"[WEBHOOK] Leads: {len(payload.get('leads', []))}")
    sys.stdout.flush()

    for attempt in range(1, 4):
        try:
            logger.info(f"[WEBHOOK] Tentativa {attempt}/3...")
            sys.stdout.flush()

            resp = http_requests.post(
                webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30,
            )

            logger.info(f"[WEBHOOK] ✅ Status: {resp.status_code}")
            logger.info(f"[WEBHOOK] ✅ Response: {resp.text[:500]}")
            sys.stdout.flush()

            if resp.status_code == 200:
                return True

        except Exception as e:
            logger.error(f"[WEBHOOK] ❌ Erro (tentativa {attempt}): {e}")
            sys.stdout.flush()

        if attempt < 3:
            time.sleep(attempt * 5)

    logger.error(f"[WEBHOOK] ❌❌❌ FALHA TOTAL após 3 tentativas")
    sys.stdout.flush()
    return False


# ============================================================
# JOB EM BACKGROUND
# ============================================================

def run_scrape_job(params):
    target = params["target_username"]
    webhook_url = params["webhook_url"]
    max_posts = params.get("max_posts", 0)
    min_comments = params.get("min_comments", 1)
    check_bio = params.get("check_bio", True)
    only_medicina = params.get("only_medicina", False)

    logger.info(f"[JOB] ========== INÍCIO ==========")
    logger.info(f"[JOB] Target: @{target}")
    logger.info(f"[JOB] Webhook: {webhook_url}")
    sys.stdout.flush()

    # Criar loader com sessão
    loader, logged_in = create_loader_with_session()
    logger.info(f"[JOB] Login: {'✅ autenticado' if logged_in else '⚠️ sem login (rate limit baixo)'}")
    sys.stdout.flush()

    try:
        result = scrape_commenters(loader, target, max_posts)
        leads = enrich_and_filter_leads(loader, result["commenters"], min_comments, check_bio)

        if only_medicina:
            leads = [l for l in leads if l["bio_match_medicina"]]
        leads.sort(key=lambda x: (-int(x["bio_match_medicina"]), -x["total_comentarios"]))

        stats = {
            "target_username": target,
            "posts_analyzed": result["posts_analyzed"],
            "total_comments": result["total_comments"],
            "unique_commenters": len(result["commenters"]),
            "leads_returned": len(leads),
            "leads_medicina": sum(1 for l in leads if l["bio_match_medicina"]),
            "logged_in": logged_in,
            "collected_at": datetime.utcnow().isoformat(),
        }
        payload = {"success": True, "stats": stats, "leads": leads}
        logger.info(f"[JOB] ✅ Coleta OK: {json.dumps(stats, indent=2)}")
        sys.stdout.flush()

    except Exception as e:
        logger.error(f"[JOB] ❌ Erro: {e}")
        logger.error(f"[JOB] Traceback:\n{traceback.format_exc()}")
        sys.stdout.flush()
        payload = {"success": False, "error": str(e), "target_username": target}

    success = send_to_webhook(webhook_url, payload)
    logger.info(f"[JOB] ========== FIM — {'SUCESSO' if success else 'WEBHOOK FALHOU'} ==========")
    sys.stdout.flush()


# ============================================================
# ENDPOINTS
# ============================================================

@app.route("/health", methods=["GET"])
def health():
    """Health check com status da sessão."""
    session_ok = bool(INSTAGRAM_USERNAME and INSTAGRAM_SESSION_B64)
    return jsonify({
        "status": "ok",
        "service": "selvia-scraper",
        "version": "4.0",
        "instagram_session": "configurada" if session_ok else "NÃO configurada",
        "instagram_user": INSTAGRAM_USERNAME or "nenhum",
    })


@app.route("/check-session", methods=["GET"])
def check_session():
    """
    🔧 DIAGNÓSTICO: Verifica se a sessão do Instagram está válida.
    Tenta fazer login e acessar o perfil.
    """
    if not INSTAGRAM_USERNAME or not INSTAGRAM_SESSION_B64:
        return jsonify({
            "success": False,
            "error": "Variáveis INSTAGRAM_USERNAME e/ou INSTAGRAM_SESSION não configuradas no Render",
            "help": "Vá em Render → Environment → adicione INSTAGRAM_USERNAME e INSTAGRAM_SESSION",
        }), 400

    loader, logged_in = create_loader_with_session()

    if logged_in:
        return jsonify({
            "success": True,
            "message": f"✅ Sessão válida! Logado como @{INSTAGRAM_USERNAME}",
            "username": INSTAGRAM_USERNAME,
        })
    else:
        return jsonify({
            "success": False,
            "message": "❌ Sessão inválida ou expirada",
            "help": f"Gere nova sessão: instaloader --login {INSTAGRAM_USERNAME} && cat ~/.config/instaloader/session-{INSTAGRAM_USERNAME} | base64",
        }), 401


@app.route("/test-webhook", methods=["POST"])
def test_webhook():
    body = request.get_json()
    webhook_url = body.get("webhook_url", "")
    if not webhook_url:
        return jsonify({"error": "Campo 'webhook_url' obrigatório"}), 400

    test_payload = {
        "success": True,
        "stats": {
            "target_username": "TESTE_DIAGNOSTICO",
            "posts_analyzed": 0,
            "total_comments": 0,
            "unique_commenters": 0,
            "leads_returned": 1,
            "leads_medicina": 1,
            "collected_at": datetime.utcnow().isoformat(),
        },
        "leads": [{
            "username": "teste_webhook_ok",
            "nome": "TESTE - Webhook Funcionando!",
            "bio": "Se apareceu na planilha, o webhook funciona.",
            "is_private": False,
            "followers": 0,
            "total_comentarios": 0,
            "bio_match_medicina": True,
            "status": "teste",
            "versao_msg": "A",
            "data_envio": "",
            "coletado_em": datetime.utcnow().isoformat(),
        }],
    }

    try:
        resp = http_requests.post(webhook_url, json=test_payload,
                                   headers={"Content-Type": "application/json"}, timeout=30)
        return jsonify({
            "success": resp.status_code == 200,
            "status_code": resp.status_code,
            "response_body": resp.text[:500],
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/scrape", methods=["POST"])
def scrape():
    """
    Inicia coleta em background.

    Body:
    {
        "target_username": "comissao_med28",
        "webhook_url": "https://growthselvia.app.n8n.cloud/webhook/selvia-leads",
        "max_posts": 0,
        "min_comments": 1,
        "check_bio": true,
        "only_medicina": false
    }

    Não precisa mais de instagram_login/password — usa a sessão do ambiente.
    """
    if not check_auth(request):
        return jsonify({"error": "Token inválido"}), 401

    body = request.get_json()
    if not body:
        return jsonify({"error": "Body JSON obrigatório"}), 400
    if "target_username" not in body:
        return jsonify({"error": "Campo 'target_username' obrigatório"}), 400
    if "webhook_url" not in body:
        return jsonify({"error": "Campo 'webhook_url' obrigatório"}), 400

    body["target_username"] = body["target_username"].strip().lstrip("@")

    logger.info(f"[API] Recebido: @{body['target_username']} → {body['webhook_url']}")
    sys.stdout.flush()

    thread = threading.Thread(target=run_scrape_job, args=(body,), daemon=True)
    thread.start()

    return jsonify({
        "status": "processing",
        "message": f"Coleta de @{body['target_username']} iniciada.",
        "target_username": body["target_username"],
        "webhook_url": body["webhook_url"],
        "session_configured": bool(INSTAGRAM_USERNAME),
    })


@app.route("/check-profile", methods=["POST"])
def check_profile():
    if not check_auth(request):
        return jsonify({"error": "Token inválido"}), 401
    body = request.get_json()
    username = body.get("username", "").strip().lstrip("@")
    if not username:
        return jsonify({"error": "Campo 'username' obrigatório"}), 400

    loader, _ = create_loader_with_session()
    info = get_profile_info(loader, username)
    if not info:
        return jsonify({"error": f"@{username} não encontrado"}), 404
    info["bio_match_medicina"] = bio_matches_medicina(info["bio"])
    return jsonify({"success": True, "profile": info})


# ============================================================
# STARTUP
# ============================================================

# Preparar sessão ao iniciar o servidor
logger.info("🚀 Selvia Scraper v4 iniciando...")
setup_session_file()
sys.stdout.flush()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

