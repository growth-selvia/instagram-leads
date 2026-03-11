"""
Selvia — Instagram Lead Scraper API (v3 — Debug + Webhook Fix)
===============================================================
Versão com logs detalhados e endpoint de diagnóstico.

Requisitos:
    pip install flask instaloader gunicorn requests

Execução:
    gunicorn app:app --bind 0.0.0.0:5000 --workers 2 --timeout 60 --access-logfile -
"""

import instaloader
import re
import json
import sys
import time
import random
import logging
import threading
import traceback
import requests as http_requests
from datetime import datetime
from flask import Flask, request, jsonify

# ============================================================
# CONFIGURAÇÃO — Logs forçam flush para aparecer no Render
# ============================================================

app = Flask(__name__)

# Força logs a aparecerem imediatamente (sem buffer)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# Força flush em todo print também
import functools
print = functools.partial(print, flush=True)

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

API_TOKEN = "SELVIA_API_TOKEN_AQUI"  # ⚠️ TROQUE!


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

        logger.info(f"  Post {posts_analyzed}/{profile.mediacount}: {post.shortcode} — {total_comments} comentários até agora")
        sys.stdout.flush()
        time.sleep(random.uniform(3, 8))

    logger.info(f"Coleta finalizada: {posts_analyzed} posts, {total_comments} comentários, {len(commenters)} únicos")
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

    logger.info(f"Leads processados: {len(leads)} total, {sum(1 for l in leads if l['bio_match_medicina'])} medicina")
    sys.stdout.flush()
    return leads


# ============================================================
# ENVIO PARA WEBHOOK — COM RETRY E LOGS DETALHADOS
# ============================================================

def send_to_webhook(webhook_url, payload):
    """
    Envia o payload para o webhook do n8n.
    Tenta até 3 vezes em caso de falha.
    """
    logger.info(f"[WEBHOOK] ==========================================")
    logger.info(f"[WEBHOOK] URL: {webhook_url}")
    logger.info(f"[WEBHOOK] Leads: {len(payload.get('leads', []))}")
    logger.info(f"[WEBHOOK] Success: {payload.get('success')}")
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
            else:
                logger.warning(f"[WEBHOOK] ⚠️ Status inesperado: {resp.status_code}")
                sys.stdout.flush()

        except http_requests.exceptions.ConnectionError as e:
            logger.error(f"[WEBHOOK] ❌ CONEXÃO FALHOU (tentativa {attempt}): {e}")
            sys.stdout.flush()
        except http_requests.exceptions.Timeout as e:
            logger.error(f"[WEBHOOK] ❌ TIMEOUT (tentativa {attempt}): {e}")
            sys.stdout.flush()
        except Exception as e:
            logger.error(f"[WEBHOOK] ❌ ERRO (tentativa {attempt}): {e}")
            logger.error(f"[WEBHOOK] Traceback:\n{traceback.format_exc()}")
            sys.stdout.flush()

        if attempt < 3:
            wait = attempt * 5
            logger.info(f"[WEBHOOK] Aguardando {wait}s...")
            sys.stdout.flush()
            time.sleep(wait)

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

    logger.info(f"[JOB] ========== INÍCIO DA COLETA ==========")
    logger.info(f"[JOB] Target: @{target}")
    logger.info(f"[JOB] Webhook: {webhook_url}")
    logger.info(f"[JOB] Config: max_posts={max_posts}, min_comments={min_comments}, check_bio={check_bio}")
    sys.stdout.flush()

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

    ig_login = params.get("instagram_login", "")
    ig_password = params.get("instagram_password", "")
    if ig_login and ig_password:
        try:
            loader.login(ig_login, ig_password)
            logger.info("[JOB] Login OK")
        except Exception as e:
            logger.warning(f"[JOB] Login falhou: {e}")
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
            "collected_at": datetime.utcnow().isoformat(),
        }
        payload = {"success": True, "stats": stats, "leads": leads}

        logger.info(f"[JOB] ✅ Coleta OK: {stats['leads_returned']} leads, {stats['leads_medicina']} medicina")
        sys.stdout.flush()

    except Exception as e:
        logger.error(f"[JOB] ❌ Erro na coleta: {e}")
        logger.error(f"[JOB] Traceback:\n{traceback.format_exc()}")
        sys.stdout.flush()
        payload = {"success": False, "error": str(e), "target_username": target}

    # Enviar para webhook
    success = send_to_webhook(webhook_url, payload)

    if success:
        logger.info(f"[JOB] ========== FIM — SUCESSO ==========")
    else:
        logger.error(f"[JOB] ========== FIM — WEBHOOK FALHOU ==========")
    sys.stdout.flush()


# ============================================================
# ENDPOINTS
# ============================================================

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "selvia-scraper", "version": "3.0"})


@app.route("/test-webhook", methods=["POST"])
def test_webhook():
    """
    🔧 DIAGNÓSTICO: Testa se o Render consegue chamar o webhook do n8n.

    Body: { "webhook_url": "https://growthselvia.app.n8n.cloud/webhook/selvia-leads" }

    Envia um lead fake. Se aparecer na planilha, o webhook funciona.
    """
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
        "leads": [
            {
                "username": "teste_webhook_ok",
                "nome": "TESTE - Webhook Funcionando!",
                "bio": "Se apareceu na planilha, o webhook funciona perfeitamente.",
                "is_private": False,
                "followers": 0,
                "total_comentarios": 0,
                "bio_match_medicina": True,
                "status": "teste",
                "versao_msg": "A",
                "data_envio": "",
                "coletado_em": datetime.utcnow().isoformat(),
            }
        ],
    }

    logger.info(f"[TEST-WEBHOOK] Testando: {webhook_url}")
    sys.stdout.flush()

    try:
        resp = http_requests.post(
            webhook_url,
            json=test_payload,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        result = {
            "webhook_url": webhook_url,
            "status_code": resp.status_code,
            "response_body": resp.text[:500],
            "success": resp.status_code == 200,
            "message": "✅ Webhook alcançável! Verifique se o lead 'teste_webhook_ok' apareceu na planilha." if resp.status_code == 200 else f"⚠️ Webhook respondeu com status {resp.status_code}",
        }
        logger.info(f"[TEST-WEBHOOK] Resultado: {json.dumps(result)}")
        sys.stdout.flush()
        return jsonify(result)

    except Exception as e:
        error_result = {
            "webhook_url": webhook_url,
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__,
            "message": "❌ Não conseguiu alcançar o webhook. Verifique se o Fluxo 2 está ATIVO no n8n.",
        }
        logger.error(f"[TEST-WEBHOOK] Erro: {json.dumps(error_result)}")
        sys.stdout.flush()
        return jsonify(error_result), 500


@app.route("/scrape", methods=["POST"])
def scrape():
    """
    Inicia coleta em background. Responde imediatamente.

    Body (JSON):
    {
        "target_username": "comissao_med28",
        "webhook_url": "https://growthselvia.app.n8n.cloud/webhook/selvia-leads",
        "max_posts": 0,
        "min_comments": 1,
        "check_bio": true,
        "only_medicina": false
    }
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

    logger.info(f"[API] Thread iniciada para @{body['target_username']}")
    sys.stdout.flush()

    return jsonify({
        "status": "processing",
        "message": f"Coleta de @{body['target_username']} iniciada. Resultado será enviado para o webhook.",
        "target_username": body["target_username"],
        "webhook_url": body["webhook_url"],
    })


@app.route("/check-profile", methods=["POST"])
def check_profile():
    if not check_auth(request):
        return jsonify({"error": "Token inválido"}), 401
    body = request.get_json()
    username = body.get("username", "").strip().lstrip("@")
    if not username:
        return jsonify({"error": "Campo 'username' obrigatório"}), 400

    loader = instaloader.Instaloader(
        download_pictures=False, download_videos=False,
        download_video_thumbnails=False, download_geotags=False,
        download_comments=False, save_metadata=False, quiet=True,
    )
    info = get_profile_info(loader, username)
    if not info:
        return jsonify({"error": f"@{username} não encontrado"}), 404
    info["bio_match_medicina"] = bio_matches_medicina(info["bio"])
    return jsonify({"success": True, "profile": info})


if __name__ == "__main__":
    print("🚀 Selvia Scraper v3 iniciando...")
    app.run(host="0.0.0.0", port=5000, debug=True)

