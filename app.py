"""
Selvia — Instagram Lead Scraper API (v2 — Async + Webhook)
===========================================================
Versão atualizada que processa a coleta em background
e envia os resultados via webhook para o n8n.

Requisitos:
    pip install flask instaloader gunicorn requests

Execução (produção):
    gunicorn app:app --bind 0.0.0.0:5000 --workers 2 --timeout 60
"""

import instaloader
import re
import json
import time
import random
import logging
import threading
import requests as http_requests
from datetime import datetime
from flask import Flask, request, jsonify

# ============================================================
# CONFIGURAÇÃO
# ============================================================

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Palavras-chave que indicam perfil de medicina/universitário
KEYWORDS_MEDICINA = [
    "medicina", "med", "medica", "médica", "médico", "medico",
    "medicine", "medical", "med student", "estudante de medicina",
    "acadêmico de medicina", "academico de medicina",
    "futuro médico", "futura médica", "futuro medico", "futura medica",
    "interno", "interna", "internato",
    "residência", "residencia",
    "⚕️", "🩺", "🏥", "💉", "🥼",
]

# Padrões regex para variações como "MED 28", "MED28", "MED2028"
REGEX_MEDICINA = [
    r"\bmed\s?\d{2,4}\b",
    r"\bmedicina\s?\d{2,4}\b",
    r"\bturma\s+med",
    r"\b\d{2,4}\s*med\b",
]

# Token de segurança — TROQUE ISSO!
API_TOKEN = "72b6834c37072a4ed459e7ba19fb1f43"


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
        logger.warning(f"Não conseguiu acessar perfil @{username}: {e}")
        return None


def scrape_commenters(loader, target_username, max_posts=0):
    profile = instaloader.Profile.from_username(loader.context, target_username)

    if profile.is_private:
        raise ValueError(f"O perfil @{target_username} é privado.")

    commenters = {}
    posts_analyzed = 0
    total_comments = 0

    logger.info(f"Coletando @{target_username} ({profile.mediacount} posts)")

    for post in profile.get_posts():
        if max_posts > 0 and posts_analyzed >= max_posts:
            break

        posts_analyzed += 1
        logger.info(f"  Post {posts_analyzed}: {post.shortcode}")

        try:
            for comment in post.get_comments():
                commenter = comment.owner.username
                total_comments += 1

                if commenter not in commenters:
                    commenters[commenter] = {"count": 0}
                commenters[commenter]["count"] += 1

        except Exception as e:
            logger.warning(f"  Erro no post {post.shortcode}: {e}")

        time.sleep(random.uniform(3, 8))

    return {
        "commenters": commenters,
        "posts_analyzed": posts_analyzed,
        "total_comments": total_comments,
    }


def enrich_and_filter_leads(loader, commenters, min_comments=1, check_bio=True):
    leads = []
    sorted_commenters = sorted(
        commenters.items(),
        key=lambda x: x[1]["count"],
        reverse=True,
    )

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

    return leads


# ============================================================
# PROCESSAMENTO EM BACKGROUND
# ============================================================

def run_scrape_job(params):
    """
    Executa a coleta em background e envia o resultado
    para o webhook do n8n quando terminar.
    """
    target = params["target_username"]
    webhook_url = params["webhook_url"]
    max_posts = params.get("max_posts", 0)
    min_comments = params.get("min_comments", 1)
    check_bio = params.get("check_bio", True)
    only_medicina = params.get("only_medicina", False)

    logger.info(f"[JOB] Iniciando coleta de @{target}")

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

    # Login opcional
    ig_login = params.get("instagram_login", "")
    ig_password = params.get("instagram_password", "")
    if ig_login and ig_password:
        try:
            loader.login(ig_login, ig_password)
        except Exception as e:
            logger.warning(f"[JOB] Falha no login: {e}")

    try:
        # Etapa 1: Coletar comentaristas
        result = scrape_commenters(loader, target, max_posts)

        # Etapa 2: Enriquecer e filtrar
        leads = enrich_and_filter_leads(
            loader, result["commenters"], min_comments, check_bio
        )

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

        payload = {
            "success": True,
            "stats": stats,
            "leads": leads,
        }

        logger.info(f"[JOB] Coleta concluída: {stats['leads_returned']} leads. Enviando para webhook...")

    except Exception as e:
        logger.exception(f"[JOB] Erro na coleta de @{target}")
        payload = {
            "success": False,
            "error": str(e),
            "target_username": target,
        }

    # Enviar resultado para o webhook do n8n
    try:
        resp = http_requests.post(
            webhook_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        logger.info(f"[JOB] Webhook respondeu: {resp.status_code}")
    except Exception as e:
        logger.error(f"[JOB] Falha ao enviar para webhook: {e}")


# ============================================================
# ENDPOINTS
# ============================================================

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "selvia-instagram-scraper", "version": "2.0"})


@app.route("/scrape", methods=["POST"])
def scrape():
    """
    Inicia a coleta em background e retorna imediatamente.

    Body (JSON):
    {
        "target_username": "comissao_med28",
        "webhook_url": "https://SEU-N8N.app.n8n.cloud/webhook/selvia-leads",
        "max_posts": 0,
        "min_comments": 1,
        "check_bio": true,
        "only_medicina": false,
        "instagram_login": "",
        "instagram_password": ""
    }

    Responde imediatamente com {"status": "processing"}.
    Quando a coleta terminar, envia o resultado para o webhook_url.
    """
    if not check_auth(request):
        return jsonify({"error": "Token inválido"}), 401

    body = request.get_json()
    if not body:
        return jsonify({"error": "Body JSON é obrigatório"}), 400
    if "target_username" not in body:
        return jsonify({"error": "Campo 'target_username' é obrigatório"}), 400
    if "webhook_url" not in body:
        return jsonify({"error": "Campo 'webhook_url' é obrigatório"}), 400

    body["target_username"] = body["target_username"].strip().lstrip("@")

    # Inicia a coleta em uma thread separada
    thread = threading.Thread(target=run_scrape_job, args=(body,), daemon=True)
    thread.start()

    logger.info(f"Coleta de @{body['target_username']} iniciada em background")

    return jsonify({
        "status": "processing",
        "message": f"Coleta de @{body['target_username']} iniciada. O resultado será enviado para {body['webhook_url']}",
        "target_username": body["target_username"],
    })


@app.route("/check-profile", methods=["POST"])
def check_profile():
    if not check_auth(request):
        return jsonify({"error": "Token inválido"}), 401

    body = request.get_json()
    username = body.get("username", "").strip().lstrip("@")
    if not username:
        return jsonify({"error": "Campo 'username' é obrigatório"}), 400

    loader = instaloader.Instaloader(
        download_pictures=False, download_videos=False,
        download_video_thumbnails=False, download_geotags=False,
        download_comments=False, save_metadata=False, quiet=True,
    )

    info = get_profile_info(loader, username)
    if not info:
        return jsonify({"error": f"Perfil @{username} não encontrado"}), 404

    info["bio_match_medicina"] = bio_matches_medicina(info["bio"])
    return jsonify({"success": True, "profile": info})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
