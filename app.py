"""
Selvia — Instagram Lead Scraper API
====================================
Microserviço que coleta comentaristas de perfis de comissões de formatura
e filtra leads com perfil de estudante de medicina.

Uso: O n8n Cloud chama essa API via HTTP Request.

Requisitos:
    pip install flask instaloader gunicorn

Execução (desenvolvimento):
    python app.py

Execução (produção):
    gunicorn app:app --bind 0.0.0.0:5000 --workers 2 --timeout 300
"""

import instaloader
import re
import json
import time
import random
import logging
from datetime import datetime
from flask import Flask, request, jsonify
from collections import Counter

# ============================================================
# CONFIGURAÇÃO
# ============================================================

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Palavras-chave que indicam perfil de medicina/universitário
# (case-insensitive, aplicadas na bio do usuário)
KEYWORDS_MEDICINA = [
    "medicina", "med", "medica", "médica", "médico", "medico",
    "medicine", "medical", "med student", "estudante de medicina",
    "acadêmico de medicina", "academico de medicina",
    "futuro médico", "futura médica", "futuro medico", "futura medica",
    "interno", "interna", "internato",
    "residência", "residencia",
    "⚕️", "🩺", "🏥", "💉", "🥼",  # emojis comuns de medicina
]

# Padrões regex para variações como "MED 28", "MED28", "MED2028", "turma med"
REGEX_MEDICINA = [
    r"\bmed\s?\d{2,4}\b",        # med 28, med2028, med28
    r"\bmedicina\s?\d{2,4}\b",   # medicina 28, medicina2028
    r"\bturma\s+med",            # turma med...
    r"\b\d{2,4}\s*med\b",        # 28med, 2028 med
]

# Token de segurança simples (defina no .env ou variável de ambiente)
API_TOKEN = "72b6834c37072a4ed459e7ba19fb1f43"  # ⚠️ TROQUE ISSO!


# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def check_auth(req):
    """Verifica o token de autenticação."""
    token = req.headers.get("Authorization", "").replace("Bearer ", "")
    return token == API_TOKEN


def bio_matches_medicina(bio: str) -> bool:
    """
    Verifica se a bio contém indicadores de estudante de medicina.
    Retorna True se encontrar match.
    """
    if not bio:
        return False
    bio_lower = bio.lower()

    # Checa palavras-chave diretas
    for kw in KEYWORDS_MEDICINA:
        if kw.lower() in bio_lower:
            return True

    # Checa padrões regex
    for pattern in REGEX_MEDICINA:
        if re.search(pattern, bio_lower):
            return True

    return False


def get_profile_info(loader: instaloader.Instaloader, username: str) -> dict:
    """
    Busca informações básicas do perfil.
    Retorna dict com bio, nome, is_private, etc.
    Retorna None se não conseguir acessar.
    """
    try:
        profile = instaloader.Profile.from_username(loader.context, username)
        return {
            "username": profile.username,
            "nome": profile.full_name or profile.username,
            "bio": profile.biography or "",
            "is_private": profile.is_private,
            "followers": profile.followers,
            "following": profile.followees,
            "profile_pic_url": profile.profile_pic_url,
        }
    except Exception as e:
        logger.warning(f"Não conseguiu acessar perfil @{username}: {e}")
        return None


def scrape_commenters(
    loader: instaloader.Instaloader,
    target_username: str,
    max_posts: int = 0,
    delay_between_posts: tuple = (3, 8),
) -> dict:
    """
    Coleta todos os comentaristas de um perfil público do Instagram.

    Args:
        loader: instância do Instaloader
        target_username: @ da comissão de formatura
        max_posts: 0 = todos os posts disponíveis
        delay_between_posts: range de delay aleatório entre posts (seg)

    Returns:
        dict com:
            - commenters: {username: {"count": N, "comments": [...]}}
            - posts_analyzed: int
            - total_comments: int
    """
    try:
        profile = instaloader.Profile.from_username(loader.context, target_username)
    except Exception as e:
        logger.error(f"Erro ao acessar perfil @{target_username}: {e}")
        raise ValueError(f"Não foi possível acessar o perfil @{target_username}. Verifique se existe e é público.")

    if profile.is_private:
        raise ValueError(f"O perfil @{target_username} é privado. Só é possível coletar de perfis públicos.")

    commenters = {}
    posts_analyzed = 0
    total_comments = 0

    logger.info(f"Iniciando coleta de @{target_username} ({profile.mediacount} posts)")

    for post in profile.get_posts():
        if max_posts > 0 and posts_analyzed >= max_posts:
            break

        posts_analyzed += 1
        post_comments = 0

        logger.info(f"  Post {posts_analyzed}: {post.shortcode} ({post.date_utc.strftime('%Y-%m-%d')})")

        try:
            for comment in post.get_comments():
                commenter = comment.owner.username
                text = comment.text or ""
                post_comments += 1
                total_comments += 1

                if commenter not in commenters:
                    commenters[commenter] = {
                        "count": 0,
                        "comments": [],
                    }

                commenters[commenter]["count"] += 1
                commenters[commenter]["comments"].append({
                    "text": text[:200],  # trunca pra não ficar gigante
                    "post_shortcode": post.shortcode,
                    "date": comment.created_at_utc.isoformat() if comment.created_at_utc else None,
                })

            logger.info(f"    → {post_comments} comentários coletados")

        except Exception as e:
            logger.warning(f"    → Erro ao coletar comentários do post {post.shortcode}: {e}")

        # Delay entre posts para não ser bloqueado
        delay = random.uniform(*delay_between_posts)
        logger.info(f"    → Aguardando {delay:.1f}s...")
        time.sleep(delay)

    logger.info(f"Coleta finalizada: {posts_analyzed} posts, {total_comments} comentários, {len(commenters)} usuários únicos")

    return {
        "commenters": commenters,
        "posts_analyzed": posts_analyzed,
        "total_comments": total_comments,
    }


def enrich_and_filter_leads(
    loader: instaloader.Instaloader,
    commenters: dict,
    min_comments: int = 1,
    check_bio: bool = True,
    delay_between_profiles: tuple = (2, 5),
) -> list:
    """
    Enriquece os dados dos comentaristas verificando suas bios
    e filtra os que têm perfil de estudante de medicina.

    Args:
        loader: instância do Instaloader
        commenters: dict retornado por scrape_commenters
        min_comments: mínimo de comentários para considerar o lead
        check_bio: se True, verifica a bio do perfil
        delay_between_profiles: delay entre checagens de perfil

    Returns:
        Lista de leads filtrados e enriquecidos
    """
    leads = []
    checked = 0
    matched = 0

    # Ordena por quantidade de comentários (mais engajados primeiro)
    sorted_commenters = sorted(
        commenters.items(),
        key=lambda x: x[1]["count"],
        reverse=True,
    )

    logger.info(f"Filtrando {len(sorted_commenters)} comentaristas (min_comments={min_comments}, check_bio={check_bio})")

    for username, data in sorted_commenters:
        # Filtro por quantidade mínima de comentários
        if data["count"] < min_comments:
            continue

        lead = {
            "username": username,
            "nome": username,  # fallback
            "bio": "",
            "is_private": None,
            "followers": None,
            "total_comentarios": data["count"],
            "bio_match_medicina": False,
            "status": "pendente",
            "versao_msg": chr(65 + (len(leads) % 6)),  # A, B, C, D, E, F rotativo
            "data_envio": "",
            "coletado_em": datetime.utcnow().isoformat(),
        }

        # Enriquecer com dados do perfil
        if check_bio:
            checked += 1
            profile_info = get_profile_info(loader, username)

            if profile_info:
                lead["nome"] = profile_info["nome"]
                lead["bio"] = profile_info["bio"]
                lead["is_private"] = profile_info["is_private"]
                lead["followers"] = profile_info["followers"]
                lead["bio_match_medicina"] = bio_matches_medicina(profile_info["bio"])

                if lead["bio_match_medicina"]:
                    matched += 1

            # Delay entre perfis
            delay = random.uniform(*delay_between_profiles)
            time.sleep(delay)

            # Log de progresso a cada 10 perfis
            if checked % 10 == 0:
                logger.info(f"  Verificados {checked} perfis, {matched} matches até agora...")

        leads.append(lead)

    logger.info(f"Filtragem concluída: {checked} perfis verificados, {matched} matches de medicina")

    return leads


# ============================================================
# ENDPOINTS DA API
# ============================================================

@app.route("/health", methods=["GET"])
def health():
    """Health check."""
    return jsonify({"status": "ok", "service": "selvia-instagram-scraper"})


@app.route("/scrape", methods=["POST"])
def scrape():
    """
    Endpoint principal: coleta leads de um perfil de comissão.

    Body (JSON):
    {
        "target_username": "comissao_med28",
        "max_posts": 0,               // 0 = todos
        "min_comments": 1,             // mínimo de comentários
        "check_bio": true,             // verificar bio de cada comentarista
        "only_medicina": false,        // retornar APENAS matches de medicina
        "instagram_login": "",         // (opcional) login do Instagram
        "instagram_password": ""       // (opcional) senha do Instagram
    }

    Nota sobre login:
        - Sem login: funciona para perfis públicos, mas tem rate limit mais agressivo
        - Com login: acessa mais dados, mas arrisca a conta se abusar
        - Recomendação: use sem login primeiro; só use login se necessário
    """
    # Autenticação da API
    if not check_auth(request):
        return jsonify({"error": "Token inválido"}), 401

    # Parse do body
    body = request.get_json()
    if not body or "target_username" not in body:
        return jsonify({"error": "Campo 'target_username' é obrigatório"}), 400

    target = body["target_username"].strip().lstrip("@")
    max_posts = body.get("max_posts", 0)
    min_comments = body.get("min_comments", 1)
    check_bio = body.get("check_bio", True)
    only_medicina = body.get("only_medicina", False)
    ig_login = body.get("instagram_login", "")
    ig_password = body.get("instagram_password", "")

    logger.info(f"Requisição recebida: target=@{target}, max_posts={max_posts}, check_bio={check_bio}")

    # Inicializar Instaloader
    loader = instaloader.Instaloader(
        download_pictures=False,
        download_videos=False,
        download_video_thumbnails=False,
        download_geotags=False,
        download_comments=False,  # usamos get_comments() manualmente
        save_metadata=False,
        compress_json=False,
        quiet=True,
    )

    # Login opcional
    if ig_login and ig_password:
        try:
            loader.login(ig_login, ig_password)
            logger.info("Login no Instagram realizado com sucesso")
        except Exception as e:
            logger.warning(f"Falha no login: {e}. Continuando sem autenticação.")

    try:
        # Etapa 1: Coletar comentaristas
        result = scrape_commenters(
            loader=loader,
            target_username=target,
            max_posts=max_posts,
            delay_between_posts=(3, 8),
        )

        # Etapa 2: Enriquecer e filtrar
        leads = enrich_and_filter_leads(
            loader=loader,
            commenters=result["commenters"],
            min_comments=min_comments,
            check_bio=check_bio,
            delay_between_profiles=(2, 5),
        )

        # Filtrar apenas medicina se solicitado
        if only_medicina:
            leads = [l for l in leads if l["bio_match_medicina"]]

        # Ordenar: matches de medicina primeiro, depois por engajamento
        leads.sort(key=lambda x: (-int(x["bio_match_medicina"]), -x["total_comentarios"]))

        # Estatísticas
        stats = {
            "target_username": target,
            "posts_analyzed": result["posts_analyzed"],
            "total_comments": result["total_comments"],
            "unique_commenters": len(result["commenters"]),
            "leads_returned": len(leads),
            "leads_medicina": sum(1 for l in leads if l["bio_match_medicina"]),
            "collected_at": datetime.utcnow().isoformat(),
        }

        logger.info(f"Resultado: {stats}")

        return jsonify({
            "success": True,
            "stats": stats,
            "leads": leads,
        })

    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        logger.exception("Erro inesperado durante scraping")
        return jsonify({"success": False, "error": f"Erro interno: {str(e)}"}), 500


@app.route("/check-profile", methods=["POST"])
def check_profile():
    """
    Endpoint auxiliar: verifica se um perfil específico é de medicina.
    Útil para validação manual.

    Body: { "username": "joao_med28" }
    """
    if not check_auth(request):
        return jsonify({"error": "Token inválido"}), 401

    body = request.get_json()
    username = body.get("username", "").strip().lstrip("@")
    if not username:
        return jsonify({"error": "Campo 'username' é obrigatório"}), 400

    loader = instaloader.Instaloader(
        download_pictures=False,
        download_videos=False,
        download_video_thumbnails=False,
        download_geotags=False,
        download_comments=False,
        save_metadata=False,
        quiet=True,
    )

    info = get_profile_info(loader, username)
    if not info:
        return jsonify({"error": f"Perfil @{username} não encontrado"}), 404

    info["bio_match_medicina"] = bio_matches_medicina(info["bio"])

    return jsonify({"success": True, "profile": info})


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
