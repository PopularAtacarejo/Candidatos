from fastapi import FastAPI, UploadFile, Form, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import requests
import json
from datetime import datetime
import base64
import os
from typing import List
import re
from cachetools import TTLCache
import hashlib

app = FastAPI()

# ==================== CONFIGURAÇÕES ====================
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = "PopularAtacarejo/Candidatos"
GITHUB_OWNER = "PopularAtacarejo"
BRANCH = "main"

headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# Cache para vagas (10 minutos)
vagas_cache = TTLCache(maxsize=1, ttl=600)

# ==================== UTILIDADES ====================
def validate_cpf(cpf: str) -> bool:
    """Valida CPF brasileiro"""
    cpf = re.sub(r'[^\d]', '', cpf)
    
    if len(cpf) != 11:
        return False
    
    if cpf == cpf[0] * 11:
        return False
    
    # Cálculo do primeiro dígito verificador
    soma = sum(int(cpf[i]) * (10 - i) for i in range(9))
    resto = soma % 11
    digito1 = 0 if resto < 2 else 11 - resto
    
    if digito1 != int(cpf[9]):
        return False
    
    # Cálculo do segundo dígito verificador
    soma = sum(int(cpf[i]) * (11 - i) for i in range(10))
    resto = soma % 11
    digito2 = 0 if resto < 2 else 11 - resto
    
    return digito2 == int(cpf[10])

def sanitize_filename(name: str) -> str:
    """Remove caracteres especiais do nome do arquivo"""
    name = re.sub(r'[^\w\s.-]', '', name)
    name = re.sub(r'\s+', '_', name)
    return name[:100]

def get_existing_candidates() -> List[dict]:
    """Obtém candidatos existentes do arquivo JSON no GitHub"""
    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/candidatos.json"
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            content = response.json()["content"]
            decoded = base64.b64decode(content).decode("utf-8")
            return json.loads(decoded)
        return []
    except:
        return []

def get_vagas_from_github() -> List[str]:
    """Busca apenas os nomes das vagas ativas do GitHub"""
    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/vagas.json"
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            content = response.json()["content"]
            decoded = base64.b64decode(content).decode("utf-8")
            vagas_data = json.loads(decoded)
            
            # Extrai apenas os nomes das vagas
            nomes_vagas = [vaga["nome"] for vaga in vagas_data]
            return nomes_vagas
        return []
    except Exception as e:
        print(f"Erro ao buscar vagas: {str(e)}")
        return []

def save_candidate(candidate: dict) -> dict:
    """Salva candidato no arquivo JSON do GitHub"""
    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/candidatos.json"
    
    # Obtém candidatos existentes
    existing = get_existing_candidates()
    
    # Verifica duplicidade
    cpf_clean = re.sub(r'[^\d]', '', candidate["cpf"])
    vaga_lower = candidate["vaga"].lower().strip()
    
    for existing_candidate in existing:
        existing_cpf = re.sub(r'[^\d]', '', existing_candidate.get("cpf", ""))
        existing_vaga = existing_candidate.get("vaga", "").lower().strip()
        
        if existing_cpf == cpf_clean and existing_vaga == vaga_lower:
            if "enviado_em" in existing_candidate:
                existing_date = datetime.fromisoformat(existing_candidate["enviado_em"].replace("Z", "+00:00"))
                days_diff = (datetime.now() - existing_date).days
                
                if days_diff < 90:
                    return {"success": False, "reason": "duplicate"}
    
    # Gera ID único
    candidate_id = hashlib.md5(f"{cpf_clean}_{vaga_lower}_{datetime.now().timestamp()}".encode()).hexdigest()[:12]
    
    # Adiciona metadados
    candidate["id"] = candidate_id
    candidate["enviado_em"] = datetime.now().isoformat()
    candidate["status"] = "Novo"
    candidate["processado_em"] = datetime.now().isoformat()
    
    existing.append(candidate)
    
    # Prepara conteúdo para GitHub
    content = json.dumps(existing, indent=2, ensure_ascii=False)
    content_b64 = base64.b64encode(content.encode("utf-8")).decode("utf-8")
    
    # Obtém SHA do arquivo atual
    sha = None
    try:
        current_response = requests.get(url, headers=headers)
        if current_response.status_code == 200:
            sha = current_response.json()["sha"]
    except:
        pass
    
    # Atualiza arquivo no GitHub
    data = {
        "message": f"Candidatura: {candidate['nome']} para {candidate['vaga']}",
        "content": content_b64,
        "branch": BRANCH
    }
    
    if sha:
        data["sha"] = sha
    
    response = requests.put(url, headers=headers, json=data)
    
    if response.status_code in [200, 201]:
        return {"success": True, "data": response.json()}
    else:
        return {"success": False, "reason": "github_error", "details": response.text}

def save_curriculum_to_github(file: UploadFile, candidate_name: str, cpf: str, vaga: str) -> str:
    """Salva arquivo do currículo na pasta curriculos do GitHub"""
    cpf_clean = re.sub(r'[^\d]', '', cpf)[:11]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Prepara nome do arquivo
    safe_name = sanitize_filename(candidate_name)
    safe_vaga = sanitize_filename(vaga)
    
    # Mantém extensão original
    original_filename = file.filename
    if "." in original_filename:
        ext = original_filename.split(".")[-1].lower()
        if ext not in ["pdf", "doc", "docx"]:
            ext = "pdf"
    else:
        ext = "pdf"
    
    # Nome do arquivo: CPF_Nome_Vaga_Data.Extensão
    filename = f"{cpf_clean}_{safe_name}_{safe_vaga}_{timestamp}.{ext}"
    filename = filename.replace(" ", "_")
    
    # Lê conteúdo do arquivo
    file_content = file.file.read()
    
    # Verifica tamanho (máximo 5MB)
    if len(file_content) > 5 * 1024 * 1024:
        raise ValueError("Arquivo muito grande. Máximo 5MB.")
    
    content_b64 = base64.b64encode(file_content).decode("utf-8")
    
    # URL para upload na pasta curriculos
    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/curriculos/{filename}"
    
    data = {
        "message": f"Currículo: {candidate_name} - {vaga}",
        "content": content_b64,
        "branch": BRANCH
    }
    
    response = requests.put(url, headers=headers, json=data)
    
    if response.status_code in [200, 201]:
        return f"https://raw.githubusercontent.com/{GITHUB_OWNER}/{GITHUB_REPO}/{BRANCH}/curriculos/{filename}"
    else:
        raise Exception(f"Erro ao salvar arquivo no GitHub: {response.status_code}")

# ==================== MIDDLEWARE CORS ====================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== ENDPOINTS ====================
@app.get("/")
async def root():
    return {"message": "API de Candidaturas - Popular Atacarejo", "status": "online"}

@app.get("/health")
async def health():
    return {"ok": True, "timestamp": datetime.now().isoformat(), "service": "candidaturas-api"}

@app.get("/wakeup")
async def wakeup():
    return {"ok": True, "message": "Servidor ativo", "timestamp": datetime.now().isoformat()}

@app.get("/status")
async def status():
    return {"status": "online", "timestamp": datetime.now().isoformat()}

@app.get("/api/vagas")
async def get_vagas():
    """Retorna apenas os nomes das vagas do GitHub"""
    try:
        # Verifica cache primeiro
        if "vagas_nomes" in vagas_cache:
            return vagas_cache["vagas_nomes"]
        
        # Busca vagas do GitHub
        nomes_vagas = get_vagas_from_github()
        
        # Se não encontrar vagas no GitHub, retorna lista padrão
        if not nomes_vagas:
            nomes_vagas = [
                "Auxiliar de Limpeza",
                "Vendedor",
                "Caixa",
                "Estoquista",
                "Repositor",
                "Atendente",
                "Gerente",
                "Supervisor",
                "Operador de Caixa"
            ]
        
        # Formata para o frontend (array de objetos simples)
        vagas_formatadas = [{"nome": nome} for nome in nomes_vagas]
        
        # Armazena no cache
        vagas_cache["vagas_nomes"] = vagas_formatadas
        
        return vagas_formatadas
    except Exception as e:
        print(f"Erro ao obter vagas: {str(e)}")
        # Fallback para vagas padrão
        return [
            {"nome": "Auxiliar de Limpeza"},
            {"nome": "Vendedor"},
            {"nome": "Caixa"},
            {"nome": "Estoquista"}
        ]

@app.post("/api/enviar")
async def enviar_curriculo(
    nome: str = Form(...),
    cpf: str = Form(...),
    telefone: str = Form(...),
    email: str = Form(...),
    cep: str = Form(...),
    cidade: str = Form(...),
    bairro: str = Form(...),
    rua: str = Form(...),
    transporte: str = Form(...),
    vaga: str = Form(...),
    arquivo: UploadFile = File(...)
):
    """Recebe e salva candidatura no GitHub"""
    
    # Validações básicas
    if not nome or len(nome.strip()) < 3:
        raise HTTPException(status_code=400, detail="Nome inválido (mínimo 3 caracteres)")
    
    if not validate_cpf(cpf):
        raise HTTPException(status_code=400, detail="CPF inválido")
    
    email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not re.match(email_regex, email):
        raise HTTPException(status_code=400, detail="Email inválido")
    
    # Valida arquivo
    if not arquivo.filename:
        raise HTTPException(status_code=400, detail="Nenhum arquivo selecionado")
    
    ext = arquivo.filename.lower().split('.')[-1] if '.' in arquivo.filename else ''
    if ext not in ['pdf', 'doc', 'docx']:
        raise HTTPException(status_code=400, detail="Formato inválido. Use PDF, DOC ou DOCX.")
    
    try:
        # Salva arquivo na pasta curriculos do GitHub
        arquivo_url = save_curriculum_to_github(arquivo, nome, cpf, vaga)
        
        # Prepara dados do candidato
        candidato = {
            "nome": nome.strip(),
            "cpf": cpf,
            "telefone": telefone,
            "email": email.lower().strip(),
            "cep": cep,
            "cidade": cidade,
            "bairro": bairro,
            "rua": rua,
            "transporte": transporte,
            "vaga": vaga,
            "arquivo_url": arquivo_url,
            "arquivo_nome": arquivo.filename,
            "tamanho_arquivo": arquivo.size
        }
        
        # Salva dados no GitHub
        result = save_candidate(candidato)
        
        if result["success"]:
            return {
                "ok": True,
                "message": "✅ Sua candidatura foi enviada com sucesso! Agradecemos seu interesse e entraremos em contato caso seu perfil seja selecionado.",
                "id": candidato.get("id"),
                "arquivo_url": arquivo_url
            }
        else:
            if result.get("reason") == "duplicate":
                raise HTTPException(
                    status_code=409,
                    detail="⚠️ Já existe uma candidatura registrada para esta vaga com o mesmo CPF. Aguarde 90 dias antes de reenviar."
                )
            else:
                raise HTTPException(
                    status_code=500,
                    detail="❌ Erro ao salvar candidatura. Tente novamente em alguns instantes."
                )
                
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        print(f"Erro interno: {str(e)}")
        raise HTTPException(status_code=500, detail="❌ Erro interno do servidor. Tente novamente mais tarde.")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
