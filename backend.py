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
from dotenv import load_dotenv

app = FastAPI()

# ==================== CONFIGURAÇÕES ====================
load_dotenv()
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = "Candidatos"  # Apenas o nome do repositório
GITHUB_OWNER = "PopularAtacarejo"

headers = {
    "Accept": "application/vnd.github.v3+json"
}

if GITHUB_TOKEN:
    headers["Authorization"] = f"token {GITHUB_TOKEN}"
    print(f"Token GitHub carregado (primeiros 5 caracteres): {GITHUB_TOKEN[:5]}...")
else:
    print("AVISO: Token GitHub não encontrado! Operações de escrita podem falhar.")

def get_repo_default_branch() -> str:
    """Busca o branch padrão do repositório no GitHub."""
    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}"
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        print(f"Status ao buscar repositório: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            default_branch = data.get("default_branch", "main")
            print(f"Branch padrão encontrado: {default_branch}")
            return default_branch
        elif response.status_code == 404:
            print(f"ERRO: Repositório {GITHUB_OWNER}/{GITHUB_REPO} não encontrado!")
        else:
            print(f"ERRO ao buscar repositório: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Erro de rede ao buscar repositório: {str(e)}")
    
    return "main"


BRANCH = os.getenv("GITHUB_BRANCH") or get_repo_default_branch()
print(f"Usando branch: {BRANCH}")

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

def check_repo_access() -> bool:
    """Verifica se temos acesso ao repositório"""
    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}"
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        print(f"Verificação de acesso ao repositório: {response.status_code}")
        
        if response.status_code == 200:
            print("Acesso ao repositório confirmado")
            return True
        elif response.status_code == 404:
            print(f"ERRO CRÍTICO: Repositório {GITHUB_OWNER}/{GITHUB_REPO} não existe!")
            return False
        elif response.status_code == 403:
            print(f"ERRO: Token GitHub não tem permissão para acessar o repositório")
            return False
        else:
            print(f"ERRO de acesso ao repositório: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"Erro ao verificar acesso ao repositório: {str(e)}")
        return False

def create_github_file(file_path: str, content: str, message: str) -> bool:
    """Cria um arquivo no GitHub via API"""
    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{file_path}"
    
    content_b64 = base64.b64encode(content.encode("utf-8")).decode("utf-8")
    
    data = {
        "message": message,
        "content": content_b64,
        "branch": BRANCH
    }
    
    try:
        response = requests.put(url, headers=headers, json=data, timeout=30)
        print(f"Tentativa de criar {file_path}: Status {response.status_code}")
        
        if response.status_code in [200, 201]:
            print(f"Arquivo {file_path} criado com sucesso")
            return True
        else:
            print(f"Erro ao criar {file_path}: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"Exceção ao criar {file_path}: {str(e)}")
        return False

def initialize_repository() -> bool:
    """Inicializa o repositório com arquivos necessários se não existirem"""
    print("Inicializando repositório...")
    
    # Verificar se o repositório existe
    if not check_repo_access():
        print("Não é possível acessar o repositório. Verifique o token e as permissões.")
        return False
    
    # Lista de arquivos a serem criados se não existirem
    files_to_check = [
        {
            "path": "vagas.json",
            "content": json.dumps([
                {"nome": "Auxiliar de Limpeza"},
                {"nome": "Vendedor"},
                {"nome": "Caixa"},
                {"nome": "Estoquista"},
                {"nome": "Repositor"},
                {"nome": "Atendente"},
                {"nome": "Gerente"},
                {"nome": "Supervisor"},
                {"nome": "Operador de Caixa"}
            ], indent=2, ensure_ascii=False),
            "message": "Criar arquivo de vagas inicial"
        },
        {
            "path": "candidatos.json",
            "content": "[]",
            "message": "Criar arquivo de candidatos inicial"
        },
        {
            "path": "curriculos/README.md",
            "content": "# Pasta de Currículos\n\nEsta pasta armazena os currículos enviados pelos candidatos.",
            "message": "Criar pasta curriculos com arquivo README"
        }
    ]
    
    created_count = 0
    
    for file_info in files_to_check:
        # Verificar se o arquivo já existe
        url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{file_info['path']}"
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                print(f"Arquivo {file_info['path']} já existe")
                continue
        except:
            pass
        
        # Criar o arquivo
        if create_github_file(file_info["path"], file_info["content"], file_info["message"]):
            created_count += 1
            # Aguardar um pouco para não sobrecarregar a API
            import time
            time.sleep(1)
    
    print(f"Inicialização concluída. {created_count} arquivos criados.")
    return created_count > 0

# Inicializar o repositório ao iniciar o servidor
print("=== Inicializando servidor ===")
initialize_repository()

def get_existing_candidates() -> List[dict]:
    """Obtém candidatos existentes do arquivo JSON no GitHub"""
    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/candidatos.json"
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            content = response.json()["content"]
            decoded = base64.b64decode(content).decode("utf-8")
            return json.loads(decoded)
        elif response.status_code == 404:
            # Tentar criar o arquivo se não existir
            print("Arquivo candidatos.json não encontrado, tentando criar...")
            if create_github_file("candidatos.json", "[]", "Criar arquivo de candidatos"):
                return []
        return []
    except Exception as e:
        print(f"Erro ao buscar candidatos: {str(e)}")
        return []

def normalize_vagas_data(vagas_data) -> List[dict]:
    """Normaliza as vagas para garantir o campo 'nome'"""
    normalized = []

    if isinstance(vagas_data, dict):
        vagas_data = [vagas_data]

    if isinstance(vagas_data, list):
        for vaga in vagas_data:
            if isinstance(vaga, dict) and "nome" in vaga:
                normalized.append({"nome": vaga["nome"]})
            elif isinstance(vaga, str):
                normalized.append({"nome": vaga})

    return normalized


def fetch_content_from_github(path: str):
    """Busca um arquivo no GitHub e retorna o JSON decodificado"""
    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{path}"

    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            payload = response.json()
            content = payload.get("content")
            if content:
                decoded = base64.b64decode(content).decode("utf-8")
                return json.loads(decoded)
        else:
            print(f"Erro ao buscar {path} via API: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Erro de rede ao buscar {path}: {str(e)}")
    except json.JSONDecodeError as e:
        print(f"Erro ao decodificar JSON de {path}: {str(e)}")
    except Exception as e:
        print(f"Erro inesperado ao buscar {path}: {str(e)}")

    return None


def get_vagas_from_github() -> List[dict]:
    """Tenta buscar vagas via API (com token) e depois pela URL RAW"""
    print(f"Buscando vagas no GitHub... BRANCH={BRANCH}")
    
    # Primeiro tenta via API com token
    api_data = fetch_content_from_github("vagas.json")
    normalized = normalize_vagas_data(api_data) if api_data else []
    if normalized:
        print(f"Vagas encontradas via API: {len(normalized)}")
        return normalized

    # Se falhar, tenta via URL pública
    raw_url = f"https://raw.githubusercontent.com/{GITHUB_OWNER}/{GITHUB_REPO}/{BRANCH}/vagas.json"
    print(f"Tentando buscar vagas via URL RAW: {raw_url}")
    try:
        response = requests.get(raw_url, timeout=10)
        print(f"Status da resposta RAW: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            normalized = normalize_vagas_data(data)
            print(f"Vagas encontradas via RAW: {len(normalized)}")
            return normalized
        elif response.status_code == 404:
            print(f"Arquivo vagas.json não encontrado")
            # Tentar criar o arquivo
            if create_github_file("vagas.json", json.dumps([
                {"nome": "Auxiliar de Limpeza"},
                {"nome": "Vendedor"},
                {"nome": "Caixa"},
                {"nome": "Estoquista"},
                {"nome": "Repositor"},
                {"nome": "Atendente"},
                {"nome": "Gerente"},
                {"nome": "Supervisor"},
                {"nome": "Operador de Caixa"}
            ], indent=2, ensure_ascii=False), "Criar arquivo de vagas"):
                # Tentar novamente após criar
                response = requests.get(raw_url, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    return normalize_vagas_data(data)
        else:
            print(f"Erro ao buscar vagas: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Erro de rede ao buscar vagas: {str(e)}")
    except json.JSONDecodeError as e:
        print(f"Erro ao decodificar JSON de vagas: {str(e)}")
    except Exception as e:
        print(f"Erro inesperado ao buscar vagas: {str(e)}")

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
        elif current_response.status_code == 404:
            print("Arquivo candidatos.json não existe, será criado")
    except Exception as e:
        print(f"Erro ao buscar SHA: {str(e)}")
    
    # Atualiza arquivo no GitHub
    data = {
        "message": f"Candidatura: {candidate['nome']} para {candidate['vaga']}",
        "content": content_b64,
        "branch": BRANCH
    }
    
    if sha:
        data["sha"] = sha
    
    try:
        response = requests.put(url, headers=headers, json=data, timeout=30)
        print(f"Status ao salvar candidato: {response.status_code}")
        
        if response.status_code in [200, 201]:
            return {"success": True, "data": response.json()}
        else:
            print(f"Erro ao salvar candidato: {response.status_code} - {response.text}")
            return {"success": False, "reason": "github_error", "details": response.text}
    except Exception as e:
        print(f"Exceção ao salvar candidato: {str(e)}")
        return {"success": False, "reason": "exception", "details": str(e)}

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
    file_path = f"curriculos/{filename}"
    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{file_path}"
    
    data = {
        "message": f"Currículo: {candidate_name} - {vaga}",
        "content": content_b64,
        "branch": BRANCH
    }
    
    try:
        response = requests.put(url, headers=headers, json=data, timeout=30)
        print(f"Status ao salvar currículo: {response.status_code}")
        
        if response.status_code in [200, 201]:
            raw_url = f"https://raw.githubusercontent.com/{GITHUB_OWNER}/{GITHUB_REPO}/{BRANCH}/{file_path}"
            print(f"Currículo salvo com sucesso: {raw_url}")
            return raw_url
        elif response.status_code == 404:
            # Pasta não existe, tentar criar
            print("Pasta curriculos não existe, criando...")
            if create_github_file("curriculos/README.md", 
                                  "# Pasta de Currículos\n\nEsta pasta armazena os currículos enviados pelos candidatos.",
                                  "Criar pasta curriculos"):
                # Tentar novamente após criar a pasta
                response = requests.put(url, headers=headers, json=data, timeout=30)
                if response.status_code in [200, 201]:
                    raw_url = f"https://raw.githubusercontent.com/{GITHUB_OWNER}/{GITHUB_REPO}/{BRANCH}/{file_path}"
                    print(f"Currículo salvo após criar pasta: {raw_url}")
                    return raw_url
                else:
                    raise Exception(f"Erro ao salvar currículo após criar pasta: {response.status_code}")
            else:
                raise Exception("Não foi possível criar a pasta curriculos")
        else:
            raise Exception(f"Erro ao salvar currículo: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Exceção ao salvar currículo: {str(e)}")
        raise Exception(f"Erro ao salvar currículo: {str(e)}")

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
    repo_accessible = check_repo_access()
    return {
        "ok": True, 
        "timestamp": datetime.now().isoformat(), 
        "service": "candidaturas-api",
        "github_repo_accessible": repo_accessible,
        "branch": BRANCH
    }

@app.get("/wakeup")
async def wakeup():
    return {"ok": True, "message": "Servidor ativo", "timestamp": datetime.now().isoformat()}

@app.get("/status")
async def status():
    return {"status": "online", "timestamp": datetime.now().isoformat(), "branch": BRANCH}

@app.get("/api/vagas")
async def get_vagas():
    """Retorna vagas do arquivo JSON no GitHub"""
    try:
        # Verifica cache primeiro
        if "vagas_data" in vagas_cache:
            return vagas_cache["vagas_data"]
        
        # Busca vagas do GitHub
        vagas_data = get_vagas_from_github()
        
        # Se não encontrar vagas no GitHub, retorna lista padrão
        if not vagas_data:
            print("Nenhuma vaga encontrada no GitHub, retornando lista padrão")
            vagas_data = [
                {"nome": "Auxiliar de Limpeza"},
                {"nome": "Vendedor"},
                {"nome": "Caixa"},
                {"nome": "Estoquista"},
                {"nome": "Repositor"},
                {"nome": "Atendente"},
                {"nome": "Gerente"},
                {"nome": "Supervisor"},
                {"nome": "Operador de Caixa"}
            ]
        
        # Filtra apenas os objetos que têm o campo 'nome'
        vagas_filtradas = []
        for vaga in vagas_data:
            if isinstance(vaga, dict) and "nome" in vaga:
                vagas_filtradas.append({"nome": vaga["nome"]})
        
        # Armazena no cache
        vagas_cache["vagas_data"] = vagas_filtradas
        
        return vagas_filtradas
        
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
    
    # Verificar token GitHub
    if not GITHUB_TOKEN:
        raise HTTPException(
            status_code=500,
            detail="❌ Token do GitHub não configurado. Configure a variável de ambiente GITHUB_TOKEN."
        )
    
    # Verificar acesso ao repositório
    if not check_repo_access():
        raise HTTPException(
            status_code=500,
            detail="❌ Não é possível acessar o repositório do GitHub. Verifique as permissões do token."
        )
    
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
        raise HTTPException(status_code=500, detail=f"❌ Erro interno do servidor: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
