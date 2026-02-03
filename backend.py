// ====== ENVIO DO FORMULÁRIO ======
form.addEventListener("submit", async (event) => {
    event.preventDefault();
    hideMessages();

    // Validar arquivo
    if (!validateFile()) {
        updateSteps();
        return;
    }

    setLoading(true);

    // Criar FormData com arrays para experiência
    const formData = new FormData();
    formData.append("nome", nome.value.trim());
    formData.append("cpf", cpf.value);
    formData.append("telefone", telefone.value);
    formData.append("email", email.value.trim().toLowerCase());
    formData.append("cep", cep.value);
    formData.append("cidade", cidade.value);
    formData.append("bairro", bairro.value);
    formData.append("rua", rua.value);
    formData.append("transporte", transporte.value);
    formData.append("vaga", vaga.value);
    formData.append("arquivo", arquivo.files[0]);

    const selectedExperienceValue = getSelectedExperienceOption()?.value || "";
    formData.append("tem_experiencia", selectedExperienceValue);

    if (selectedExperienceValue === "Sim" && experiencesList) {
        const experienceCards = experiencesList.querySelectorAll(".experience-card");
        
        // Criar arrays para cada campo de experiência
        const empresas = [];
        const cargos = [];
        const admissoes = [];
        const demissoes = [];
        
        experienceCards.forEach((card) => {
            const companyInput = card.querySelector('input[name="experiencia_empresa[]"]');
            const roleInput = card.querySelector('input[name="experiencia_cargo[]"]');
            const admissaoInput = card.querySelector('input[name="experiencia_admissao[]"]');
            const demissaoInput = card.querySelector('input[name="experiencia_demissao[]"]');

            if (companyInput) empresas.push(companyInput.value.trim());
            if (roleInput) cargos.push(roleInput.value.trim());
            if (admissaoInput) admissoes.push(admissaoInput.value);
            if (demissaoInput) demissoes.push(demissaoInput.value);
        });
        
        // Adicionar cada valor como item separado no FormData
        empresas.forEach(empresa => {
            formData.append("experiencia_empresa", empresa);
        });
        
        cargos.forEach(cargo => {
            formData.append("experiencia_cargo", cargo);
        });
        
        admissoes.forEach(admissao => {
            formData.append("experiencia_admissao", admissao);
        });
        
        demissoes.forEach(demissao => {
            formData.append("experiencia_demissao", demissao);
        });
        
        console.log("DEBUG - Enviando experiências:", {
            empresas, cargos, admissoes, demissoes
        });
    }

    // Enviar dados
    let response, data = {};
    try {
        response = await fetchWithTimeout(
            `${API_BASE}/api/enviar`,
            {
                method: "POST",
                body: formData
            },
            60000
        );

        try {
            data = await response.json();
        } catch (parseError) {
            console.error("Erro ao parsear resposta:", parseError);
        }
    } catch (networkError) {
        setLoading(false);
        showError("❌ Erro de conexão. Verifique sua internet e tente novamente.");
        return;
    }

    setLoading(false);

    if (response.status === 409) {
        showError(data?.detail || "⚠️ Já existe uma candidatura para esta vaga com seu CPF. Aguarde 90 dias para reenviar.");
        return;
    }

    if (!response.ok) {
        const errorText = data?.detail || data?.message || "❌ Erro ao enviar candidatura. Tente novamente.";
        showError(errorText);
        return;
    }

    // Sucesso!
    showSuccess(data?.message || "✅ Candidatura enviada com sucesso! Entraremos em contato em breve.");
    
    // Efeito de confirmação
    submitBtn.style.background = "var(--gradient-accent)";
    setTimeout(() => {
        submitBtn.style.background = "";
    }, 2000);
    
    // Resetar formulário
    form.reset();
    resetAddressFields();
    resetExperiencesSection();
    
    // Resetar máscaras
    cpfMask.update("");
    cepMask.update("");
    telefoneMask.update("");
    
    // Resetar validação de CPF
    cpfValidationIndicator.className = 'cpf-validation-indicator';
    const cpfHelp = document.getElementById('cpf-help');
    cpfHelp.textContent = 'Digite apenas números, a formatação é automática';
    cpfHelp.className = 'help-text';
    
    // Resetar classes de validação
    document.querySelectorAll('.input-valid, .input-invalid').forEach(el => {
        el.classList.remove('input-valid', 'input-invalid');
    });
    
    // Atualizar steps
    updateSteps();
    
    // Recarregar vagas
    setTimeout(loadVagasFromGitHub, 2000);
    
    // Focar no primeiro campo novamente
    setTimeout(() => nome.focus(), 1000);
    
    // Scroll para topo
    window.scrollTo({ top: 0, behavior: 'smooth' });
});
