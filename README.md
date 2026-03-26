Passo a Passo para rodar o projeto
Faça o clone desse repositório para sua máquina
inicie o Docker Desktop na su máquina
abra o terminal na raiz do projeto e use o comando: "docker-compose up -d"
abra outro terminal e digite o comando "uvicorn api.main:app -reload"
abra um terceiro terminal e digite "dagster dev -m orchestration.definition"
cole http://127.0.0.1:3000/ no seu navegador para acessar o dagster
clique em linege e depois no botão materialize, escolha uma data para a extração
você pode acessar os bancos fonte e alvo pelo dbeaver para ver o resultado da extração
Banco Fonte:
Host: localhost
Porta: 5433
Banco de dados: db_fonte
usuário: user
senha: password
Banco Alvo:
Host: localhost
Porta: 5434
Banco de dados: db_alvo
usuário: user
senha: password