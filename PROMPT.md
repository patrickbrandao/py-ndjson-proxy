
# Middleware HTTP que converte streaming progressivo para NDJSON via polling no Redis.

## Descrição do sistema
Criar um sistema em python que atue como middleware de http entre um cliente http
que suporta NDJSON (application/json-lines, application/x-ndjson) e uma aplicação
que não suporta mas que é capaz de produzir o resultado em partes.

# Nome do programa
O sistema se chama: py-ndjson-proxy.
O arquivo unificado contendo todo o codigo se chama: json-proxy.py

# Padrões de desenvolvimento

Você deverá atuar como desenvolvedor para gerar código em linguagem python versão 3.14.

A codificação de caracteres deve ser utf-8

Os nomes de funções, variáveis e objetos devem utilizar palavras da lingua inglesa (US)

Os comentários em Portugues do Brasil (PT-BR).

Documente em cada função o que ela faz, os argumentos necessários, e se houver retorno detalhe o formato e os detalhes do retorno.

As variáveis que armazenam argumentos e variáveis de ambiente devem ser variáveis globais.

Todas as variaveis que recebem valores importados de variáveis de ambiente
ou argumentos (ARGV) devem ser escritas com letras maiúsculas.

As demais funções, variáveis, objetos criados e objetos importados devem ser escritas em letras minúsculas.


## Autor e Contato

**Patrick Brandao**  
Email: patrickbrandao@gmail.com


## Arquitetura e Funcionamento

### Componentes Principais

1. **json-proxy.py** - Servidor Flask que:
   - Recebe requisições HTTP de clientes
   - Cria tarefas únicas (UUID) no Redis
   - Publica notificação do UUID da tarefa no canal Redis (Pub/Sub)
   - Faz polling (LPOP) em lista Redis para streaming NDJSON
   - Retorna resposta em formato NDJSON ao cliente

2. **worker-pooling-example.py** - Worker que:
   - Assina canal Redis via Pub/Sub
   - Recebe notificações de novas tarefas
   - Processa tarefas
   - Envia resultados parciais via RPUSH para lista Redis
   - Remove tarefa ao concluir (sinaliza fim do streaming)

### Estrutura de Chaves Redis

**IMPORTANTE:** Cada tarefa usa DUAS chaves separadas:

1. **Chave de dados (SET/STRING):**
   ```
   ndjson_task_<uuid>
   ```
   - Armazena metadados da tarefa (JSON com headers, body, timestamp)
   - Criada com comando `SET`
   - Usada para verificar existência da tarefa (`EXISTS`)
   - Removida ao finalizar (`DEL`)

2. **Chave de stream (LIST):**
   ```
   ndjson_list_<uuid>_stream
   ```
   - Armazena mensagens NDJSON para streaming
   - Worker adiciona mensagens: `RPUSH`
   - Proxy consome mensagens: `LPOP`
   - Removida junto com chave de dados ao finalizar

### Fluxo de Dados

```
1. Cliente HTTP → POST /endpoint → json-proxy.py
2. Proxy → Cria task_key (SET) com dados da requisição
3. Proxy → PUBLISH no canal "ndjson_jobs" com task_key
4. Proxy → Inicia polling LPOP em task_list
5. Worker → SUBSCRIBE no canal "ndjson_jobs"
6. Worker → GET task_key (lê dados da tarefa)
7. Worker → RPUSH em task_list (envia resultados)
8. Proxy → LPOP task_list (consome e envia ao cliente)
9. Worker → DEL task_key e task_list (finaliza)
10. Proxy → Detecta que task_key não existe, encerra streaming
```


# Configurações iniciais

## Argumentos e variáveis de ambiente

Lista de variáveis de ambiente que alteram o valor padrão.
Argumentos devem prevalecer sobre variáveis de ambiente.

Ordem de preferência do menor para o maior: valor padrão, variável de ambiente, argumento.

Nome da variável de ambiente, argumento e valor padrão:
   - HTTP_PORT: A porta http (--port ou -p), padrao: 8771;
   - REDIS_SERVER: O acesso do redis (-R ou --redis, ip:porta/database), padrao: "127.0.0.1:6379/1";
   - REDIS_PASSWORD: A senha do redis (-S ou --secret), padrao ausente "";
   - REDIS_CHANNEL: Canal (channel) para envio de tarefas (-C), padrao: "ndjson_jobs";
   - REDIS_KEY_PREFIX: Prefixo da chave da tarefa (-X), padrao "ndjson_task_";
   - REDIS_LIST_PREFIX: Prefixo da chave da tarefa (-L), padrao "ndjson_list_";
   - INTERVAL: Intervalo entre verificacoes de trabalhos no REDIS (-i), padrão 200 ms;
   - PAUSE: intervalo de pause apos enviar uma linha json (-P), padrao 20 ms;
   - MAXTIME: tempo maximo para que a tarefa seja concluida (-t), em milisegundos, padrao 45000 (45 segundos);
   - MAXTIME_ERROR: mensagem de erro a enviar quando a tarefa atingir o tempo limite (-E), padrao: {"error": "timeout"};
   - SERVERNAME: nome de servidor http (-s), padrao: py-ndjson-proxy;

# Recursos do software

## Servidor HTTP

O servidor deve abrir a porta HTTP utilizando biblioteca Flask.
O nome do servidor (cabeçalho Server) será enviado usando a variável SERVERNAME.
Enviar cabeçalho X-Author com o valor "Patrick Brandao <patrickbrandao@gmail.com>"

Argumentos do Flask:
   - Incluir o recurso: threaded = True
   - Utilize a opção "threaded = True", exemplo: app.run(host='0.0.0.0', port=HTTP_PORT, threaded=True)


O servidor deve ser multithread e atender varios clientes paralelamente, cada cliente
submetendo sua tarefa e recebendo seu streaming de linhas JSON.

Crie uma endpoint "/ping" que testa a conexão com o REDIS usando a funcao redis_ping
e retorna HTTP 200 com conteudo "pong" para sucesso, ou erro 500 para falhas.

Todas as demais endpoints devem ser atendidas pela funcao principal do programa (streaming).


## Cliente REDIS

A conexão com o servidor REDIS é critica.
Falha na conexão inicial deve abortar o programa.
Crie uma funcao redis_ping para fazer o teste com o comando PING.
Ao receber um novo cliente HTTP teste a funcao redis_ping e em caso
de falha retorne erro HTTP 504 (Gateway Timeout) ao cliente.


## Requisição enviada pelo cliente HTTP

O cliente HTTP enviará um payload JSON.

Variável task_name: para cada requisição o nome da tarefa será gerado usando UUIDv4. 

Compor um objeto JSON para armazenar os detalhes da tarefa.
Propriedades:
   - task_name: nome da tarefa (UUIDv4)
   - request_time: timestamp de recebimento da requisicao;
   - headers: objeto contendo os cabecalhos enviados pelo cliente;
   - body: o payload enviado no corpo da requisicao, ou string vazia se omitido

Variável task_key: define o nome da chave  para armazenar o JSON da tarefa no REDIS.
O nome da chave em task_key será o prefixo definido em REDIS_KEY_PREFIX e o UUIDv4 da tarefa.

O JSON da tarefa será transformado em string e armazenado no REDIS em uma chave SET (armazenamento da tarefa principal).

Ao gravar a chave task_key com sucesso, enviar o valor de task_key no canal REDIS definido em REDIS_CHANNEL.


## Streaming

Variável task_list: define o nome da chave para armazenar a lista de linhas JSON com os resultados
da tarefa.
O nome da chave em task_list será o prefixo definido em REDIS_LIST_PREFIX e o UUIDv4 da tarefa.

Para realizar o streaming dos resultados parciais da tarefa ao cliente a funcao de streaming
deve monitorar a chave tipo lista usando o comando LPOP no valor definido em task_list.

Se o comando LPOP não retornar valor o loop deve fazer uma pause em milisegundos
pelo valor definido em INTERVAL.

Se o comando LPOP retornar valor, enviar o conteudo ao cliente a nova linha JSON e fazer
uma pause em milisegundos pelo valor definido em PAUSE antes de voltar a consultar a lista.

A cada consulta LPOP no loop de streaming, consulte se a chave SET da tarefa (task_key) existe.
Se a chave deixar de existir encerre o atendimento ao cliente.

Se o tempo total de espera desde a ultima mensagem extraida via LPOP for superior ao valor
definido em MAXTIME a tarefa deve ser encerrada enviando ao cliente o conteudo da variavel
MAXTIME_ERROR.


# Scripts de teste

- client-curl.sh: Criar script de teste usando curl;
- worker-pooling-example.py: exemplo de worker de exemplo, entrar no Pub/Sub, coletar tarefa, enviar 5 linhas json de exemplo com intervalos entre 1 e 3 segundos e encerrar apagando a chave SET e a chave LIST. Suportar modo loop infinito para testar servidor continuamente;

# Arquivos finais

Produzir:
- Dockerfile baseado em Alpine Linux, iniciar usando Gunicorn;
- Scripts para usar Gunicorn em produção;
- Script de teste com curl;
- requirements.txt com requisitos (bibliotecas);
- json-proxy.py: programa principal;
- worker-pooling-example.py: worker de exemplo;
- docker-compose.yml: stack para subir ambiente com REDIS (redis-server) e PROXY (json-proxy)
- README.md: manual do projeto

