#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
py-ndjson-proxy - Middleware HTTP que converte streaming progressivo para NDJSON via polling no Redis

Autor: Patrick Brandao <patrickbrandao@gmail.com>
"""

import os
import sys
import argparse
import json
import time
import uuid
from flask import Flask, request, Response, stream_with_context
import redis


# ==============================================================================
# VARIÁVEIS GLOBAIS DE CONFIGURAÇÃO
# ==============================================================================

DEBUG = False
HTTP_PORT = 8771
REDIS_SERVER = "127.0.0.1:6379/1"
REDIS_PASSWORD = ""
REDIS_CHANNEL = "ndjson_jobs"
REDIS_KEY_PREFIX = "ndjson_task"
REDIS_LIST_PREFIX = "ndjson_list"
REDIS_TTL = 600
INTERVAL = 200  # milissegundos
PAUSE = 20  # milissegundos
MAXTIME = 45000  # milissegundos
MAXTIME_ERROR = ""

# Cliente Redis global
redis_client = None


# ==============================================================================
# FUNÇÕES AUXILIARES
# ==============================================================================

def parse_arguments():
    """
    Processa argumentos da linha de comando e variáveis de ambiente.
    
    Ordem de precedência: valor padrão < variável de ambiente < argumento CLI
    
    Retorno:
        None (atualiza variáveis globais)
    """
    global HTTP_PORT, REDIS_SERVER, REDIS_PASSWORD, REDIS_CHANNEL
    global REDIS_KEY_PREFIX, REDIS_LIST_PREFIX, REDIS_TTL
    global INTERVAL, PAUSE, MAXTIME, DEBUG
    global MAXTIME_ERROR
    
    parser = argparse.ArgumentParser(
        description='py-ndjson-proxy - Middleware HTTP para streaming NDJSON via Redis'
    )
    
    # Argumentos com valores padrão de variáveis de ambiente
    parser.add_argument(
        '-p', '--port',
        type=int,
        default=int(os.getenv('HTTP_PORT', HTTP_PORT)),
        help=f'Porta HTTP (padrão: {HTTP_PORT})'
    )
    
    parser.add_argument(
        '-R', '--redis',
        default=os.getenv('REDIS_SERVER', REDIS_SERVER),
        help=f'Servidor Redis ip:porta/database (padrão: {REDIS_SERVER})'
    )
    
    parser.add_argument(
        '-S', '--secret',
        default=os.getenv('REDIS_PASSWORD', REDIS_PASSWORD),
        help='Senha do Redis'
    )
    
    parser.add_argument(
        '-C', '--channel',
        default=os.getenv('REDIS_CHANNEL', REDIS_CHANNEL),
        help=f'Canal Redis para tarefas (padrão: {REDIS_CHANNEL})'
    )
    
    parser.add_argument(
        '-X', '--key-prefix',
        default=os.getenv('REDIS_KEY_PREFIX', REDIS_KEY_PREFIX),
        help=f'Prefixo da chave de tarefa (padrão: {REDIS_KEY_PREFIX})'
    )
    
    parser.add_argument(
        '-L', '--list-prefix',
        default=os.getenv('REDIS_LIST_PREFIX', REDIS_LIST_PREFIX),
        help=f'Prefixo da chave de lista (padrão: {REDIS_LIST_PREFIX})'
    )

    parser.add_argument(
        '-t', '--ttl',
        type=int,
        default=int(os.getenv('REDIS_TTL', REDIS_TTL)),
        help=f'TTL das chaves no REDIS (padrão: {REDIS_TTL})'
    )

    parser.add_argument(
        '-i', '--interval',
        type=int,
        default=int(os.getenv('INTERVAL', INTERVAL)),
        help=f'Intervalo entre verificações Redis em ms (padrão: {INTERVAL})'
    )
    
    parser.add_argument(
        '-P', '--pause',
        type=int,
        default=int(os.getenv('PAUSE', PAUSE)),
        help=f'Pausa após enviar linha JSON em ms (padrão: {PAUSE})'
    )
    
    parser.add_argument(
        '-m', '--maxtime',
        type=int,
        default=int(os.getenv('MAXTIME', MAXTIME)),
        help=f'Tempo máximo da tarefa em ms (padrão: {MAXTIME})'
    )

    parser.add_argument(
        '-E', '--maxtime-error',
        default=os.getenv('MAXTIME_ERROR', MAXTIME_ERROR),
        help=f'Mensagem de erro timeout (padrão: {MAXTIME_ERROR})'
    )

    parser.add_argument(
        '-d', '--debug',
        action='store_true',
        default=os.getenv('DEBUG', '').lower() in ('true', '1', 'yes'),
        help='Ativa modo debug'
    )

    args = parser.parse_args()
    
    # Atualizar variáveis globais
    HTTP_PORT = args.port
    REDIS_SERVER = args.redis
    REDIS_PASSWORD = args.secret
    REDIS_CHANNEL = args.channel
    REDIS_KEY_PREFIX = args.key_prefix
    REDIS_LIST_PREFIX = args.list_prefix
    REDIS_TTL = args.ttl
    INTERVAL = args.interval
    PAUSE = args.pause
    MAXTIME = args.maxtime
    MAXTIME_ERROR = args.maxtime_error
    DEBUG = args.debug


def connect_redis():
    """
    Estabelece conexão com o servidor Redis.
    
    Retorno:
        redis.Redis: Cliente Redis conectado
        
    Exceções:
        SystemExit: Encerra programa se conexão falhar
    """
    global redis_client
    
    try:
        # Parse da string de conexão Redis
        parts = REDIS_SERVER.split('/')
        host_port = parts[0].split(':')
        host = host_port[0]
        port = int(host_port[1]) if len(host_port) > 1 else 6379
        db = int(parts[1]) if len(parts) > 1 else 0
        
        # Criar cliente Redis
        if REDIS_PASSWORD:
            redis_client = redis.Redis(
                host=host,
                port=port,
                db=db,
                password=REDIS_PASSWORD,
                decode_responses=True,
                socket_connect_timeout=5
            )
        else:
            redis_client = redis.Redis(
                host=host,
                port=port,
                db=db,
                decode_responses=True,
                socket_connect_timeout=5
            )
        
        # Testar conexão
        redis_client.ping()
        print(f"[OK] Conectado ao Redis: {host}:{port}/{db}")
        return redis_client
        
    except Exception as e:
        print(f"[ERRO] Falha ao conectar no Redis: {e}", file=sys.stderr)
        sys.exit(1)


def redis_ping():
    """
    Testa a conexão com o Redis usando comando PING.
    
    Retorno:
        bool: True se conexão OK, False se falhou
    """
    try:
        redis_client.ping()
        return True
    except Exception as e:
        print(f"[ERRO] Redis PING falhou: {e}", file=sys.stderr)
        return False


# ==============================================================================
# FUNÇÕES DE STREAMING
# ==============================================================================

def create_task(headers_dict, body_data):
    """
    Cria uma nova tarefa no Redis e publica notificação.
    
    Argumentos:
        headers_dict (dict): Dicionário com cabeçalhos HTTP da requisição
        body_data (str): Corpo da requisição HTTP
        
    Retorno:
        tuple: (task_uuid, task_key, task_list) - UUID da tarefa e chaves Redis
    """
    # Gerar UUID da tarefa
    task_uuid = str(uuid.uuid4())
    
    # Criar chaves Redis
    task_key  = f"{REDIS_KEY_PREFIX}_{task_uuid}"
    task_list = f"{REDIS_LIST_PREFIX}_{task_uuid}"

    # Criar objeto JSON da tarefa
    task_data = {
        "uuid": task_uuid,
        "task_key": task_key,
        "task_list": task_list,
        "request_time": time.time(),
        "headers": headers_dict,
        "body": body_data
    }
    
    # Armazenar tarefa no Redis (chave SET/STRING)
    redis_client.set(task_key, json.dumps(task_data), ex=REDIS_TTL)

    # Publicar notificação no canal
    redis_client.publish(REDIS_CHANNEL, task_key)
    
    print(f"[INFO] Tarefa criada...: {task_uuid}")
    print(f"[INFO] Chave da tarefa.: {task_key}")
    print(f"[INFO] Lista da tarefa.: {task_list}")
    print(f"[INFO] Cabecalhos http.: {headers_dict}")
    print(f"[INFO] Payload recebido: {body_data}")
    
    return task_uuid, task_key, task_list


def stream_generator(task_uuid, task_key, task_list):
    """
    Gerador para streaming NDJSON consumindo lista Redis via LPOP.
    
    Argumentos:
        task_uuid (str): UUID da tarefa
        task_key (str): Chave Redis da tarefa (SET/STRING)
        task_list (str): Chave Redis da lista de streaming (LIST)
        
    Yield:
        str: Linhas NDJSON com resultados parciais
    """
    start_time = time.time()
    last_message_time = start_time
    task_completed = False
    slow_interval = INTERVAL / 1000.0
    fast_interval = PAUSE / 1000.0
    step_interval = slow_interval

    print(f"[INFO] Iniciando tarefa")
    print(f"[INFO] - Nova chave: {task_key}")

    try:
        while True:

            # Verificar se a chave da tarefa ainda existe
            if not redis_client.exists(task_key):
                if DEBUG:
                    print(f"[DEBUG] Chave ausente: {task_key}, descarregar lista e finalizar")
                task_completed = True
            #endif

            # Contar mensagens no buffer
            list_length = redis_client.llen(task_list)
            if DEBUG:
                print(f"[DEBUG] Itens na lista: {list_length}")
            #endif

            # Fim, sem chave, sem lista
            if task_completed and not list_length:
                if DEBUG:
                    print(f"[DEBUG] Chave ausente e lista vazia, finalizar streaming")
                #endif
                # Sair do loop principal
                break;
            #endif


            # Lista vazia, pause demorada para espera do worker
            if not list_length:

                # Verificar timeout
                current_time = time.time()
                elapsed_since_last = (current_time - last_message_time) * 1000
                if elapsed_since_last > MAXTIME:
                    if DEBUG:
                        print(f"[DEBUG] Timeout atingido: {task_key}, elapsed {elapsed_since_last}, maxtime {MAXTIME}")
                    #endif
                    if MAXTIME_ERROR:
                        yield MAXTIME_ERROR + '\n'
                    #endif
                    break
                #endif

                if DEBUG:
                    print(f"[DEBUG] Lista vazia, aguardar worker {slow_interval}s, elapsed {elapsed_since_last}")
                #endif

                # esperar...
                time.sleep(slow_interval)
                continue;
            #endif


            # Faz um loop para dar LPOP até esvaziar
            for item_id in range(list_length):
                # Coletar mensagem
                raw_item = redis_client.lpop(task_list)
                if raw_item is None:
                    if DEBUG:
                        print(f"[DEBUG] Item de entrada vazio (2), ignorando")
                    #endif
                    continue;
                #endif

                # Retirar espacos
                message = raw_item.strip()

                # Sem mensagem, sumiu misteriosamente...
                if not message:
                    if DEBUG:
                        print(f"[DEBUG] Item de entrada vazio (2), ignorando")
                    #endif
                    continue
                #endif

                # COMANDO
                if message in ("END-OF-FILE", "END-OF-LIST", "EOL", "EOF"):
                    # Tarefa concluida a pedido do worker
                    task_completed = True
                    if DEBUG:
                        print(f"[DEBUG] Worker sinalizou conclusao ({message}), encerrando")
                    #endif
                    break
                #endif

                # JSON de saida ao cliente:
                last_message_time = time.time()
                json_line = message.replace("\n", "")
                if DEBUG:
                    print(f"[DEBUG] Worker enviou: {json_line}")
                #endif
                yield json_line + '\n'

                # Pause entre envio de mensagens
                time.sleep(fast_interval)
            #done
        #endwhile
    #endtry

    finally:
        # Remover chaves Redis ao final
        print(f"[INFO] Finalizando tarefa")
        print(f"[INFO] - Removendo chave: {task_key}")
        redis_client.delete(task_key)
        print(f"[INFO] - Removendo chave: {task_list}")
        redis_client.delete(task_list)
        print(f"[INFO] Tarefa finalizada")
    #endfinally
#enddef


# ==============================================================================
# ROTAS FLASK
# ==============================================================================

app = Flask(__name__)


@app.route('/ping', methods=['GET'])
def ping():
    """
    Endpoint de health check que testa conexão com Redis.
    
    Retorno:
        Response: HTTP 200 com "pong" ou HTTP 500 em caso de erro
    """
    if redis_ping():
        return Response("pong\n", status=200, mimetype='text/plain')
    else:
        return Response("Redis connection failed\n", status=500, mimetype='text/plain')


@app.route('/', defaults={'path': ''}, methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH'])
@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH'])
def handle_request(path):
    """
    Handler principal que processa todas as requisições e retorna streaming NDJSON.
    
    Argumentos:
        path (str): Caminho da requisição HTTP
        
    Retorno:
        Response: Streaming NDJSON ou erro HTTP
    """
    # Testar conexão Redis
    if not redis_ping():
        return Response(
            '{"error": "event-driver unavailable"}\n',
            status=504,
            mimetype='application/json'
        )
    
    # Coletar cabeçalhos da requisição
    headers_dict = dict(request.headers)
    
    # Coletar corpo da requisição
    try:
        if request.data:
            body_data = request.data.decode('utf-8')
        else:
            body_data = ""
    except Exception as e:
        body_data = ""
        print(f"[WARN] Erro ao decodificar body: {e}")
    
    # Criar tarefa
    task_uuid, task_key, task_list = create_task(headers_dict, body_data)
    
    # Criar resposta de streaming
    response = Response(
        stream_with_context(stream_generator(task_uuid, task_key, task_list)),
        mimetype='application/x-ndjson',
        headers={
            'X-Author': 'Patrick Brandao <patrickbrandao@gmail.com>',
            'X-Task-UUID': task_uuid,
            'X-Task-Key': task_key,
            'X-Task-List': task_list,
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no'
        }
    )
    
    return response

@app.after_request
def add_server_header(response):
    """
    Adiciona cabeçalhos customizados a todas as respostas.
    
    Argumentos:
        response (Response): Objeto de resposta Flask
        
    Retorno:
        Response: Resposta com cabeçalhos adicionados
    """
    if 'X-Author' not in response.headers:
        response.headers['X-Author'] = 'Patrick Brandao <patrickbrandao@gmail.com>'
    return response


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    """
    Função principal do programa.
    
    Inicializa configurações, conecta ao Redis e inicia servidor Flask.
    """
    print("=" * 70)
    print("py-ndjson-proxy - Middleware HTTP para streaming NDJSON via Redis")
    print("Autor: Patrick Brandao <patrickbrandao@gmail.com>")
    print("=" * 70)
    
    # Processar argumentos
    parse_arguments()
    
    # Conectar ao Redis
    connect_redis()
    
    # Exibir configurações
    print(f"\n[CONFIG] Porta HTTP: {HTTP_PORT}")
    print(f"[CONFIG] Redis: {REDIS_SERVER}")
    print(f"[CONFIG] Canal: {REDIS_CHANNEL}")
    print(f"[CONFIG] Prefixo chave: {REDIS_KEY_PREFIX}")
    print(f"[CONFIG] Prefixo lista: {REDIS_LIST_PREFIX}")
    print(f"[CONFIG] Intervalo: {INTERVAL}ms")
    print(f"[CONFIG] Pausa: {PAUSE}ms")
    print(f"[CONFIG] Timeout: {MAXTIME}ms")
    
    # Iniciar servidor Flask
    print(f"[INFO] Iniciando servidor na porta {HTTP_PORT}...\n")
    app.run(host='0.0.0.0', port=HTTP_PORT, threaded=True)


if __name__ == '__main__':
    main()
