#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
worker-pooling-example.py - Worker de exemplo para py-ndjson-proxy

Este worker assina o canal Redis, recebe tarefas e envia resultados parciais.

Autor: Patrick Brandao <patrickbrandao@gmail.com>
"""

import os
import sys
import argparse
import json
import time
import random
import redis


# ==============================================================================
# VARIÁVEIS GLOBAIS DE CONFIGURAÇÃO
# ==============================================================================

REDIS_SERVER = "127.0.0.1:6379/1"
REDIS_PASSWORD = ""
REDIS_CHANNEL = "ndjson_jobs"
REDIS_KEY_PREFIX = "ndjson_task"
REDIS_LIST_PREFIX = "ndjson_list"
LOOP_MODE = False

# Cliente Redis global
redis_client = None
pubsub = None


# ==============================================================================
# FUNÇÕES AUXILIARES
# ==============================================================================

def parse_arguments():
    """
    Processa argumentos da linha de comando e variáveis de ambiente.
    
    Retorno:
        None (atualiza variáveis globais)
    """
    global REDIS_SERVER, REDIS_PASSWORD, REDIS_CHANNEL
    global REDIS_KEY_PREFIX, REDIS_LIST_PREFIX, LOOP_MODE
    
    parser = argparse.ArgumentParser(
        description='Worker de exemplo para py-ndjson-proxy'
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
        '--loop',
        action='store_true',
        help='Modo loop infinito para teste contínuo'
    )
    
    args = parser.parse_args()
    
    # Atualizar variáveis globais
    REDIS_SERVER = args.redis
    REDIS_PASSWORD = args.secret
    REDIS_CHANNEL = args.channel
    REDIS_KEY_PREFIX = args.key_prefix
    REDIS_LIST_PREFIX = args.list_prefix
    LOOP_MODE = args.loop


def connect_redis():
    """
    Estabelece conexão com o servidor Redis.
    
    Retorno:
        redis.Redis: Cliente Redis conectado
        
    Exceções:
        SystemExit: Encerra programa se conexão falhar
    """
    global redis_client, pubsub
    
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
        
        # Criar Pub/Sub
        pubsub = redis_client.pubsub()
        
        return redis_client
        
    except Exception as e:
        print(f"[ERRO] Falha ao conectar no Redis: {e}", file=sys.stderr)
        sys.exit(1)


# ==============================================================================
# FUNÇÕES DE PROCESSAMENTO
# ==============================================================================

def process_task(task_key):
    """
    Processa uma tarefa recebida do canal Redis.
    
    Argumentos:
        task_key (str): Chave Redis da tarefa (SET/STRING)
        
    Retorno:
        None
    """
    try:
        # Verificar se tarefa existe
        if not redis_client.exists(task_key):
            print(f"[WARN] Tarefa não encontrada: {task_key}")
            return
        
        # Ler dados da tarefa
        task_data_str = redis_client.get(task_key)
        task_data = json.loads(task_data_str)
        
        task_name = task_data.get('task_name', 'unknown')
        print(f"\n[INFO] Processando tarefa: {task_name}")
        print(f"[INFO] Headers: {len(task_data.get('headers', {}))} cabeçalhos")
        print(f"[INFO] Body: {len(task_data.get('body', ''))} bytes")
        
        # Extrair task_name do task_key para formar task_list
        # task_key = "ndjson_task_UUID"
        # task_list = "ndjson_list_UUID_stream"
        task_uuid = task_key.replace(REDIS_KEY_PREFIX, '')
        task_list = f"{REDIS_LIST_PREFIX}{task_uuid}"
        
        # Enviar 5 mensagens NDJSON de exemplo
        for i in range(1, 6):
            message = {
                "message_id": i,
                "task": task_name,
                "timestamp": time.time(),
                "data": f"Exemplo de mensagem {i} de 5",
                "random": random.randint(100, 999)
            }
            
            # Serializar para JSON
            message_json = json.dumps(message)
            
            # Enviar para lista Redis (RPUSH)
            redis_client.rpush(task_list, message_json)
            
            print(f"[WORKER] Enviada mensagem {i}/5")
            
            # Aguardar intervalo aleatório entre 1 e 3 segundos
            delay = random.uniform(1.0, 3.0)
            time.sleep(delay)
        
        # Enviar mensagem final
        final_message = {
            "status": "completed",
            "task": task_name,
            "total_messages": 5,
            "timestamp": time.time()
        }
        redis_client.rpush(task_list, json.dumps(final_message))
        print(f"[WORKER] Tarefa concluída")
        
        # Aguardar um pouco antes de limpar
        time.sleep(0.5)
        
        # Limpar chaves Redis (finalizar tarefa)
        redis_client.delete(task_key)
        redis_client.delete(task_list)
        
        print(f"[INFO] Chaves removidas: {task_key}, {task_list}\n")
        
    except Exception as e:
        print(f"[ERRO] Falha ao processar tarefa {task_key}: {e}", file=sys.stderr)


def subscribe_and_listen():
    """
    Assina o canal Redis e processa tarefas recebidas.
    
    Retorno:
        None
    """
    print(f"[INFO] Assinando canal: {REDIS_CHANNEL}")
    pubsub.subscribe(REDIS_CHANNEL)
    
    print("[INFO] Aguardando tarefas...\n")
    
    try:
        for message in pubsub.listen():
            if message['type'] == 'message':
                task_key = message['data']
                print(f"[INFO] Nova tarefa recebida: {task_key}")
                
                # Processar tarefa
                process_task(task_key)
                
                # Se não estiver em modo loop, encerrar após primeira tarefa
                if not LOOP_MODE:
                    print("[INFO] Modo single-task, encerrando...")
                    break
                
    except KeyboardInterrupt:
        print("\n[INFO] Interrompido pelo usuário")
    finally:
        pubsub.unsubscribe()
        print("[INFO] Desconectado do canal")


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    """
    Função principal do worker.
    
    Conecta ao Redis, assina canal e processa tarefas.
    """
    print("=" * 70)
    print("Worker de exemplo - py-ndjson-proxy")
    print("Autor: Patrick Brandao <patrickbrandao@gmail.com>")
    print("=" * 70)
    
    # Processar argumentos
    parse_arguments()
    
    # Conectar ao Redis
    connect_redis()
    
    # Exibir configurações
    print(f"\n[CONFIG] Redis: {REDIS_SERVER}")
    print(f"[CONFIG] Canal: {REDIS_CHANNEL}")
    print(f"[CONFIG] Prefixo chave: {REDIS_KEY_PREFIX}")
    print(f"[CONFIG] Prefixo lista: {REDIS_LIST_PREFIX}")
    print(f"[CONFIG] Modo loop: {'Sim' if LOOP_MODE else 'Não'}\n")
    
    # Iniciar escuta
    subscribe_and_listen()
    
    print("\n[INFO] Worker finalizado")


if __name__ == '__main__':
    main()
