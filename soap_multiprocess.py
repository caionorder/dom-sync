import time
import sys
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from tqdm import tqdm
import logging
from config.networks import get_enabled_networks
from core.multiprocess.config import get_args
from core.multiprocess.logging_config import setup_logging
from core.multiprocess.progress import update_progress, print_final_report
from utils.network_job_manager import NetworkJobManager
from utils.network_rate_limiter import NetworkRateLimiter
from core.multiprocess.worker import NetworkWorker

logger = logging.getLogger(__name__)


def main():
    args = get_args()

    # Configurar logging
    setup_logging(args.debug)

    # Inicializar rate limiter unico por network
    network_rate_limiter = NetworkRateLimiter(requests_per_second=30)

    print(f"Iniciando processamento de relatorios DOM")
    print(f"Configuracao: workers={args.workers}")
    print(f"Rate limit: 2 requisicoes por segundo POR NETWORK")

    if args.type and args.day:
        print(f"Filtro: type={args.type}, day={args.day}")
    if args.network:
        print(f"Network especifica: {args.network}")
    if args.run:
        print("Modo: EXECUCAO de relatorios")
    else:
        print("Modo: LISTAGEM de relatorios")

    start_time = time.time()

    try:
        print("Obtendo lista de networks...")
        networks = get_enabled_networks()

        if not networks:
            print("Nenhuma network encontrada")
            sys.exit(0)

        # Filtrar por network especifica se informado
        if args.network:
            networks = [n for n in networks if n['network_code'] == args.network]
            if not networks:
                print(f"Network {args.network} nao encontrada")
                sys.exit(1)
            print(f"Filtrado para network: {args.network}")

        if args.limit:
            networks = networks[:args.limit]
            print(f"Limitando processamento a {args.limit} networks")

        print(f"Total de networks para processar: {len(networks)}")

        # Se nao esta em modo run, apenas lista as networks
        if not args.run:
            print("\n--- Networks disponiveis ---")
            for n in networks:
                print(f"  [{n['network_code']}] {n['name']}")
            print(f"\nTotal: {len(networks)} networks")
            print("Use --run para executar o processamento")
            sys.exit(0)

        # Criar jobs
        jobs = []
        for i, network in enumerate(networks):
            jobs.append((i, network, args))

        print(f"Total de jobs criados: {len(jobs)}")

        # Estrutura thread-safe para estatisticas
        stats = {
            'success': 0,
            'error': 0,
            'auth_error': 0,
            'rate_limit': 0,
            'total': 0,
            'workers': args.workers
        }
        stats_lock = threading.Lock()

        # Callback para atualizar progresso
        def progress_callback(result):
            update_progress(pbar, stats, stats_lock, result)

        # Criar gerenciador de jobs
        job_manager = NetworkJobManager(max_concurrent_networks=args.workers)
        job_manager.add_jobs(jobs)

        print(f"Networks unicas: {len(job_manager.network_jobs)}")
        print(f"Iniciando processamento com {args.workers} workers...")
        print("-----  Dashboard de progresso -----")
        print(f"[ 🟢: sucesso, 🔴: erro, ♦️: erro de autenticacao, 🔸: rate limit ]")

        # Silencia warnings
        warnings.filterwarnings('ignore')

        # Criar worker com rate limiter centralizado
        worker = NetworkWorker(network_rate_limiter)

        def worker_thread(worker_id):
            """Thread worker que processa networks"""
            thread_logger = logging.getLogger(f"worker_{worker_id}")

            while True:
                # Pega proxima network disponivel
                network_code, jobs = job_manager.get_next_network()

                if network_code is None:
                    # Se nao ha mais networks e tudo esta completo, termina
                    if job_manager.is_complete():
                        break
                    time.sleep(0.1)
                    continue

                try:
                    thread_logger.info(
                        f"Worker {worker_id} processando network {network_code} "
                        f"com {len(jobs)} jobs"
                    )

                    # Processa todos os jobs desta network sequencialmente
                    for job in jobs:
                        result = worker.process_network(job)
                        progress_callback(result)

                except Exception as e:
                    thread_logger.error(f"Erro ao processar network {network_code}: {e}")

                finally:
                    # Marca network como completa
                    job_manager.mark_completed(network_code)

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            with tqdm(total=len(jobs), desc="Processando", ncols=100, leave=True) as pbar:

                # Iniciar workers
                futures = []
                for worker_id in range(args.workers):
                    future = executor.submit(worker_thread, worker_id)
                    futures.append(future)

                # Esperar todos os workers terminarem
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Erro em worker: {e}")

        # Relatorio final
        elapsed_time = time.time() - start_time
        print_final_report(stats, elapsed_time)

        # Mostrar eficiencia
        if stats['total'] > 0:
            actual_requests_per_second = stats['total'] / elapsed_time
            print(f"Taxa real: {actual_requests_per_second:.2f} requisicoes/segundo")

    except KeyboardInterrupt:
        print("\n\nProcessamento interrompido pelo usuario")
        sys.exit(1)
    except Exception as e:
        print(f"Erro fatal: {e}")
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
