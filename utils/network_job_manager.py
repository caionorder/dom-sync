import time
import threading
from queue import Queue


class NetworkJobManager:
    """Gerenciador de jobs que garante apenas 1 processamento por network"""

    def __init__(self, max_concurrent_networks=30):
        self.max_concurrent = max_concurrent_networks
        self.active_networks = set()
        self.processing_lock = threading.Lock()
        self.job_queue = Queue()
        self.network_jobs = {}  # network_code -> list of jobs
        self.completed_networks = set()

    def add_jobs(self, jobs):
        """Organiza jobs por network"""
        for job in jobs:
            job_id, network, params = job
            network_code = network['network_code']

            if network_code not in self.network_jobs:
                self.network_jobs[network_code] = []
                # Adiciona network na fila apenas uma vez
                self.job_queue.put(network_code)

            self.network_jobs[network_code].append(job)

    def get_next_network(self):
        """Retorna proxima network disponivel para processamento"""
        while True:
            with self.processing_lock:
                # Se ja tem o maximo de networks ativas, espera
                if len(self.active_networks) >= self.max_concurrent:
                    time.sleep(0.1)
                    continue

                try:
                    # Pega proxima network da fila
                    network_code = self.job_queue.get_nowait()

                    # Se ja esta processando ou completa, ignora
                    if network_code in self.active_networks or network_code in self.completed_networks:
                        self.job_queue.task_done()
                        continue

                    # Marca como ativa e retorna jobs
                    self.active_networks.add(network_code)
                    return network_code, self.network_jobs[network_code]

                except Exception:
                    # Fila vazia
                    return None, None

    def mark_completed(self, network_code):
        """Marca network como completa"""
        with self.processing_lock:
            self.active_networks.discard(network_code)
            self.completed_networks.add(network_code)
            self.job_queue.task_done()

    def is_complete(self):
        """Verifica se todos os jobs foram processados"""
        with self.processing_lock:
            return (
                self.job_queue.empty()
                and len(self.active_networks) == 0
                and len(self.completed_networks) == len(self.network_jobs)
            )
