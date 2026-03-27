def update_progress(pbar, stats, stats_lock, result):
    """Atualiza a barra de progresso e estatisticas"""
    with stats_lock:
        stats['total'] += 1
        status = result.get('status', 'error')
        if status in stats:
            stats[status] += 1

        if 'request_count' in result:
            stats.setdefault('total_requests', 0)
            stats['total_requests'] += result['request_count']

    pbar.update(1)

    # Atualiza descricao com estatisticas
    success_rate = (stats['success'] / stats['total'] * 100) if stats['total'] > 0 else 0
    pbar.set_description(
        f"[ 🟢: {stats['success']}, 🔴: {stats['error']}, "
        f"♦️: {stats['auth_error']}, 🔸: {stats['rate_limit']} ] "
        f"- 💹: {success_rate:.1f}%"
    )


def print_final_report(stats, elapsed_time):
    """Imprime relatorio final de processamento"""
    print(f"\n{'='*60}")
    print("Relatorio Final de Processamento DOM")
    print(f"{'='*60}")
    print(f"Tempo total: {elapsed_time:.2f} segundos")
    print(f"Networks processadas: {stats['total']}")
    print(f"Sucesso: {stats['success']}")
    print(f"Erros: {stats['error']}")
    print(f"Erros de autenticacao: {stats['auth_error']}")
    print(f"Rate limit excedido: {stats['rate_limit']}")

    if stats['total'] > 0:
        success_rate = (stats['success'] / stats['total']) * 100
        print(f"Taxa de sucesso: {success_rate:.1f}%")

    if 'total_requests' in stats and elapsed_time > 0:
        requests_per_second = stats['total_requests'] / elapsed_time
        print(f"Requisicoes totais: {stats['total_requests']}")
        print(f"Taxa media: {requests_per_second:.2f} requisicoes/segundo")

    print(f"{'='*60}")
