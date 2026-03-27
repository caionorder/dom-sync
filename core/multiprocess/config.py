import argparse
import multiprocessing as mp


def get_args():
    """Configura e retorna argumentos do parser"""
    parser = argparse.ArgumentParser(
        description='Processa relatorios DOM (Revenue by Domain / UTM Campaign) com multiprocessing'
    )
    parser.add_argument(
        '--type',
        choices=['domain', 'utm_campaign'],
        help='Tipo de relatorio: domain ou utm_campaign'
    )
    parser.add_argument(
        '--day',
        type=str,
        help='Periodo: today, yesterday, last_7_days, last_30_days ou last_X_days (ex: last_90_days)'
    )
    parser.add_argument('--run', action='store_true', help='Modo execucao (sem flag = modo listagem)')
    parser.add_argument('--workers', type=int, default=mp.cpu_count())
    parser.add_argument('--limit', type=int, help='Limitar quantidade de networks processadas')
    parser.add_argument('--debug', action='store_true', help='Ativa logging em modo debug')
    parser.add_argument(
        '--network',
        type=str,
        help='Network code especifico para sincronizar (ex: --network=123456)'
    )

    return parser.parse_args()
