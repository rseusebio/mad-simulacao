import heapq
import sys
import numpy as np

TAXA_LAMBDA_DE_CHEGADA = 2 # Taxa de chegada de jobs (jobs/segundo)
NUM_JOBS = 50000 # Número total de jobs a serem processados

jobs = {} # Dicionário para armazenar os jobs processados

s2_queue = [] # Fila de prioridade para o servidor S2
s3_queue = [] # Fila de espera para o servidor S3

# criar uma seed
np.random.seed(42)

# Função auxiliar para gerar chegadas com processo de Poisson
def gerar_tempos_chegada(taxa, num_jobs):
    return np.cumsum(np.random.exponential(1 / taxa, num_jobs))

def executar_s1(tempos_chegada, tmp_servico_s1):
    # popular os jobs com os tempos de chegada
    ultimo_processado_s1 = None
    for i in range(NUM_JOBS):
        tempo_servico_s1 = tmp_servico_s1()
    
        tempo_chegada = tempos_chegada[i]
        job_id = str(i)
        s1_status = None

        if ultimo_processado_s1 != None and ultimo_processado_s1['tempo_saida'] > tempo_chegada:
            tempo_espera = ultimo_processado_s1['tempo_saida'] - tempo_chegada
            s1_status = {
                "tempo_chegada": tempo_chegada,
                "tempo_espera": tempo_espera,
                "tempo_saida": tempo_chegada + tempo_espera + tempo_servico_s1,
                "tempo_total_no_sistema": tempo_espera + tempo_servico_s1
            }
        else:
            s1_status = {
                "tempo_chegada": tempo_chegada,
                "tempo_espera": 0,
                "tempo_saida": tempo_chegada + tempo_servico_s1,
                "tempo_total_no_sistema": tempo_servico_s1
            }

        jobs[job_id] = {
            "id": str(i),
            "s1": s1_status,
            "tempo_total_no_sistema": s1_status["tempo_total_no_sistema"],
        }
        ultimo_processado_s1 = s1_status


        # Decidir se o job vai para o servidor S2 ou S3
        u = np.random.uniform(0, 1)

        if u < 0.5:
            jobs[job_id]['s2'] = []
            heapq.heappush(s2_queue, (jobs[job_id]['s1']["tempo_saida"], job_id))
        else:
            s3_queue.append(job_id)

def executar_s2(tmp_servico_s2):
    ultimo_processado_s2 = None
    while s2_queue:
        tempo_servico_s2 = tmp_servico_s2()

        tempo_chegada_s2, job_id = heapq.heappop(s2_queue)
        job = jobs[job_id]
        rodada = len(job['s2']) + 1
        s2_status = None
        
        if ultimo_processado_s2 != None and ultimo_processado_s2["tempo_saida"] > tempo_chegada_s2:
            tempo_espera = ultimo_processado["tempo_saida"] - tempo_chegada_s2
            s2_status = {
                "tempo_chegada": tempo_chegada_s2,
                "tempo_espera": tempo_espera,
                "tempo_saida": tempo_chegada_s2 + tempo_espera + tempo_servico_s2,
                "tempo_total_no_sistema": tempo_espera + tempo_servico_s2,
                "round": rodada,
            }
        else:
            s2_status = {
                "tempo_chegada": tempo_chegada_s2,
                "tempo_espera": 0,
                "tempo_saida": tempo_chegada_s2 + tempo_servico_s2,
                "tempo_total_no_sistema": tempo_servico_s2,
                "round": rodada,
            }
        job['s2'].append(s2_status)
        job['tempo_total_no_sistema'] += s2_status["tempo_total_no_sistema"]
        ultimo_processado = s2_status

        # Decidir se o job vai para o servidor S2 novamente
        u = np.random.uniform(0, 1)
        if u < 0.2:
            heapq.heappush(s2_queue, (ultimo_processado["tempo_saida"], job_id))

def executar_s3(tmp_servico_s3):
    ultimo_processado_s3 = None
    for job_id in s3_queue:
        tempo_servico_s3 = tmp_servico_s3()
        
        job = jobs[job_id]
        s3_status = None
        tempo_chegada_s3 = job['s1']["tempo_saida"]

        if ultimo_processado_s3 != None and ultimo_processado_s3["tempo_saida"] > tempo_chegada_s3:
            tempo_espera = ultimo_processado_s3["tempo_saida"] - tempo_chegada_s3
            s3_status = {
                "tempo_chegada": tempo_chegada_s3,
                "tempo_espera": tempo_espera,
                "tempo_saida": tempo_chegada_s3 + tempo_espera + tempo_servico_s3,
                "tempo_total_no_sistema": tempo_espera + tempo_servico_s3,
            }
        else:
            s3_status = {
                "tempo_chegada": tempo_chegada_s3,
                "tempo_espera": 0,
                "tempo_saida": tempo_chegada_s3 + tempo_servico_s3,
                "tempo_total_no_sistema": tempo_servico_s3,
            }

        job['s3'] = s3_status
        job['tempo_total_no_sistema'] += s3_status["tempo_total_no_sistema"]
        ultimo_processado_s3 = job['s3']


execucoes = [
    {
        'descricao': 'Execução 1',
        'lambdas': [lambda: 0.4, lambda: 0.6, lambda: 0.95],
    },
    {
        'descricao': 'Execução 2',
        'lambdas': [lambda: np.random.uniform(0.1, 0.7), lambda: np.random.uniform(0.1, 1.1), lambda: np.random.uniform(0.1, 1.8)],
    },
    {
        'descricao': 'Execução 3',
        'lambdas': [lambda: np.random.exponential(0.4), lambda: np.random.exponential(0.6), lambda: np.random.exponential(0.95)],
    },
]

def simular_sistema(idx_execucao):
    execucao = execucoes[idx_execucao]
    print(f"\n{execucao['descricao']}")

    [tmp_servico_s1, tmp_servico_s2, tmp_servico_s3] = execucao['lambdas']

    tempos_chegada = gerar_tempos_chegada(TAXA_LAMBDA_DE_CHEGADA, NUM_JOBS)

    executar_s1(tempos_chegada, tmp_servico_s1)
    executar_s2(tmp_servico_s2)
    executar_s3(tmp_servico_s3)

    # tira os 10k primeiros jobs
    jobs_considerar = {k: v for k, v in jobs.items() if int(k) > 10000}

    #  calcule a media entre a chegada de dois jobs
    media_entre_chegadas = np.mean(np.diff(tempos_chegada))
    print(f"Media entre chegadas: {media_entre_chegadas:.4f}s")

    # numero medio de rodadas no servidor S2
    s2_jobs = filter(lambda job: 's2' in job and len(job['s2']) > 0, jobs_considerar.values())
    rodadas = [job['s2'][-1]['round'] for job in s2_jobs]
    media_rodadas = np.mean(rodadas)
    print(f"Media de rodadas no servidor S2: {media_rodadas:.4f}")

    # calcule o tempo medio no sistema
    tempos_total_no_sistema = [job['tempo_total_no_sistema'] for job in jobs_considerar.values()]
    tempo_medio_no_sistema = np.mean(tempos_total_no_sistema)
    desvio_padrao = np.std(tempos_total_no_sistema)
    print(f"\n\nTempo médio no sistema: {tempo_medio_no_sistema:.4f}s")
    print(f"Desvio padrão do tempo no sistema: {desvio_padrao:.4f}s")


idx_execucao = int(sys.argv[1]) if len(sys.argv) > 1 else 0
simular_sistema(idx_execucao)