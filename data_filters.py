import pandas as pd
import multiprocessing
from random import shuffle
import numpy as np
from functools import partial


def desordenar_respostas(row, colunas_id_resposta, num_respostas):
    # Código para trocar a ordem das perguntas, incompleto
    #
    # chuncks_divididos = []
    # for resposta in range(0, num_respostas):
    #     chuncks_divididos.append(row[colunas_respostas[resposta * num_respostas:(resposta + 1) * num_respostas]])
    # shuffle(chuncks_divididos)
    #
    # counter = 0
    # for chunck in chuncks_divididos:
    #     if chunck[0] == row[1]:
    #         row[1] = counter
    #     row[counter * num_respostas:num_respostas * (counter + 1)] = chunck[counter]
    #     counter += 1

    # define qual das respostas (de 0 a num_respostas) é a melhor
    conditions = [row['target'] == row[id_resposta] for id_resposta in colunas_id_resposta]
    r = np.array(range(0, len(colunas_id_resposta)))[conditions]
    return r[0] if r.size == 1 else -1


def filter_num_questions(num_respostas: int, df: pd.DataFrame):
    print("começando thread para csv com {} respostas".format(num_respostas))
    colunas_importantes = ['id_pergunta', 'target']
    colunas_respostas = [r + str(j) for j in range(0, num_respostas) for r in
                         ['id_resposta', 'pontos_resposta', 'tempo_resposta', 'user_reputation', 'tem_site_proprio',
                          'maestria_tags']]

    colunas_importantes.extend(colunas_respostas)

    df_filtrado = df[colunas_importantes]

    colunas_id_resposta = list(filter(lambda s: "id_resposta" in s, colunas_respostas))
    df_final = df_filtrado.apply(desordenar_respostas, axis=1, args=(colunas_id_resposta, ))

    print("Salvando exemplo com {} repostas para csv...".format(num_respostas))
    df_final.to_csv('perguntas_2019 - {} respostas - Pronto para uso (com bias).csv'.format(num_respostas))
    print("Csv com {} respostas salvo!".format(num_respostas))


if __name__ == "__main__":
    perguntas_2019 = pd.read_csv("perguntas_2019.csv")

    temp = perguntas_2019.groupby(['id_pergunta', 'target']).cumcount()

    df1 = perguntas_2019.set_index(['id_pergunta', 'target', temp]).unstack().sort_index(level=1, axis=1)
    df1.columns = [f'{x}{y}' for x, y in df1.columns]
    df1 = df1.reset_index()

    threads = []
    for i in range(3, 6):
        t = multiprocessing.Process(group=None, target=filter_num_questions, args=(i, df1))
        threads.append(t)
        t.start()

    [t.join() for t in threads]
