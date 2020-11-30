import pandas as pd
from multiprocessing import Pool, Process
from random import shuffle
import numpy as np
from functools import partial


def parallelize(data, func, num_of_processes=8):
    data_split = np.array_split(data, num_of_processes)
    pool = Pool(num_of_processes)
    data = pd.concat(pool.map(func, data_split))
    pool.close()
    pool.join()
    return data


def run_on_subset(func, data_subset):
    return data_subset.apply(func, axis=1)


def parallelize_on_rows(data, func, num_of_processes=8):
    return parallelize(data, partial(run_on_subset, func), num_of_processes)


def desordenar_respostas(row, num_respostas):
    temp = row[2:].copy()
    nova_ordem = list(range(0, num_respostas))
    shuffle(nova_ordem)
    for counter in range(0, num_respostas):
        row[2 + (counter * 5):2 + ((counter + 1) * 5)] = temp[nova_ordem[counter] * 5:(nova_ordem[counter] + 1) * 5]
        if row['target'] == row['id_resposta' + str(counter)]:
            row['alvo'] = counter
    if 'alvo' not in row:
        row['alvo'] = -1

    return row


def filter_num_questions(num_respostas: int, df: pd.DataFrame):
    print("começando thread para csv com {} respostas".format(num_respostas))
    colunas_importantes = ['id_pergunta', 'target']
    # Atualizado para desconsiderar a coluna 'tem_site_proprio', que não existe mais
    colunas_respostas = [r + str(j) for j in range(0, num_respostas) for r in
                         ['id_resposta', 'pontos_resposta', 'tempo_resposta', 'user_reputation',
                          'maestria_tags']]

    colunas_importantes.extend(colunas_respostas)

    df_filtrado = df[colunas_importantes]

    df_final = parallelize_on_rows(df_filtrado, partial(desordenar_respostas, num_respostas=num_respostas), 4)

    print("Salvando exemplo com {} repostas para csv...".format(num_respostas))
    df_final.to_csv('Perguntas 2018+ - {} respostas.csv'.format(num_respostas), index=False)
    print("Csv com {} respostas salvo!".format(num_respostas))


if __name__ == "__main__":
    # Alterado para pegar o novo arquivo gerado
    perguntas = pd.read_csv("perguntas_2018+.csv", index_col=[0])

    temp = perguntas.groupby(['id_pergunta', 'target']).cumcount()

    df1 = perguntas.set_index(['id_pergunta', 'target', temp]).unstack().sort_index(level=1, axis=1)
    df1.columns = [f'{x}{y}' for x, y in df1.columns]
    df1 = df1.reset_index()

    for i in range(3, 6):
        filter_num_questions(i, df1)
