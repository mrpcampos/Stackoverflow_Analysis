import pandas as pd
import multiprocessing
from random import choice


def desordenar_respostas(row, colunas_respostas, num_respostas):
    columns_map = {}
    new_row = pd.Series()
    rg = list(range(0, num_respostas))
    for j in range(0, num_respostas):
        nova_posicao = choice(rg)
        rg.remove(nova_posicao)

        nome_colunas_da_vez = colunas_respostas[-num_respostas:]
        colunas_da_vez = row[nome_colunas_da_vez]
        columns_map[j] = nome_colunas_da_vez

        if colunas_da_vez[nome_colunas_da_vez[0]] == row['id_pergunta']:
            row['id_pergunta'] = j
        colunas_da_vez[nome_colunas_da_vez[0]] = j



def filter_num_questions(num_respostas: int, df: pd.DataFrame):
    print("começando thread para csv com {} respostas".format(num_respostas))
    colunas_importantes = ['id_pergunta', 'target']
    colunas_respostas = [r + str(j) for j in range(0, num_respostas) for r in
                         ['id_resposta', 'pontos_resposta', 'tempo_resposta', 'user_reputation', 'tem_site_proprio',
                          'maestria_tags']]

    colunas_importantes.extend(colunas_respostas)

    pool = multiprocessing.Pool(4)

    pool.map(desordenar_respostas, df[colunas_importantes].iterrows())
    df_final = df[colunas_importantes].apply(lambda row: desordenar_respostas(row, colunas_respostas, num_respostas), axis=1)

    print("Salvando exemplo com {} repostas para csv...".format(num_respostas))
    df_final.to_csv('perguntas_2019 - {} respostas - Randomizado.csv'.format(num_respostas))
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
