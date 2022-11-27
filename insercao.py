import os
from neo4j import GraphDatabase
import pandas as pd
from unidecode import unidecode

senha = os.environ['NEO4J_PW']
url = os.environ['NEO4J_URL']
usuario = "neo4j"


class CriarDB:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def limpa_base(self):
        with self.driver.session() as session:
            session.write_transaction(self._zera_base)

    def cria_pessoa(self, id, nome):
        with self.driver.session() as session:
            session.execute_write(self._cria_pessoa, id, nome)

    def cria_genero(self, genero):
        with self.driver.session() as session:
            session.execute_write(self._cria_genero, genero)

    def cria_filme(self, id, tipo, titulo, nota):
        with self.driver.session() as session:
            session.execute_write(self._cria_filme, id, tipo, titulo, nota)

    def cria_relacao_dirigiu(self, idFilme, diretor):
        with self.driver.session() as session:
            session.execute_write(self._cria_relacao_dirigiu, idFilme, diretor)

    def cria_relacao_escreveu(self, idFilme, escritor):
        with self.driver.session() as session:
            session.execute_write(self._cria_relacao_escreveu, idFilme, escritor)

    def cria_relacao_atuou(self, idFilme, idAtor):
        with self.driver.session() as session:
            session.execute_write(self._cria_relacao_atuou, idFilme, idAtor)

    def cria_relacao_esta_em(self, idFilme, genero):
        with self.driver.session() as session:
            session.execute_write(self._cria_relacao_esta_em, idFilme, genero)

    @staticmethod
    def _zera_base(tx):
        tx.run("""
                MATCH (n)
                DETACH DELETE n
                """)

    @staticmethod
    def _cria_pessoa(tx, id, nome):
        nome = unidecode(nome).replace("'", ' ')
        return tx.run(f"""
                CREATE (p:Pessoa {{
                    id: '{id}',
                    nome: '{nome}'
                }})
                """)

    @staticmethod
    def _cria_genero(tx, genero):
        return tx.run(f"""
                CREATE (g:Genero {{
                    nome: '{genero}'
                }})
                """)

    @staticmethod
    def _cria_filme(tx, id, tipo, titulo, nota):
        titulo = unidecode(titulo).replace("'", ' ')
        return tx.run(f"""
                CREATE (f:Filme {{
                    id: '{id}',
                    tipo: '{tipo}',
                    titulo: '{titulo}',
                    nota: {nota}
                }})
                """)

    @staticmethod
    def _cria_relacao_dirigiu(tx, idFilme, diretor):
        return tx.run(f"""
                        MATCH (f:Filme), (p:Pessoa)
                            WHERE f.id = '{idFilme}' AND p.id = '{diretor}'
                        CREATE (p)-[r:DIRIGIU]->(f)
                        RETURN type(r)
                    """)

    @staticmethod
    def _cria_relacao_escreveu(tx, idFilme, escritor):
        return tx.run(f"""
                        MATCH (f:Filme), (p:Pessoa)
                            WHERE f.id = '{idFilme}' AND p.id = '{escritor}'
                        CREATE (p)-[r:ESCREVEU]->(f)
                        RETURN type(r)
                    """)

    @staticmethod
    def _cria_relacao_atuou(tx, idFilme, idAtor):
        return tx.run(f"""
                        MATCH (f:Filme), (p:Pessoa)
                            WHERE f.id = '{idFilme}' AND p.id = '{idAtor}'
                        CREATE (p)-[r:ATUOU]->(f)
                        RETURN type(r)
                    """)

    @staticmethod
    def _cria_relacao_esta_em(tx, idFilme, genero):
        return tx.run(f"""
                        MATCH (f:Filme), (g:Genero)
                            WHERE f.id = '{idFilme}' AND g.nome = '{genero}'
                        CREATE (f)-[r:ESTA_EM]->(g)
                        RETURN type(r)
                    """)

def criar_database():
    db = CriarDB(url, "neo4j", senha)
    db.limpa_base()

    df = pd.read_feather('final.feather')

    print(df.info())

    ######################################################################################

    # Criando nodes no banco

    # Criar pessoas
    aux = df[['nconst', 'primaryName']]
    aux = aux.drop_duplicates()
    for id, nome in zip(aux['nconst'], aux['primaryName']):
        db.cria_pessoa(id, nome)

    # Criar generos
    generos = set()
    aux = df[['genres']]
    aux = aux.drop_duplicates()
    for genre in aux['genres']:
        genAux = set(str(genre).split(','))
        for g in genAux:
            generos.add(g)

    for genero in generos:
        db.cria_genero(genero)

    # Criar filmes
    aux = df[['tconst', 'titleType', 'primaryTitle', 'averageRating']]
    aux = aux.drop_duplicates()
    for id, tipo, titulo, nota in zip(aux['tconst'], aux['titleType'], aux['primaryTitle'], aux['averageRating']):
        db.cria_filme(id, tipo, titulo, nota)

    ######################################################################################

    # Criando relacoes no banco

    # Criar relacao de dirigiu o filme
    aux = df[['tconst', 'directors']]
    aux = aux.drop_duplicates()
    for idFilme, diretores_str in zip(aux['tconst'], aux['directors']):
        diretores = list(str(diretores_str).split(','))
        for diretor in diretores:
            db.cria_relacao_dirigiu(idFilme, diretor)

    # Criar relacao em quem escreveu o enredo do filme
    aux = df[['tconst', 'writers']]
    aux = aux.drop_duplicates()
    for idFilme, escritores_str in zip(aux['tconst'], aux['writers']):
        escritores = list(str(escritores_str).split(','))
        for escritor in escritores:
            db.cria_relacao_escreveu(idFilme, escritor)

    # Criar relacao em quem atuou no filme
    aux = df[['tconst', 'nconst']]
    aux = aux.drop_duplicates()
    for idFilme, idAtor in zip(aux['tconst'], aux['nconst']):
        db.cria_relacao_atuou(idFilme, idAtor)

    # Criar relacao em qual genero o filme esta
    aux = df[['tconst', 'genres']]
    aux = aux.drop_duplicates()
    for idFilme, lista_genero in zip(aux['tconst'], aux['genres']):
        generos_l = list(str(lista_genero).split(','))
        for genero in generos_l:
            db.cria_relacao_esta_em(idFilme, genero)

    ######################################################################################

    db.close()
    print("base carregada")
