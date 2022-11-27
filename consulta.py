# https://towardsdatascience.com/create-a-graph-database-in-neo4j-using-python-4172d40f89c4
# Exemplos
# https://gist.github.com/wjgilmore/8ba5f31ef1435dc04c52

import os
from neo4j import GraphDatabase
from movie import Movie

linked_movies = {}


class Neo4jConnection:
    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(
                self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver:", e)

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query, parameters=None, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try:
            session = self.__driver.session(
                database=db) if db is not None else self.__driver.session()
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()
        return response


conn = Neo4jConnection(uri=os.environ['NEO4J_URL'],
                       user="neo4j",
                       pwd=os.environ['NEO4J_PW'])


def generic_query(query):
  res = []
  for line in conn.query(query):
      res.append(dict(line))
  return res

def updateRelatedPoints(query, linked_movies: dict, points: float):
    for line in conn.query(query):
        response = dict(line)
        id = response['id']
        movie_info = Movie(response['titulo'])
        if linked_movies.get(id) == None:
            linked_movies[id] = movie_info

        linked_movies[id].multiplyPoints(points)

def get_random_movie():
  return generic_query(f"""
                  MATCH (f: Filme)
                  WITH f.id as id, f.titulo as titulo, rand() as r
                  ORDER BY r LIMIT 1
                  RETURN id, titulo
                """)


def search_top_related_movies(movieId: str, how_much_movies: int):
    linked_movies = {}

    # Filmes com o(s) mesmo(s) genero(s)
    updateRelatedPoints(f"""
    MATCH (f:Filme)-[:ESTA_EM]->(g:Genero)<-[:ESTA_EM]-(fRelacionado:Filme)
    WHERE f.id = '{movieId}'
    RETURN fRelacionado.id as id, fRelacionado.titulo as titulo
    """, linked_movies, 1.2)

    # Filmes com o mesmo diretor
    updateRelatedPoints(f"""
    MATCH (f:Filme)<-[:DIRIGIU]-(p:Pessoa)-[:DIRIGIU]->(fRelacionado:Filme)
    WHERE f.id = '{movieId}'
    RETURN fRelacionado.id as id, fRelacionado.titulo as titulo
    """, linked_movies, 1.5)

    # Filmes com o mesmo escritor
    updateRelatedPoints(f"""
    MATCH (f:Filme)<-[:ESCREVEU]-(p:Pessoa)-[:ESCREVEU]->(fRelacionado:Filme)
    WHERE f.id = '{movieId}'
    RETURN fRelacionado.id as id, fRelacionado.titulo as titulo
    """, linked_movies, 1.4)


    # Filmes com o(s) mesmo(s) atore(s)/atriz(es)
    updateRelatedPoints(f"""
    MATCH (f:Filme)<-[:ATUOU]-(p:Pessoa)-[:ATUOU]->(fRelacionado:Filme)
    WHERE f.id = '{movieId}'
    RETURN fRelacionado.id as id, fRelacionado.titulo as titulo
    """, linked_movies, 1.6)

    top_related_movies = sorted(list(linked_movies.values()), key=lambda value: value.related_points, reverse=True)
    return top_related_movies[:how_much_movies]



def search_for_top_5():
  filme = get_random_movie()
  titulo_filme = filme[0]['titulo']
  print(f'O filme sorteado foi: {titulo_filme}')

  # Bom exemplo id = 'tt0001386'
  id_filme = filme[0]['id']
  top_movies = search_top_related_movies(id_filme, 5)

  print("Filmes recomendados:")
  for i, movie in enumerate(top_movies):
    print(f'{i + 1}º - {movie.title}')
