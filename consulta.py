# https://towardsdatascience.com/create-a-graph-database-in-neo4j-using-python-4172d40f89c4
# Exemplos
# https://gist.github.com/wjgilmore/8ba5f31ef1435dc04c52

import os
from neo4j import GraphDatabase

linked_movies = {}

class Neo4jConnection:
  def __init__(self, uri, user, pwd):
    self.__uri = uri
    self.__user = user
    self.__pwd = pwd
    self.__driver = None
    try:
        self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
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
      session = self.__driver.session(database=db) if db is not None else self.__driver.session() 
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

def consultar(query, resType: str):
  response = []
  for line in conn.query(query):
        if resType == 'dict':
            response.append(dict(line)['relatedMovie'])
        elif resType == 'str':
            response.append(dict(line)['relatedMovie'])
  return response

def search_top_related_movies(movieId: str, how_much_movies: str):
    linked_movies = {}
    
    print("Filmes com o(s) mesmo(s) genero(s)")
    response = consultar(f"""
    MATCH (curMovie:Movie)-[:IN_GENRE]->(g:Genre)<-[:IN_GENRE]-(relatedMovie:Movie)
    WHERE curMovie.movieId = '{movieId}'
    RETURN relatedMovie.movieId as relatedMovie
    """, "str")
    for mId in response:
        if linked_movies.get(mId) != None:
            linked_movies[mId] += 5
        else:
            linked_movies[mId] = 5

    print()        

search_top_related_movies('614', '5')

# print("Filme e genero")
# consultar("""
# MATCH (m:Movie)-[:IN_GENRE]->(g:Genre)<-[:IN_GENRE]-(m2:Movie)
# WITH m.title AS nome1, g.name as genero, m2.title as nome2
# WHERE m.movieId = '614' and g.name <> 'Thriller'
# RETURN nome1, genero, nome2
# """, "str")

# print("Pegando um filme aleatorio")
# consultar("""
# MATCH (m:Movie)
# WITH m.movieId as movieId, m.title AS nome, rand() as randNumber
# order by randNumber
# limit 1
# RETURN movieId, nome
# """)


# print("Pegando um filme especifico")
# consultar("""
# MATCH (m:Movie)
# WITH m.movieId as idImdb, m.title AS nome, m.year as year, m.url as url
# WHERE m.imdbId = '0110374'
# RETURN idImdb, nome, year, url
# """)


# print("DISCIPLINA COM MAIOR NÚMERO DE INSCRIÇÕES")
# consultar("""
# MATCH (d:DISCIPLINA)<-[i:INSCRITO]-()
# WITH count(d.nome) as counter, d.nome as nome
# order by counter desc
# LIMIT 1
# RETURN nome
# """)

# print("ALUNOS POR SALA DE AULA")
# consultar("""
# MATCH  (e:ESTUDANTE)-[:INSCRITO]->(d:DISCIPLINA)-[:ALOCADO]->(s:SALA)
# WITH count(s.nome) as counter, s.nome as nome, s.capacidade as cap
# RETURN nome, ((counter * 100) / cap) + '%' as porcentagem
# """)

# print("Quantidade de alunos por blocos")
# consultar("""
# MATCH  (e:ESTUDANTE)-[:INSCRITO]->(d:DISCIPLINA)-[:ALOCADO]->(s:SALA)
# WITH count(s.nome) as counter, s.bloco as bloco
# order by bloco asc
# RETURN bloco, counter
# """)
