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


def updateRelatedPoints(query, linked_movies: dict, points: float):
    for line in conn.query(query):
        response = dict(line)
        mId = response['rMovieId']
        movie = Movie(response['rMovieTitle'], response['rMovieUrl'])
        if linked_movies.get(mId) == None:
            linked_movies[mId] = movie

        linked_movies[mId].multiplyPoints(points)


def search_top_related_movies(movieId: str, how_much_movies: int):
    linked_movies = {}

    # Filmes com o(s) mesmo(s) genero(s)
    updateRelatedPoints(f"""
    MATCH (curMovie:Movie)-[:IN_GENRE]->(g:Genre)<-[:IN_GENRE]-(relatedMovie:Movie)
    WHERE curMovie.movieId = '{movieId}'
    RETURN relatedMovie.movieId as rMovieId, relatedMovie.title as rMovieTitle, relatedMovie.url as rMovieUrl
    """, linked_movies, 1.2)

    # Filmes com o mesmo diretor
    updateRelatedPoints(f"""
    MATCH (curMovie:Movie)<-[:DIRECTED]-(d:Director)-[:DIRECTED]->(relatedMovie:Movie)
    WHERE curMovie.movieId = '{movieId}'
    RETURN relatedMovie.movieId as rMovieId, relatedMovie.title as rMovieTitle, relatedMovie.url as rMovieUrl
    """, linked_movies, 1.4)

    # Filmes com o(s) mesmo(s) atore(s)/atriz(es)
    updateRelatedPoints(f"""
    MATCH (curMovie:Movie)<-[:ACTED_IN]-(a:Actor)-[:ACTED_IN]->(relatedMovie:Movie)
    WHERE curMovie.movieId = '{movieId}'
    RETURN relatedMovie.movieId as rMovieId, relatedMovie.title as rMovieTitle, relatedMovie.url as rMovieUrl
    """, linked_movies, 1.6)

    top_related_movies = sorted(list(linked_movies.values()), key=lambda value: value.related_points, reverse=True)
    return top_related_movies[:how_much_movies]


top_movies = search_top_related_movies('614', 5)

print("Filmes recomendados:")
for i, movie in enumerate(top_movies):
  print(f'{i + 1}º - {movie.title}')
