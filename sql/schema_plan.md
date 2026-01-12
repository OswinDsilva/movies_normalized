1. movies : id(PK), title, release_year, Cert,rating , metascore, runtime, votes, gross,directors_id, overview(FK to directors.id)
2. directors: id(PK), name
3. genres : id(PK), genres()
4. movies_genres : m_id , g_id 
5. actors: id(PK) , name
6. movies_actors : m_id, a_id