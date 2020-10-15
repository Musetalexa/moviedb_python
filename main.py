from typing import List, Optional, Tuple
import logging

from fastapi import Depends, FastAPI, HTTPException
from fastapi.logger import logger as fastapi_logger
from sqlalchemy.orm import Session

import crud, models, schemas
from database import SessionLocal, engine

models.Base.metadata.create_all(bind=engine)

app = FastAPI()
logger = logging.getLogger("uvicorn")
fastapi_logger.handlers = logger.handlers
fastapi_logger.setLevel(logger.level)
logger.error("API Started")

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

#Get Movies

@app.get("/movies/", response_model=List[schemas.Movie])
def read_movies(skip: Optional[int] = 0, limit: Optional[int] = 100, db: Session = Depends(get_db)):
    # read movies from database
    movies = crud.get_movies(db, skip=skip, limit=limit)
    # return them as json
    return movies

@app.get("/movies/by_id/{movie_id}", response_model=schemas.MovieDetail)
def read_movie(movie_id: int, db: Session = Depends(get_db)):
    db_movie = crud.get_movie(db, movie_id=movie_id)
    if db_movie is None:
        raise HTTPException(status_code=404, detail="Movie to read not found")
    return db_movie

@app.get("/movies/by_title", response_model=List[schemas.Movie])
def read_movies_by_title(t: str, db: Session = Depends(get_db)):
    # read movies from database
    movies = crud.get_movies_by_title(db=db, title=t)
    # return them as json
    if movies is None:
        raise HTTPException(status_code=404, detail="Movie to read not found")
    # return movies
    return movies

@app.get("/movies/by_range_year")
def read_movie_by_range_year(d1 : int, d2 : int, db: Session = Depends(get_db)):
    movies = crud.get_movies_by_range_year(db = db, date1 = d1, date2 = d2)
    if movies is None:
        raise HTTPException(status_code=404, detail="Movie to read not found")
    return movies

@app.get("/movies/by_director", response_model=List[schemas.Movie])
def read_movies_by_director(n : str, db:Session = Depends(get_db)):
    return crud.get_movies_by_director_endname(db=db, endname=n)

@app.get("/movies/by_director_partial", response_model=List[schemas.Movie])
def read_movies_by_director_partial(n: str, db:Session = Depends(get_db)):
    return crud.get_movies_by_director_partialname(db=db, partialname=n)


@app.get("/movies/by_actor", response_model=List[schemas.Movie])
def read_movies_by_actor(n : str, db:Session = Depends(get_db)):
    return crud.get_movies_by_actor_endname(db=db, endname=n)

@app.get("/movies/count_by_year")
def read_count_movies_by_year(db: Session = Depends(get_db)) -> List[Tuple[int, int]]:
    return crud.get_movies_count_by_year(db=db)

@app.get("/movies/duration_min_by_year")
def read_duration_min_movies_by_year(db: Session = Depends(get_db)) -> List[Tuple[int, int]]:
    return crud.get_min_duration_movie_by_year(db=db)

@app.get("/movies/duration_max_by_year")
def read_duration_max_movies_by_year(db: Session = Depends(get_db)) -> List[Tuple[int, int]]:
    return crud.get_max_duration_movie_by_year(db=db)

@app.get("/movies/duration_average_by_year")
def read_duration_average_movies_by_year(db: Session = Depends(get_db)) -> List[Tuple[int, int]]:
    return crud.get_average_duration_movie_by_year(db=db)

@app.get("/movies/stats_by_year")
def read_stats_movies_by_year(db: Session = Depends(get_db)) -> List[Tuple[int, int, int, int]]:
    return crud.get_stats_by_year(db=db)

#Post and put Movies

@app.post("/movies/", response_model=schemas.Movie)
def create_movie(movie: schemas.MovieCreate, db: Session = Depends(get_db)):
    # receive json movie without id and return json movie from database with new id
    return crud.create_movie(db=db, movie=movie)

@app.put("/movies/director")
def update_movie_director(mid, sid, db: Session = Depends(get_db)):
    db_movie = crud.update_movie_director(db=db, movie_id=mid, director_id=sid)
    if db_movie is None:
        raise HTTPException(status_code=404, detail="Movie or Star not found")
    return db_movie

@app.post("/movies/actor/", response_model=schemas.MovieDetail)
def add_movie_actor(mid: int, sid: int, db: Session = Depends(get_db)):
    db_movie = crud.add_movie_actor(db=db, movie_id=mid, actor_id=sid)
    if db_movie is None:
        raise HTTPException(status_code=404, detail="Movie or Star not found")
    return db_movie

@app.put("/movies/actors/", response_model=schemas.MovieDetail)
def update_movie_actors(mid: int, sids: List[int], db: Session = Depends(get_db)):
    """ replace actors from a movie
        mid (query param): movie id
        sids (body param): list of star id to replace movie.actors
    """
    db_movie = crud.update_movie_actors(db=db, movie_id = mid, actors_id=sids)
    if db_movie is None:
        raise HTTPException(status_code=404, detail="Movie or Star not found")
    return db_movie    

@app.put("/movies/", response_model=schemas.Movie)
def update_movie(movie: schemas.Movie, db: Session = Depends(get_db)):
    db_movie = crud.update_movie(db, movie=movie)
    if db_movie is None:
        raise HTTPException(status_code=404, detail="Movie to update not found")
    return db_movie

@app.delete("/movies/{movie_id}", response_model=schemas.Movie)
def delete_movie(movie_id: int, db: Session = Depends(get_db)):
    db_movie = crud.delete_movie(db, movie_id=movie_id)
    if db_movie is None:
        raise HTTPException(status_code=404, detail="Movie to delete not found")
    return db_movie


#Get Stars 

@app.get("/stars/", response_model=List[schemas.Star])
def read_stars(skip: Optional[int] = 0, limit: Optional[int] = 100, db: Session = Depends(get_db)):
    # read moviess from database
    stars = crud.get_stars(db, skip=skip, limit=limit)
    # return them as json
    return stars


@app.get("/stars/by_id/{star_id}", response_model=schemas.Star)
def read_star(star_id: int, db: Session = Depends(get_db)):
    db_star = crud.get_star(db, star_id=star_id)
    if db_star is None:
        raise HTTPException(status_code=404, detail="Star to read not found")
    return db_star

@app.get("/stars/by_name", response_model=schemas.Star)
def read_star_by_name(n: str, db: Session = Depends(get_db)):
    stars = crud.get_stars_by_name(db = db, name=n)
    if stars is None:
        raise HTTPException(status_code=404, detail="Stars to read not found")
    # return stars
    return stars


@app.get("/stars/by_birthyear/{year}", response_model=List[schemas.Star])
def read_stars_by_birthyear(year: int, db: Session = Depends(get_db)):
    # read items from database
    stars = crud.get_stars_by_birthyear(db=db, year=year)
    return stars

@app.get("/stars/by_movie_directed/{movie_id}", response_model=Optional[schemas.Star])
def read_stars_by_movie_directed_id(movie_id: int, db: Session = Depends(get_db)):
    return crud.get_star_director_movie(db = db, movie_id=movie_id)

@app.get("/stars/by_movie_directed_title", response_model=List[schemas.Star])
def read_stars_by_movie_directed_title(t: str, db: Session = Depends(get_db)):
    return crud.get_star_director_movie_by_title(db=db, title=t)

@app.get("/stars/by_movie_acted_title", response_model=List[List[schemas.Star]])
def read_stars_by_movie_acted_title(t: str, db: Session = Depends(get_db)):
    return crud.get_star_actor_movie_by_title(db = db, title=t)

@app.get("/stars/stats_movie_by_director")
def read_stats_movie_by_director(minc: Optional[int] = 10, db: Session = Depends(get_db)):
    return crud.get_stats_movie_by_director(db=db, min_count=minc)


#Post and put Stars

@app.post("/stars/", response_model=schemas.Star)
def create_star(star: schemas.StarCreate, db: Session = Depends(get_db)):
    # receive json movie without id and return json movie from database with new id
    return crud.create_star(db=db, star=star)

@app.put("/stars/", response_model=schemas.Star)
def update_star(star: schemas.Star, db: Session = Depends(get_db)):
    db_star = crud.update_star(db, star=star)
    if db_star is None:
        raise HTTPException(status_code=404, detail="Star to update not found")
    return db_star

@app.delete("/stars/{star_id}", response_model=schemas.Star)
def delete_star(star_id: int, db: Session = Depends(get_db)):
    db_star = crud.delete_star(db, star_id=star_id)
    if db_star is None:
        raise HTTPException(status_code=404, detail="Star to delete not found")
    return db_star