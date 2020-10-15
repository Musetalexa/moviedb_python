"""
file crud.py
manage CRUD and adapt model data from db to schema data to api rest
"""

from typing import Optional, List
from sqlalchemy.orm import Session
from sqlalchemy import desc, extract, between
from sqlalchemy import func
from fastapi.logger import logger
import models, schemas


#Movies

def get_movie(db: Session, movie_id: int):
    # read from the database (get method read from cache)
    # return object read or None if not found
    db_movie = db.query(models.Movie).filter(models.Movie.id == movie_id).first()
    logger.error(f"Movie retrieved from DB: {db_movie.title}")
    logger.error("director: {}".format(
                 db_movie.director.name if db_movie.director is not None else "No director" ))
    logger.error(f"actors: {db_movie.actors}")
    return db_movie;

def get_movies(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.Movie).offset(skip).limit(limit).all()

def _get_movies_by_predicate(*predicate, db: Session):
    """ partial request to apply one or more predicate(s) to model Movie"""
    return db.query(models.Movie)   \
            .filter(*predicate)
            
def get_movies_by_title(db: Session, title: str):
    return db.query(models.Movie).filter(models.Movie.title.like(f'%{title}%')).all()

def get_movies_by_range_year(db: Session, date1 : int, date2 : int):
    return db.query(models.Movie).filter(models.Movie.year >= date1, models.Movie.year <= date2) \
            .order_by(models.Movie.title, models.Movie.year) \
            .all()
            
def get_movies_by_director_endname(db: Session, endname: str):
    return db.query(models.Movie).join(models.Movie.director) \
        .filter(models.Star.name.like(f'%{endname}')) \
        .order_by(desc(models.Movie.year)) \
        .all()
        
def get_movies_by_director_partialname(db: Session, partialname: str):
    return db.query(models.Movie).join(models.Movie.director) \
        .filter(models.Star.name.like(f'%{partialname}%')) \
        .order_by(desc(models.Movie.year)) \
        .all() 

def get_movies_by_actor_endname(db: Session, endname: str):
    return db.query(models.Movie).join(models.Movie.actors) \
        .filter(models.Star.name.like(f'%{endname}')) \
        .all()

def get_star_director_movie(db: Session, movie_id: int):
    movie_director = db.query(models.Movie) \
        .filter(models.Movie.id == movie_id) \
        .join(models.Movie.director).first() 
    return movie_director.director

def get_star_director_movie_by_title(db: Session, title: str):
    db_movies = db.query(models.Movie) \
        .filter(models.Movie.title.like(f'%{title}%')) \
        .join(models.Movie.director)
    return [db_movie.director for db_movie in db_movies]

def get_star_actor_movie_by_title(db: Session, title: str):
    db_stars = db.query(models.Movie) \
        .filter(models.Movie.title.like(f'%{title}%')) \
        .join(models.Movie.actors)
    return  [db_star for db_star in db_stars]

def get_movies_count_by_year(db: Session):
    return db.query(models.Movie.year, func.count()) \
            .group_by(models.Movie.year) \
            .order_by(models.Movie.year) \
            .all()
        
        
def get_stats_by_year(db: Session):
    return db.query(models.Movie.year, func.min(models.Movie.duration), func.max(models.Movie.duration), func.avg(models.Movie.duration) ) \
        .group_by(models.Movie.year) \
        .order_by(models.Movie.year) \
        .all()

def create_movie(db: Session, movie: schemas.MovieCreate):
    # convert schema object from rest api to db model object
    db_movie = models.Movie(title=movie.title, year=movie.year, duration=movie.duration)
    # add in db cache and force insert
    db.add(db_movie)
    db.commit()
    # retreive object from db (to read at least generated id)
    db.refresh(db_movie)
    return db_movie

def update_movie(db: Session, movie: schemas.Movie):
    db_movie = db.query(models.Movie).filter(models.Movie.id == movie.id).first()
    if db_movie is not None:
        # update data from db
        db_movie.title = movie.title
        db_movie.year = movie.year
        db_movie.duration = movie.duration
        # validate update in db
        db.commit()
    # return updated object or None if not found
    return db_movie


def delete_movie(db: Session, movie_id: int):
     db_movie = db.query(models.Movie).filter(models.Movie.id == movie_id).first()
     if db_movie is not None:
         # delete object from ORM
         db.delete(db_movie)
         # validate delete in db
         db.commit()
     # return deleted object or None if not found
     return db_movie
 
# CRUD association
def update_movie_director(db: Session, movie_id: int, director_id: int):
    db_movie = get_movie(db=db, movie_id=movie_id)
    db_star = get_star(db=db, star_id=director_id)
    if db_movie is None or db_star is None:
        return None
    #update object association
    db_movie.director = db_star
    #commit transaction : update SQL
    db.commit()
    
def add_movie_actor(db: Session, movie_id: int, actor_id: int):
    db_movie = get_movie(db=db, movie_id=movie_id)
    db_star = get_star(db=db, star_id=actor_id)
    if db_movie is None or db_star is None:
        return None
    if db_star not in db_movie.actors:
        db_movie.stars.append(db_star)
        db.commit()
    return(db_movie)

def update_movie_actors(db: Session, movie_id: int, actor_ids: List[int]):
    db_movie = get_movie(db=db, movie_id=movie_id)
    db_stars = db.query(models.Star).filter(models.Star.id.in_(actor_ids))
    if db_movie is None or db_stars is None:
        return None
    db_movie.stars = db_stars
    db.commit()
    return(db_movie)

    
#Stars

def get_star(db: Session, star_id: int):
    # read from the database (get method read from cache)
    # return object read or None if not found
    return db.query(models.Star).filter(models.Star.id == star_id).first()

def get_stars(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.Star).offset(skip).limit(limit).all()

def get_stars_by_name(db : Session, name : str):
    return db.query(models.Star).filter(models.Star.name.like(f'%{name}%')).all()

def get_stars_by_birthyear(db : Session, year : int):
    return db.query(models.Star).filter(extract('year', models.Star.birthdate) == year).all()

def get_stats_movie_by_director(db: Session, min_count: int):
    return db.query(models.Star, func.count(models.Movie.id)) \
        .join(models.Movie.director) \
        .group_by(models.Star) \
        .having(func.count(models.Movie.id) >= min_count) \
        .order_by(desc(func.count(models.Movie.id))) \
        .all()
        
def get_stats_movie_by_actor(db: Session, min_count):
    return db.query(models.Star.name, func.count(models.Movie.id), func.min(models.Movie.year), func.max(models.Movie.year) ) \
        .join(models.Movie.actors) \
        .group_by(models.Star) \
        .having(func.count(models.Movie.id) >= min_count) \
        .order_by(desc(func.count(models.Movie.id))) \
        .all()
        
# def get_birth_count_by_year(db: Session):
#     return db.query(models.Star.birthdate.year, func.count()) \
#             .group_by(models.Star.birthdate.year) \
#             .order_by(models.star.birthdate) \
#             .all()

def create_star(db: Session, star: schemas.StarCreate):
    # convert schema object from rest api to db model object
    db_star = models.Star(name=star.name, birthdate=star.birthdate)
    # add in db cache and force insert
    db.add(db_star)
    db.commit()
    # retreive object from db (to read at least generated id)
    db.refresh(db_star)
    return db_star

def update_star(db: Session, star: schemas.Star):
    db_star = db.query(models.Star).filter(models.Star.id == star.id).first()
    if db_star is not None:
        # update data from db
        db_star.name = star.name
        db_star.birthdate = star.birthdate
        # validate update in db
        db.commit()
    # return updated object or None if not found
    return db_star


def delete_star(db: Session, star_id: int):
     db_star = db.query(models.Star).filter(models.Star.id == star_id).first()
     if db_star is not None:
         # delete object from ORM
         db.delete(db_star)
         # validate delete in db
         db.commit()
     # return deleted object or None if not found
     return db_star
 
    
#exemple of alias
#    actor = db.query(models.Stars).alias("actor")
#    director = db.query(models.Movies).alias("director")
#    actor.join(models.Movie.actors)
#    ....   
#    .join(director)