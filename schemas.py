"""
schema.py : model to be converted in json by fastapi
"""
from typing import Optional, List
from datetime import date

from pydantic import BaseModel




class StarBase(BaseModel):
    name: str
    birthdate: Optional[date]

class StarCreate(StarBase):
    pass

class Star(StarBase):
    id: int 
    
    class Config:
        orm_mode = True
        
# common Base Class for Movie (abstract class)
class MovieBase(BaseModel):
    title: str
    year: int
    duration: Optional[int] = None

# item witout id, only for creation purpose
class MovieCreate(MovieBase):
    pass

# item from database with id
class Movie(MovieBase):
    id: int

    class Config:
        orm_mode = True
        
class MovieDetail(Movie):
    director: Optional[Star] = None
    actors: List[Star] = []

