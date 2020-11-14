from sqlalchemy.orm import sessionmaker
from database_setup import engine

Session = sessionmaker(bind=engine)