from sqlalchemy.orm import Session


def get_session():
    from db_interactor import model
    return Session(model.engine)


def init_db():
    from db_interactor import model
    model.metadata_obj.create_all(model.engine)