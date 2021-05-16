from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import logging


class GenericSqlAlchemyDao(object):
    # constructor
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)

        url = 'postgresql://{username}:{password}@{host}:{port}/{db_name}'.format(
            username='postgres',
            password='postgres',
            port='5432',
            host='localhost',
            db_name='stocks'
        )
        engine = create_engine(url, client_encoding='utf8', echo=False)
        Session = sessionmaker()
        Session.configure(bind=engine)
        self.session = Session()

    def __del__(self):
        self.session.close()

    def add_record(self, record, rec_count=0):
        self.session.add(record)
        return record

    def merge_record(self, record):
        returned_val = self.session.merge(record)
        return returned_val

    def find_entity_by_attr_val(self, attr, value, entity):
        result = self.session.query(entity).filter(attr == value).all()
        return result

    def find_all_entities(self, entity_type):
        result = self.session.query(entity_type).all()
        return result

    def bulk_save(self, bulk_list):
        # self.session.bulk_save_objects(bulk_list)
        self.session.add_all(bulk_list)

    def commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()
