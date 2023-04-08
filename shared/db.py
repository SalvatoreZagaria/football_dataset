import os


def get_db_url():
    return f'postgresql://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@{os.getenv("DB_HOST")}:{os.getenv("DB_PORT", 5432)}/{os.getenv("DB_NAME")}'
