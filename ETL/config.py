from environs import Env

env = Env()
env.read_env('.env')


class Config:
    DRIVER = env('DRIVER')
    DB_HOST = env('DB_HOST')
    DB_NAME = env('DB_NAME')
    DB_USER = env('DB_USER')
    DB_PASS = env('DB_PASS')
