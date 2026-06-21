class Config:
    SECRET_KEY = "your_secret_key"
    # SQLALCHEMY_DATABASE_URI = "sqlite:///trading.db"
    SQLALCHEMY_DATABASE_URI = "postgresql://ryantan:@localhost:5432/trading_system"
    SQLALCHEMY_TRACK_MODIFICATIONS = False


class DevelopmentConfig(Config):
    DEBUG = True


class TestingConfig(Config):
    TESTING = True
    SQLALCHEMY_DATABASE_URI = "sqlite:///test.db"


class ProductionConfig(Config):
    DEBUG = False
