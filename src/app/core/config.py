from pydantic import Field
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    MONGODB_URI: str = Field(..., env="MONGODB_URI")
    MONGODB_DB: str = Field(..., env="MONGODB_DB")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()