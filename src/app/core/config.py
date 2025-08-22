from pydantic import Field
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    MONGODB_URI: str = Field(..., alias="MONGODB_URI")
    MONGODB_DB: str = Field(..., alias="MONGODB_DB")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"

settings = Settings()