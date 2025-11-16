from pydantic_settings import BaseSettings
from pathlib import Path
from dotenv import load_dotenv

# .env を読み込む
load_dotenv()
    
class Settings(BaseSettings):
    HOST: str
    PORT: int
    USER: str
    PASSWORD: str
    DATABASE: str

    class Config:
        env_file = str(Path(__file__).parent / ".env") 
        env_file_encoding =  "utf-8"

settings = Settings()
