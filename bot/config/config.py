from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_ignore_empty=True)

    API_ID: int
    API_HASH: str

    USE_RANDOM_DELAY_IN_RUN: bool = False
    RANDOM_DELAY_IN_RUN: list[int] = [0, 600]

    REF_ID: str = 'wBuWS75s'

    JOIN_TG_CHANNELS: bool = True
    MUTE_AND_ARCHIVE_TG_CHANNELS: bool = True

    AUTO_TASK: bool = True

    SLEEP_TIME: list[int] = [37200, 67800]
    USE_PROXY: bool = False

settings = Settings()
