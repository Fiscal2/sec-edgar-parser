from typing import List, Optional
from pydantic_settings import BaseSettings
from pydantic import Field

class Settings(BaseSettings):
    supabase_url: Optional[str] = Field(default=None, alias="SUPABASE_URL")
    supabase_key: Optional[str] = Field(default=None, alias="SUPABASE_KEY")
    edgar_base_url: str = "https://www.sec.gov/Archives/"
    edgar_index_url: str = "https://www.sec.gov/Archives/edgar/full-index/"
    request_timeout: int = 30
    max_retries: int = 3
    cache_ttl: int = 3600
    user_agent: str = "SEC-EDGAR-Parser/1.0"
    
    requests_per_second: float = 10.0
    delay_between_requests: float = 0.1
    
    supported_forms: List[str] = Field(
        default=["10-K", "10-K/A", "10-Q", "10-Q/A", "20-F"],
        description="Supported SEC form types"
    )
    
    income_statement_names: List[str] = Field(
        default=[
            "consolidated statements of income",
            "consolidated statement of income",
            "consolidated income statements",
            "consolidated statements of operations",
            "income statements",
            "consolidated statement of operations",
            "consolidated statement of earnings",
            "consolidated statements of earnings",
            "consolidated statements of operations and comprehensive income (loss)",
            "consolidated statements of operations and comprehensive income",
            "condensed consolidated statements of income (unaudited)",
            "condensed consolidated statements of income",
            "condensed consolidated statements of operations (unaudited)",
            "condensed consolidated statements of operations",
            "condensed consolidated statement of earnings (unaudited)",
            "condensed consolidated statement of earnings",
            "condensed statements of income",
            "condensed statements of operations",
            "condensed statements of operations and comprehensive loss",
            "consolidated statements of comprehensive income",
            "statements of consolidated income",
            "consolidated statements of profit or loss and other comprehensive income",
        ]
    )
    
    balance_sheet_names: List[str] = Field(
        default=[
            "consolidated balance sheets",
            "consolidated balance sheet",
            "consolidated statement of financial position",
            "consolidated statements of financial position",
            "condensed consolidated statement of financial position (current period unaudited)",
            "condensed consolidated statement of financial position (unaudited)",
            "condensed consolidated statement of financial position",
            "condensed consolidated balance sheets (current period unaudited)",
            "condensed consolidated balance sheets (unaudited)",
            "condensed consolidated balance sheets",
            "condensed balance sheets",
            "balance sheets",
        ]
    )
    
    cash_flow_names: List[str] = Field(
        default=[
            "consolidated statements of cash flows",
            "consolidated statement of cash flows",
            "consolidated statements of cash flows (unaudited)",
            "condensed consolidated statements of cash flows (unaudited)",
            "condensed consolidated statements of cash flows",
            "condensed statements of cash flows",
            "cash flows statements",
            "statements of consolidated cash flows"
        ]
    )
    
    class Config:
        env_file = ".env"
        env_prefix = "EDGAR_"

settings = Settings()
