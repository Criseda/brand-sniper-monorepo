from typing import Dict, Type
from scrapers.base import BaseScraper
from scrapers.skinport import SkinportScraper

class ScraperFactory:
    """Central registry mapping marketplace identifiers to their respective client architectures."""
    
    _registry: Dict[str, Type[BaseScraper]] = {
        "skinport": SkinportScraper,
        # To add a new platform in the future, you simply drop it here:
        # "dmarket": DMarketScraper,
        # "bitskins": BitSkinsScraper,
    }

    @classmethod
    def get_scraper(cls, platform_id: str) -> BaseScraper:
        """Instantiates and returns the requested market client driver."""
        scraper_class = cls._registry.get(platform_id.lower())
        if not scraper_class:
            raise ValueError(f"Unsupported trading platform driver requested: '{platform_id}'")
        return scraper_class()