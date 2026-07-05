from scrapers.base import BaseScraper
from scrapers.skinport import SkinportScraper


class ScraperFactory:
    """Central registry mapping marketplace identifiers to their respective client architectures.
    Uses singleton caching to ensure shared state (history cache, cooldowns) across all consumers."""

    _registry: dict[str, type[BaseScraper]] = {
        "skinport": SkinportScraper,
        # To add a new platform in the future, you simply drop it here:
        # "dmarket": DMarketScraper,
        # "bitskins": BitSkinsScraper,
    }

    _instances: dict[str, BaseScraper] = {}

    @classmethod
    def get_scraper(cls, platform_id: str) -> BaseScraper:
        """Returns a cached scraper instance, creating it on first access."""
        key = platform_id.lower()
        if key not in cls._instances:
            scraper_class = cls._registry.get(key)
            if not scraper_class:
                raise ValueError(f"Unsupported trading platform driver requested: '{platform_id}'")
            cls._instances[key] = scraper_class()
        return cls._instances[key]
