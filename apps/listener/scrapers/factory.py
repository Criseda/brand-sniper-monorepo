from scrapers.base import BaseScraper
from scrapers.skinport import SkinportScraper
from scrapers.waxpeer import WaxpeerScraper


class ScraperFactory:
    """Central registry mapping marketplace identifiers to their respective client architectures.
    Uses singleton caching to ensure shared state (history cache, cooldowns) across all consumers."""

    _registry: dict[str, type[BaseScraper]] = {
        "skinport": SkinportScraper,
        "waxpeer": WaxpeerScraper,
    }

    _instances: dict[str, BaseScraper] = {}

    @classmethod
    def get_scraper(cls, venue: str) -> BaseScraper:
        """Returns a cached scraper instance, creating it on first access."""
        key = venue.lower()
        if key not in cls._instances:
            scraper_class = cls._registry.get(key)
            if not scraper_class:
                raise ValueError(f"Unsupported venue requested: '{venue}'")
            cls._instances[key] = scraper_class()
        return cls._instances[key]
