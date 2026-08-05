import pytest
from scrapers.factory import ScraperFactory
from scrapers.skinport import SkinportScraper


@pytest.fixture(autouse=True)
def clear_factory_state():
    ScraperFactory._instances.clear()
    yield
    ScraperFactory._instances.clear()


def test_get_scraper_returns_singleton_instance():
    first = ScraperFactory.get_scraper("skinport")
    second = ScraperFactory.get_scraper("skinport")

    assert first is second
    assert isinstance(first, SkinportScraper)
    assert first.platform_name == "skinport"


def test_get_scraper_is_case_insensitive():
    first = ScraperFactory.get_scraper("skinport")
    second = ScraperFactory.get_scraper("SKINPORT")

    assert first is second


def test_get_scraper_unknown_platform_raises():
    with pytest.raises(ValueError, match="Unsupported trading platform driver requested: 'dmarket'"):
        ScraperFactory.get_scraper("dmarket")
