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
    assert first.venue == "skinport"


def test_get_scraper_is_case_insensitive():
    first = ScraperFactory.get_scraper("skinport")
    second = ScraperFactory.get_scraper("SKINPORT")

    assert first is second


def test_get_scraper_builds_dmarket_from_its_keys(monkeypatch):
    from scrapers.dmarket import DMarketScraper

    monkeypatch.setenv("DMARKET_PUBLIC_KEY", "ab" * 32)
    monkeypatch.setenv("DMARKET_SECRET_KEY", "01" * 64)

    scraper = ScraperFactory.get_scraper("dmarket")

    assert isinstance(scraper, DMarketScraper)
    assert scraper.venue == "dmarket"
    assert not scraper.polls_rest


def test_get_scraper_dmarket_without_keys_fails_at_startup(monkeypatch):
    monkeypatch.delenv("DMARKET_PUBLIC_KEY", raising=False)
    monkeypatch.delenv("DMARKET_SECRET_KEY", raising=False)

    with pytest.raises(ValueError, match="must both be set"):
        ScraperFactory.get_scraper("dmarket")


def test_get_scraper_unknown_venue_raises():
    with pytest.raises(ValueError, match="Unsupported venue requested: 'csfloat'"):
        ScraperFactory.get_scraper("csfloat")
