import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from multidict import CIMultiDict
from shared_utils.dmarket import (
    CUSTOMIZED_FEES_PATH,
    LAST_SALES_PATH,
    SALE_TYPE_OFFER,
    SALE_TYPE_TARGET,
    SIGNATURE_PREFIX,
    DMarketAPIError,
    DMarketClient,
    DMarketKeys,
    DMarketSale,
    FeeSchedule,
    ReducedFee,
    cents_to_int,
    dollars_to_cents,
    encode_path,
    encode_query,
    fraction_to_bps,
    parse_fee_page,
    parse_sale,
    parse_sales,
    phase_version,
    retry_after_seconds,
    sale_keys,
    signed_headers,
)

# Sanitized live responses from 2026-10-25 (docs/dmarket_api.md).
FIXTURES = Path(__file__).resolve().parents[3] / "apps" / "listener" / "tests" / "fixtures"

SEED = bytes(range(32))


def make_keys() -> tuple[DMarketKeys, bytes]:
    private_key = Ed25519PrivateKey.from_private_bytes(SEED)
    public = private_key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    # DMarket's secret key is the seed followed by the public key.
    return DMarketKeys.from_hex(public.hex(), (SEED + public).hex()), public


def utc_naive(seconds: int) -> datetime:
    return datetime.fromtimestamp(seconds, UTC).replace(tzinfo=None)


def load_fixture(name: str):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_keys_take_the_seed_from_the_64_byte_secret():
    keys, public = make_keys()
    assert keys.public_key == public.hex()
    assert keys.private_key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw) == public


@pytest.mark.parametrize("secret", ["zz", "00" * 31, "00" * 48])
def test_unusable_secret_keys_are_rejected(secret):
    with pytest.raises(ValueError, match="DMARKET_SECRET_KEY"):
        DMarketKeys.from_hex("ab", secret)


def test_keys_from_env(monkeypatch):
    keys, public = make_keys()
    monkeypatch.setenv("DMARKET_PUBLIC_KEY", public.hex())
    monkeypatch.setenv("DMARKET_SECRET_KEY", (SEED + public).hex())
    assert DMarketKeys.from_env().public_key == keys.public_key
    monkeypatch.delenv("DMARKET_SECRET_KEY")
    with pytest.raises(ValueError, match="must both be set"):
        DMarketKeys.from_env()


def test_query_and_path_are_percent_encoded_like_the_reference_client():
    query = encode_query({"gameId": "a8db", "title": "AK-47 | Redline (Field-Tested)", "limit": 20})
    assert query == "?gameId=a8db&title=AK-47%20%7C%20Redline%20%28Field-Tested%29&limit=20"
    assert encode_query({}) == ""
    assert encode_path("/marketplace-api/v1/targets-by-title/a8db/AK-47 | Redline") == (
        "/marketplace-api/v1/targets-by-title/a8db/AK-47%20%7C%20Redline"
    )


def test_signature_covers_method_decoded_path_query_body_and_time():
    keys, public = make_keys()
    query = encode_query({"gameId": "a8db", "title": "★ Karambit | Doppler (Factory New)"})
    headers = signed_headers(keys, "get", LAST_SALES_PATH, query, "", 1792935756)

    assert headers["X-Api-Key"] == public.hex()
    assert headers["X-Sign-Date"] == "1792935756"
    assert headers["X-Request-Sign"].startswith(SIGNATURE_PREFIX)
    signature = bytes.fromhex(headers["X-Request-Sign"].removeprefix(SIGNATURE_PREFIX))
    unsigned = f"GET{LAST_SALES_PATH}{query}1792935756".encode()
    # Raises InvalidSignature when the signed text differs.
    keys.private_key.public_key().verify(signature, unsigned)


@pytest.mark.parametrize(
    ("value", "expected"),
    [("24.56", 2456), ("1550", 155000), ("0.18", 18), ("1135.01", 113501), ("0.005", 1), (12, 1200)],
)
def test_sale_prices_are_dollars(value, expected):
    assert dollars_to_cents(value) == expected


@pytest.mark.parametrize("value", ["0", "-1", "abc", None, 1.5, True, "NaN", "Infinity"])
def test_unusable_sale_prices_are_none(value):
    assert dollars_to_cents(value) is None


@pytest.mark.parametrize(("value", "expected"), [("21345", 21345), ("0", None), ("12.5", None), ("x", None), (None, None)])
def test_listing_prices_are_cents(value, expected):
    assert cents_to_int(value) == expected


@pytest.mark.parametrize(
    ("slug", "expected"),
    [("phase-2", "Phase 2"), ("black-pearl", "Black Pearl"), ("ruby", "Ruby"), ("", None), (None, None), ("  ", None)],
)
def test_phase_slugs_become_skinport_versions(slug, expected):
    assert phase_version(slug) == expected


def test_recorded_sales_parse():
    recorded = load_fixture("dmarket_last_sales.json")
    sales = parse_sales("AWP | Asiimov (Field-Tested)", recorded["asiimov"])

    assert len(sales) == len(recorded["asiimov"]["sales"]) == 20
    assert {sale.sale_type for sale in sales} == {SALE_TYPE_OFFER, SALE_TYPE_TARGET}
    assert all(sale.market_hash_name == "AWP | Asiimov (Field-Tested)" for sale in sales)
    assert [sale.sold_at for sale in sales] == sorted((sale.sold_at for sale in sales), reverse=True)
    first = recorded["asiimov"]["sales"][0]
    assert sales[0].sold_at == utc_naive(int(first["date"]))
    assert sales[0].float_value == first["offerAttributes"]["floatValue"]


def test_recorded_doppler_sales_carry_the_phase_in_the_name():
    sales = parse_sales("★ Karambit | Doppler (Factory New)", load_fixture("dmarket_last_sales.json")["karambit_doppler_fn"])
    names = {sale.market_hash_name for sale in sales}
    assert names
    assert all(name.startswith("★ Karambit | Doppler (Phase ") and name.endswith(") (Factory New)") for name in names)


@pytest.mark.parametrize(
    "entry",
    [
        {"price": "1", "date": "1792935756", "txOperationType": "Unknown"},
        {"price": "", "date": "1792935756", "txOperationType": "Offer"},
        {"price": "1", "date": "0", "txOperationType": "Offer"},
        "not a sale",
    ],
)
def test_unusable_sales_are_dropped(entry):
    assert parse_sale("X", entry) is None


def test_sale_without_attributes_has_no_wear_or_pattern():
    sale = parse_sale("Revolution Case", {"price": "0.18", "date": "1792895621", "txOperationType": "Offer"})
    assert sale is not None
    assert (sale.float_value, sale.paint_seed, sale.phase) == (None, None, None)


def test_parse_sales_ignores_a_malformed_payload():
    assert parse_sales("X", {"sales": "nope"}) == []
    assert parse_sales("X", None) == []


def _sale(price_cents: int, second: int = 1792895621, float_value: float | None = None) -> DMarketSale:
    return DMarketSale(
        title="Revolution Case",
        market_hash_name="Revolution Case",
        sold_at=utc_naive(second),
        price_cents=price_cents,
        sale_type=SALE_TYPE_OFFER,
        float_value=float_value,
        paint_seed=None,
        phase=None,
    )


def test_identical_sales_in_one_second_get_their_own_keys():
    keys = sale_keys([_sale(18), _sale(18), _sale(19), _sale(18)])
    assert len(set(keys)) == 4
    assert keys[0].endswith("#0") and keys[1].endswith("#1") and keys[3].endswith("#2")
    # Reading the same sales again gives the same keys, so a rerun inserts nothing new.
    assert sale_keys([_sale(18), _sale(18), _sale(19), _sale(18)]) == keys


def test_sale_keys_tell_floats_apart():
    assert sale_keys([_sale(500, float_value=0.1)]) != sale_keys([_sale(500, float_value=0.2)])


@pytest.mark.parametrize(
    ("value", "expected"), [("0.02", 200), ("0.1", 1000), ("0", 0), ("1", None), ("-0.1", None), (None, None)]
)
def test_fee_fractions(value, expected):
    assert fraction_to_bps(value) == expected


def test_recorded_fee_page_parses():
    default_bps, reduced = parse_fee_page(load_fixture("dmarket_customized_fees.json"))
    assert default_bps == 1000
    assert len(reduced) == 10
    assert {fee.fee_bps for fee in reduced.values()} == {200}
    assert all(fee.max_price_cents and fee.max_price_cents > 0 for fee in reduced.values())


def test_fee_page_skips_unusable_entries():
    default_bps, reduced = parse_fee_page(
        {
            "reducedFees": [
                {"title": "", "fraction": "0.02", "maxPrice": 0},
                {"title": "A", "fraction": "x"},
                {"title": "B", "fraction": "0.02", "maxPrice": 0},
                "nope",
            ]
        }
    )
    assert default_bps is None
    assert reduced == {"B": ReducedFee(fee_bps=200, max_price_cents=None)}
    assert parse_fee_page("nope") == (None, {})


def test_fee_schedule_applies_the_reduced_fee_up_to_its_cap():
    schedule = FeeSchedule(default_fee_bps=1000, reduced={"A": ReducedFee(200, 5000), "B": ReducedFee(200, None)})
    assert schedule.fee_bps("A", 5000) == 200
    assert schedule.fee_bps("A", 5001) == 1000
    assert schedule.fee_bps("B", 10**9) == 200
    assert schedule.fee_bps("C", 100) == 1000


@pytest.mark.parametrize(("headers", "expected"), [({"Retry-After": "1"}, 1.0), ({"Retry-After": "x"}, None), ({}, None)])
def test_retry_after(headers, expected):
    assert retry_after_seconds(headers) == expected


class FakeResponse:
    def __init__(self, status: int, payload, headers: dict[str, str] | None = None) -> None:
        self.status = status
        self.payload = payload
        self.headers = CIMultiDict(headers or {"x-ratelimit-remaining-second": "29"})

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False

    async def json(self, content_type=None):
        return self.payload

    async def text(self):
        return json.dumps(self.payload)


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = list(responses)
        self.requests: list[tuple[str, dict[str, str]]] = []

    def get(self, url, headers):
        self.requests.append((str(url), headers))
        return self.responses.pop(0)


class FakeTime:
    """A monotonic clock that only moves when the client sleeps."""

    def __init__(self) -> None:
        self.now = 100.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(round(seconds, 6))
        self.now += seconds


def make_client(responses, rate=10.0, **kwargs):
    keys, _public = make_keys()
    session = FakeSession(responses)
    clock = FakeTime()
    client = DMarketClient(
        keys, session, rate, monotonic=clock.monotonic, wall_clock=lambda: 1792935756.4, sleep=clock.sleep, **kwargs
    )
    return client, session, clock


def test_client_rejects_a_non_positive_rate():
    keys, _public = make_keys()
    with pytest.raises(ValueError):
        DMarketClient(keys, FakeSession([]), 0)


@pytest.mark.asyncio
async def test_client_sends_a_signed_request_to_the_encoded_url():
    client, session, _clock = make_client([FakeResponse(200, {"sales": []})])
    sales, entries = await client.last_sales("AK-47 | Redline (Field-Tested)", offset=40)

    assert (sales, entries) == ([], 0)
    url, headers = session.requests[0]
    assert url == (
        "https://api.dmarket.com/trade-aggregator/v1/last-sales"
        "?gameId=a8db&title=AK-47%20%7C%20Redline%20%28Field-Tested%29&limit=20&offset=40"
    )
    assert headers["X-Sign-Date"] == "1792935756"
    assert headers["X-Request-Sign"].startswith(SIGNATURE_PREFIX)


@pytest.mark.asyncio
async def test_client_paces_requests_to_its_share_of_the_limit():
    client, _session, clock = make_client([FakeResponse(200, {}) for _ in range(3)], rate=4.0)
    for _ in range(3):
        await client.get(LAST_SALES_PATH)
    assert clock.sleeps == [0.25, 0.25]


@pytest.mark.asyncio
async def test_client_waits_a_second_when_none_are_left_this_second():
    exhausted = FakeResponse(200, {}, {"X-RateLimit-Remaining-Second": "0"})
    client, _session, clock = make_client([exhausted, FakeResponse(200, {})], rate=10.0)
    await client.get(LAST_SALES_PATH)
    await client.get(LAST_SALES_PATH)
    assert clock.sleeps == [1.0]


@pytest.mark.asyncio
async def test_client_retries_a_429_after_retry_after_and_doubles_on_repeats():
    recorded = load_fixture("dmarket_rate_limited.json")
    refused = [FakeResponse(429, recorded["body"], recorded["headers"]) for _ in range(3)]
    refusals = []
    responses = [*refused, FakeResponse(200, {"ok": True})]
    client, session, clock = make_client(responses, on_rate_limited=lambda: refusals.append(1))

    assert await client.get(LAST_SALES_PATH) == {"ok": True}
    assert len(session.requests) == 4
    assert len(refusals) == 3
    # Retry-After is 1 second; repeated refusals in a row double the wait. Pacing sleeps are 0.1 s.
    assert [sleep for sleep in clock.sleeps if sleep >= 1] == [1.0, 2.0, 4.0]


@pytest.mark.asyncio
async def test_client_gives_up_after_max_retries():
    refused = [FakeResponse(429, {}, {"Retry-After": "1"}) for _ in range(3)]
    client, _session, _clock = make_client(refused, max_retries=2)
    with pytest.raises(DMarketAPIError, match="still rate limited"):
        await client.get(LAST_SALES_PATH)


@pytest.mark.asyncio
async def test_client_raises_on_other_errors():
    client, _session, _clock = make_client([FakeResponse(401, {"code": "Unauthorized"})])
    with pytest.raises(DMarketAPIError) as raised:
        await client.get(LAST_SALES_PATH)
    assert raised.value.status == 401


@pytest.mark.asyncio
async def test_fee_schedule_reads_every_page():
    first = {"defaultFee": {"fraction": "0.1"}, "reducedFees": [{"title": f"T{i}", "fraction": "0.02"} for i in range(100)]}
    second = {"defaultFee": {"fraction": "0.1"}, "reducedFees": [{"title": "Last", "fraction": "0.02", "maxPrice": 900}]}
    client, session, _clock = make_client([FakeResponse(200, first), FakeResponse(200, second)])

    schedule = await client.fee_schedule()

    assert schedule.default_fee_bps == 1000
    assert len(schedule.reduced) == 101
    assert schedule.reduced["Last"] == ReducedFee(200, 900)
    assert session.requests[1][0].startswith(f"https://api.dmarket.com{CUSTOMIZED_FEES_PATH}")
    assert "offset=100" in session.requests[1][0]


@pytest.mark.asyncio
async def test_fee_schedule_falls_back_to_the_worst_case_default():
    client, _session, _clock = make_client([FakeResponse(200, {"reducedFees": []})])
    assert (await client.fee_schedule()).default_fee_bps == 1000


def test_pacing_is_shared_by_concurrent_requests():
    async def run():
        client, _session, clock = make_client([FakeResponse(200, {}) for _ in range(3)], rate=2.0)
        await asyncio.gather(*(client.get(LAST_SALES_PATH) for _ in range(3)))
        return clock.sleeps

    assert asyncio.run(run()) == [0.5, 0.5]
