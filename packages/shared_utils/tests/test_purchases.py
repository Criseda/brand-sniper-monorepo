from datetime import date

from shared_utils import listing_purchase_key, purchase_keys, snapshot_purchase_key

DAY = date(2026, 10, 13)


def test_a_listing_is_keyed_by_venue_and_listing_id():
    key, snapshot_key = purchase_keys("waxpeer", "Item", 1000, "58903454", DAY)

    assert key == listing_purchase_key("waxpeer", "58903454")
    assert key != listing_purchase_key("skinport", "58903454")
    assert snapshot_key == snapshot_purchase_key("waxpeer", "Item", 1000, DAY)


def test_a_rest_snapshot_is_keyed_by_venue_item_price_and_utc_day():
    key, snapshot_key = purchase_keys("skinport", "Item", 1000, None, DAY)

    assert key == snapshot_key
    assert key == purchase_keys("skinport", "Item", 1000, None, date(2026, 10, 13))[0]
    assert key != purchase_keys("skinport", "Item", 1001, None, DAY)[0]
    assert key != purchase_keys("skinport", "Item", 1000, None, date(2026, 10, 14))[0]
    assert key != purchase_keys("waxpeer", "Item", 1000, None, DAY)[0]
    assert key != purchase_keys("skinport", "Other", 1000, None, DAY)[0]
