import json

def evaluate_opportunity(tick, redis_client) -> bool:
    """
    Evaluates a market anomaly deterministically using macro baselines and sticker valuations.
    """
    # 1. Fetch baseline from Redis
    baseline_raw = redis_client.get(f"baseline:{tick.market_hash_name}")
    if not baseline_raw:
        return False
        
    baseline = json.loads(baseline_raw)
    support_floor_cents = baseline.get("support_floor_cents", 0)
    latest_price_cents = baseline.get("latest_price_cents", 0)
    
    # 2. Base Discount Check
    if tick.price_cents <= support_floor_cents:
        return True
        
    # 3. Sticker Premium Valuation
    total_sticker_value_cents = 0
    
    if hasattr(tick, "stickers") and tick.stickers:
        for sticker in tick.stickers:
            name = sticker.get("name")
            if name:
                # Fetch sticker price from Redis hashmap
                price_str = redis_client.hget("sticker_prices", name)
                if price_str:
                    try:
                        total_sticker_value_cents += int(price_str)
                    except ValueError:
                        pass
                        
    # 4. Sticker Premium Percentage (SP%) Logic
    if total_sticker_value_cents > 10000:  # Minimum $100 sticker value required to care
        premium_cents = tick.price_cents - latest_price_cents
        
        # If the premium is negative, we are getting stickers for free below base price
        if premium_cents <= 0:
            return True
            
        sp_percentage = (premium_cents / total_sticker_value_cents) * 100
        
        if sp_percentage <= 3.0:
            return True
            
    return False
