# Electricity plan cost simulator

def calculate_annual_cost(peak_rate_cents, offpeak_rate_cents, daily_charge_dollars, 
                          annual_peak_kwh, annual_offpeak_kwh):
    """
    Calculates the annual electricity cost.
    """
    peak_cost = (peak_rate_cents / 100) * annual_peak_kwh
    offpeak_cost = (offpeak_rate_cents / 100) * annual_offpeak_kwh
    daily_cost = daily_charge_dollars * 365
    return round(peak_cost + offpeak_cost + daily_cost, 2)

# Define plans (customise these)
plans = [
        {
        "name": "Current Plan",
        "peak_rate": 30.36*(1-0.03),  # cents per kWh
        "offpeak_rate": 23.76*(1-0.03), # cents per kWh
        "daily_charge": 1.0120*(1-0.03) # dollars per day
    }
    ,
    {
        "name": "New tariff Plan",
        "peak_rate": 56.551,
        "offpeak_rate": 29.663,
        "daily_charge": 1.2804
    }
    ,
    {
        "name": "Energy Locals New Plan",
        "peak_rate": 54.00,
        "offpeak_rate": 27.5,
        "daily_charge": 1.835
    }
]

# User consumption inputs
annual_peak_kwh = 21.47*11   # e.g. 2600 kWh/year during peak
annual_offpeak_kwh = 54.91*11   # e.g. 1300 kWh/year during off-peak

# Calculate and compare
print("Annual Cost Comparison - Electricity:\n")
for plan in plans:
    cost = calculate_annual_cost(
        peak_rate_cents=plan["peak_rate"],
        offpeak_rate_cents=plan["offpeak_rate"],
        daily_charge_dollars=plan["daily_charge"],
        annual_peak_kwh=annual_peak_kwh,
        annual_offpeak_kwh=annual_offpeak_kwh
    )
    print(f"{plan['name']}: ${cost}")
print(f"Estimated annual peak consumption: {annual_peak_kwh}[kWh]")
print(f"Estimated annual off-peak consumption: {annual_offpeak_kwh}[kWh]")