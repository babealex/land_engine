def load_crp_payments(path):
    return pd.read_excel(path, sheet_name="payment_schedules_CRP_2025")


def estimate_crp_revenue_for_practice(parcel, crp_code, lookup):
    df = lookup
    row = df[
        (df["state"] == parcel["state"]) &
        (df["county"] == parcel["county"]) &
        (df["crp_practice_code"] == crp_code)
    ]

    if row.empty:
        return {"annual_payment": 0.0, "total_contract_payment": 0.0}

    rate = float(row.iloc[0]["base_rental_rate"])
    contract = int(row.iloc[0]["contract_length_years"])

    acres = float(parcel["acres"])

    annual = rate * acres
    total = annual * contract

    return {
        "annual_payment": annual,
        "total_contract_payment": total
    }


def estimate_crp_revenue(parcel, eligible_codes, crp_lookup):
    results = {}
    for code in eligible_codes:
        results[code] = estimate_crp_revenue_for_practice(parcel, code, crp_lookup)
    return results
