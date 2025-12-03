import pandas as pd

def load_crp_payments(path):
    return pd.read_excel(path, sheet_name="payment_schedules_CRP_2025")

def load_crp_rules(path):
    return pd.read_excel(path, sheet_name="CRP_eligibility_rules")
