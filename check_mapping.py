# Save as check_mapping.py
from src.services.onelake_data_service import OneLakeDataService

service = OneLakeDataService()

# Get both datasets
accounts_df = service.get_accounts()
actual_df = service.get_actual_data()

print("=" * 60)
print("ACCOUNT CODES IN ACTUAL DATA (sample):")
print("=" * 60)
actual_accounts = actual_df['Account'].unique()[:10]
print(actual_accounts)

print("\n" + "=" * 60)
print("CHECKING IF CODES EXIST IN ACCOUNT MASTER:")
print("=" * 60)

# Check if any actual account codes exist in the account master
for code in actual_accounts[:5]:
    match = accounts_df[accounts_df['Account'] == code]
    if len(match) > 0:
        print(f"✓ {code} -> Parent: {match.iloc[0]['Parent']}")
    else:
        print(f"✗ {code} -> NOT FOUND in account master")

print("\n" + "=" * 60)
print("FINDING CHILD ACCOUNTS UNDER 'FCCS_Operating Income':")
print("=" * 60)
children = accounts_df[accounts_df['Parent'] == 'FCCS_Operating Income']
print(children[['Account', 'Parent']].to_string())

print("\n" + "=" * 60)
print("FINDING ALL ACCOUNTS UNDER 'FCCS_Gross Profit':")
print("=" * 60)
children = accounts_df[accounts_df['Parent'] == 'FCCS_Gross Profit']
print(children[['Account', 'Parent']].to_string())