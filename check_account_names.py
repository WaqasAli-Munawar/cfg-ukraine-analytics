# Save as check_account_names.py and run it
from src.services.onelake_data_service import OneLakeDataService

service = OneLakeDataService()

# Get account master data
accounts_df = service.get_accounts()

print("=" * 60)
print("ACCOUNT MASTER - COLUMNS:")
print(accounts_df.columns.tolist())
print("=" * 60)

print("\nSAMPLE ACCOUNT RECORDS (first 20):")
print(accounts_df.head(20).to_string())

print("\n" + "=" * 60)
print("SEARCHING FOR EBITDA/EARNINGS/PROFIT/REVENUE:")
for col in accounts_df.columns:
    if accounts_df[col].dtype == 'object':
        mask = accounts_df[col].str.lower().str.contains('ebitda|earnings|profit|revenue|income|margin', na=False)
        matches = accounts_df[mask]
        if len(matches) > 0:
            print(f"\nFound in column '{col}':")
            print(matches[[col]].head(10).to_string())