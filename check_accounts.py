# Save as: check_accounts.py and run it
from src.services.onelake_data_service import OneLakeDataService

service = OneLakeDataService()
df = service.get_actual_data()

print("=" * 60)
print("AVAILABLE COLUMNS:")
print(df.columns.tolist())
print("=" * 60)

print("\nSAMPLE ACCOUNT NAMES (first 30):")
accounts = df['Account'].unique()
for i, acc in enumerate(accounts[:30]):
    print(f"  {i+1}. {acc}")

print(f"\nTotal unique accounts: {len(accounts)}")

# Search for EBITDA-like accounts
print("\n" + "=" * 60)
print("SEARCHING FOR 'EBITDA' OR 'EARNINGS' OR 'OPERATING':")
for acc in accounts:
    acc_lower = str(acc).lower()
    if any(kw in acc_lower for kw in ['ebitda', 'earnings', 'operating', 'profit', 'income']):
        print(f"  ✓ {acc}")