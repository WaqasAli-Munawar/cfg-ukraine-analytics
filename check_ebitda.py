# Save as check_ebitda.py
from src.services.onelake_data_service import OneLakeDataService

service = OneLakeDataService()
accounts_df = service.get_accounts()

print("=" * 60)
print("SEARCHING FOR EBITDA, OPERATING, GROSS, PROFIT:")
print("=" * 60)

search_terms = ['ebitda', 'operating', 'gross', 'profit', 'margin', 'revenue', 'sales', 'income statement', 'p&l']

for term in search_terms:
    mask = accounts_df['Account'].str.lower().str.contains(term, na=False)
    matches = accounts_df[mask]
    if len(matches) > 0:
        print(f"\n'{term.upper()}' matches:")
        for _, row in matches.iterrows():
            print(f"  • {row['Account']} (Parent: {row['Parent']})")

# Also check what parent categories exist
print("\n" + "=" * 60)
print("TOP-LEVEL CATEGORIES (no parent):")
print("=" * 60)
top_level = accounts_df[accounts_df['Parent'].isna()]
for _, row in top_level.iterrows():
    print(f"  • {row['Account']}")