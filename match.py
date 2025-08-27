import os
import pandas as pd
from rapidfuzz import process
from datetime import datetime

# ---------------- Configuration ---------------- #
CONTACTS_XLSX = 'contacts.xlsx'
CONTACTS_CSV = 'contacts.csv'
ACCOUNT_NAMES_XLSX = 'account_names.xlsx'
OUTPUT_FILE = 'account_contact_matches.xlsx'
MIN_SCORE = 0  # adjust if you want to filter low scores later

# ---------------- Helpers ---------------- #

def load_contacts():
    if os.path.exists(CONTACTS_XLSX):
        df = pd.read_excel(CONTACTS_XLSX)
        source = CONTACTS_XLSX
    elif os.path.exists(CONTACTS_CSV):
        df = pd.read_csv(CONTACTS_CSV)
        source = CONTACTS_CSV
    else:
        raise FileNotFoundError(f"Neither {CONTACTS_XLSX} nor {CONTACTS_CSV} found.")
    print(f"Loaded contacts from {source} rows={len(df)}")
    return df


def load_accounts():
    if not os.path.exists(ACCOUNT_NAMES_XLSX):
        raise FileNotFoundError(f"Required file {ACCOUNT_NAMES_XLSX} not found. Place it next to this script.")
    accounts_df = pd.read_excel(ACCOUNT_NAMES_XLSX)
    # Normalize column name possibilities
    if 'Username' not in accounts_df.columns:
        # Try to find a similar column
        lower_map = {c.lower(): c for c in accounts_df.columns}
        for candidate in ['username', 'user', 'account', 'name']:
            if candidate in lower_map:
                accounts_df.rename(columns={lower_map[candidate]: 'Username'}, inplace=True)
                break
    if 'Username' not in accounts_df.columns:
        raise ValueError("Could not find a 'Username' column in account_names.xlsx")
    accounts_df['Username'] = accounts_df['Username'].astype(str).str.strip()
    print("Columns in accounts_df:", accounts_df.columns.tolist())
    return accounts_df


def build_username(df: pd.DataFrame) -> pd.DataFrame:
    # Define candidate name columns (only use those that exist)
    name_columns = [c for c in ['First Name', 'Middle Name', 'Last Name', 'Organization Name'] if c in df.columns]
    if not name_columns:
        raise ValueError("No expected name columns found in contacts file.")
    df[name_columns] = df[name_columns].fillna("")
    df['username'] = df[name_columns].apply(
        lambda row: " ".join(part for part in (str(v).strip() for v in row) if part), axis=1
    )
    df.loc[df['username'] == '', 'username'] = 'N/A'
    return df


def find_number_column(df: pd.DataFrame) -> str:
    possible_number_cols = ['Phone 1 - Value', 'Phone', 'Phone Number', 'Mobile']
    for col in possible_number_cols:
        if col in df.columns:
            return col
    raise ValueError("No column found for phone numbers. Checked: " + ", ".join(possible_number_cols))


def safe_write_excel(df: pd.DataFrame, filename: str):
    try:
        df.to_excel(filename, index=False)
        print(f"Wrote {filename}")
    except PermissionError:
        alt = f"{os.path.splitext(filename)[0]}_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
        print(f"PermissionError writing {filename}. Using {alt} instead.")
        df.to_excel(alt, index=False)


# ---------------- Main Logic ---------------- #

def main():
    contacts_raw = load_contacts()
    contacts_raw = build_username(contacts_raw)

    number_col = find_number_column(contacts_raw)
    contacts_raw[number_col] = contacts_raw[number_col].astype(str).str.strip()

    contacts_df = contacts_raw[['username', number_col]].copy()
    contacts_df.rename(columns={number_col: 'number'}, inplace=True)
    contacts_df.reset_index(drop=True, inplace=True)

    accounts_df = load_accounts()

    contacts_list = contacts_df['username'].tolist()
    account_names = accounts_df['Username'].tolist()

    matches = []
    for account in account_names:
        if not contacts_list:
            break
        match, score, idx = process.extractOne(account, contacts_list)
        phone_number = contacts_df.loc[idx, 'number'] if idx is not None else ''
        if score >= MIN_SCORE:
            matches.append({
                'account_name': account,
                'matched_contact': match,
                'phone_number': phone_number,
                'similarity_score': score
            })

    results_df = pd.DataFrame(matches)
    print(results_df)
    if not results_df.empty:
        safe_write_excel(results_df, OUTPUT_FILE)
    else:
        print("No matches produced.")


if __name__ == '__main__':
    main()
