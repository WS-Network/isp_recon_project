"""
matching_pipeline.py
======================

This module implements a simple matching pipeline for aligning user account names from an
ISP customer database with contact names from a separate phone-book export. It
supports a first-pass match based purely on fuzzy name comparison, after removing a
common prefix from account usernames, and produces two outputs: a table of matched
accounts with their corresponding phone numbers and contact names, and a list of
unmatched (or leftover) contact phone numbers and names for further processing.

The pipeline assumes the following inputs in CSV format:

* ``Radius kyro.csv`` – a table of customer account information with columns
  such as ``Full Name`` and ``Username``. If ``Full Name`` is missing, the
  pipeline derives an account name from ``Username`` after stripping a leading
  ``kyrillos_`` prefix and splitting on underscores.
* ``contacts.csv`` – an export of the user's phone book from a platform like
  Google Contacts. This file contains numerous columns (e.g., ``First Name``,
  ``Last Name``, ``Phone 1 - Value``) which are used to assemble full names
  and extract phone numbers.

The matching process proceeds as follows:

1. **Parse contacts**: assemble a full name from the ``First Name``, ``Middle Name``,
   ``Last Name`` and ``Nickname`` columns; fall back to ``File As`` if no
   name parts are present. Names containing digits or certain keywords
   indicative of device names are discarded. Phone numbers are normalized to
   E.164 format with a default ``+961`` country code if no country code is
   present. Duplicate numbers retain the first associated name encountered.

2. **Prepare account names**: for each customer row, use the ``Full Name`` if
   provided; otherwise derive a name by stripping ``kyrillos_`` from the
   ``Username``, splitting on underscores and capitalizing each token.

3. **Fuzzy match**: compare each account name against all contact names using
   ``rapidfuzz``'s token sort ratio and record the best match. If the best
   similarity score meets or exceeds a supplied threshold (default 90), record
   the match (account index, contact phone, contact name, score). Otherwise,
   mark the row as unmatched.

4. **Output**: write an Excel file with the customer table augmented by
   ``Matched Phone``, ``Matched Contact Name``, and ``Match Score`` columns.
   Write a separate Excel file with the list of phone numbers that were not
   matched to any account; these can be used for subsequent lookups via
   services like the TrueCaller Telegram bot.

To execute this module from the command line, run::

    python matching_pipeline.py --customers /path/to/Radius kyro.csv \
        --contacts /path/to/contacts.csv --out_dir /path/to/output \
        --prefix kyrillos_ --threshold 90

This will write two Excel files to ``out_dir`` with a timestamp in their
names.
"""

import argparse
import os
import re
from datetime import datetime
from typing import List, Tuple, Dict

import pandas as pd
from rapidfuzz import fuzz


def compile_contact_name(row: pd.Series) -> str:
    """Assemble a full name from a contacts row.

    Concatenates the first, middle, last and nickname fields if present;
    otherwise falls back to the ``File As`` field. Extra whitespace is
    collapsed.

    Parameters
    ----------
    row : pandas.Series
        A row from the contacts DataFrame.

    Returns
    -------
    str
        The assembled name, or an empty string if none available.
    """
    parts: List[str] = []
    for col in ["First Name", "Middle Name", "Last Name", "Nickname"]:
        value = row.get(col)
        if pd.notnull(value) and str(value).strip():
            parts.append(str(value).strip())
    if not parts:
        fallback = row.get("File As")
        if pd.notnull(fallback) and str(fallback).strip():
            parts.append(str(fallback).strip())
    full = " ".join(parts)
    # Normalize whitespace
    full = re.sub(r"\s+", " ", full).strip()
    return full


def name_is_valid(name: str) -> bool:
    """Check whether a contact name appears to be a real person name.

    Excludes names that are too short, contain digits or certain keywords
    suggestive of device names. Also requires at least one alphabetic or
    Arabic letter.

    Parameters
    ----------
    name : str
        The name to check.

    Returns
    -------
    bool
        True if the name is considered valid, False otherwise.
    """
    if not name or name in {".", "--"}:
        return False
    if re.search(r"\d", name):
        return False
    if len(name) < 3:
        return False
    forbidden = [
        "samsung",
        "iphone",
        "series",
        "watch",
        "phone",
        "camera",
        "gear",
        "unknown",
    ]
    low = name.lower()
    for kw in forbidden:
        if kw in low:
            return False
    return bool(re.search(r"[A-Za-z\u0621-\u064A]", name))


def normalize_phone_number(raw: str, default_country: str = "+961") -> str:
    """Normalize a phone number to E.164 format.

    Removes non-digit characters and ensures a leading ``+`` followed by the
    country code. If the number begins with ``00``, it is replaced with ``+``.
    Otherwise, any leading zeros are stripped and the default country code is
    prepended.

    Parameters
    ----------
    raw : str
        The raw phone number string.
    default_country : str
        The country code to use if the number lacks a country prefix.

    Returns
    -------
    str
        The normalized phone number.
    """
    if not raw:
        return ""
    s = re.sub(r"[^0-9+]", "", str(raw))
    if s.startswith("+"):
        return s
    if s.startswith("00"):
        return "+" + s[2:]
    s = s.lstrip("0")
    return default_country + s


def parse_contacts(contacts_path: str) -> List[Tuple[str, str]]:
    """Load and parse the contacts CSV into a list of (name, phone) tuples.

    Parameters
    ----------
    contacts_path : str
        Path to the contacts CSV file.

    Returns
    -------
    list of (name, phone) tuples
        Valid names paired with normalized phone numbers. Duplicates are
        removed by retaining only the first name encountered for each phone.
    """
    contacts = pd.read_csv(contacts_path)
    phone_cols = [c for c in contacts.columns if c.startswith("Phone") and c.endswith("Value")]
    seen: Dict[str, str] = {}
    for _, row in contacts.iterrows():
        name = compile_contact_name(row)
        if not name_is_valid(name):
            continue
        for col in phone_cols:
            value = row.get(col)
            if pd.isnull(value) or not str(value).strip():
                continue
            phone = normalize_phone_number(value)
            if phone and phone not in seen:
                seen[phone] = name
    # Convert dict to list
    return [(name, phone) for phone, name in seen.items()]


def prepare_account_name(row: pd.Series, prefix: str) -> str:
    """Derive a display name for an account row.

    Uses the ``Full Name`` column if present; otherwise, removes the specified
    prefix from ``Username``, splits on underscores, and capitalizes each
    token.

    Parameters
    ----------
    row : pandas.Series
        A row from the customers DataFrame.
    prefix : str
        The prefix to remove from the ``Username``.

    Returns
    -------
    str
        The derived account name.
    """
    full = row.get("Full Name")
    if pd.notnull(full) and str(full).strip():
        return str(full).strip()
    username = str(row.get("Username", ""))
    if username.startswith(prefix):
        username = username[len(prefix):]
    parts = [p for p in username.split("_") if p]
    return " ".join(p.title() for p in parts)


def match_accounts(
    customers_path: str,
    contacts: List[Tuple[str, str]],
    prefix: str,
    threshold: float,
) -> Tuple[pd.DataFrame, List[Tuple[str, str]]]:
    """Match account names to contact names using fuzzy matching.

    Parameters
    ----------
    customers_path : str
        Path to the customers CSV file.
    contacts : list of (name, phone)
        Parsed contacts list.
    prefix : str
        Prefix to strip from usernames when deriving account names.
    threshold : float
        Minimum similarity score (0–100) required to consider a match valid.

    Returns
    -------
    DataFrame
        The customers table augmented with ``Account Name``, ``Matched Phone``,
        ``Matched Contact Name`` and ``Match Score`` columns.
    list of (name, phone)
        The subset of contacts that were not matched to any account.
    """
    customers = pd.read_csv(customers_path)
    # Prepare account names
    customers["Account Name"] = customers.apply(
        lambda r: prepare_account_name(r, prefix), axis=1
    )
    customers["Matched Phone"] = ""
    customers["Matched Contact Name"] = ""
    customers["Match Score"] = 0.0
    # Invert contacts into list of (phone, name) for matching
    contact_items = [(phone, name) for name, phone in contacts]
    # Perform fuzzy matching
    for idx, row in customers.iterrows():
        acc_name = row["Account Name"]
        if not acc_name:
            continue
        best_score = 0.0
        best_phone = ""
        best_name = ""
        for phone, name in contact_items:
            score = fuzz.token_sort_ratio(acc_name.lower(), name.lower())
            if score > best_score:
                best_score = score
                best_phone = phone
                best_name = name
        if best_score >= threshold:
            customers.at[idx, "Matched Phone"] = best_phone
            customers.at[idx, "Matched Contact Name"] = best_name
            customers.at[idx, "Match Score"] = best_score
        else:
            customers.at[idx, "Match Score"] = best_score
    # Identify matched phone numbers
    matched_phones = set(
        customers.loc[customers["Match Score"] >= threshold, "Matched Phone"]
    )
    # Build leftover contacts list
    leftover = [
        (name, phone)
        for phone, name in contact_items
        if phone not in matched_phones
    ]
    return customers, leftover


def save_outputs(
    customers: pd.DataFrame,
    leftover: List[Tuple[str, str]],
    out_dir: str,
    iteration_tag: str,
) -> Tuple[str, str]:
    """Write matched and leftover results to Excel files.

    File names include a timestamp and an iteration tag to avoid collisions.

    Parameters
    ----------
    customers : pandas.DataFrame
        The augmented customers table.
    leftover : list of (name, phone)
        The contacts that were not matched.
    out_dir : str
        Directory to write the output files to.
    iteration_tag : str
        A short label describing this iteration (e.g. ``firstpass``).

    Returns
    -------
    (str, str)
        Paths to the matched and leftover Excel files.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    matched_path = os.path.join(
        out_dir, f"customers_matched_{iteration_tag}_{timestamp}.xlsx"
    )
    leftover_path = os.path.join(
        out_dir, f"leftover_contacts_{iteration_tag}_{timestamp}.xlsx"
    )
    customers.to_excel(matched_path, index=False)
    pd.DataFrame(leftover, columns=["Name", "Phone"]).to_excel(
        leftover_path, index=False
    )
    return matched_path, leftover_path


def run_pipeline(args: argparse.Namespace) -> None:
    """Execute the matching pipeline based on command-line arguments."""
    contacts = parse_contacts(args.contacts)
    customers, leftover = match_accounts(
        args.customers, contacts, args.prefix, args.threshold
    )
    matched_path, leftover_path = save_outputs(
        customers, leftover, args.out_dir, args.iteration
    )
    print(f"Matched records written to {matched_path}")
    print(f"Unmatched contacts written to {leftover_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run contact-account matching pipeline")
    parser.add_argument(
        "--customers",
        required=True,
        help="Path to the customers CSV file (e.g. 'Radius kyro.csv').",
    )
    parser.add_argument(
        "--contacts",
        required=True,
        help="Path to the contacts CSV file (e.g. 'contacts.csv').",
    )
    parser.add_argument(
        "--out_dir",
        required=True,
        help="Directory to write output files to.",
    )
    parser.add_argument(
        "--prefix",
        default="kyrillos_",
        help="Prefix to remove from usernames when deriving account names (default: 'kyrillos_').",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=90.0,
        help="Similarity threshold for considering a match (default: 90).",
    )
    parser.add_argument(
        "--iteration",
        default="firstpass",
        help="Tag to identify this iteration of the matching process.",
    )
    args = parser.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)
    run_pipeline(args)


if __name__ == "__main__":
    main()