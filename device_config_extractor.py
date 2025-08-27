import concurrent.futures
import ipaddress
import re
import time
from typing import List, Tuple, Optional, Dict
import shutil
from datetime import datetime

import pandas as pd
import paramiko

# --------------------------------------------------
# Configuration
# --------------------------------------------------
EXCEL_PATH = 'final_customer_data.xlsx'  # Source workbook
EXCEL_IP_COLUMN = 'K'  # Excel column letter containing IPs
OUTPUT_EXCEL = 'device_config_results.xlsx'
FAILED_LIST_TXT = 'device_config_failed_ips.txt'
MAX_WORKERS = 15
SSH_TIMEOUT = 8  # seconds per credential attempt
COMMAND_IDENTITY = '/system identity print'
COMMAND_WIRELESS = '/interface wireless print detail without-paging'
COMMAND_WIRELESS_ALT = '/interface wifiwave2 print detail without-paging'
# Fallback full export (heavy); used only if parsing fails
COMMAND_EXPORT = '/export terse'

# Ordered credential attempts (username, password)
CREDENTIALS: List[Tuple[str, str]] = [
    ('admin', 'kol5ara'),
    ('admin', 'Admin'),
    ('admin', 'ADmin'),
    ('admin', 'admin'),
]

# Add result merge column names
RESULT_COLUMNS = [
    'ssh_status', 'ssh_username', 'ssh_password', 'system_identity', 'wireless_ssids', 'radio_names', 'ssh_error'
]

# --------------------------------------------------
# Helpers
# --------------------------------------------------

def load_ip_list(path: str, column_letter: str) -> List[str]:
    """Load unique IPs from a specific Excel column (by letter)."""
    try:
        df = pd.read_excel(path, usecols=column_letter)
    except Exception as e:
        raise RuntimeError(f'Failed reading {path} column {column_letter}: {e}')
    series = df.iloc[:, 0].dropna().astype(str).str.strip()
    ips: List[str] = []
    for val in series:
        # Extract first IP-like token if cell has extra text
        m = re.search(r'(\d+\.\d+\.\d+\.\d+)', val)
        if not m:
            continue
        candidate = m.group(1)
        try:
            ipaddress.ip_address(candidate)
            if candidate not in ips:
                ips.append(candidate)
        except ValueError:
            continue
    return ips


def ssh_run(ip: str, username: str, password: str, command: str) -> str:
    """Execute a single command over SSH and return stdout."""
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(ip, username=username, password=password, timeout=SSH_TIMEOUT, banner_timeout=SSH_TIMEOUT, auth_timeout=SSH_TIMEOUT, look_for_keys=False, allow_agent=False)
        stdin, stdout, stderr = client.exec_command(command, timeout=SSH_TIMEOUT)
        out = stdout.read().decode(errors='replace')
        err = stderr.read().decode(errors='replace')
        return out if out.strip() else err
    finally:
        client.close()


def try_credentials(ip: str) -> Tuple[Optional[Tuple[str, str]], Dict[str, str], str]:
    """Attempt all credentials; return first success, gathered command outputs, or failure reason."""
    for user, pwd in CREDENTIALS:
        try:
            # Quick identity run to validate credentials
            identity_raw = ssh_run(ip, user, pwd, COMMAND_IDENTITY)
            if not identity_raw:
                raise RuntimeError('Empty response')
            # If we got here, credentials worked; collect more
            outputs = {'identity': identity_raw}
            try:
                wireless_raw = ssh_run(ip, user, pwd, COMMAND_WIRELESS)
            except Exception:
                wireless_raw = ''
            if not wireless_raw:
                try:
                    wireless_raw = ssh_run(ip, user, pwd, COMMAND_WIRELESS_ALT)
                except Exception:
                    wireless_raw = ''
            outputs['wireless'] = wireless_raw
            # Optional export only if needed later
            return (user, pwd), outputs, ''
        except paramiko.AuthenticationException:
            continue
        except Exception as e:
            # Connection or exec failure; try next credential
            last_error = str(e)
            continue
    return None, {}, locals().get('last_error', 'Auth/connection failed')


def parse_identity(raw: str) -> str:
    # Expected line like: name: MyRouter
    for line in raw.splitlines():
        if 'name:' in line:
            return line.split('name:', 1)[1].strip()
    # Fallback from export style: set name=MyRouter
    m = re.search(r'set\s+name=([^\s]+)', raw)
    if m:
        return m.group(1).strip().strip('"')
    return ''


def parse_wireless(raw: str) -> Tuple[str, str]:
    ssids = set()
    radios = set()
    for line in raw.splitlines():
        if 'ssid=' in line:
            m = re.search(r'ssid="?([^"\s]+)"?', line)
            if m:
                ssids.add(m.group(1))
        if 'radio-name=' in line:
            m = re.search(r'radio-name="?([^"\s]+)"?', line)
            if m:
                radios.add(m.group(1))
    return ';'.join(sorted(ssids)), ';'.join(sorted(radios))


def process_ip(ip: str) -> Dict[str, str]:
    start = time.time()
    cred, outputs, error = try_credentials(ip)
    duration = round(time.time() - start, 2)
    if not cred:
        return {
            'ip': ip,
            'status': 'FAIL',
            'username': '',
            'password': '',
            'system_identity': '',
            'ssids': '',
            'radio_names': '',
            'error': error,
            'seconds': duration,
        }
    # If wireless outputs missing, attempt export terse to parse
    identity_raw = outputs.get('identity', '')
    wireless_raw = outputs.get('wireless', '')
    if not wireless_raw:
        try:
            export_raw = ssh_run(ip, cred[0], cred[1], COMMAND_EXPORT)
        except Exception:
            export_raw = ''
        if export_raw:
            # Try to extract ssid and radio-name lines from export
            for line in export_raw.splitlines():
                if 'ssid=' in line or 'radio-name=' in line:
                    wireless_raw += '\n' + line
    system_identity = parse_identity(identity_raw)
    ssids, radios = parse_wireless(wireless_raw)
    return {
        'ip': ip,
        'status': 'OK',
        'username': cred[0],
        'password': cred[1],
        'system_identity': system_identity,
        'ssids': ssids,
        'radio_names': radios,
        'error': '',
        'seconds': duration,
    }


def column_letter_to_index(letter: str) -> int:
    letter = letter.upper()
    return ord(letter) - ord('A')


def merge_results_into_source(results_df: pd.DataFrame, source_path: str, ip_column_letter: str):
    """Merge results back into the original Excel workbook, adding new columns.
    Creates a timestamped backup before overwriting.
    """
    backup = f"{source_path}.bak_{datetime.now():%Y%m%d_%H%M%S}"
    shutil.copyfile(source_path, backup)
    print(f"Backup created: {backup}")
    # Load entire sheet
    master_df = pd.read_excel(source_path)
    ip_col_idx = column_letter_to_index(ip_column_letter)
    if ip_col_idx >= master_df.shape[1]:
        raise ValueError(f"IP column index derived from letter {ip_column_letter} out of range")
    # Extract IPs from that column (may contain extra text)
    ip_series = master_df.iloc[:, ip_col_idx].astype(str)
    # Build map from IP -> result row
    result_map = {row['ip']: row for row in results_df.to_dict(orient='records')}
    # Ensure new columns exist
    for col in RESULT_COLUMNS:
        if col not in master_df.columns:
            master_df[col] = ''
    for i, cell in ip_series.items():
        m = re.search(r'(\d+\.\d+\.\d+\.\d+)', cell)
        if not m:
            continue
        ip = m.group(1)
        data = result_map.get(ip)
        if not data:
            continue
        master_df.at[i, 'ssh_status'] = data['status']
        master_df.at[i, 'ssh_username'] = data['username']
        master_df.at[i, 'ssh_password'] = data['password']
        master_df.at[i, 'system_identity'] = data['system_identity']
        master_df.at[i, 'wireless_ssids'] = data['ssids']
        master_df.at[i, 'radio_names'] = data['radio_names']
        master_df.at[i, 'ssh_error'] = data['error']
    # Write back
    master_df.to_excel(source_path, index=False)
    print(f"Merged results into {source_path}")


# --------------------------------------------------
# Main
# --------------------------------------------------

def main():
    ips = load_ip_list(EXCEL_PATH, EXCEL_IP_COLUMN)
    print(f'Loaded {len(ips)} IPs from {EXCEL_PATH} column {EXCEL_IP_COLUMN}')
    results: List[Dict[str, str]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        for res in pool.map(process_ip, ips):
            results.append(res)
            print(f"[{res['status']}] {res['ip']} time={res['seconds']}s identity={res.get('system_identity','')}")
    df = pd.DataFrame(results)
    # Separate failed list
    failed_ips = df.loc[df['status'] != 'OK', 'ip'].tolist()
    # Write outputs
    with pd.ExcelWriter(OUTPUT_EXCEL, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='results')
        df[df['status'] == 'OK'].to_excel(writer, index=False, sheet_name='success')
        df[df['status'] != 'OK'].to_excel(writer, index=False, sheet_name='failed')
    if failed_ips:
        with open(FAILED_LIST_TXT, 'w', encoding='utf-8') as f:
            f.write('\n'.join(failed_ips))
    print(f'Written {OUTPUT_EXCEL}. Failed IP count: {len(failed_ips)}')
    # Merge back into original workbook
    try:
        merge_results_into_source(df, EXCEL_PATH, EXCEL_IP_COLUMN)
    except Exception as e:
        print(f'Failed merging back into source workbook: {e}')


if __name__ == '__main__':
    main()
