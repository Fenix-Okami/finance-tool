import pandas as pd
import glob
import os
import re
from datetime import datetime
import hashlib
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Optional, List, Tuple
try:
    from tqdm import tqdm  # type: ignore
except Exception:  # pragma: no cover
    tqdm = None

# Import all parser modules and the common utility
from parsers.utils import extract_text_from_pdf
from parsers.boa_credit_card import parse_boa_credit_card
from parsers.chase_credit_card import parse_chase_credit_card
from parsers.boa_checking import parse_boa_checking
from parsers.bestbuy_credit_card import parse_bestbuy_credit_card

def detect_statement_type(text):
    """
    Analyze the PDF text content to determine the appropriate parser
    Returns a tuple (name, parser_func) or None if no match
    """
    text_lower = text.lower()

    # Simplified Bank of America domain-based detection
    if 'www.bankofamerica.com' in text_lower:
        return ("BoA Credit Card", parse_boa_credit_card)
    if 'bankofamerica.com' in text_lower:
        return ("BoA Checking", parse_boa_checking)

    # Chase credit card detection by domain only (avoid false positives on 'chase' in descriptions)
    if 'www.chase.com' in text_lower:
        return ("Chase Credit Card", parse_chase_credit_card)

    # Best Buy credit card (Citibank issued). Heuristic based on brand name in statement content
    if 'best buy' in text_lower or 'bestbuy' in text_lower:
        return ("Best Buy Credit Card", parse_bestbuy_credit_card)

    # No matching parser found
    return None

def parse_statement_with_text(pdf_path, text) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Parse a statement using the pre-extracted text and appropriate parser
    """
    detection_result = detect_statement_type(text)
    
    if detection_result is None:
        print(f"Warning: No parser found for {pdf_path}")
        return None, "no_parser"
    
    parser_name, parser_func = detection_result
    
    try:
        # Pass the text directly to the parser to avoid re-extraction
        df = parser_func(pdf_path, text)
        if df is not None and not df.empty:
            # Success: no noisy per-file print; rely on progress bar
            return df, None
        else:
            print(f"Warning: {parser_name} parser returned empty results for {pdf_path}")
            return None, "empty_result"
    except Exception as e:
        print(f"Error: {parser_name} parser failed for {pdf_path}: {e}")
        return None, f"parser_failed: {e}"


def _process_single_pdf(pdf_file: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """Worker-safe function to process a single PDF file.
    Extracts text, detects the statement type, and parses to a DataFrame.
    Returns a DataFrame or None if parsing fails or yields no rows.
    """
    try:
        text = extract_text_from_pdf(pdf_file)
    except Exception as e:
        print(f"Error extracting text from {pdf_file}: {e}")
        return None, f"extract_failed: {e}"

    try:
        return parse_statement_with_text(pdf_file, text)
    except Exception as e:
        print(f"Unexpected error processing {pdf_file}: {e}")
        return None, f"unexpected_error: {e}"

def update_year(row):
    """Update transaction dates with correct year based on filename"""
    # Match both filename patterns
    date_match = re.search(r'(\d{4})-(\d{2})-\d{2}', row['Source File']) or re.search(r'(\d{4})(\d{2})\d{2}-statements-', row['Source File'])
    if date_match:
        file_year, file_month = date_match.groups()
    else:
        return "Date format error"  # Handle error or use a default date

    # Check transaction date format
    if '-' in str(row['Transaction Date']):
        transaction_date = datetime.strptime(str(row['Transaction Date']), "%Y-%m-%d")
    else:
        # Assuming "MM/dd" format for the transaction date
        if '/' in str(row['Transaction Date']):
            transaction_month, day = str(row['Transaction Date']).split('/')
        else:
            # Handle other formats as needed
            transaction_month = str(row['Transaction Date'])[:2]
            day = str(row['Transaction Date'])[-2:]
        
        # Default to file_year initially
        new_date_str = f"{file_year}-{transaction_month.zfill(2)}-{day.zfill(2)}"
        transaction_date = datetime.strptime(new_date_str, "%Y-%m-%d")

    # Adjust for December transactions in a January statement
    if file_month == '01' and transaction_date.month == 12:
        transaction_date = transaction_date.replace(year=transaction_date.year - 1)

    return transaction_date.date()

def process_all_pdfs_in_folder(folder_path: str):
    """Process all PDFs in the folder using appropriate parsers.

    Uses a process pool for parallel processing with 8 workers.
    """
    all_files = glob.glob(os.path.join(folder_path, '**', '*.pdf'), recursive=True)
    print(f"Found {len(all_files)} PDF files to process...")

    if not all_files:
        return pd.DataFrame()

    # Hardcode to 8 workers
    max_workers = 8

    all_dfs: List[pd.DataFrame] = []
    problems: List[str] = []

    # Group files by statement type (immediate parent directory name)
    groups = {}
    for f in all_files:
        group = os.path.basename(os.path.dirname(f))
        groups.setdefault(group, []).append(f)

    print(f"Using multiprocessing with {max_workers} workers...")
    for group_name, files in sorted(groups.items()):
        total = len(files)
        if total == 0:
            continue
        print(f"\n{group_name}: {total} files")

        pbar = None
        if tqdm is not None:
            pbar = tqdm(total=total, desc=group_name, unit="file")
        else:
            print(f"{group_name}: 0/{total}", end="", flush=True)
        done = 0

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {executor.submit(_process_single_pdf, f): f for f in files}
            for future in as_completed(future_to_file):
                pdf_file = future_to_file[future]
                try:
                    df, reason = future.result()
                except Exception as e:
                    print(f"\nWorker crashed on {pdf_file}: {e}")
                    df, reason = None, f"worker_crash: {e}"
                if df is not None and not df.empty:
                    all_dfs.append(df)
                if reason:
                    problems.append(f"{group_name}\t{pdf_file}\t{reason}")
                done += 1
                if pbar is not None:
                    pbar.update(1)
                else:
                    print(f"\r{group_name}: {done}/{total}", end="", flush=True)
        if pbar is not None:
            pbar.close()
        else:
            print()  # newline after fallback progress

    if not all_dfs:
        print("No valid transactions found in any PDF files.")
        return pd.DataFrame()

    print(f"Successfully parsed {len(all_dfs)} files")
    # Write problematic files list if any
    if problems:
        os.makedirs('output', exist_ok=True)
        problems_path = os.path.join('output', 'problem_pdfs.txt')
        with open(problems_path, 'w', encoding='utf-8') as fh:
            fh.write("# group\tfile\treason\n")
            fh.write("\n".join(sorted(problems)))
        print(f"Saved problematic PDF list to {problems_path} ({len(problems)} entries)")
    final_df = pd.concat(all_dfs, ignore_index=True)
    
    # Update transaction dates
    final_df['Transaction Date'] = final_df.apply(update_year, axis=1)
    
    # Drop 'Posting Date' column if it exists
    if 'Posting Date' in final_df.columns:
        final_df.drop('Posting Date', axis=1, inplace=True)
    
    # Create hash for deduplication
    final_df['Hash'] = final_df.apply(
        lambda row: hashlib.sha256(
            f"{row['Transaction Date']}{row['Description']}{row['Amount']}{row['Source File']}".encode()
        ).hexdigest(), 
        axis=1
    )
    
    # Reorder columns
    final_df = final_df[['Hash', 'Source File', 'Transaction Date', 'Description', 'Amount']]
    final_df = final_df.sort_values(by=['Transaction Date', 'Description', 'Amount'], ascending=True)
    
    print(f"Final dataset contains {len(final_df)} transactions")
    return final_df

if __name__ == '__main__':
    # Hardcode base folder and workers
    folder_path = 'Statements'
    final_transactions_df = process_all_pdfs_in_folder(folder_path)

    if not final_transactions_df.empty:
        os.makedirs('output', exist_ok=True)
        output_file = 'output/parsed_transactions_combined.csv'
        final_transactions_df.to_csv(output_file, index=False)
        print(f"Results saved to {output_file}")
    else:
        print("No transactions to save.")