import pandas as pd
import re
import os

def parse_boa_credit_card(pdf_path, text=None):
    """Parse Bank of America credit card statement"""
    filename = os.path.basename(pdf_path)
    card_type = os.path.basename(os.path.dirname(pdf_path))
    
    # If text is not provided, extract it (for backward compatibility)
    if text is None:
        from .utils import extract_text_from_pdf
        text = extract_text_from_pdf(pdf_path)
    
    # Check if this is a BoA statement
    if 'www.bankofamerica.com' not in text:
        return None
    
    # Extract specific section for BoA credit card transactions
    start_idx = text.find("Page 3 of")
    end_idx = text.find("TOTAL PURCHASES AND ADJUSTMENTS", start_idx)
    specific_text = text[start_idx:end_idx] if start_idx != -1 and end_idx != -1 else text
    
    # Parse transactions using BoA credit card pattern
    transaction_pattern = re.compile(r'(\d{2}/\d{2})\s(\d{2}/\d{2})\s([\w\s\.\*\-]+?)\s(\d{4})\s(\d{4})\s(-?\d+\.\d{2})')
    transactions = transaction_pattern.findall(specific_text)
    
    if not transactions:
        print(f"Warning: No transactions found in BoA credit card statement {pdf_path}")
        return pd.DataFrame(columns=['Transaction Date', 'Posting Date', 'Description', 'Amount', 'Source File', 'Source Directory'])
    
    df_columns = ['Transaction Date', 'Posting Date', 'Description', 'Reference Number', 'Account Number', 'Amount']
    df = pd.DataFrame(transactions, columns=df_columns)
    
    df['Source File'] = filename
    df['Source Directory'] = card_type
    df = df[['Transaction Date', 'Posting Date', 'Description', 'Amount', 'Source File', 'Source Directory']]
    df['Amount'] = pd.to_numeric(df['Amount'])
    
    return df