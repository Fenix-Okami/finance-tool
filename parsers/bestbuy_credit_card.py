import pandas as pd
import re
import os

def parse_bestbuy_credit_card(pdf_path, text=None):
    """Parse Best Buy credit card statement"""
    filename = os.path.basename(pdf_path)
    card_type = os.path.basename(os.path.dirname(pdf_path))
    
    # If text is not provided, extract it (for backward compatibility)
    if text is None:
        from .utils import extract_text_from_pdf
        text = extract_text_from_pdf(pdf_path)
    
    # Check if this is a Best Buy statement
    if 'best buy' not in text.lower() and 'bestbuy' not in text.lower():
        return None
    
    # TODO: Define the specific pattern for Best Buy credit card statements
    # This will need to be customized based on the actual format
    # Placeholder pattern - needs to be updated based on actual statement format
    transaction_pattern = re.compile(r'(\d{2}/\d{2})\s+(.*?)\s+(-?\d+\.\d{2})')
    transactions = transaction_pattern.findall(text)
    
    if not transactions:
        print(f"Warning: No transactions found in Best Buy credit card statement {pdf_path}")
        return pd.DataFrame(columns=['Transaction Date', 'Description', 'Amount', 'Source File', 'Source Directory'])
    
    df = pd.DataFrame(transactions, columns=['Transaction Date', 'Description', 'Amount'])
    df['Source File'] = filename
    df['Source Directory'] = card_type
    df['Amount'] = pd.to_numeric(df['Amount'])
    
    return df