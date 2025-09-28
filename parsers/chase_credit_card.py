import pandas as pd
import re
import os

def parse_chase_statement(text):
    """Parse Chase credit card transactions from text"""
    # Simplified parsing based on the Chase statement's format
    pattern = re.compile(r'(\d{2}/\d{2})\s+(.*?)\s+(-?\d+\.\d{2})')
    matches = pattern.findall(text)
    df = pd.DataFrame(matches, columns=['Transaction Date', 'Description', 'Amount'])
    # Convert 'Amount' from string to numeric (float)
    df['Amount'] = pd.to_numeric(df['Amount'])
    return df.dropna()

def parse_chase_credit_card(pdf_path, text=None):
    """Parse Chase credit card statement"""
    filename = os.path.basename(pdf_path)
    card_type = os.path.basename(os.path.dirname(pdf_path))
    
    # If text is not provided, extract it (for backward compatibility)
    if text is None:
        from .utils import extract_text_from_pdf
        text = extract_text_from_pdf(pdf_path)
    
    # Check if this is a Chase statement
    if 'www.chase.com' not in text:
        return None
    
    # Extract specific section for Chase credit card transactions
    start_idx = text.find("Page2 of")
    end_idx = text.find("Total fees charged", start_idx)
    specific_text = text[start_idx:end_idx] if start_idx != -1 and end_idx != -1 else text
    
    # Parse transactions
    df = parse_chase_statement(specific_text)
    
    if df.empty:
        print(f"Warning: No transactions found in Chase credit card statement {pdf_path}")
        return pd.DataFrame(columns=['Transaction Date', 'Description', 'Amount', 'Source File', 'Source Directory'])
    
    df['Source File'] = filename
    df['Source Directory'] = card_type
    
    return df