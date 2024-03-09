import pandas as pd
import re
import PyPDF2
import glob
import os
from datetime import datetime

def extract_text_from_pdf(pdf_path):
    with open(pdf_path, 'rb') as file:
        pdf_reader = PyPDF2.PdfReader(file)
        text = ''
        for page in pdf_reader.pages:
            try:
                text += page.extract_text() + '\n'
            except:
                pass

        if 'www.bankofamerica.com' in text:
            statement_type='boa'
            start_idx = text.find("Page 3 of 4")
            end_idx = text.find("TOTAL PURCHASES AND ADJUSTMENTS", start_idx)
            specific_text = text[start_idx:end_idx] if start_idx != -1 and end_idx != -1 else "Specified text range not found."

        if 'www.chase.com' in text:
            statement_type='chase'
            start_idx = text.find("Page2 of 3")
            end_idx = text.find("Total fees charged", start_idx)
            specific_text = text[start_idx:end_idx] if start_idx != -1 and end_idx != -1 else "Specified text range not found."
        
    return specific_text,statement_type

def parse_chase_statement(text):
    # Simplified parsing based on the Chase statement's format
    pattern = re.compile(r'(\d{2}/\d{2})\s+(.*?)\s+(-?\d+\.\d{2})')
    matches = pattern.findall(text)
    df = pd.DataFrame(matches, columns=['Transaction Date', 'Description', 'Amount'])
    # Convert 'Amount' from string to numeric (float)
    df['Amount'] = pd.to_numeric(df['Amount'])
    df.head(20)
    return df.dropna()

def parse_bank_statement(pdf_path):
    filename = os.path.basename(pdf_path)
    card_type = os.path.basename(os.path.dirname(pdf_path))  # Extracts the folder name directly containing the file
    text,statement_type = extract_text_from_pdf(pdf_path)
    if statement_type=='boa':
        transaction_pattern = re.compile(r'(\d{2}/\d{2})\s(\d{2}/\d{2})\s([\w\s\.\*\-]+?)\s(\d{4})\s(\d{4})\s(-?\d+\.\d{2})')
        transactions = transaction_pattern.findall(text)
        df_columns = ['Transaction Date', 'Posting Date', 'Description', 'Reference Number', 'Account Number', 'Amount', 'Source File', 'Source Directory']
        df = pd.DataFrame(transactions, columns=df_columns[:-2])  # Exclude Filename and Card Type for initial DataFrame creation
        df['Source File'] = filename
        df['Source Directory'] = card_type
        df = df[['Transaction Date', 'Posting Date', 'Description', 'Amount', 'Source File', 'Source Directory']]
        df['Amount'] = pd.to_numeric(df['Amount'])
        return df
    if statement_type=='chase':
        df=parse_chase_statement(text)
        df['Source File'] = filename
        df['Source Directory'] = card_type
        return df

def update_year(row):
    # Match both filename patterns
    date_match = re.search(r'(\d{4})-(\d{2})-\d{2}', row['Source File']) or re.search(r'(\d{4})(\d{2})\d{2}-statements-', row['Source File'])
    if date_match:
        file_year, file_month = date_match.groups()
    else:
        return "Date format error"  # Handle error or use a default date

    # Check transaction date format
    if '-' in row['Transaction Date']:
        transaction_date = datetime.strptime(row['Transaction Date'], "%Y-%m-%d")
    else:
        # Assuming "MMdd" format for the transaction date
        transaction_month = row['Transaction Date'][:2]
        day = row['Transaction Date'][-2:]
        # Default to file_year initially
        new_date_str = f"{file_year}-{transaction_month}-{day}"
        transaction_date = datetime.strptime(new_date_str, "%Y-%m-%d")

    # Adjust for December transactions in a January statement
    if file_month == '01' and transaction_date.month == 12:
        transaction_date = transaction_date.replace(year=transaction_date.year - 1)

    return transaction_date.date()

    

def process_all_pdfs_in_folder(folder_path):
    all_files = glob.glob(folder_path + '/**/*.pdf', recursive=True)  # Modified to search subdirectories
    all_dfs = []
    for pdf_file in all_files:
        df = parse_bank_statement(pdf_file)
        all_dfs.append(df)
    final_df = pd.concat(all_dfs, ignore_index=True)
    final_df['Transaction Date'] = final_df.apply(update_year, axis=1)

    return final_df

if __name__=='__main__':
    folder_path = 'statements'  # Base folder containing subfolders for Visa and Mastercard
    final_transactions_df = process_all_pdfs_in_folder(folder_path)

    if not os.path.exists('output'):
        os.makedirs('output')
    final_transactions_df.to_csv('output/parsed_transactions_combined.csv', index=False)