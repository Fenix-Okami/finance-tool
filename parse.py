import pandas as pd
import re
import PyPDF2
import glob
import os

def extract_text_from_pdf(pdf_path):
    with open(pdf_path, 'rb') as file:
        pdf_reader = PyPDF2.PdfReader(file)
        text = ''
        for page in pdf_reader.pages:
            text += page.extract_text() + '\n'
    
    start_idx = text.find("Page 3 of 4")
    end_idx = text.find("TOTAL PURCHASES AND ADJUSTMENTS", start_idx)
    specific_text = text[start_idx:end_idx] if start_idx != -1 and end_idx != -1 else "Specified text range not found."
    return specific_text

def parse_bank_statement(pdf_path):
    filename = os.path.basename(pdf_path)
    card_type = os.path.basename(os.path.dirname(pdf_path))  # Extracts the folder name directly containing the file
    text = extract_text_from_pdf(pdf_path)
    transaction_pattern = re.compile(r'(\d{2}/\d{2})\s(\d{2}/\d{2})\s([\w\s\.\*\-]+?)\s(\d{4})\s(\d{4})\s(-?\d+\.\d{2})')
    transactions = transaction_pattern.findall(text)
    df_columns = ['Transaction Date', 'Posting Date', 'Description', 'Reference Number', 'Account Number', 'Amount', 'Source File', 'Source Directory']
    df = pd.DataFrame(transactions, columns=df_columns[:-2])  # Exclude Filename and Card Type for initial DataFrame creation
    df['Source File'] = filename
    df['Source Directory'] = card_type
    df = df[['Transaction Date', 'Posting Date', 'Description', 'Amount', 'Source File', 'Source Directory']]
    df['Amount'] = pd.to_numeric(df['Amount'])
    return df

def process_all_pdfs_in_folder(folder_path):
    all_files = glob.glob(folder_path + '/**/*.pdf', recursive=True)  # Modified to search subdirectories
    all_dfs = []
    for pdf_file in all_files:
        df = parse_bank_statement(pdf_file)
        all_dfs.append(df)
    
    final_df = pd.concat(all_dfs, ignore_index=True)
    return final_df

if __name__=='__main__':
    folder_path = 'statements'  # Base folder containing subfolders for Visa and Mastercard
    final_transactions_df = process_all_pdfs_in_folder(folder_path)

    if not os.path.exists('output'):
        os.makedirs('output')
    final_transactions_df.to_csv('output/parsed_transactions_combined.csv', index=False)