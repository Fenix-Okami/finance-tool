import re
import pandas as pd
import PyPDF2


def extract_text_from_pdf(file):
    """Return all text from a PDF file or file-like object."""
    reader = PyPDF2.PdfReader(file)
    text = ""
    for page in reader.pages:
        try:
            text += page.extract_text() + "\n"
        except Exception:
            pass
    return text


def parse_chase_statement(text, filename):
    """Parse Chase PDF text into a DataFrame."""
    pattern = re.compile(r'(\d{2}/\d{2})\s+(.*?)\s+(-?\d+\.\d{2})')
    matches = pattern.findall(text)
    df = pd.DataFrame(matches, columns=["Transaction Date", "Description", "Amount"])
    df["Amount"] = pd.to_numeric(df["Amount"])
    df["Source File"] = filename
    return df


def parse_boa_statement(text, filename):
    """Parse Bank of America PDF text into a DataFrame."""
    pattern = re.compile(
        r'(\d{2}/\d{2})\s(\d{2}/\d{2})\s([\w\s\.\*\-]+?)\s(\d{4})\s(\d{4})\s(-?\d+\.\d{2})'
    )
    transactions = pattern.findall(text)
    df = pd.DataFrame(
        transactions,
        columns=[
            "Transaction Date",
            "Posting Date",
            "Description",
            "Reference Number",
            "Account Number",
            "Amount",
        ],
    )
    df = df[["Transaction Date", "Posting Date", "Description", "Amount"]]
    df["Amount"] = pd.to_numeric(df["Amount"])
    df["Source File"] = filename
    return df


def parse_pdf(file):
    """Parse a Chase or Bank of America PDF statement."""
    text = extract_text_from_pdf(file)
    filename = getattr(file, "name", "uploaded.pdf")
    if "www.bankofamerica.com" in text:
        return parse_boa_statement(text, filename)
    if "www.chase.com" in text:
        return parse_chase_statement(text, filename)
    raise ValueError("Unsupported bank statement")


def parse_csv(file):
    """Load transactions from a CSV file."""
    return pd.read_csv(file)
