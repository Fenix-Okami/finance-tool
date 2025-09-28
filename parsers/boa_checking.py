import pandas as pd
import re
import os


def parse_boa_checking(pdf_path, text=None):
    """Parse Bank of America checking account statement into transactions.

    Output columns:
    - Transaction Date (MM/DD/YY)
    - Description (string)
    - Amount (float, positive for deposits, negative for withdrawals)
    - Source File
    - Source Directory
    """
    filename = os.path.basename(pdf_path)
    card_type = os.path.basename(os.path.dirname(pdf_path))

    # If text is not provided, extract it (for backward compatibility)
    if text is None:
        from .utils import extract_text_from_pdf
        text = extract_text_from_pdf(pdf_path)

    # Quick domain check: checking statements use bankofamerica.com (no www)
    if 'bankofamerica.com' not in text.lower():
        return None

    # Prepare to parse sections
    lines = text.splitlines()

    # Patterns
    date_start_re = re.compile(r"^(\d{2}/\d{2}/\d{2})\s+(.*)")
    amount_tail_re = re.compile(r"(-?\$?\d[\d,]*\.\d{2})\s*$")

    section = None
    valid_sections = {
        'deposits': ['deposits and other additions'],
        'atm_debit': ['atm and debit card subtractions'],
        'other_sub': ['other subtractions'],
    }

    records = []
    pending = None  # {date, desc_parts}

    def clean_description(desc: str, amount_value) -> str:
        """Clean extraneous tokens from description.
        - Cut at ' ID:' or ' Conf#' or ';'
        - Cut at a number equal to the amount (supports comma formatting and sign)
        - Remove embedded repeated dates like MM/DD/YY at the end if left
        """
        s = desc
        # Normalize spaces
        s = re.sub(r"\s+", " ", s).strip()

        # Markers to cut at (case-insensitive)
        cut_markers = [r"\bID:", r"\bConf#", r";"]
        cut_positions = []
        for pat in cut_markers:
            m = re.search(pat, s, flags=re.IGNORECASE)
            if m:
                cut_positions.append(m.start())

        # Amount token cut
        if amount_value is not None:
            # Prepare possible string forms: 1190.00, -1190.00, 1,190.00, -1,190.00
            abs_amt = abs(float(amount_value))
            amt_str = f"{abs_amt:,.2f}"  # with comma
            amt_str_nocomma = amt_str.replace(',', '')
            variants = [amt_str, amt_str_nocomma]
            # try both with and without leading minus
            variants += ["-" + v for v in variants]
            for v in variants:
                m = re.search(re.escape(v), s)
                if m:
                    cut_positions.append(m.start())
                    break

        if cut_positions:
            s = s[: min(cut_positions)].rstrip()

        # Remove any trailing standalone date token
        s = re.sub(r"\s*\b\d{2}/\d{2}/\d{2}\b\s*$", "", s)

        # Final whitespace cleanup
        s = re.sub(r"\s+", " ", s).strip()
        # Trim trailing spaces and '-' characters specifically
        s = re.sub(r"[\s-]+$", "", s)
        return s

    def finalize_pending_if_amount(line):
        nonlocal pending
        if pending is None:
            return False
        m_amt = amount_tail_re.search(line)
        if m_amt:
            amt_str = m_amt.group(1)
            # Clean amount
            amt_clean = amt_str.replace('$', '').replace(',', '')
            try:
                amount = float(amt_clean)
            except ValueError:
                amount = None
            # Build description up to before amount occurrence on this line
            # Keep the full line in description minus trailing amount token
            desc_extra = line[: m_amt.start()].rstrip()
            if desc_extra:
                pending['desc_parts'].append(desc_extra)
            desc_raw = ' '.join(part.strip() for part in pending['desc_parts'] if part and part.strip())
            desc = clean_description(desc_raw, amount)
            records.append({
                'Transaction Date': pending['date'],
                'Description': re.sub(r"\s+", " ", desc),
                'Amount': amount,
            })
            pending = None
            return True
        return False

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue

        low = line.lower()

        # Skip non-transactional noise lines
        if low.startswith('page ') or 'continued on the next page' in low:
            continue
        if low.startswith('customer service information'):
            continue
        if low.startswith('important information') or low.startswith('bank deposit accounts'):
            continue
        if low in ('date description amount',):
            continue
        if low.startswith('total deposits and other additions') or low.startswith('total atm and debit card subtractions') or low.startswith('total other subtractions'):
            # Section totals, ignore
            continue

        # Section switches
        if any(h in low for h in valid_sections['deposits']):
            section = 'deposits'
            pending = None
            continue
        if any(h in low for h in valid_sections['atm_debit']):
            section = 'atm_debit'
            pending = None
            continue
        if any(h in low for h in valid_sections['other_sub']):
            section = 'other_sub'
            pending = None
            continue

        # Only parse lines within known sections
        if section is None:
            # Try to finalize a pending if amount appears on a stray line
            if finalize_pending_if_amount(line):
                continue
            else:
                continue

        # If line starts with a date, start new pending record
        m_date = date_start_re.match(line)
        if m_date:
            # finalize any previous pending if it never captured an amount (unlikely); discard
            pending = {
                'date': m_date.group(1),
                'desc_parts': [m_date.group(2).strip()],
            }
            # If amount is already on this line, finalize immediately
            if finalize_pending_if_amount(line):
                # finalized; pending cleared inside function
                pending = None
            continue

        # Not a date-start line; if we have a pending, it's a wrapped description or continuation
        if pending is not None:
            # Try to see if this line contains the amount at the end; if so, finalize
            if finalize_pending_if_amount(line):
                pending = None
                continue
            # Otherwise, just append to description
            pending['desc_parts'].append(line)
            continue

        # Any other lines outside of a pending record are ignored
        continue

    # Build DataFrame
    if not records:
        print(f"Warning: No transactions found in BoA checking statement {pdf_path}")
        return pd.DataFrame(columns=['Transaction Date', 'Description', 'Amount', 'Source File', 'Source Directory'])

    df = pd.DataFrame(records)
    # Attach source metadata
    df['Source File'] = filename
    df['Source Directory'] = card_type

    # Normalize amounts: ensure float dtype
    df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')

    # Drop rows with NaN amount or date just in case
    df = df.dropna(subset=['Amount', 'Transaction Date'])

    # Clean transaction date to MM/DD (to match other parsers) or keep MM/DD/YY? Others used MM/DD for CC.
    # We'll keep MM/DD for consistency with other outputs where year is inferred later.
    df['Transaction Date'] = df['Transaction Date'].str.slice(0, 5)

    # Reorder columns
    df = df[['Transaction Date', 'Description', 'Amount', 'Source File', 'Source Directory']]
    return df