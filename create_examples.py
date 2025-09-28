import os
import glob
from parsers.utils import extract_text_from_pdf
from parsers.boa_credit_card import parse_boa_credit_card
from parsers.chase_credit_card import parse_chase_credit_card
from parsers.boa_checking import parse_boa_checking
from parsers.bestbuy_credit_card import parse_bestbuy_credit_card

def detect_statement_type(text):
    """
    Analyze the PDF text content to determine the appropriate parser
    Returns the parser name or 'Unknown' if no match
    """
    text_lower = text.lower()

    # Simplified Bank of America detection by domain only
    if 'www.bankofamerica.com' in text_lower:
        return "BoA Credit Card Parser"
    if 'bankofamerica.com' in text_lower:
        return "BoA Checking Parser"

    # Chase credit card detection by domain only
    if 'www.chase.com' in text_lower:
        return "Chase Credit Card Parser"

    # Best Buy credit card
    if 'best buy' in text_lower or 'bestbuy' in text_lower:
        return "Best Buy Credit Card Parser"

    # No matching parser found
    return "Unknown - No Parser Identified"

def create_text_examples(folder_path):
    """
    Extract text from the latest PDF in each statement type folder and save as examples with parser identification
    """
    # Create output directory
    output_dir = 'output/examples'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created directory: {output_dir}")
    
    # Find all subdirectories (statement types)
    subdirs = [d for d in os.listdir(folder_path) if os.path.isdir(os.path.join(folder_path, d))]
    print(f"Found {len(subdirs)} statement type folders: {subdirs}")
    
    processed_count = 0
    
    for subdir in subdirs:
        subdir_path = os.path.join(folder_path, subdir)
        
        # Find all PDFs in this subdirectory
        pdf_files = glob.glob(os.path.join(subdir_path, '*.pdf'))
        
        if not pdf_files:
            print(f"No PDF files found in {subdir}")
            continue
        
        # Get the latest (last alphabetically) PDF
        latest_pdf = max(pdf_files, key=lambda x: os.path.basename(x))
        
        try:
            # Extract filename info for output naming
            filename = os.path.basename(latest_pdf)
            
            # Create a safe filename for the text output
            base_name = os.path.splitext(filename)[0]  # Remove .pdf extension
            safe_filename = f"{subdir}_LATEST_{base_name}.txt"
            output_file = os.path.join(output_dir, safe_filename)
            
            print(f"Processing latest from {subdir}: {filename}")
            
            # Extract text
            text = extract_text_from_pdf(latest_pdf)
            
            # Detect appropriate parser
            parser_name = detect_statement_type(text)
            
            # Create output content
            header = f"PARSER IDENTIFICATION: Will be sent to {parser_name}\n"
            header += f"SOURCE FILE: {latest_pdf}\n"
            header += f"STATEMENT TYPE FOLDER: {subdir}\n"
            header += f"EXTRACTED TEXT LENGTH: {len(text)} characters\n"
            header += "=" * 80 + "\n\n"
            
            full_content = header + text
            
            # Save text example to file
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(full_content)
            
            print(f"  -> Saved to: {output_file}")
            print(f"  -> Identified as: {parser_name}")
            print(f"  -> Text length: {len(text)} characters\n")
            
            # Attempt to parse and save CSV using the detected parser
            parsers_map = {
                "BoA Credit Card Parser": parse_boa_credit_card,
                "BoA Checking Parser": parse_boa_checking,
                "Chase Credit Card Parser": parse_chase_credit_card,
                "Best Buy Credit Card Parser": parse_bestbuy_credit_card,
            }

            parser_func = parsers_map.get(parser_name)
            if parser_func is not None:
                try:
                    df = parser_func(latest_pdf, text)
                    csv_output_file = os.path.join(output_dir, f"{subdir}_LATEST_{base_name}.csv")
                    if df is not None and not df.empty:
                        df.to_csv(csv_output_file, index=False)
                        print(f"  -> Parsed CSV saved to: {csv_output_file}")
                        print(f"  -> Rows: {len(df)} | Columns: {list(df.columns)}\n")
                    else:
                        # Save an empty CSV with a note file to aid debugging
                        empty_note = os.path.join(output_dir, f"{subdir}_LATEST_{base_name}__EMPTY.txt")
                        with open(empty_note, 'w', encoding='utf-8') as nf:
                            nf.write("Parser returned empty or None DataFrame. Check parser patterns.\n")
                        print(f"  -> Parser returned empty/None. Wrote note: {empty_note}\n")
                except Exception as pe:
                    error_note = os.path.join(output_dir, f"{subdir}_LATEST_{base_name}__PARSE_ERROR.txt")
                    with open(error_note, 'w', encoding='utf-8') as ef:
                        ef.write(f"Parser error: {pe}\n")
                    print(f"  -> Parser error. Wrote note: {error_note}\n")
            else:
                print("  -> No matching parser function for CSV export (unknown type).\n")

            processed_count += 1
            
        except Exception as e:
            print(f"Error processing {latest_pdf}: {e}\n")
    
    print(f"Successfully processed {processed_count} files (latest from each statement type)")
    print(f"Text examples saved in: {output_dir}")

if __name__ == '__main__':
    folder_path = 'Statements'  # Base folder containing PDF statements
    # Process just the latest file from each statement type folder
    create_text_examples(folder_path)