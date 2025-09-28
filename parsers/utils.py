import PyPDF2

def extract_text_from_pdf(pdf_path):
    """Extract text from PDF file"""
    with open(pdf_path, 'rb') as file:
        pdf_reader = PyPDF2.PdfReader(file)
        text = ''
        for page in pdf_reader.pages:
            try:
                text += page.extract_text() + '\n'
            except Exception:
                pass
    return text