from txt2dataset import DatasetBuilder
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
import csv

from datamule import Portfolio

class SingleDividend(BaseModel):
    dividend_per_share: Optional[float] = None
    payment_date: Optional[datetime] = None
    record_date: Optional[datetime] = None
    stock_type_specified: Optional[str] = None

class DividendExtraction(BaseModel):
    info_found: bool
    data: List[SingleDividend] = []

portfolio = Portfolio('8k_8k')
portfolio.download_submissions(submission_type=['8-K','8-K/A'],document_type=['8-K','8-K/A'],filing_date=('2020-01-01','2020-01-31'))

# construct entries
rows = []
for sub in portfolio:
    try:
        accession = sub.metadata.content['accession-number']
        filing_date = sub.metadata.content['filing-date']

        # validation check if any item is a list - issue w/metadata in malformed sgml
        if isinstance(accession,list):
            accession = accession[0]

        if isinstance(filing_date,list):
            filing_date = filing_date[0]

        filer = sub.metadata.content['filer']
        
        # handles when company names change
        if not isinstance(filer, list):
            filer = [filer]
        
        filer_cik = filer[0]['company-data']['cik']
        for doc in sub.document_type(['8-K','8-K/A']):
            if doc.extension == '.htm':
                item701 = doc.get_section(title='item7.01',title_class='item', format='text')
                item801 = doc.get_section(title='item8.01',title_class='item', format='text')
                if len(item701) != 0:
                    rows.append({'accession':accession,'cik':filer_cik,'filing_date':filing_date,'text':item701[0]})

                if len(item801) != 0:
                    rows.append({'accession':accession,'cik':filer_cik,'filing_date':filing_date,'text':item801[0]})

    except Exception as e:
        print(e)


with open('entries.csv', 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['accession', 'cik', 'filing_date', 'text']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
    
    writer.writeheader()
    writer.writerows(rows)

# construct entries from rows and accession (accession,text)
entries = [(row['accession'], row['text']) for row in rows]

# Create builder
builder = DatasetBuilder(
    prompt="Extract ALL dividend information from this text.",
    schema=DividendExtraction,
    model="gemini-2.5-flash-lite",
    entries=entries,
    rpm=4000,
    max_concurrent = 20,
    timeout = 5,
)

# Build dataset
builder.build()

# Save to csv
builder.save('results.csv')

# Create enhanced CSV with metadata using only csv module
metadata_lookup = {row['accession']: {'cik': row['cik'], 'filing_date': row['filing_date']} for row in rows}

# Read original CSV and create enhanced version
with open('results.csv', 'r', newline='', encoding='utf-8') as infile:
    reader = csv.DictReader(infile)
    
    with open('dividends_per_share.csv', 'w', newline='', encoding='utf-8') as outfile:
        # Get original fieldnames and add new ones
        original_fields = reader.fieldnames
        new_fieldnames = ['accession', 'cik', 'filing_date'] + [f for f in original_fields if f != '_id']
        
        writer = csv.DictWriter(outfile, fieldnames=new_fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        
        for row in reader:
            # Get metadata for this accession
            accession = row['_id']
            metadata = metadata_lookup.get(accession, {})
            
            # Create new row with metadata
            new_row = {
                'accession': accession,
                'cik': metadata.get('cik', ''),
                'filing_date': metadata.get('filing_date', '')
            }
            
            # Add all other fields except _id
            for field in original_fields:
                if field != '_id':
                    new_row[field] = row[field]
            
            # Only write if dividend_per_share has actual data
            if new_row.get('dividend_per_share') and new_row.get('dividend_per_share').strip():
                writer.writerow(new_row)