from txt2dataset import DatasetBuilder
from pydantic import BaseModel
from typing import Optional, List, Literal
from datetime import datetime
import csv
import gzip
import os
from datamule import Portfolio
from datetime import datetime
import calendar
from tqdm import tqdm
import logging
logging.getLogger('google_genai.models').setLevel(logging.WARNING)

def generate_monthly_date_ranges(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    
    monthly_ranges = []
    current_date = start_date
    
    while current_date <= end_date:
        # Get the last day of the current month
        last_day_of_month = calendar.monthrange(current_date.year, current_date.month)[1]
        month_end = datetime(current_date.year, current_date.month, last_day_of_month)
        
        # Use the earlier of month_end or end_date
        range_end = min(month_end, end_date)
        
        # Add tuple of (start_date_str, end_date_str) for this month
        monthly_ranges.append((
            current_date.strftime('%Y-%m-%d'),
            range_end.strftime('%Y-%m-%d')
        ))
        
        # Move to the first day of the next month
        if current_date.month == 12:
            current_date = datetime(current_date.year + 1, 1, 1)
        else:
            current_date = datetime(current_date.year, current_date.month + 1, 1)
    
    return monthly_ranges


class ProposalResult(BaseModel):
    proposal_description: Optional[str] = None  # What was being voted on
    presentation_order: Optional[int] = None  # Sequential order (1, 2, 3...)
    assigned_number: Optional[str] = None  # Assigned numbering (1a, 1b, 1c...)
    votes_for: Optional[int] = None  # Votes in favor
    votes_against: Optional[int] = None  # Votes against
    abstentions: Optional[int] = None  # Abstention votes
    broker_non_votes: Optional[int] = None  # Broker non-votes (if present)
    proponent_type: Optional[str] = None  # "Management" or "Shareholder"
    meeting_date: Optional[str] = None  # Date of the meeting, should specfic to date YYYY-MM-DD
    meeting_type: Optional[Literal["Annual", "Special"]] = None  # Only "Annual" or "Special"


class ProposalResultsExtraction(BaseModel):
    info_found: bool
    data: List[ProposalResult] = []


# Load existing results if they exist
all_results = []
if os.path.exists('proposal_results.csv.gz'):
    with gzip.open('proposal_results.csv.gz', 'rt', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        all_results = list(reader)

portfolio = Portfolio('8k_proposals')
# Item 5.07 is adopted around 2010
date_tuples = generate_monthly_date_ranges('2010-04-01',datetime.today().strftime('%Y-%m-%d'))
for date_tuple in date_tuples:
    portfolio.download_submissions(submission_type=['8-K','8-K/A'],document_type=['8-K','8-K/A'],filing_date=(date_tuple[0],date_tuple[1]))

    # construct entries
    rows = []
    for sub in tqdm(portfolio):
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
                if doc.extension in ['.htm','.html']:
                    item507 = doc.get_section(title='item5.07',title_class='item', format='text')
                    #print(doc.get_section(title_regex='item',title_class='item', format='text'))
                    if len(item507) != 0:
                        rows.append({'accession':accession,'cik':filer_cik,'filing_date':filing_date,'text':item507[0]})

        except Exception as e:
            print(e)

    # if empty skip
    if len(rows) == 0:
        portfolio.delete()
        continue

    
    with gzip.open('entries.csv.gz', 'wt', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['accession', 'cik', 'filing_date', 'text']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        
        writer.writeheader()
        writer.writerows(rows)

    # construct entries from rows and accession (accession,text)
    entries = [(row['accession'], row['text']) for row in rows]

    # Create builder
    builder = DatasetBuilder(
        prompt="""Extract shareholder proposal voting results from shareholder meeting reports. For each proposal voted on, extract:
        1. Proposal description (what was being voted on). Summarize in your own words.
        2. Presentation order (sequential: 1st, 2nd, 3rd proposal presented)
        3. Assigned number/letter (e.g. if the text has a defined numbering system for proposals, use it)
        4. Vote counts: For, Against, Abstentions, Broker Non-Votes (if mentioned)
        5. Proponent type: 'Management' (if proposed by company/board) or 'Shareholder' (if shareholder proposal)
        6. Meeting date and type (Annual or Special meeting)
        
        Only extract when actual voting results with vote counts are reported. Skip general descriptions without vote tallies.""",
        schema=ProposalResultsExtraction,
        model="gemini-2.5-flash-lite", # should use more powerful model as prompt is more complex, but google rate limits is being annoying
        entries=entries,
        rpm=4000,
        max_concurrent = 40,
        timeout = 60,
    )

    # Build dataset
    builder.build()

    # Save to regular csv first, then gzip it
    builder.save('results.csv')

    # Gzip the results file
    with open('results.csv', 'rb') as f_in:
        with gzip.open('results.csv.gz', 'wb') as f_out:
            f_out.writelines(f_in)

    # Remove the uncompressed file
    os.remove('results.csv')

    # Create enhanced CSV with metadata using only csv module
    metadata_lookup = {row['accession']: {'cik': row['cik'], 'filing_date': row['filing_date']} for row in rows}

    # Read this month's results and add to all_results
    current_month_results = []
    with gzip.open('results.csv.gz', 'rt', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        original_fields = reader.fieldnames
        
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
            
            # only add if proposal data has actual data
            if new_row.get('proposal_description') and new_row.get('proposal_description').strip():
                current_month_results.append(new_row)
    
    # Add current month results to all results
    all_results.extend(current_month_results)
    
    # Save cumulative results
    if all_results:  # Only write if we have data
        with gzip.open('proposal_results.csv.gz', 'wt', newline='', encoding='utf-8') as outfile:
            # Use fieldnames from the structure we know
            new_fieldnames = ['accession', 'cik', 'filing_date'] + [f for f in original_fields if f != '_id']
            
            writer = csv.DictWriter(outfile, fieldnames=new_fieldnames, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(all_results)

    # Clean up intermediate files
    if os.path.exists('entries.csv.gz'):
        os.remove('entries.csv.gz')
    if os.path.exists('results.csv.gz'):
        os.remove('results.csv.gz')

    # cleanup portfolio
    portfolio.delete()
