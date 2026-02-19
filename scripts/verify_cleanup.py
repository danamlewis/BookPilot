#!/usr/bin/env python3
"""
Verify cleanup results - show what books were removed and why they were flagged as non-English
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import init_db, get_session, Author, AuthorCatalogBook
from src.catalog import cleanup_non_english_books
import re

def check_book_language(title, isbn=None, open_library_key=None):
    """
    Check why a book would be flagged as non-English (same logic as cleanup function)
    Returns list of reasons
    """
    reasons = []
    
    if not title:
        return reasons
    
    # Pattern matching
    non_english_languages = (
        'french|russian|spanish|german|italian|portuguese|chinese|japanese|korean|arabic|hebrew|'
        'polish|dutch|swedish|norwegian|danish|finnish|greek|turkish|hindi|thai|vietnamese|'
        'indonesian|malay|tagalog|romanian|hungarian|czech|slovak|croatian|serbian|bulgarian|'
        'ukrainian|persian|urdu|bengali|tamil|telugu|marathi|gujarati|kannada|malayalam|'
        'punjabi|nepali|sinhala|myanmar|khmer|lao|mongolian|georgian|armenian|azerbaijani|'
        'kazakh|uzbek|turkmen|kyrgyz|tajik|afrikaans|swahili|zulu|xhosa|amharic|hausa|'
        'yoruba|igbo|somali|maltese|icelandic|basque|catalan|galician|welsh|irish|scottish|'
        'breton|cornish|manx'
    )
    
    paren_pattern = re.compile(
        rf'\([^)]*(?:{non_english_languages})\s*(?:edition|version|translation)?[^)]*\)',
        re.IGNORECASE
    )
    bracket_pattern = re.compile(
        rf'\[[^\]]*(?:{non_english_languages})\s*(?:edition|version|translation)?[^\]]*\]',
        re.IGNORECASE
    )
    standalone_pattern = re.compile(
        rf'\b(?:{non_english_languages})\s+(?:edition|version|translation)\b',
        re.IGNORECASE
    )
    spanish_indicators = re.compile(
        r'\b(?:edici[oó]n|colecci[oó]n|estuche|libro|libros|misterio|pr[ií]ncipe)\b',
        re.IGNORECASE
    )
    
    # Check patterns
    if paren_pattern.search(title):
        match = paren_pattern.search(title)
        reasons.append(f"Language edition in parentheses: '{match.group()}'")
    if bracket_pattern.search(title):
        match = bracket_pattern.search(title)
        reasons.append(f"Language edition in brackets: '{match.group()}'")
    if standalone_pattern.search(title):
        match = standalone_pattern.search(title)
        reasons.append(f"Standalone language edition: '{match.group()}'")
    if 'house edition' not in title.lower() and spanish_indicators.search(title):
        match = spanish_indicators.search(title)
        reasons.append(f"Spanish text indicator: '{match.group()}'")
    
    # Character-based detection
    major_non_english_pattern = re.compile(
        r'[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ff\u0400-\u04ff\u0600-\u06ff\u0590-\u05ff]'
    )
    if major_non_english_pattern.search(title):
        reasons.append("Non-English script detected (CJK/Cyrillic/Arabic/Hebrew)")
    
    accented_chars_pattern = re.compile(
        r'[àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿąćčđęěğıłńňřśşšťůźżž]',
        re.IGNORECASE
    )
    spanish_punct = re.compile(r'[¿¡]')
    german_eszett = re.compile(r'ß')
    
    if spanish_punct.search(title):
        reasons.append("Spanish punctuation (¿ or ¡)")
    if german_eszett.search(title):
        reasons.append("German ß character")
    if accented_chars_pattern.search(title):
        accented_count = len(accented_chars_pattern.findall(title))
        total_alpha_chars = len([c for c in title if c.isalpha()])
        if total_alpha_chars > 0:
            ratio = accented_count / total_alpha_chars
            if ratio > 0.05 or (len(title) < 15 and accented_count >= 2) or accented_count >= 3:
                reasons.append(f"High accented character ratio ({accented_count}/{total_alpha_chars}, {ratio:.1%})")
    
    return reasons

def verify_cleanup_results(limit=None, offset=0, sample_size=20, dry_run_check=True):
    """Show sample of what would be removed"""
    db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    print("=" * 80)
    if dry_run_check:
        print("DRY RUN: Checking what WOULD be removed (no changes made)")
    else:
        print("Checking what WAS removed (checking remaining books)")
    print("=" * 80)
    print()
    
    # Get all catalog books
    query = session.query(AuthorCatalogBook).order_by(AuthorCatalogBook.id)
    if offset:
        query = query.offset(offset)
    if limit:
        query = query.limit(limit)
    catalog_books = query.all()
    
    total_in_db = session.query(AuthorCatalogBook).count()
    print(f"Checking {len(catalog_books)} books (of {total_in_db} total in database)...\n")
    
    # Check each book
    flagged_books = []
    for book in catalog_books:
        reasons = check_book_language(book.title, book.isbn, book.open_library_key)
        if reasons:
            # Get author name
            try:
                author = session.query(Author).filter_by(id=book.author_id).first()
                author_name = author.name if author else f"Author ID {book.author_id}"
            except:
                author_name = f"Author ID {book.author_id}"
            
            flagged_books.append({
                'id': book.id,
                'title': book.title,
                'author': author_name,
                'isbn': book.isbn,
                'reasons': reasons
            })
    
    print(f"Found {len(flagged_books)} books that would be flagged as non-English\n")
    
    if flagged_books:
        print(f"Showing sample of {min(sample_size, len(flagged_books))} books:\n")
        print("-" * 80)
        
        for i, book in enumerate(flagged_books[:sample_size], 1):
            print(f"\n{i}. ID {book['id']}: {book['title']}")
            print(f"   Author: {book['author']}")
            if book['isbn']:
                print(f"   ISBN: {book['isbn']}")
            print(f"   Reasons flagged:")
            for reason in book['reasons']:
                print(f"     - {reason}")
        
        if len(flagged_books) > sample_size:
            print(f"\n... and {len(flagged_books) - sample_size} more books")
        
        print("\n" + "-" * 80)
        print(f"\nTotal: {len(flagged_books)} books would be removed")
        
        # Show breakdown by reason type
        print("\nBreakdown by detection method:")
        reason_types = {}
        for book in flagged_books:
            for reason in book['reasons']:
                reason_type = reason.split(':')[0] if ':' in reason else reason
                reason_types[reason_type] = reason_types.get(reason_type, 0) + 1
        
        for reason_type, count in sorted(reason_types.items(), key=lambda x: -x[1]):
            print(f"  {reason_type}: {count} books")
    else:
        print("✓ No non-English books found in this batch")
    
    session.close()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Verify cleanup results')
    parser.add_argument('--limit', type=int, help='Limit number of books to check')
    parser.add_argument('--offset', type=int, default=0, help='Offset for batch processing')
    parser.add_argument('--sample-size', type=int, default=20, help='Number of examples to show')
    args = parser.parse_args()
    
    verify_cleanup_results(limit=args.limit, offset=args.offset, sample_size=args.sample_size)
