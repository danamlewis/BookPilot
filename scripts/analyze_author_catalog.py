#!/usr/bin/env python3
"""
Analyze an author's catalog books to identify:
1. Duplicate books
2. Non-English editions
3. Catalog completeness issues
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import init_db, get_session, Author, AuthorCatalogBook, Book
from collections import defaultdict
import re

def normalize_title_for_comparison(title):
    """Normalize title for duplicate detection (same as de-dupe function)"""
    if not title:
        return ''
    # Remove split edition markers like [1/2], [1/4], [2/2], etc.
    title = re.sub(r'\s*\[\d+/\d+\]\s*', ' ', title)
    # Remove common edition markers that don't affect content
    title = re.sub(r'\s*\([^)]*(?:edition|version|translation)[^)]*\)', '', title, flags=re.IGNORECASE)
    title = re.sub(r'\s*\[[^\]]*(?:edition|version|translation)[^\]]*\]', '', title, flags=re.IGNORECASE)
    # Normalize whitespace and case
    return ' '.join(title.lower().split()).strip()

def analyze_author_catalog(author_name):
    """Analyze an author's catalog books"""
    db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    # Find author(s)
    authors = session.query(Author).filter(
        (Author.name.ilike(f'%{author_name}%')) | 
        (Author.normalized_name.ilike(f'%{author_name}%'))
    ).all()
    
    if not authors:
        print(f"❌ Author matching '{author_name}' not found in database")
        print("\nSearching for similar authors...")
        all_authors = session.query(Author).all()
        similar = [a for a in all_authors if author_name.lower() in a.name.lower() or author_name.lower() in a.normalized_name.lower()]
        if similar:
            print("Found similar authors:")
            for a in similar[:10]:  # Show first 10
                print(f"  - ID {a.id}: {a.name} (normalized: {a.normalized_name})")
        session.close()
        return
    
    print(f"✓ Found {len(authors)} author(s) matching '{author_name}':\n")
    for author in authors:
        print(f"  Author ID {author.id}: {author.name}")
        print(f"    Normalized: {author.normalized_name}")
        print(f"    Open Library ID: {author.open_library_id}")
        print()
    
    # Analyze each author
    for author in authors:
        print(f"\n{'='*80}")
        print(f"ANALYZING: {author.name} (ID: {author.id})")
        print(f"{'='*80}\n")
        
        # Get all catalog books for this author
        catalog_books = session.query(AuthorCatalogBook).filter_by(author_id=author.id).all()
        print(f"Total catalog books: {len(catalog_books)}\n")
        
        if not catalog_books:
            print("  No catalog books found for this author\n")
            continue
        
        # Group by normalized title (same logic as de-dupe function)
        title_groups = defaultdict(list)
        for book in catalog_books:
            title_key = normalize_title_for_comparison(book.title)
            if title_key:
                title_groups[title_key].append(book)
        
        # Find duplicates
        duplicates = {k: v for k, v in title_groups.items() if len(v) > 1}
        
        print(f"📊 DUPLICATE ANALYSIS:")
        print(f"  Unique titles: {len(title_groups)}")
        print(f"  Duplicate groups: {len(duplicates)}")
        print(f"  Total duplicate books: {sum(len(v) - 1 for v in duplicates.values())}\n")
        
        if duplicates:
            print("  Duplicate groups found:")
            for title_key, books in sorted(duplicates.items()):
                print(f"\n    Title: '{books[0].title}'")
                print(f"    Normalized key: '{title_key}'")
                print(f"    Count: {len(books)}")
                for i, book in enumerate(books, 1):
                    score = calculate_completeness_score(book)
                    print(f"      {i}. ID {book.id}: ISBN={book.isbn or 'N/A'}, "
                          f"Description={'Yes' if book.description else 'No'}, "
                          f"OL Key={book.open_library_key or 'N/A'}, "
                          f"Score={score}")
        else:
            print("  ✓ No exact duplicates found (by normalized title)")
        
        # Check for near-duplicates (titles that differ only by edition markers)
        print(f"\n📚 NEAR-DUPLICATE ANALYSIS (titles that differ by edition markers):")
        near_duplicates = find_near_duplicates(catalog_books)
        if near_duplicates:
            print(f"  Found {len(near_duplicates)} near-duplicate groups:")
            for base_title, books in sorted(near_duplicates.items()):
                print(f"\n    Base title: '{base_title}'")
                print(f"    Count: {len(books)}")
                for book in books:
                    print(f"      - ID {book.id}: '{book.title}'")
        else:
            print("  ✓ No near-duplicates found")
        
        # Check for non-English editions (using improved pattern)
        print(f"\n🌍 NON-ENGLISH EDITION ANALYSIS (with improved patterns):")
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
            r'\b(?:edici[oó]n|colecci[oó]n|estuche|libro|libros|misterio|pr[ií]ncipe|gryffindor|hufflepuff|slytherin|ravenclaw)\b',
            re.IGNORECASE
        )
        
        non_english_books = []
        for book in catalog_books:
            if book.title:
                if (paren_pattern.search(book.title) or 
                    bracket_pattern.search(book.title) or
                    standalone_pattern.search(book.title) or
                    spanish_indicators.search(book.title)):
                    non_english_books.append(book)
        
        if non_english_books:
            print(f"  ⚠ Found {len(non_english_books)} books flagged by pattern matching:")
            for book in non_english_books[:20]:  # Show first 20
                print(f"      - ID {book.id}: '{book.title}'")
            if len(non_english_books) > 20:
                print(f"      ... and {len(non_english_books) - 20} more")
        else:
            print("  ✓ No language edition markers found")
        
        # Check for character-based detection
        print(f"\n🔤 CHARACTER-BASED DETECTION:")
        major_non_english_pattern = re.compile(
            r'[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ff\u0400-\u04ff\u0600-\u06ff\u0590-\u05ff]'
        )
        accented_chars_pattern = re.compile(
            r'[àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿąćčđęěğıłńňřśşšťůźżž]',
            re.IGNORECASE
        )
        spanish_punct = re.compile(r'[¿¡]')
        german_eszett = re.compile(r'ß')
        
        char_based_non_english = []
        for book in catalog_books:
            if book.title:
                flagged = False
                reason = []
                
                # Check major scripts
                if major_non_english_pattern.search(book.title):
                    flagged = True
                    reason.append("CJK/Cyrillic/Arabic/Hebrew")
                
                # Check Spanish punctuation or German ß
                if spanish_punct.search(book.title):
                    flagged = True
                    reason.append("Spanish punctuation")
                if german_eszett.search(book.title):
                    flagged = True
                    reason.append("German ß")
                
                # Check accented characters (matching cleanup function logic)
                if accented_chars_pattern.search(book.title):
                    accented_count = len(accented_chars_pattern.findall(book.title))
                    total_alpha_chars = len([c for c in book.title if c.isalpha()])
                    if total_alpha_chars > 0:
                        ratio = accented_count / total_alpha_chars
                        # Flag if more than 5% of characters are accented, or if title is short (< 15 chars) and has 2+ accented chars, or if 3+ accented chars
                        if ratio > 0.05 or (len(book.title) < 15 and accented_count >= 2) or accented_count >= 3:
                            flagged = True
                            reason.append(f"accented chars ({accented_count}/{total_alpha_chars}, {ratio:.1%})")
                
                if flagged:
                    char_based_non_english.append((book, ', '.join(reason)))
        
        if char_based_non_english:
            print(f"  ⚠ Found {len(char_based_non_english)} books flagged by character detection:")
            for book, reason in char_based_non_english[:20]:
                print(f"      - ID {book.id}: '{book.title}' ({reason})")
            if len(char_based_non_english) > 20:
                print(f"      ... and {len(char_based_non_english) - 20} more")
        else:
            print("  ✓ No character-based issues found")
        
        # Combined list (pattern + character based)
        all_non_english_ids = {book.id for book in non_english_books}
        all_non_english_ids.update({book.id for book, _ in char_based_non_english})
        print(f"\n📊 TOTAL NON-ENGLISH BOOKS (combined): {len(all_non_english_ids)}")
        
        # Show all titles for manual inspection
        print(f"\n📖 ALL TITLES (first 50):")
        for i, book in enumerate(sorted(catalog_books, key=lambda b: b.title or ''), 1):
            if i > 50:
                print(f"  ... and {len(catalog_books) - 50} more")
                break
            print(f"  {i}. ID {book.id}: '{book.title}'")
    
    session.close()

def calculate_completeness_score(book):
    """Calculate completeness score (same as de-dupe function)"""
    score = 0
    if book.isbn:
        score += 10
    if book.description:
        score += 5
    if book.open_library_key:
        score += 3
    if book.google_books_id:
        score += 2
    if book.publication_date:
        score += 1
    return score

def find_near_duplicates(books):
    """Find titles that are similar but differ by edition markers"""
    # Remove common edition markers and normalize
    def normalize_for_near_match(title):
        if not title:
            return ''
        # Remove edition markers
        title = re.sub(r'\([^)]*(?:edition|version|translation)[^)]*\)', '', title, flags=re.IGNORECASE)
        title = re.sub(r'\[[^\]]*(?:edition|version|translation)[^\]]*\]', '', title, flags=re.IGNORECASE)
        # Normalize
        return title.lower().strip()
    
    near_groups = defaultdict(list)
    for book in books:
        if book.title:
            base_key = normalize_for_near_match(book.title)
            if base_key:
                near_groups[base_key].append(book)
    
    # Only return groups with multiple books
    return {k: v for k, v in near_groups.items() if len(v) > 1}

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Analyze an author\'s catalog books for duplicates and non-English editions',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze an author's catalog
  python scripts/analyze_author_catalog.py --author "Author Name"
  
  # Analyze with partial name match
  python scripts/analyze_author_catalog.py --author "Partial Name"
        """
    )
    parser.add_argument('--author', required=True, help='Author name to analyze')
    args = parser.parse_args()
    
    analyze_author_catalog(args.author)
