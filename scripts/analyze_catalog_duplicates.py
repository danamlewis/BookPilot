#!/usr/bin/env python3
"""
Analyze duplicate patterns in author catalogs (not just recommendations).

This analyzes AuthorCatalogBook entries to find prolific authors and duplicates.
"""

import sys
from pathlib import Path
from collections import defaultdict
from typing import List, Dict
import re
import json
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import init_db, get_session, Author, AuthorCatalogBook
from sqlalchemy import func
# Import normalization functions (will use improved versions)
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / 'scripts'))
from check_duplicate_recommendations import (
    normalize_title_advanced, extract_base_title, similarity_score,
    normalize_isbn
)


def find_catalog_duplicates(catalog_books: List[AuthorCatalogBook], 
                           similarity_threshold: float = 0.85) -> Dict:
    """
    Find duplicate groups in catalog books using multiple techniques.
    """
    groups = defaultdict(list)
    
    # Group 1: Exact normalized title match
    normalized_groups = defaultdict(list)
    for book in catalog_books:
        normalized = normalize_title_advanced(book.title)
        if normalized:
            normalized_groups[normalized].append(book)
    
    # Group 2: Base title match
    base_title_groups = defaultdict(list)
    for book in catalog_books:
        base_title = extract_base_title(book.title)
        if base_title:
            base_title_groups[base_title.lower().strip()].append(book)
    
    # Group 3: ISBN match
    isbn_groups = defaultdict(list)
    for book in catalog_books:
        if book.isbn:
            normalized_isbn = normalize_isbn(book.isbn)
            if normalized_isbn:
                isbn_groups[normalized_isbn].append(book)
    
    # Combine groups
    all_groups = {}
    group_id = 0
    
    # Add normalized title groups
    for key, books in normalized_groups.items():
        if len(books) > 1:
            all_groups[f"exact_{group_id}"] = books
            group_id += 1
    
    # Add base title groups
    for key, books in base_title_groups.items():
        if len(books) > 1:
            already_grouped = set()
            for existing_key, existing_books in all_groups.items():
                already_grouped.update(id(b) for b in existing_books)
            
            new_books = [b for b in books if id(b) not in already_grouped]
            if len(new_books) > 1:
                all_groups[f"base_{group_id}"] = new_books
                group_id += 1
    
    # Add ISBN groups
    for key, books in isbn_groups.items():
        if len(books) > 1:
            already_grouped = set()
            for existing_key, existing_books in all_groups.items():
                already_grouped.update(id(b) for b in existing_books)
            
            new_books = [b for b in books if id(b) not in already_grouped]
            if len(new_books) > 1:
                all_groups[f"isbn_{group_id}"] = new_books
                group_id += 1
    
    # Group 4: Fuzzy matching
    processed_ids = set()
    for i, book1 in enumerate(catalog_books):
        book1_id = book1.id
        if book1_id in processed_ids:
            continue
        
        similar_group = [book1]
        title1_norm = normalize_title_advanced(book1.title)
        title1_base = extract_base_title(book1.title).lower()
        
        for j, book2 in enumerate(catalog_books[i+1:], i+1):
            book2_id = book2.id
            if book2_id in processed_ids:
                continue
            
            # Check if already grouped
            in_same_group = False
            for existing_key, existing_books in all_groups.items():
                book1_in_group = any(b.id == book1.id for b in existing_books)
                book2_in_group = any(b.id == book2.id for b in existing_books)
                if book1_in_group and book2_in_group:
                    in_same_group = True
                    break
            if in_same_group:
                continue
            
            title2_norm = normalize_title_advanced(book2.title)
            title2_base = extract_base_title(book2.title).lower()
            
            sim = similarity_score(title1_norm, title2_norm)
            base_sim = similarity_score(title1_base, title2_base) if title1_base and title2_base else 0.0
            max_sim = max(sim, base_sim)
            
            if max_sim >= similarity_threshold:
                similar_group.append(book2)
                processed_ids.add(book2_id)
        
        if len(similar_group) > 1:
            all_groups[f"fuzzy_{group_id}"] = similar_group
            group_id += 1
            processed_ids.add(book1_id)
    
    return all_groups


def analyze_author_catalog(author: Author, catalog_books: List[AuthorCatalogBook]) -> Dict:
    """
    Analyze duplicates in an author's catalog.
    """
    print(f"\n{'='*80}")
    print(f"ANALYZING: {author.name} (ID: {author.id})")
    print(f"Eligible catalog books (not read): {len(catalog_books)}")
    print(f"{'='*80}")
    
    # Find duplicate groups
    duplicate_groups = find_catalog_duplicates(catalog_books, similarity_threshold=0.85)
    
    # Analyze patterns
    patterns = {
        'exact_normalized': 0,
        'base_title_match': 0,
        'isbn_match': 0,
        'fuzzy_match': 0,
        'apostrophe_variations': 0,
        'punctuation_variations': 0,
        'series_variations': 0,
        'edition_variations': 0,
        'volume_variations': 0,
        'non_english_detected': 0
    }
    
    duplicate_details = []
    
    for group_key, group_books in duplicate_groups.items():
        if len(group_books) < 2:
            continue
        
        # Score books to determine which to keep
        def score_book(book):
            score = 0
            if book.isbn:
                score += 10
            if book.description:
                score += 5
            if book.open_library_key:
                score += 3
            if book.publication_date:
                score += 1
            return score
        
        sorted_books = sorted(group_books, key=score_book, reverse=True)
        keep_book = sorted_books[0]
        remove_books = sorted_books[1:]
        
        # Analyze why these are duplicates
        keep_title = keep_book.title
        all_reasons = []
        pattern_types = []
        
        for book in remove_books:
            book_title = book.title
            reasons = []
            
            keep_norm = normalize_title_advanced(keep_title)
            book_norm = normalize_title_advanced(book_title)
            keep_base = extract_base_title(keep_title)
            book_base = extract_base_title(book_title)
            
            if keep_norm == book_norm:
                reasons.append("exact normalized match")
                pattern_types.append('exact_normalized')
                patterns['exact_normalized'] += 1
            
            if keep_base.lower() == book_base.lower():
                reasons.append("base title match")
                pattern_types.append('base_title_match')
                patterns['base_title_match'] += 1
            
            if keep_book.isbn and book.isbn and normalize_isbn(keep_book.isbn) == normalize_isbn(book.isbn):
                reasons.append("ISBN match")
                pattern_types.append('isbn_match')
                patterns['isbn_match'] += 1
            
            sim = similarity_score(keep_norm, book_norm)
            if sim >= 0.85:
                reasons.append(f"fuzzy match ({sim:.2f})")
                pattern_types.append('fuzzy_match')
                patterns['fuzzy_match'] += 1
            
            # Check for apostrophe variations (improved)
            # Normalize all apostrophe types: ' ' ` → remove for comparison
            keep_no_apos = re.sub(r"[''`]", '', keep_title.lower())
            book_no_apos = re.sub(r"[''`]", '', book_title.lower())
            # Also handle possessives: "Daughter's" vs "Daughters"
            keep_no_poss = re.sub(r"s'", 's', keep_no_apos)
            book_no_poss = re.sub(r"s'", 's', book_no_apos)
            if (keep_no_apos == book_no_apos or keep_no_poss == book_no_poss) and keep_title != book_title:
                reasons.append("apostrophe variation")
                pattern_types.append('apostrophe_variations')
                patterns['apostrophe_variations'] += 1
            
            # Check for punctuation variations
            keep_no_punct = re.sub(r'[^\w\s]', '', keep_title.lower())
            book_no_punct = re.sub(r'[^\w\s]', '', book_title.lower())
            if keep_no_punct == book_no_punct and keep_title != book_title:
                reasons.append("punctuation variation")
                pattern_types.append('punctuation_variations')
                patterns['punctuation_variations'] += 1
            
            # Check for series variations
            if '(series' in keep_title.lower() or '(series' in book_title.lower():
                if keep_base.lower() == book_base.lower():
                    reasons.append("series variation")
                    pattern_types.append('series_variations')
                    patterns['series_variations'] += 1
            
            # Check for edition variations (improved - now handles "ed." and "Edition")
            if re.search(r'\b(?:edition|ed\.)', keep_title.lower()) or re.search(r'\b(?:edition|ed\.)', book_title.lower()):
                if keep_base.lower() == book_base.lower():
                    reasons.append("edition variation")
                    pattern_types.append('edition_variations')
                    patterns['edition_variations'] += 1
            
            # Check for volume variations (new detection)
            if re.search(r'\b(?:volume|vol\.?)', keep_title.lower()) or re.search(r'\b(?:volume|vol\.?)', book_title.lower()):
                # Compare base titles after removing volume indicators
                keep_no_vol = re.sub(r'\b(?:volume|vol\.?)\s*(?:i{1,3}|iv|v{1,3}|vi{0,3}|[0-9]+)\b', '', keep_title.lower(), flags=re.IGNORECASE)
                book_no_vol = re.sub(r'\b(?:volume|vol\.?)\s*(?:i{1,3}|iv|v{1,3}|vi{0,3}|[0-9]+)\b', '', book_title.lower(), flags=re.IGNORECASE)
                keep_no_vol = re.sub(r'\bvolume\s+\d+\b', '', keep_no_vol, flags=re.IGNORECASE)
                book_no_vol = re.sub(r'\bvolume\s+\d+\b', '', book_no_vol, flags=re.IGNORECASE)
                keep_no_vol = re.sub(r'\bvol\.?\s*\d+\b', '', keep_no_vol, flags=re.IGNORECASE)
                book_no_vol = re.sub(r'\bvol\.?\s*\d+\b', '', book_no_vol, flags=re.IGNORECASE)
                keep_no_vol = normalize_title_advanced(keep_no_vol)
                book_no_vol = normalize_title_advanced(book_no_vol)
                if keep_no_vol == book_no_vol and keep_title != book_title:
                    reasons.append("volume variation")
                    pattern_types.append('volume_variations')
                    if 'volume_variations' not in patterns:
                        patterns['volume_variations'] = 0
                    patterns['volume_variations'] += 1
            
            all_reasons.extend(reasons)
        
        duplicate_details.append({
            'group_key': group_key,
            'keep': {
                'id': keep_book.id,
                'title': keep_title,
                'isbn': keep_book.isbn,
                'description': bool(keep_book.description)
            },
            'remove': [
                {
                    'id': book.id,
                    'title': book.title,
                    'isbn': book.isbn,
                    'reasons': list(set([r for r in all_reasons if r]))
                }
                for book in remove_books
            ],
            'pattern_types': list(set(pattern_types)),
            'count': len(group_books)
        })
    
    # Check for non-English titles
    non_english_patterns = [
        r'[\u4e00-\u9fff]',  # Chinese
        r'[\u3040-\u309f\u30a0-\u30ff]',  # Japanese
        r'[\u0400-\u04ff]',  # Cyrillic
        r'[\u0600-\u06ff]',  # Arabic
        r'[\u0590-\u05ff]',  # Hebrew
    ]
    
    for book in catalog_books:
        title = book.title or ''
        for pattern in non_english_patterns:
            if re.search(pattern, title):
                patterns['non_english_detected'] += 1
                break
    
    total_duplicates = sum(len(group) - 1 for group in duplicate_groups.values())
    
    return {
        'author_id': author.id,
        'author_name': author.name,
        'total_catalog_books': len(catalog_books),  # This is now filtered to eligible books only
        'duplicate_groups': len(duplicate_groups),
        'total_duplicates': total_duplicates,
        'patterns': patterns,
        'duplicate_details': duplicate_details,
        'non_english_count': patterns['non_english_detected'],
        'note': 'Analysis includes only catalog books eligible for recommendations (is_read=False, not non-English)'
    }


def analyze_prolific_catalogs(min_books: int = 100, 
                             author_limit: int = None,
                             output_file: str = None) -> Dict:
    """
    Analyze authors with many catalog books for duplicate patterns.
    """
    db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    print("="*80)
    print("PROLIFIC AUTHOR CATALOG DUPLICATE ANALYSIS")
    print("="*80)
    print(f"\nAnalyzing authors with >{min_books} eligible catalog books...")
    print("(Only analyzing books eligible for recommendations: is_read=False, not non-English)")
    print()
    
    # Get all authors with catalog books
    # Count only books that would be eligible for recommendations (is_read=False)
    authors_with_catalogs = session.query(Author).join(AuthorCatalogBook).distinct().all()
    
    prolific_authors = []
    for author in authors_with_catalogs:
        # Count only eligible catalog books (not already read)
        catalog_count = session.query(AuthorCatalogBook).filter_by(
            author_id=author.id,
            is_read=False
        ).count()
        if catalog_count >= min_books:
            prolific_authors.append((author, catalog_count))
    
    # Sort by catalog count
    prolific_authors.sort(key=lambda x: x[1], reverse=True)
    
    print(f"Found {len(prolific_authors)} author(s) with >{min_books} catalog books\n")
    
    if author_limit:
        prolific_authors = prolific_authors[:author_limit]
        print(f"Processing first {len(prolific_authors)} author(s)...\n")
    
    results = []
    total_patterns = defaultdict(int)
    
    for author, catalog_count in prolific_authors:
        # Only analyze catalog books that would actually be recommended
        # Filter to books that are NOT already read (is_read=False)
        # This matches what the recommendation system uses
        catalog_books = session.query(AuthorCatalogBook).filter_by(
            author_id=author.id,
            is_read=False
        ).all()
        
        # Also filter out non-English books using enhanced detection
        from src.deduplication.language_detection import is_english_title
        
        # Filter out non-English books
        catalog_books = [b for b in catalog_books if is_english_title(b.title, b.isbn, b.open_library_key)]
        
        print(f"  Filtered: {catalog_count} total → {len(catalog_books)} eligible for recommendations")
        
        result = analyze_author_catalog(author, catalog_books)
        results.append(result)
        
        # Aggregate patterns
        for pattern, count in result['patterns'].items():
            total_patterns[pattern] += count
    
    # Generate summary report
    print("\n" + "="*80)
    print("SUMMARY REPORT")
    print("="*80)
    
    print(f"\nAuthors analyzed: {len(results)}")
    print(f"Total catalog books: {sum(r['total_catalog_books'] for r in results)}")
    print(f"Total duplicate groups: {sum(r['duplicate_groups'] for r in results)}")
    print(f"Total duplicates found: {sum(r['total_duplicates'] for r in results)}")
    
    print(f"\nDuplicate Patterns Detected:")
    for pattern, count in sorted(total_patterns.items(), key=lambda x: -x[1]):
        if count > 0:
            print(f"  {pattern}: {count}")
    
    print(f"\nTop Authors by Duplicate Count:")
    sorted_results = sorted(results, key=lambda x: x['total_duplicates'], reverse=True)
    for i, result in enumerate(sorted_results[:10], 1):
        print(f"  {i}. {result['author_name']}: {result['total_duplicates']} duplicates in {result['duplicate_groups']} groups ({result['total_catalog_books']} total books)")
    
    # Create detailed report
    report = {
        'analysis_date': datetime.utcnow().isoformat(),
        'min_books': min_books,
        'authors_analyzed': len(results),
        'summary': {
            'total_catalog_books': sum(r['total_catalog_books'] for r in results),
            'total_duplicate_groups': sum(r['duplicate_groups'] for r in results),
            'total_duplicates': sum(r['total_duplicates'] for r in results),
            'pattern_counts': dict(total_patterns)
        },
        'authors': results
    }
    
    # Save report
    if output_file:
        output_path = Path(__file__).parent.parent / output_file
    else:
        output_path = Path(__file__).parent.parent / 'data' / f'catalog_duplicate_analysis_{datetime.utcnow().strftime("%Y%m%d_%H%M%S")}.json'
    
    with open(output_path, 'w') as f:
        json.dump(report, f, indent=2)
    print(f"\n✓ Detailed report saved to: {output_path}")
    
    session.close()
    
    return report


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Analyze duplicate patterns in prolific author catalogs'
    )
    parser.add_argument('--min-books', type=int, default=100,
                       help='Minimum number of catalog books to analyze (default: 100, use 1 for all authors)')
    parser.add_argument('--limit', type=int,
                       help='Limit number of authors to process')
    parser.add_argument('--output', type=str,
                       help='Output file path')
    
    args = parser.parse_args()
    
    analyze_prolific_catalogs(
        min_books=args.min_books,
        author_limit=args.limit,
        output_file=args.output
    )
