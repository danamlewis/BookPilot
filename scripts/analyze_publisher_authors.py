#!/usr/bin/env python3
"""Analyze all authors to find publisher/company names incorrectly cataloged as authors"""
import sys
from pathlib import Path
import re

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import init_db, get_session, Author, AuthorCatalogBook
from sqlalchemy import func
from collections import defaultdict


def is_likely_publisher(name):
    """Check if a name looks like a publisher/company rather than an author"""
    name_lower = name.lower()
    
    # Patterns that suggest publisher/company
    patterns = [
        r'\.com$',  # Ends with .com
        r'\.org$',  # Ends with .org
        r'\.net$',  # Ends with .net
        r'\bstaff\b',  # Contains "staff"
        r'\beditors?\b',  # Contains "editor" or "editors"
        r'\bcontributors?\b',  # Contains "contributor" or "contributors"
        r'\bpress\b',  # Contains "press"
        r'\bpublishing\b',  # Contains "publishing"
        r'\bpublications?\b',  # Contains "publication" or "publications"
        r'\binc\.?$',  # Ends with "inc" or "inc."
        r'\bllc\.?$',  # Ends with "llc" or "llc."
        r'\bcorp\.?$',  # Ends with "corp" or "corp."
        r'\bcompany\b',  # Contains "company"
        r'\bgroup\b',  # Contains "group"
        r'^the\s+\w+\s+press$',  # "The [Name] Press"
        r'^[A-Z]+\s+[A-Z]+$',  # All caps (likely acronym)
    ]
    
    for pattern in patterns:
        if re.search(pattern, name_lower):
            return True
    
    # Check for common publisher names
    common_publishers = [
        'guideposts',
        'instructables',
        'penguin',
        'harpercollins',
        'simon',
        'schuster',
        'random house',
        'macmillan',
        'hachette',
        'scholastic',
        'disney',
        'marvel',
        'dc comics',
        'time life',
        'national geographic',
        'oxford university press',
        'cambridge university press',
        'mit press',
        'harvard university press',
        'yale university press',
        'princeton university press',
        'stanford university press',
        'university of chicago press',
        'university press',
    ]
    
    for publisher in common_publishers:
        if publisher in name_lower:
            return True
    
    return False


def analyze_authors():
    """Analyze all authors for publisher/company names"""
    db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    print("=" * 80)
    print("ANALYZING AUTHORS FOR PUBLISHER/COMPANY NAMES")
    print("=" * 80)
    print()
    
    all_authors = session.query(Author).order_by(Author.name).all()
    print(f"Total authors in database: {len(all_authors)}\n")
    
    suspicious_authors = []
    publisher_patterns = defaultdict(list)
    
    for author in all_authors:
        if is_likely_publisher(author.name):
            catalog_count = session.query(AuthorCatalogBook).filter_by(author_id=author.id).count()
            suspicious_authors.append({
                'author': author,
                'catalog_count': catalog_count,
                'reason': 'matches publisher pattern'
            })
            
            # Categorize by pattern
            name_lower = author.name.lower()
            if '.com' in name_lower or '.org' in name_lower or '.net' in name_lower:
                publisher_patterns['Website domains'].append(author)
            elif 'staff' in name_lower or 'editors' in name_lower or 'contributors' in name_lower:
                publisher_patterns['Staff/Editors'].append(author)
            elif 'press' in name_lower or 'publishing' in name_lower or 'publications' in name_lower:
                publisher_patterns['Press/Publishing'].append(author)
            elif 'inc' in name_lower or 'llc' in name_lower or 'corp' in name_lower or 'company' in name_lower:
                publisher_patterns['Corporate entities'].append(author)
            elif author.name.isupper() and len(author.name.split()) <= 3:
                publisher_patterns['Acronyms'].append(author)
            else:
                publisher_patterns['Other'].append(author)
    
    # Also check for authors with suspiciously high catalog counts (might be publishers)
    high_count_authors = []
    for author in all_authors:
        catalog_count = session.query(AuthorCatalogBook).filter_by(author_id=author.id).count()
        if catalog_count > 50:  # Threshold for suspiciously high count
            # Skip if already flagged
            if not any(s['author'].id == author.id for s in suspicious_authors):
                high_count_authors.append({
                    'author': author,
                    'catalog_count': catalog_count
                })
    
    # Print results by category
    print("=" * 80)
    print("SUSPICIOUS AUTHORS BY CATEGORY")
    print("=" * 80)
    print()
    
    total_suspicious = 0
    for category, authors in publisher_patterns.items():
        if authors:
            print(f"\n{category} ({len(authors)} authors):")
            print("-" * 80)
            for author in sorted(authors, key=lambda a: a.name):
                catalog_count = session.query(AuthorCatalogBook).filter_by(author_id=author.id).count()
                print(f"  {author.name} (ID: {author.id})")
                print(f"    Normalized: {author.normalized_name}")
                print(f"    Catalog books: {catalog_count}")
                
                # Show sample titles
                if catalog_count > 0:
                    sample_books = session.query(AuthorCatalogBook).filter_by(
                        author_id=author.id
                    ).limit(5).all()
                    print(f"    Sample titles:")
                    for book in sample_books:
                        print(f"      - {book.title}")
                print()
                total_suspicious += 1
    
    # Print high-count authors
    if high_count_authors:
        print("\n" + "=" * 80)
        print(f"AUTHORS WITH HIGH CATALOG COUNTS (>50 books) - Possible Publishers")
        print("=" * 80)
        print()
        for item in sorted(high_count_authors, key=lambda x: x['catalog_count'], reverse=True):
            author = item['author']
            catalog_count = item['catalog_count']
            print(f"  {author.name} (ID: {author.id})")
            print(f"    Normalized: {author.normalized_name}")
            print(f"    Catalog books: {catalog_count}")
            
            # Show sample titles
            sample_books = session.query(AuthorCatalogBook).filter_by(
                author_id=author.id
            ).limit(5).all()
            print(f"    Sample titles:")
            for book in sample_books:
                print(f"      - {book.title}")
            print()
    
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total authors analyzed: {len(all_authors)}")
    print(f"Suspicious authors found: {total_suspicious}")
    print(f"High-count authors (>50 books): {len(high_count_authors)}")
    print()
    
    # Check for authors with mismatched normalized names (could indicate grouping issues)
    print("=" * 80)
    print("AUTHORS WITH POTENTIALLY MISMATCHED NORMALIZED NAMES")
    print("=" * 80)
    print()
    
    mismatched = []
    for author in all_authors:
        # If normalized name is very different from actual name, might be a grouping issue
        if author.normalized_name and author.name:
            # Check if normalized name is a completely different person
            name_words = set(author.name.lower().split())
            norm_words = set(author.normalized_name.lower().split())
            
            # If they share less than 50% of words and both have multiple words, might be mismatched
            if len(name_words) > 1 and len(norm_words) > 1:
                overlap = len(name_words & norm_words)
                total_unique = len(name_words | norm_words)
                if total_unique > 0:
                    similarity = overlap / total_unique
                    if similarity < 0.3:  # Less than 30% word overlap
                        catalog_count = session.query(AuthorCatalogBook).filter_by(author_id=author.id).count()
                        if catalog_count > 0:  # Only show if they have catalog books
                            mismatched.append({
                                'author': author,
                                'similarity': similarity,
                                'catalog_count': catalog_count
                            })
    
    # Show top mismatches
    mismatched.sort(key=lambda x: x['catalog_count'], reverse=True)
    for item in mismatched[:20]:  # Show top 20
        author = item['author']
        print(f"  {author.name}")
        print(f"    Normalized: {author.normalized_name}")
        print(f"    Similarity: {item['similarity']:.2%}")
        print(f"    Catalog books: {item['catalog_count']}")
        print()


if __name__ == '__main__':
    analyze_authors()
