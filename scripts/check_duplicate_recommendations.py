#!/usr/bin/env python3
"""
Sophisticated duplicate detection for recommendations, focusing on authors with >10 books.

This script uses multiple techniques to detect duplicates:
1. Title normalization (removing series info, edition markers)
2. ISBN matching
3. Fuzzy string matching (Levenshtein distance)
4. Substring matching (e.g., "Ruby" vs "Ruby (series name)")
5. Series name extraction and comparison
"""

import sys
from pathlib import Path
from collections import defaultdict
from typing import List, Dict, Tuple, Set
import re

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import init_db, get_session, Author, Recommendation
from sqlalchemy import func


def normalize_title_advanced(title: str) -> str:
    """
    Advanced title normalization for duplicate detection.
    Removes series info, edition markers, volume indicators, and normalizes formatting.
    """
    if not title:
        return ''
    
    # Remove series info in parentheses: "Ruby (series name)" -> "Ruby"
    # But be careful - only remove if it looks like a series name
    # Pattern: (Series Name), (Series Name #1), etc.
    title = re.sub(r'\s*\([^)]*(?:series|book\s+\d+|#\d+)[^)]*\)', '', title, flags=re.IGNORECASE)
    
    # Remove edition markers (including "ed." and "edition")
    title = re.sub(r'\s*\([^)]*(?:edition|version|translation|ed\.)[^)]*\)', '', title, flags=re.IGNORECASE)
    title = re.sub(r'\s*\[[^\]]*(?:edition|version|translation|ed\.)[^\]]*\]', '', title, flags=re.IGNORECASE)
    title = re.sub(r'\b(?:edition|ed\.)\b', '', title, flags=re.IGNORECASE)
    
    # Remove volume indicators (Volume, Vol., vol., etc.)
    title = re.sub(r'\b(?:volume|vol\.?)\s*(?:i{1,3}|iv|v{1,3}|vi{0,3}|[0-9]+)\b', '', title, flags=re.IGNORECASE)
    title = re.sub(r'\bvolume\s+\d+\b', '', title, flags=re.IGNORECASE)
    title = re.sub(r'\bvol\.?\s*\d+\b', '', title, flags=re.IGNORECASE)
    
    # Remove split edition markers like [1/2], [1/4]
    title = re.sub(r'\s*\[\d+/\d+\]\s*', ' ', title)
    
    # Remove common prefixes/suffixes that don't affect content
    title = re.sub(r'^(the|a|an)\s+', '', title, flags=re.IGNORECASE)
    
    # Normalize apostrophes and possessives
    # Standardize all apostrophe types: ' ' ` → (remove for comparison)
    title = re.sub(r"[''`]", '', title)
    
    # Normalize whitespace and case
    title = ' '.join(title.split()).strip().lower()
    
    return title


def extract_base_title(title: str) -> str:
    """
    Extract the base title, removing series info, volume/edition indicators.
    Handles cases like "Ruby (Red River Valley)" -> "Ruby"
    Also handles "Book Title Volume 2" -> "Book Title"
    """
    if not title:
        return ''
    
    # Remove everything in parentheses at the end: "Ruby (series name)" -> "Ruby"
    # This is the most common pattern for series info
    base = re.sub(r'\s*\([^)]+\)\s*$', '', title)
    
    # Also remove from middle if it looks like series info
    # Pattern: "Title (Series Name)" -> "Title"
    base = re.sub(r'\s*\([^)]*(?:series|book\s+\d+|#\d+)[^)]*\)', '', base, flags=re.IGNORECASE)
    
    # Remove volume indicators (Volume, Vol., vol., etc.)
    base = re.sub(r'\b(?:volume|vol\.?)\s*(?:i{1,3}|iv|v{1,3}|vi{0,3}|[0-9]+)\b', '', base, flags=re.IGNORECASE)
    base = re.sub(r'\bvolume\s+\d+\b', '', base, flags=re.IGNORECASE)
    base = re.sub(r'\bvol\.?\s*\d+\b', '', base, flags=re.IGNORECASE)
    
    # Remove edition indicators
    base = re.sub(r'\b(?:edition|ed\.)\b', '', base, flags=re.IGNORECASE)
    
    # Also handle brackets
    base = re.sub(r'\s*\[[^\]]+\]\s*$', '', base)
    
    # Normalize apostrophes for comparison
    base = re.sub(r"[''`]", '', base)
    
    return base.strip()


def extract_series_info(title: str) -> Tuple[str, str]:
    """
    Extract base title and series name from a title.
    Returns: (base_title, series_name)
    """
    if not title:
        return '', ''
    
    # Try to extract series from parentheses
    paren_match = re.search(r'\(([^)]+)\)', title)
    series_from_paren = paren_match.group(1) if paren_match else ''
    
    # Try to extract series from brackets
    bracket_match = re.search(r'\[([^\]]+)\]', title)
    series_from_bracket = bracket_match.group(1) if bracket_match else ''
    
    # Prefer parentheses series info
    series_name = series_from_paren or series_from_bracket
    
    # Get base title (remove series info)
    base_title = extract_base_title(title)
    
    return base_title, series_name


def levenshtein_distance(s1: str, s2: str) -> int:
    """
    Calculate Levenshtein distance between two strings.
    """
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)
    
    if len(s2) == 0:
        return len(s1)
    
    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row
    
    return previous_row[-1]


def similarity_score(s1: str, s2: str) -> float:
    """
    Calculate similarity score between two strings (0.0 to 1.0).
    Uses Levenshtein distance normalized by max length.
    """
    if not s1 or not s2:
        return 0.0
    
    max_len = max(len(s1), len(s2))
    if max_len == 0:
        return 1.0
    
    distance = levenshtein_distance(s1, s2)
    return 1.0 - (distance / max_len)


def normalize_isbn(isbn: str) -> str:
    """Normalize ISBN by removing hyphens and spaces."""
    if not isbn:
        return ''
    return re.sub(r'[-\s]', '', isbn).strip()


def find_duplicate_groups(recommendations: List[Recommendation], 
                          similarity_threshold: float = 0.85) -> Dict[str, List[Recommendation]]:
    """
    Find groups of duplicate recommendations using multiple techniques.
    
    Returns:
        Dictionary mapping group keys to lists of duplicate recommendations
    """
    groups = defaultdict(list)
    
    # Group 1: Exact normalized title match
    normalized_groups = defaultdict(list)
    for rec in recommendations:
        normalized = normalize_title_advanced(rec.title)
        if normalized:
            normalized_groups[normalized].append(rec)
    
    # Group 2: Base title match (handles "Ruby" vs "Ruby (series)")
    base_title_groups = defaultdict(list)
    for rec in recommendations:
        base_title = extract_base_title(rec.title)
        if base_title:
            base_title_groups[base_title.lower().strip()].append(rec)
    
    # Group 3: ISBN match
    isbn_groups = defaultdict(list)
    for rec in recommendations:
        if rec.isbn:
            normalized_isbn = normalize_isbn(rec.isbn)
            if normalized_isbn:
                isbn_groups[normalized_isbn].append(rec)
    
    # Combine groups
    all_groups = {}
    group_id = 0
    
            # Add normalized title groups
    for key, recs in normalized_groups.items():
        if len(recs) > 1:
            all_groups[f"exact_{group_id}"] = recs
            group_id += 1
    
    # Add base title groups (if not already in exact groups)
    for key, recs in base_title_groups.items():
        if len(recs) > 1:
            # Check if any of these are already grouped
            already_grouped = set()
            for existing_key, existing_recs in all_groups.items():
                for r in existing_recs:
                    rec_id = r.id if r.id else id(r)
                    already_grouped.add(rec_id)
            
            new_recs = [r for r in recs if (r.id if r.id else id(r)) not in already_grouped]
            if len(new_recs) > 1:
                all_groups[f"base_{group_id}"] = new_recs
                group_id += 1
    
    # Add ISBN groups (if not already grouped)
    for key, recs in isbn_groups.items():
        if len(recs) > 1:
            already_grouped = set()
            for existing_key, existing_recs in all_groups.items():
                for r in existing_recs:
                    rec_id = r.id if r.id else id(r)
                    already_grouped.add(rec_id)
            
            new_recs = [r for r in recs if (r.id if r.id else id(r)) not in already_grouped]
            if len(new_recs) > 1:
                all_groups[f"isbn_{group_id}"] = new_recs
                group_id += 1
    
    # Group 4: Fuzzy matching for similar titles
    # Check all pairs for high similarity
    processed_ids = set()
    for i, rec1 in enumerate(recommendations):
        rec1_id = rec1.id if rec1.id else id(rec1)
        if rec1_id in processed_ids:
            continue
        
        similar_group = [rec1]
        title1_norm = normalize_title_advanced(rec1.title)
        title1_base = extract_base_title(rec1.title).lower()
        
        for j, rec2 in enumerate(recommendations[i+1:], i+1):
            rec2_id = rec2.id if rec2.id else id(rec2)
            if rec2_id in processed_ids:
                continue
            
            # Check if already in a group together
            in_same_group = False
            for existing_key, existing_recs in all_groups.items():
                rec1_in_group = any(r.id == rec1.id if r.id else id(r) == id(rec1) for r in existing_recs)
                rec2_in_group = any(r.id == rec2.id if r.id else id(r) == id(rec2) for r in existing_recs)
                if rec1_in_group and rec2_in_group:
                    in_same_group = True
                    break
            if in_same_group:
                continue
            
            title2_norm = normalize_title_advanced(rec2.title)
            title2_base = extract_base_title(rec2.title).lower()
            
            # Check similarity on normalized titles
            sim = similarity_score(title1_norm, title2_norm)
            
            # Also check base title similarity (handles "Ruby" vs "Ruby (series)")
            base_sim = similarity_score(title1_base, title2_base) if title1_base and title2_base else 0.0
            
            # Use the higher similarity score
            max_sim = max(sim, base_sim)
            
            if max_sim >= similarity_threshold:
                similar_group.append(rec2)
                processed_ids.add(rec2_id)
        
        if len(similar_group) > 1:
            all_groups[f"fuzzy_{group_id}"] = similar_group
            group_id += 1
            processed_ids.add(rec1_id)
    
    # Group 5: Substring matching (one title contains the other)
    # This handles cases like "Ruby" vs "Ruby (Red River Valley)"
    for i, rec1 in enumerate(recommendations):
        rec1_id = rec1.id if rec1.id else id(rec1)
        if rec1_id in processed_ids:
            continue
        
        base1 = extract_base_title(rec1.title).lower().strip()
        if not base1:
            continue
        
        substring_group = [rec1]
        
        for j, rec2 in enumerate(recommendations[i+1:], i+1):
            rec2_id = rec2.id if rec2.id else id(rec2)
            if rec2_id in processed_ids:
                continue
            
            # Check if already grouped
            in_same_group = False
            for existing_key, existing_recs in all_groups.items():
                rec1_in_group = any(r.id == rec1.id if r.id else id(r) == id(rec1) for r in existing_recs)
                rec2_in_group = any(r.id == rec2.id if r.id else id(r) == id(rec2) for r in existing_recs)
                if rec1_in_group and rec2_in_group:
                    in_same_group = True
                    break
            if in_same_group:
                continue
            
            base2 = extract_base_title(rec2.title).lower().strip()
            if not base2:
                continue
            
            # Check if one base title is substring of the other
            # This catches "Ruby" vs "Ruby (Red River Valley)"
            if base1 == base2:
                # Exact base match - definitely duplicate
                substring_group.append(rec2)
                processed_ids.add(rec2_id)
            elif (base1 in base2 or base2 in base1) and abs(len(base1) - len(base2)) < 30:
                # One contains the other and they're similar length
                # Make sure the shorter one is at least 3 chars (avoid false positives)
                shorter = min(base1, base2, key=len)
                if len(shorter) >= 3:
                    substring_group.append(rec2)
                    processed_ids.add(rec2_id)
        
        if len(substring_group) > 1:
            # Check if this group overlaps with existing groups
            group_ids = {r.id if r.id else id(r) for r in substring_group}
            merged = False
            for existing_key, existing_recs in list(all_groups.items()):
                existing_ids = {r.id if r.id else id(r) for r in existing_recs}
                if group_ids & existing_ids:  # Overlap
                    # Merge groups (avoid duplicates)
                    combined = existing_recs + [r for r in substring_group if (r.id if r.id else id(r)) not in existing_ids]
                    all_groups[existing_key] = combined
                    merged = True
                    break
            
            if not merged:
                all_groups[f"substring_{group_id}"] = substring_group
                group_id += 1
    
    return all_groups


def analyze_author_recommendations(author: Author, recommendations: List[Recommendation],
                                   min_books: int = 10, dry_run: bool = True) -> Dict:
    """
    Analyze recommendations for a single author and find duplicates.
    """
    if len(recommendations) < min_books:
        return None
    
    print(f"\n{'='*80}")
    print(f"AUTHOR: {author.name} (ID: {author.id})")
    print(f"Total recommendations: {len(recommendations)}")
    print(f"{'='*80}")
    
    # Find duplicate groups
    duplicate_groups = find_duplicate_groups(recommendations)
    
    if not duplicate_groups:
        print("\n✓ No duplicates found")
        return {
            'author_id': author.id,
            'author_name': author.name,
            'total_recommendations': len(recommendations),
            'duplicate_groups': 0,
            'duplicate_count': 0
        }
    
    print(f"\n📊 DUPLICATE ANALYSIS:")
    print(f"  Found {len(duplicate_groups)} duplicate group(s)")
    
    total_duplicates = 0
    for group_key, group_recs in duplicate_groups.items():
        total_duplicates += len(group_recs) - 1  # -1 because we keep one
    
    print(f"  Total duplicate recommendations: {total_duplicates}")
    
    # Show details
    print(f"\n  Duplicate groups:")
    for group_key, group_recs in sorted(duplicate_groups.items()):
        print(f"\n    Group: {group_key}")
        print(f"    Count: {len(group_recs)}")
        
        # Determine which one to keep (prefer one with ISBN, not already flagged as duplicate)
        def score_rec(rec):
            score = 0
            if rec.isbn:
                score += 10
            if not rec.duplicate:
                score += 5
            # Check if it has a catalog_book_id (links to AuthorCatalogBook which has description)
            if rec.catalog_book_id:
                score += 3
            # Check if it has a reason (more complete data)
            if rec.reason:
                score += 2
            return score
        
        sorted_recs = sorted(group_recs, key=score_rec, reverse=True)
        keep_rec = sorted_recs[0]
        remove_recs = sorted_recs[1:]
        
        print(f"    KEEP: '{keep_rec.title}' (ID: {keep_rec.id})")
        if keep_rec.isbn:
            print(f"           ISBN: {keep_rec.isbn}")
        
        for rec in remove_recs:
            print(f"    REMOVE: '{rec.title}' (ID: {rec.id})")
            if rec.isbn:
                print(f"            ISBN: {rec.isbn}")
            
            # Show why it's a duplicate
            keep_norm = normalize_title_advanced(keep_rec.title)
            rec_norm = normalize_title_advanced(rec.title)
            keep_base = extract_base_title(keep_rec.title)
            rec_base = extract_base_title(rec.title)
            
            reasons = []
            if keep_norm == rec_norm:
                reasons.append("exact normalized match")
            if keep_base.lower() == rec_base.lower():
                reasons.append("base title match")
            if keep_rec.isbn and rec.isbn and normalize_isbn(keep_rec.isbn) == normalize_isbn(rec.isbn):
                reasons.append("ISBN match")
            if similarity_score(keep_norm, rec_norm) >= 0.85:
                reasons.append(f"fuzzy match ({similarity_score(keep_norm, rec_norm):.2f})")
            
            if reasons:
                print(f"            Reason: {', '.join(reasons)}")
    
    return {
        'author_id': author.id,
        'author_name': author.name,
        'total_recommendations': len(recommendations),
        'duplicate_groups': len(duplicate_groups),
        'duplicate_count': total_duplicates,
        'groups': duplicate_groups
    }


def check_authors_with_many_recommendations(min_books: int = 10, 
                                           author_limit: int = None,
                                           dry_run: bool = True,
                                           auto_flag: bool = False) -> Dict:
    """
    Check authors with >min_books recommendations for duplicates.
    
    Args:
        min_books: Minimum number of recommendations to check an author
        author_limit: Limit number of authors to process (None for all)
        dry_run: If True, only report without flagging duplicates
        auto_flag: If True, automatically flag duplicates (requires dry_run=False)
    """
    db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    print("="*80)
    print("DUPLICATE RECOMMENDATION DETECTION")
    print("="*80)
    print(f"\nChecking authors with >{min_books} recommendations...")
    if dry_run:
        print("(DRY RUN - no changes will be made)")
    elif auto_flag:
        print("(AUTO-FLAG MODE - duplicates will be flagged)")
    print()
    
    # Get all recommendations that are visible in the UI (not filtered out)
    # Filter out: thumbs_down, already_read, non_english, duplicate
    # This matches the filtering logic in web/app.py
    # A recommendation is visible if NONE of these flags are True
    from sqlalchemy import or_, and_
    
    # Get all recommendations
    all_recs = session.query(Recommendation).all()
    total_count = len(all_recs)
    
    # Filter to only visible ones (where all flags are False or None)
    visible_recs = [
        rec for rec in all_recs
        if not (rec.thumbs_down == True or 
                rec.already_read == True or 
                rec.non_english == True or 
                rec.duplicate == True)
    ]
    
    print(f"Total recommendations in database: {total_count}")
    print(f"Visible recommendations (not filtered): {len(visible_recs)}")
    if total_count > len(visible_recs):
        filtered_count = total_count - len(visible_recs)
        print(f"  ({filtered_count} recommendation(s) are filtered out)")
    print()
    
    # Group by author
    recs_by_author = defaultdict(list)
    for rec in visible_recs:
        recs_by_author[rec.author].append(rec)
    
    # Filter to authors with >min_books recommendations
    authors_to_check = {
        author_name: recs 
        for author_name, recs in recs_by_author.items() 
        if len(recs) >= min_books
    }
    
    print(f"Found {len(authors_to_check)} author(s) with >{min_books} recommendations\n")
    
    if author_limit:
        # Limit to first N authors
        authors_to_check = dict(list(authors_to_check.items())[:author_limit])
        print(f"Processing first {len(authors_to_check)} author(s)...\n")
    
    results = []
    total_duplicate_groups = 0
    total_duplicate_count = 0
    
    for author_name, recommendations in sorted(authors_to_check.items()):
        # Find author record
        author = session.query(Author).filter(
            func.lower(Author.name) == author_name.lower()
        ).first()
        
        if not author:
            print(f"⚠ Warning: Author '{author_name}' not found in Author table")
            continue
        
        result = analyze_author_recommendations(author, recommendations, min_books, dry_run)
        if result:
            results.append(result)
            total_duplicate_groups += result['duplicate_groups']
            total_duplicate_count += result['duplicate_count']
            
            # Auto-flag duplicates if requested
            if not dry_run and auto_flag and result['duplicate_groups'] > 0:
                for group_key, group_recs in result['groups'].items():
                    # Score and determine which to keep
                    def score_rec(rec):
                        score = 0
                        if rec.isbn:
                            score += 10
                        if not rec.duplicate:
                            score += 5
                        # Check if it has a catalog_book_id (links to AuthorCatalogBook which has description)
                        if rec.catalog_book_id:
                            score += 3
                        # Check if it has a reason (more complete data)
                        if rec.reason:
                            score += 2
                        return score
                    
                    sorted_recs = sorted(group_recs, key=score_rec, reverse=True)
                    keep_rec = sorted_recs[0]
                    remove_recs = sorted_recs[1:]
                    
                    # Flag the ones to remove as duplicates
                    for rec in remove_recs:
                        rec.duplicate = True
                        print(f"    ✓ Flagged '{rec.title}' as duplicate")
                
                try:
                    session.commit()
                    print(f"    ✓ Committed changes for {author.name}")
                except Exception as e:
                    print(f"    ⚠ Error committing: {e}")
                    session.rollback()
    
    # Summary
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    print(f"Authors checked: {len(results)}")
    print(f"Total duplicate groups found: {total_duplicate_groups}")
    print(f"Total duplicate recommendations: {total_duplicate_count}")
    
    if results:
        print(f"\nAuthors with duplicates:")
        for result in sorted(results, key=lambda x: x['duplicate_count'], reverse=True):
            if result['duplicate_count'] > 0:
                print(f"  {result['author_name']}: {result['duplicate_count']} duplicates in {result['duplicate_groups']} group(s)")
    
    session.close()
    
    return {
        'authors_checked': len(results),
        'total_duplicate_groups': total_duplicate_groups,
        'total_duplicate_count': total_duplicate_count,
        'results': results
    }


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Check for duplicate recommendations, focusing on authors with many books'
    )
    parser.add_argument('--min-books', type=int, default=10,
                       help='Minimum number of recommendations to check an author (default: 10)')
    parser.add_argument('--limit', type=int,
                       help='Limit number of authors to process')
    parser.add_argument('--auto-flag', action='store_true',
                       help='Automatically flag duplicates (requires --no-dry-run)')
    parser.add_argument('--no-dry-run', action='store_true',
                       help='Actually make changes (default is dry run)')
    
    args = parser.parse_args()
    
    check_authors_with_many_recommendations(
        min_books=args.min_books,
        author_limit=args.limit,
        dry_run=not args.no_dry_run,
        auto_flag=args.auto_flag
    )
