#!/usr/bin/env python3
"""
Analyze duplicate patterns in prolific authors (>100 recommendations).

This script:
1. Finds authors with >100 recommendations
2. Analyzes duplicate patterns using multiple techniques
3. Generates a detailed report
4. Creates an approval-ready deduplication plan
"""

import sys
from pathlib import Path
from collections import defaultdict
from typing import List, Dict, Tuple, Set
import re
import json
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import init_db, get_session, Author, Recommendation
from sqlalchemy import func
from scripts.check_duplicate_recommendations import (
    normalize_title_advanced, extract_base_title, similarity_score,
    normalize_isbn, find_duplicate_groups
)


def analyze_prolific_author(author: Author, recommendations: List[Recommendation]) -> Dict:
    """
    Comprehensive analysis of duplicates for a prolific author.
    """
    print(f"\n{'='*80}")
    print(f"ANALYZING: {author.name} (ID: {author.id})")
    print(f"Total recommendations: {len(recommendations)}")
    print(f"{'='*80}")
    
    # Find duplicate groups using existing logic
    duplicate_groups = find_duplicate_groups(recommendations, similarity_threshold=0.85)
    
    # Analyze patterns
    patterns = {
        'exact_normalized': 0,
        'base_title_match': 0,
        'isbn_match': 0,
        'fuzzy_match': 0,
        'substring_match': 0,
        'apostrophe_variations': 0,
        'punctuation_variations': 0,
        'series_variations': 0,
        'edition_variations': 0,
        'non_english_detected': 0
    }
    
    duplicate_details = []
    
    for group_key, group_recs in duplicate_groups.items():
        if len(group_recs) < 2:
            continue
        
        # Determine which one to keep
        def score_rec(rec):
            score = 0
            if rec.isbn:
                score += 10
            if not rec.duplicate:
                score += 5
            if rec.catalog_book_id:
                score += 3
            if rec.reason:
                score += 2
            return score
        
        sorted_recs = sorted(group_recs, key=score_rec, reverse=True)
        keep_rec = sorted_recs[0]
        remove_recs = sorted_recs[1:]
        
        # Analyze why these are duplicates
        keep_title = keep_rec.title
        reasons = []
        pattern_types = []
        
        for rec in remove_recs:
            rec_title = rec.title
            
            # Check different match types
            keep_norm = normalize_title_advanced(keep_title)
            rec_norm = normalize_title_advanced(rec_title)
            keep_base = extract_base_title(keep_title)
            rec_base = extract_base_title(rec_title)
            
            if keep_norm == rec_norm:
                reasons.append("exact normalized match")
                pattern_types.append('exact_normalized')
                patterns['exact_normalized'] += 1
            
            if keep_base.lower() == rec_base.lower():
                reasons.append("base title match")
                pattern_types.append('base_title_match')
                patterns['base_title_match'] += 1
            
            if keep_rec.isbn and rec.isbn and normalize_isbn(keep_rec.isbn) == normalize_isbn(rec.isbn):
                reasons.append("ISBN match")
                pattern_types.append('isbn_match')
                patterns['isbn_match'] += 1
            
            sim = similarity_score(keep_norm, rec_norm)
            if sim >= 0.85:
                reasons.append(f"fuzzy match ({sim:.2f})")
                pattern_types.append('fuzzy_match')
                patterns['fuzzy_match'] += 1
            
            # Check for apostrophe variations (improved)
            # Normalize all apostrophe types: ' ' ` → remove for comparison
            keep_no_apos = re.sub(r"[''`]", '', keep_title.lower())
            rec_no_apos = re.sub(r"[''`]", '', rec_title.lower())
            # Also handle possessives: "Daughter's" vs "Daughters"
            keep_no_poss = re.sub(r"s'", 's', keep_no_apos)
            rec_no_poss = re.sub(r"s'", 's', rec_no_apos)
            if (keep_no_apos == rec_no_apos or keep_no_poss == rec_no_poss) and keep_title != rec_title:
                reasons.append("apostrophe variation")
                pattern_types.append('apostrophe_variations')
                patterns['apostrophe_variations'] += 1
            
            # Check for punctuation variations
            keep_no_punct = re.sub(r'[^\w\s]', '', keep_title.lower())
            rec_no_punct = re.sub(r'[^\w\s]', '', rec_title.lower())
            if keep_no_punct == rec_no_punct and keep_title != rec_title:
                reasons.append("punctuation variation")
                pattern_types.append('punctuation_variations')
                patterns['punctuation_variations'] += 1
            
            # Check for series variations
            if '(series' in keep_title.lower() or '(series' in rec_title.lower():
                if keep_base.lower() == rec_base.lower():
                    reasons.append("series variation")
                    pattern_types.append('series_variations')
                    patterns['series_variations'] += 1
            
            # Check for edition variations (improved - now handles "ed." and "Edition")
            if re.search(r'\b(?:edition|ed\.)', keep_title.lower()) or re.search(r'\b(?:edition|ed\.)', rec_title.lower()):
                if keep_base.lower() == rec_base.lower():
                    reasons.append("edition variation")
                    pattern_types.append('edition_variations')
                    patterns['edition_variations'] += 1
            
            # Check for volume variations (new detection)
            if re.search(r'\b(?:volume|vol\.?)', keep_title.lower()) or re.search(r'\b(?:volume|vol\.?)', rec_title.lower()):
                # Compare base titles after removing volume indicators
                keep_no_vol = re.sub(r'\b(?:volume|vol\.?)\s*(?:i{1,3}|iv|v{1,3}|vi{0,3}|[0-9]+)\b', '', keep_title.lower(), flags=re.IGNORECASE)
                rec_no_vol = re.sub(r'\b(?:volume|vol\.?)\s*(?:i{1,3}|iv|v{1,3}|vi{0,3}|[0-9]+)\b', '', rec_title.lower(), flags=re.IGNORECASE)
                keep_no_vol = re.sub(r'\bvolume\s+\d+\b', '', keep_no_vol, flags=re.IGNORECASE)
                rec_no_vol = re.sub(r'\bvolume\s+\d+\b', '', rec_no_vol, flags=re.IGNORECASE)
                keep_no_vol = re.sub(r'\bvol\.?\s*\d+\b', '', keep_no_vol, flags=re.IGNORECASE)
                rec_no_vol = re.sub(r'\bvol\.?\s*\d+\b', '', rec_no_vol, flags=re.IGNORECASE)
                keep_no_vol = normalize_title_advanced(keep_no_vol)
                rec_no_vol = normalize_title_advanced(rec_no_vol)
                if keep_no_vol == rec_no_vol and keep_title != rec_title:
                    reasons.append("volume variation")
                    pattern_types.append('volume_variations')
                    if 'volume_variations' not in patterns:
                        patterns['volume_variations'] = 0
                    patterns['volume_variations'] += 1
        
        duplicate_details.append({
            'group_key': group_key,
            'keep': {
                'id': keep_rec.id,
                'title': keep_title,
                'isbn': keep_rec.isbn,
                'reason': keep_rec.reason
            },
            'remove': [
                {
                    'id': rec.id,
                    'title': rec.title,
                    'isbn': rec.isbn,
                    'reasons': list(set(reasons))
                }
                for rec in remove_recs
            ],
            'pattern_types': list(set(pattern_types)),
            'count': len(group_recs)
        })
    
    # Check for non-English titles
    non_english_patterns = [
        r'[\u4e00-\u9fff]',  # Chinese
        r'[\u3040-\u309f\u30a0-\u30ff]',  # Japanese
        r'[\u0400-\u04ff]',  # Cyrillic
        r'[\u0600-\u06ff]',  # Arabic
        r'[\u0590-\u05ff]',  # Hebrew
    ]
    
    for rec in recommendations:
        title = rec.title or ''
        for pattern in non_english_patterns:
            if re.search(pattern, title):
                patterns['non_english_detected'] += 1
                break
    
    total_duplicates = sum(len(group) - 1 for group in duplicate_groups.values())
    
    return {
        'author_id': author.id,
        'author_name': author.name,
        'total_recommendations': len(recommendations),
        'duplicate_groups': len(duplicate_groups),
        'total_duplicates': total_duplicates,
        'patterns': patterns,
        'duplicate_details': duplicate_details,
        'non_english_count': patterns['non_english_detected']
    }


def analyze_prolific_authors(min_recommendations: int = 100, 
                             author_limit: int = None,
                             output_file: str = None) -> Dict:
    """
    Analyze authors with many recommendations for duplicate patterns.
    """
    db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    print("="*80)
    print("PROLIFIC AUTHOR DUPLICATE ANALYSIS")
    print("="*80)
    print(f"\nAnalyzing authors with >{min_recommendations} recommendations...")
    print()
    
    # Get all visible recommendations
    all_recs = session.query(Recommendation).all()
    visible_recs = [
        rec for rec in all_recs
        if not (rec.thumbs_down == True or 
                rec.already_read == True or 
                rec.non_english == True or 
                rec.duplicate == True)
    ]
    
    # Group by author
    recs_by_author = defaultdict(list)
    for rec in visible_recs:
        recs_by_author[rec.author].append(rec)
    
    # Filter to prolific authors
    prolific_authors = {
        author_name: recs 
        for author_name, recs in recs_by_author.items() 
        if len(recs) >= min_recommendations
    }
    
    print(f"Found {len(prolific_authors)} author(s) with >{min_recommendations} recommendations\n")
    
    if author_limit:
        prolific_authors = dict(list(prolific_authors.items())[:author_limit])
        print(f"Processing first {len(prolific_authors)} author(s)...\n")
    
    results = []
    total_patterns = defaultdict(int)
    
    for author_name, recommendations in sorted(prolific_authors.items(), 
                                               key=lambda x: len(x[1]), 
                                               reverse=True):
        # Find author record
        author = session.query(Author).filter(
            func.lower(Author.name) == author_name.lower()
        ).first()
        
        if not author:
            print(f"⚠ Warning: Author '{author_name}' not found in Author table")
            continue
        
        result = analyze_prolific_author(author, recommendations)
        results.append(result)
        
        # Aggregate patterns
        for pattern, count in result['patterns'].items():
            total_patterns[pattern] += count
    
    # Generate summary report
    print("\n" + "="*80)
    print("SUMMARY REPORT")
    print("="*80)
    
    print(f"\nAuthors analyzed: {len(results)}")
    print(f"Total recommendations: {sum(r['total_recommendations'] for r in results)}")
    print(f"Total duplicate groups: {sum(r['duplicate_groups'] for r in results)}")
    print(f"Total duplicates found: {sum(r['total_duplicates'] for r in results)}")
    
    print(f"\nDuplicate Patterns Detected:")
    for pattern, count in sorted(total_patterns.items(), key=lambda x: -x[1]):
        if count > 0:
            print(f"  {pattern}: {count}")
    
    print(f"\nTop Authors by Duplicate Count:")
    sorted_results = sorted(results, key=lambda x: x['total_duplicates'], reverse=True)
    for i, result in enumerate(sorted_results[:10], 1):
        print(f"  {i}. {result['author_name']}: {result['total_duplicates']} duplicates in {result['duplicate_groups']} groups")
    
    # Create detailed report
    report = {
        'analysis_date': datetime.utcnow().isoformat(),
        'min_recommendations': min_recommendations,
        'authors_analyzed': len(results),
        'summary': {
            'total_recommendations': sum(r['total_recommendations'] for r in results),
            'total_duplicate_groups': sum(r['duplicate_groups'] for r in results),
            'total_duplicates': sum(r['total_duplicates'] for r in results),
            'pattern_counts': dict(total_patterns)
        },
        'authors': results
    }
    
    # Save report
    if output_file:
        output_path = Path(__file__).parent.parent / output_file
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"\n✓ Detailed report saved to: {output_path}")
    else:
        # Default output
        output_path = Path(__file__).parent.parent / 'data' / f'duplicate_analysis_{datetime.utcnow().strftime("%Y%m%d_%H%M%S")}.json'
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"\n✓ Detailed report saved to: {output_path}")
    
    session.close()
    
    return report


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Analyze duplicate patterns in prolific authors'
    )
    parser.add_argument('--min-recommendations', type=int, default=100,
                       help='Minimum number of recommendations to analyze (default: 100)')
    parser.add_argument('--limit', type=int,
                       help='Limit number of authors to process')
    parser.add_argument('--output', type=str,
                       help='Output file path (default: data/duplicate_analysis_TIMESTAMP.json)')
    
    args = parser.parse_args()
    
    analyze_prolific_authors(
        min_recommendations=args.min_recommendations,
        author_limit=args.limit,
        output_file=args.output
    )
