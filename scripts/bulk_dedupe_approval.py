#!/usr/bin/env python3
"""
Bulk deduplication approval system for prolific authors.

This script:
1. Loads a duplicate analysis report
2. Displays duplicates in an approval-friendly format
3. Allows bulk approval/rejection of deduplication actions
4. Executes approved deduplications
"""

import sys
from pathlib import Path
import json
from typing import Dict, List
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import init_db, get_session, AuthorCatalogBook, Recommendation
from sqlalchemy import func


def load_analysis_report(report_path: str) -> Dict:
    """Load duplicate analysis report"""
    report_file = Path(report_path)
    if not report_file.exists():
        raise FileNotFoundError(f"Report file not found: {report_path}")
    
    with open(report_file, 'r') as f:
        return json.load(f)


def display_duplicates_for_approval(author_data: Dict, show_details: bool = True) -> List[Dict]:
    """
    Display duplicates in approval format and return list of actions to take.
    """
    print(f"\n{'='*80}")
    print(f"AUTHOR: {author_data['author_name']} (ID: {author_data['author_id']})")
    print(f"Total catalog books: {author_data['total_catalog_books']}")
    print(f"Duplicate groups: {author_data['duplicate_groups']}")
    print(f"Total duplicates: {author_data['total_duplicates']}")
    print(f"{'='*80}")
    
    actions = []
    
    for i, dup_group in enumerate(author_data['duplicate_details'], 1):
        print(f"\n  Group {i}/{len(author_data['duplicate_details'])}: {dup_group['count']} books")
        print(f"    Pattern types: {', '.join(dup_group['pattern_types'])}")
        
        keep = dup_group['keep']
        print(f"\n    KEEP:")
        print(f"      ID {keep['id']}: '{keep['title']}'")
        if keep['isbn']:
            print(f"        ISBN: {keep['isbn']}")
        if keep.get('description'):
            print(f"        Has description: Yes")
        
        print(f"\n    REMOVE ({len(dup_group['remove'])} book(s)):")
        for j, remove_book in enumerate(dup_group['remove'], 1):
            print(f"      {j}. ID {remove_book['id']}: '{remove_book['title']}'")
            if remove_book['isbn']:
                print(f"         ISBN: {remove_book['isbn']}")
            if remove_book.get('reasons'):
                print(f"         Reasons: {', '.join(remove_book['reasons'][:3])}")
        
        # Create action record
        action = {
            'author_id': author_data['author_id'],
            'author_name': author_data['author_name'],
            'group_key': dup_group['group_key'],
            'keep_id': keep['id'],
            'keep_title': keep['title'],
            'remove_ids': [b['id'] for b in dup_group['remove']],
            'remove_titles': [b['title'] for b in dup_group['remove']],
            'pattern_types': dup_group['pattern_types'],
            'confidence': 'high' if 'isbn_match' in dup_group['pattern_types'] or 'exact_normalized' in dup_group['pattern_types'] else 'medium'
        }
        actions.append(action)
    
    return actions


def execute_deduplication(actions: List[Dict], dry_run: bool = True) -> Dict:
    """
    Execute approved deduplications.
    """
    db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    stats = {
        'catalog_books_deleted': 0,
        'recommendations_flagged': 0,
        'errors': []
    }
    
    print(f"\n{'='*80}")
    if dry_run:
        print("DRY RUN - No changes will be made")
    else:
        print("EXECUTING DEDUPLICATION")
    print(f"{'='*80}\n")
    
    for i, action in enumerate(actions, 1):
        print(f"Processing {i}/{len(actions)}: {action['author_name']} - {action['keep_title']}")
        
        if dry_run:
            print(f"  [DRY RUN] Would delete {len(action['remove_ids'])} catalog book(s)")
            print(f"  [DRY RUN] Would flag {len(action['remove_ids'])} recommendation(s) as duplicate")
            stats['catalog_books_deleted'] += len(action['remove_ids'])
            stats['recommendations_flagged'] += len(action['remove_ids'])
        else:
            try:
                # Delete catalog books
                for book_id in action['remove_ids']:
                    catalog_book = session.query(AuthorCatalogBook).filter_by(id=book_id).first()
                    if catalog_book:
                        session.delete(catalog_book)
                        stats['catalog_books_deleted'] += 1
                
                # Flag recommendations as duplicate
                for book_id in action['remove_ids']:
                    # Find recommendations for this catalog book
                    recommendations = session.query(Recommendation).filter_by(
                        catalog_book_id=book_id
                    ).all()
                    
                    for rec in recommendations:
                        rec.duplicate = True
                        rec.feedback_date = datetime.utcnow()
                        stats['recommendations_flagged'] += 1
                
                session.commit()
                print(f"  ✓ Deleted {len(action['remove_ids'])} catalog book(s) and flagged recommendations")
            except Exception as e:
                session.rollback()
                error_msg = f"Error processing {action['keep_title']}: {str(e)}"
                stats['errors'].append(error_msg)
                print(f"  ✗ {error_msg}")
    
    if not dry_run:
        print(f"\n✓ Deduplication complete!")
        print(f"  Catalog books deleted: {stats['catalog_books_deleted']}")
        print(f"  Recommendations flagged: {stats['recommendations_flagged']}")
        if stats['errors']:
            print(f"  Errors: {len(stats['errors'])}")
    
    session.close()
    return stats


def interactive_approval(report: Dict, author_limit: int = None) -> List[Dict]:
    """
    Interactive approval process for deduplication.
    """
    print("="*80)
    print("BULK DEDUPLICATION APPROVAL")
    print("="*80)
    print("\nThis will show duplicates found in the analysis report.")
    print("You can approve or reject each author's deduplication plan.\n")
    
    authors = report['authors']
    if author_limit:
        authors = authors[:author_limit]
    
    all_approved_actions = []
    
    for i, author_data in enumerate(authors, 1):
        print(f"\n{'='*80}")
        print(f"AUTHOR {i}/{len(authors)}")
        print(f"{'='*80}")
        
        actions = display_duplicates_for_approval(author_data)
        
        if not actions:
            print("\n  No duplicates found for this author.")
            continue
        
        print(f"\n  Summary: {len(actions)} duplicate group(s), {sum(len(a['remove_ids']) for a in actions)} book(s) to remove")
        
        # Approval prompt
        while True:
            response = input(f"\n  Approve deduplication for {author_data['author_name']}? [y/n/skip/all/quit]: ").strip().lower()
            
            if response == 'y':
                all_approved_actions.extend(actions)
                print(f"  ✓ Approved {len(actions)} duplicate group(s)")
                break
            elif response == 'n':
                print(f"  ✗ Rejected - skipping {len(actions)} duplicate group(s)")
                break
            elif response == 'skip':
                print(f"  ⊘ Skipped")
                break
            elif response == 'all':
                # Approve all remaining
                all_approved_actions.extend(actions)
                for remaining_author in authors[i:]:
                    remaining_actions = display_duplicates_for_approval(remaining_author, show_details=False)
                    all_approved_actions.extend(remaining_actions)
                print(f"\n  ✓ Approved all remaining authors")
                return all_approved_actions
            elif response == 'quit':
                print("\n  Exiting approval process...")
                return all_approved_actions
            else:
                print("  Invalid response. Use: y (yes), n (no), skip, all, or quit")
    
    return all_approved_actions


def batch_approval_from_file(report: Dict, approval_file: str = None) -> List[Dict]:
    """
    Load approval decisions from a JSON file.
    Format: {"author_ids": [123, 456], "group_keys": ["exact_0", "base_1"], "approve_all": false}
    """
    if not approval_file:
        return []
    
    approval_path = Path(approval_file)
    if not approval_path.exists():
        print(f"Approval file not found: {approval_file}")
        return []
    
    with open(approval_path, 'r') as f:
        approval_data = json.load(f)
    
    all_approved_actions = []
    
    for author_data in report['authors']:
        author_id = author_data['author_id']
        
        # Check if approve_all is set
        if approval_data.get('approve_all', False):
            actions = []
            for dup_group in author_data['duplicate_details']:
                keep = dup_group['keep']
                action = {
                    'author_id': author_id,
                    'author_name': author_data['author_name'],
                    'group_key': dup_group['group_key'],
                    'keep_id': keep['id'],
                    'keep_title': keep['title'],
                    'remove_ids': [b['id'] for b in dup_group['remove']],
                    'remove_titles': [b['title'] for b in dup_group['remove']],
                    'pattern_types': dup_group['pattern_types'],
                }
                actions.append(action)
            all_approved_actions.extend(actions)
        else:
            # Check specific author_ids or group_keys
            approved_author_ids = approval_data.get('author_ids', [])
            approved_group_keys = approval_data.get('group_keys', [])
            
            if author_id in approved_author_ids:
                for dup_group in author_data['duplicate_details']:
                    if dup_group['group_key'] in approved_group_keys or not approved_group_keys:
                        keep = dup_group['keep']
                        action = {
                            'author_id': author_id,
                            'author_name': author_data['author_name'],
                            'group_key': dup_group['group_key'],
                            'keep_id': keep['id'],
                            'keep_title': keep['title'],
                            'remove_ids': [b['id'] for b in dup_group['remove']],
                            'remove_titles': [b['title'] for b in dup_group['remove']],
                            'pattern_types': dup_group['pattern_types'],
                        }
                        all_approved_actions.append(action)
    
    return all_approved_actions


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Bulk deduplication approval system'
    )
    parser.add_argument('report', type=str,
                       help='Path to duplicate analysis report JSON file')
    parser.add_argument('--interactive', action='store_true',
                       help='Interactive approval mode')
    parser.add_argument('--approval-file', type=str,
                       help='JSON file with approval decisions')
    parser.add_argument('--author-limit', type=int,
                       help='Limit number of authors to process')
    parser.add_argument('--execute', action='store_true',
                       help='Execute approved deduplications (default is dry run)')
    parser.add_argument('--auto-approve-all', action='store_true',
                       help='Auto-approve all duplicates (use with caution)')
    
    args = parser.parse_args()
    
    # Load report
    report = load_analysis_report(args.report)
    
    print(f"Loaded analysis report from: {args.report}")
    print(f"Analysis date: {report.get('analysis_date', 'Unknown')}")
    print(f"Authors in report: {len(report['authors'])}")
    print(f"Total duplicates found: {report['summary']['total_duplicates']}")
    
    # Get approved actions
    if args.auto_approve_all:
        print("\n⚠️  AUTO-APPROVING ALL DUPLICATES")
        all_actions = []
        for author_data in report['authors']:
            actions = []
            for dup_group in author_data['duplicate_details']:
                keep = dup_group['keep']
                action = {
                    'author_id': author_data['author_id'],
                    'author_name': author_data['author_name'],
                    'group_key': dup_group['group_key'],
                    'keep_id': keep['id'],
                    'keep_title': keep['title'],
                    'remove_ids': [b['id'] for b in dup_group['remove']],
                    'remove_titles': [b['title'] for b in dup_group['remove']],
                    'pattern_types': dup_group['pattern_types'],
                }
                actions.append(action)
            all_actions.extend(actions)
    elif args.approval_file:
        all_actions = batch_approval_from_file(report, args.approval_file)
    elif args.interactive:
        all_actions = interactive_approval(report, args.author_limit)
    else:
        print("\nNo approval method specified. Use --interactive, --approval-file, or --auto-approve-all")
        sys.exit(1)
    
    if not all_actions:
        print("\nNo actions approved. Exiting.")
        sys.exit(0)
    
    print(f"\n{'='*80}")
    print(f"APPROVED ACTIONS SUMMARY")
    print(f"{'='*80}")
    print(f"Total duplicate groups approved: {len(all_actions)}")
    print(f"Total books to remove: {sum(len(a['remove_ids']) for a in all_actions)}")
    
    # Execute
    stats = execute_deduplication(all_actions, dry_run=not args.execute)
    
    if not args.execute:
        print(f"\nThis was a dry run. Use --execute to actually perform the deduplication.")
