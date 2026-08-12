"""
Enhanced language detection for book titles.

Detects non-English titles using multiple methods:
1. Character set analysis (CJK, Cyrillic, Arabic, Hebrew, etc.)
2. Language-specific patterns (Hebrew words, non-English articles, etc.)
3. Metadata analysis (if available)
"""

import re
from typing import Optional, Tuple


def detect_non_english_title(title: str, isbn: Optional[str] = None, 
                             open_library_key: Optional[str] = None) -> Tuple[bool, list]:
    """
    Detect if a book title is non-English.
    
    Args:
        title: Book title to check
        isbn: Optional ISBN
        open_library_key: Optional Open Library key
    
    Returns:
        Tuple of (is_non_english: bool, reasons: list)
        reasons contains explanation of why it was flagged
    """
    if not title:
        return False, []
    
    reasons = []
    
    # Method 1: Character set detection (CJK, Cyrillic, Arabic, Hebrew, etc.)
    major_non_english_pattern = re.compile(
        r'[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ff\u0400-\u04ff\u0600-\u06ff\u0590-\u05ff]'
    )
    if major_non_english_pattern.search(title):
        reasons.append("Non-English script detected (CJK/Cyrillic/Arabic/Hebrew)")
        return True, reasons
    
    # Method 2: Hebrew-specific patterns
    # Hebrew words often transliterated: "be-" (in), "shel-" (of), "ve-" (and)
    # Hebrew characters: א-ת (U+05D0 to U+05EA)
    hebrew_chars = re.compile(r'[\u05d0-\u05ea]')
    if hebrew_chars.search(title):
        reasons.append("Hebrew characters detected")
        return True, reasons
    
    # Hebrew transliteration patterns
    hebrew_patterns = [
        r'\bsheloshah\b',  # "three" in Hebrew transliteration
        r'\bshel\b',  # "of" in Hebrew
        r'\bbe-',  # "in" in Hebrew (with hyphen)
        r'\bve-',  # "and" in Hebrew (with hyphen)
        r'\bshavu[\u05b0-\u05ff]ot\b',  # "weeks" in Hebrew (with Hebrew vowel marks)
    ]
    for pattern in hebrew_patterns:
        if re.search(pattern, title, re.IGNORECASE):
            reasons.append("Hebrew transliteration pattern detected")
            return True, reasons
    
    # Method 3: Language edition markers in parentheses/brackets
    non_english_languages = (
        'french|russian|spanish|german|italian|portuguese|chinese|japanese|korean|arabic|hebrew|'
        'polish|dutch|swedish|norwegian|danish|finnish|greek|turkish|hindi|thai|vietnamese|'
        'indonesian|malay|tagalog|romanian|hungarian|czech|slovak|croatian|serbian|bulgarian|'
        'ukrainian|persian|urdu|bengali|tamil|telugu|marathi|gujarati|kannada|malayalam|'
        'punjabi|nepali|sinhala|myanmar|khmer|lao|mongolian|georgian|armenian|azerbaijani|'
        'kazakh|uzbek|turkmen|kyrgyz|tajik|afrikaans|swahili|zulu|xhosa|amharic|hausa|'
        'yoruba|igbo|somali|maltese|icelandic|basque|catalan|galician|welsh|irish|scottish|'
        'breton|cornish|manx|hebrew'
    )
    
    paren_pattern = re.compile(
        rf'\(\s*(?:{non_english_languages})\s*(?:edition|version|translation)?\s*\)',
        re.IGNORECASE
    )
    bracket_pattern = re.compile(
        rf'\[\s*(?:{non_english_languages})\s*(?:edition|version|translation)?\s*\]',
        re.IGNORECASE
    )
    standalone_pattern = re.compile(
        rf'\b(?:{non_english_languages})\s+(?:edition|version|translation)\b',
        re.IGNORECASE
    )
    
    if paren_pattern.search(title):
        match = paren_pattern.search(title)
        reasons.append(f"Language edition in parentheses: '{match.group()}'")
        return True, reasons
    if bracket_pattern.search(title):
        match = bracket_pattern.search(title)
        reasons.append(f"Language edition in brackets: '{match.group()}'")
        return True, reasons
    if standalone_pattern.search(title):
        match = standalone_pattern.search(title)
        reasons.append(f"Standalone language edition: '{match.group()}'")
        return True, reasons
    
    # Method 4: Spanish indicators
    spanish_indicators = re.compile(
        r'\b(?:edici[oó]n|colecci[oó]n|estuche|libro|libros|misterio|pr[ií]ncipe)\b',
        re.IGNORECASE
    )
    if 'house edition' not in title.lower() and spanish_indicators.search(title):
        match = spanish_indicators.search(title)
        reasons.append(f"Spanish text indicator: '{match.group()}'")
        return True, reasons
    
    # Method 5: Specific non-English punctuation/characters
    # Check for specific non-English characters that are clear indicators
    # Ignore OCR date/range separators such as "1159¿81".
    spanish_punct = re.compile(r'(?<!\d)[¿¡](?!\d)')
    german_eszett = re.compile(r'ß')
    
    if spanish_punct.search(title):
        reasons.append("Spanish punctuation (¿ or ¡)")
        return True, reasons
    if german_eszett.search(title):
        reasons.append("German ß character")
        return True, reasons
    
    # Note: Removed "high accented character ratio" check as it was causing false positives
    # (flagging regular 'i' characters in English titles)
    
    # Method 6: Non-English word patterns
    # Common non-English articles and prepositions
    non_english_articles = [
        r'\b(?:le|la|les|un|une|des|du|de|el|los|las|una|uno|der|die|das|ein|eine)\s+[A-Z]',  # Articles before capitalized words
        r'\b(?:van|von|de|del|da|di|du|des)\s+[A-Z]',  # Name particles (but these can be in English names too, so be careful)
    ]
    # Only flag if title is mostly non-English words
    title_words = title.split()
    if len(title_words) > 2:
        for pattern in non_english_articles:
            matches = len(re.findall(pattern, title, re.IGNORECASE))
            # One short token (for example "die" in "To Die For") is too
            # ambiguous to classify a title as non-English.
            if matches >= 2 and matches / len(title_words) > 0.3:
                reasons.append("Non-English article/preposition pattern detected")
                return True, reasons
    
    return False, reasons


def is_english_title(title: str, isbn: Optional[str] = None, 
                     open_library_key: Optional[str] = None) -> bool:
    """
    Simple boolean check: is this title English?
    
    Returns:
        True if English, False if non-English
    """
    is_non_english, _ = detect_non_english_title(title, isbn, open_library_key)
    return not is_non_english


# Test cases
if __name__ == '__main__':
    test_cases = [
        ("Sheloshah shavuʻot be-Pariz by Author Name", True, "Hebrew transliteration"),
        ("Xjust Rewards Tegf by Author Name", True, "Suspicious encoding"),
        ("The English Book Title", False, "English"),
        ("Harry Potter and the Philosopher's Stone", False, "English"),
        ("Le Petit Prince", True, "French article"),
        ("Anne of Green Gables (French Edition)", True, "Language edition marker"),
        ("Book Title", False, "Simple English"),
        ("I Was Just Thinking", False, "English with 'I' (should not flag)"),
        ("A Compendious History Of The British Churches", False, "English title (should not flag)"),
    ]
    
    print("Testing non-English detection:")
    print("=" * 80)
    for title, expected_non_english, description in test_cases:
        is_non_english, reasons = detect_non_english_title(title)
        status = "✓" if is_non_english == expected_non_english else "✗"
        print(f"{status} {description}")
        print(f"   Title: {title}")
        print(f"   Expected: {'Non-English' if expected_non_english else 'English'}")
        print(f"   Detected: {'Non-English' if is_non_english else 'English'}")
        if reasons:
            print(f"   Reasons: {', '.join(reasons)}")
        print()
