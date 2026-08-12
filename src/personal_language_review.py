"""Learn conservative non-English title signals from the user's flags."""

import html
import math
import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Iterable, List, Sequence, Tuple

from .deduplication.language_detection import detect_non_english_title


LANGUAGE_NAMES = (
    "afrikaans|arabic|armenian|belarusian|bengali|bulgarian|catalan|chinese|croatian|czech|"
    "danish|dutch|finnish|french|georgian|german|greek|hebrew|hindi|hungarian|indonesian|"
    "italian|japanese|korean|norwegian|persian|polish|portuguese|romanian|russian|serbian|"
    "spanish|swedish|thai|turkish|ukrainian|urdu|vietnamese|yiddish"
)
EXPLICIT_LANGUAGE_RE = re.compile(rf"\b(?:{LANGUAGE_NAMES})\b", re.IGNORECASE)


def normalize_language_title(title: str) -> str:
    """Normalize markup and punctuation while retaining language characters."""
    text = html.unescape(re.sub(r"<[^>]+>", " ", title or ""))
    text = unicodedata.normalize("NFKC", text).casefold()
    text = re.sub(r"\d+", "0", text)
    text = re.sub(r"[^\w\u00c0-\u024f]+", " ", text, flags=re.UNICODE)
    return " ".join(text.split())


def title_features(title: str) -> set:
    words = re.findall(r"[^\W\d_]+", normalize_language_title(title), re.UNICODE)
    features = set(words)
    features.update(" ".join(words[index:index + 2]) for index in range(len(words) - 1))
    return features


@dataclass(frozen=True)
class LanguageAssessment:
    confidence: str
    score: float
    reasons: Tuple[str, ...]


class PersonalLanguageModel:
    """Conservative classifier trained from flagged and imported titles."""

    def __init__(self, flagged_examples: Sequence[Tuple[str, str]], english_titles: Iterable[str]):
        self.flagged_examples = [
            (author, title, normalize_language_title(title))
            for author, title in flagged_examples if normalize_language_title(title)
        ]
        self.flagged_exact = {normalized for _, _, normalized in self.flagged_examples}
        self.english_titles = list({title for title in english_titles if title})
        self.learned_features = self._learn_features()
        self._flagged_trigram_index = defaultdict(set)
        for index, (_, _, normalized) in enumerate(self.flagged_examples):
            for trigram in self._trigrams(normalized):
                self._flagged_trigram_index[trigram].add(index)

    @staticmethod
    def _trigrams(normalized: str):
        return {normalized[index:index + 3] for index in range(max(0, len(normalized) - 2))}

    @staticmethod
    def _definitive_reasons(title: str, detector_reasons: Sequence[str]):
        definitive = []
        for reason in detector_reasons:
            if reason.startswith(("Non-English script", "Hebrew characters", "Hebrew transliteration", "German ß")):
                definitive.append(reason)
            elif reason.startswith(("Language edition", "Standalone language edition")):
                # The legacy regex matches language names inside other words
                # ("Cornish" -> "Irish", "Russians" -> "Russian"). Require
                # a complete language word and avoid explicitly bilingual editions.
                if EXPLICIT_LANGUAGE_RE.search(title) and not re.search(
                    rf"\b(?:{LANGUAGE_NAMES})\b[^()[\]]*\band\s+english\b|"
                    rf"\benglish\b[^()[\]]*\band\s+(?:{LANGUAGE_NAMES})\b",
                    title,
                    re.IGNORECASE,
                ):
                    definitive.append(reason)
            elif reason.startswith("Spanish text indicator"):
                definitive.append(reason)
            elif reason.startswith("Spanish punctuation"):
                # Avoid OCR date separators such as "1159¿81".
                if re.search(r"(?:^|[\s(])[¿¡]", title):
                    definitive.append(reason)
        return definitive

    def _learn_features(self):
        positive_docs = Counter()
        positive_authors = defaultdict(set)
        negative_docs = Counter()

        for author, title, _ in self.flagged_examples:
            for feature in title_features(title):
                positive_docs[feature] += 1
                positive_authors[feature].add(author.casefold())
        for title in self.english_titles:
            for feature in title_features(title):
                negative_docs[feature] += 1

        positive_total = max(1, len(self.flagged_examples))
        negative_total = max(1, len(self.english_titles))
        learned = {}
        for feature, positive_count in positive_docs.items():
            negative_count = negative_docs[feature]
            author_count = len(positive_authors[feature])
            is_bigram = " " in feature
            if author_count < 2 or positive_count < 3:
                continue
            if negative_count > max(1, int(positive_count * 0.10)):
                continue
            if not is_bigram and len(feature) < 3:
                continue
            score = math.log((positive_count + 1) / (positive_total + 2)) - math.log(
                (negative_count + 1) / (negative_total + 2)
            )
            if score >= 1.5:
                learned[feature] = score
        return learned

    def assess(self, title: str) -> LanguageAssessment:
        normalized = normalize_language_title(title)
        if not normalized:
            return LanguageAssessment("none", 0.0, ())
        if normalized in self.flagged_exact:
            return LanguageAssessment("high", 10.0, ("exact match to a manually flagged title",))

        detected, detector_reasons = detect_non_english_title(title)
        definitive = self._definitive_reasons(title, detector_reasons)
        if detected and definitive:
            return LanguageAssessment("high", 9.0, tuple(definitive))

        matches = sorted(
            (
                (feature, self.learned_features[feature])
                for feature in title_features(title)
                if feature in self.learned_features
            ),
            key=lambda item: item[1],
            reverse=True,
        )
        learned_score = sum(score for _, score in matches)
        single_matches = [feature for feature, _ in matches if " " not in feature]
        bigram_matches = [feature for feature, _ in matches if " " in feature]
        learned_reasons = [f"learned phrase: {feature!r}" for feature, _ in matches[:5]]

        # Requiring multiple independently learned signals prevents proper names
        # and ambiguous words such as English "die" from being auto-flagged.
        if learned_score >= 4.0 and (len(single_matches) >= 2 or bigram_matches):
            reasons = list(definitive or detector_reasons) + learned_reasons
            return LanguageAssessment("high", learned_score, tuple(reasons))

        nearest_title = None
        nearest_ratio = 0.0
        if len(normalized) >= 10:
            shared = Counter()
            for trigram in self._trigrams(normalized):
                for index in self._flagged_trigram_index.get(trigram, ()):
                    shared[index] += 1
            # Compare only the most plausible examples instead of every one of
            # hundreds of flags for every catalog title.
            likely_indexes = [index for index, _ in shared.most_common(12)]
            for index in likely_indexes:
                _, flagged_title, flagged_normalized = self.flagged_examples[index]
                if abs(len(flagged_normalized) - len(normalized)) > max(8, len(normalized) // 3):
                    continue
                ratio = SequenceMatcher(None, normalized, flagged_normalized).ratio()
                if ratio > nearest_ratio:
                    nearest_ratio = ratio
                    nearest_title = flagged_title
        if nearest_ratio >= 0.90:
            return LanguageAssessment(
                "high",
                8.0 + nearest_ratio,
                (f"near match to flagged title: {nearest_title!r}",),
            )

        # Legacy warnings such as "Cornish" containing "Irish", series names
        # containing "Russians", or packaging abbreviations such as "Lib/E"
        # are not language evidence on their own. Medium confidence still
        # requires a phrase learned from the user's flags.
        if bigram_matches or learned_score >= 2.2:
            reasons = learned_reasons
            return LanguageAssessment("medium", max(learned_score, 1.0), tuple(reasons))
        return LanguageAssessment("none", learned_score, tuple(learned_reasons))
