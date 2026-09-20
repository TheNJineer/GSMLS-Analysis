"""Explainable, rule-based classification of a listing's current repair needs.

This module is independent of pandas, Kafka, and database clients. It does not
write logs or change sale/investment flags. Callers may persist ``to_dict()``
alongside a verified record key and the exact inputs for auditing and replay.

``Unknown`` means insufficient accepted evidence, not good physical condition.
Only ``Fixer Upper`` and ``Unknown`` are currently supported. Review these rules
against labelled listings before using this first version for historical updates.
Context checks are bounded heuristics, not a general understanding of language.

Example::

    result = classify_condition("Ranch", "The roof requires significant repairs.")
    assert result.condition == "Fixer Upper"
    audit_record = result.to_dict()

Only the primary style is considered. A historical label based exclusively on
a discarded secondary style cannot be reproduced from these inputs.
"""

from __future__ import annotations

import re
import json
from dataclasses import asdict, dataclass
from typing import Literal
from pprint import pprint


CLASSIFIER_VERSION = "1.1.0"
ConditionLabel = Literal["Fixer Upper", "Unknown"]

"""
----------------------------------------------------------------------------------------------------------------
                                                CLASSES SECTION
----------------------------------------------------------------------------------------------------------------
"""
@dataclass(frozen=True)
class ConditionRule:
    """
    Represents a condition rule, which defines a specific pattern-matching
    criterion for evaluating certain conditions.

    This class is intended to encapsulate a single rule's attributes, including
    its identifier, the pattern used for matching, and whether the rule is
    considered sufficient on its own.

    :ivar rule_id: A unique identifier for the rule.
    :type rule_id: str
    :ivar pattern: A compiled regular expression pattern for matching.
    :type pattern: re.Pattern[str]
    :ivar sufficient: A flag indicating whether this rule is independently
        sufficient for the condition it evaluates. Defaults to True.
    :type sufficient: bool
    """

    rule_id: str
    pattern: re.Pattern[str]
    sufficient: bool = True


@dataclass(frozen=True)
class MatchDecision:
    """
    Represents a decision made during a text-matching process.

    Encapsulates details about a matching rule, its source, the associated matched
    text, the surrounding context, and the reason for the match. This data class
    is immutable, ensuring thread-safety and integrity of match decisions.

    :ivar rule_id: The unique identifier of the rule that triggered the decision.
    :type rule_id: str
    :ivar source: The source of the match, specifying whether it originated
        from 'primary_style' or 'remarks'.
    :type source: Literal["primary_style", "remarks"]
    :ivar matched_text: The exact text segment that matched the rule criteria.
    :type matched_text: str
    :ivar context: The context or surrounding text within which the match occurred.
    :type context: str
    :ivar start: The starting index of the matched text within the context.
    :type start: int
    :ivar end: The ending index of the matched text within the context.
    :type end: int
    :ivar reason: The reason or justification for why the match occurred.
    :type reason: str
    """

    rule_id: str
    source: Literal["primary_style", "remarks"]
    matched_text: str
    context: str
    start: int
    end: int
    reason: str


@dataclass(frozen=True)
class ConditionResult:
    """
    Represents the result of evaluating a condition, encapsulating the details
    of matches and review reasons.

    This class is immutable and is used to store the outcome of a condition
    evaluation along with associated metadata. It provides functionality for
    conversion into a dictionary format suitable for JSON serialization.

    :ivar condition: The label of the condition being evaluated.
    :type condition: ConditionLabel
    :ivar classifier_version: The version of the classifier responsible for the
        evaluation.
    :type classifier_version: str
    :ivar accepted_matches: A tuple containing decisions that were considered
        acceptable matches during the evaluation.
    :type accepted_matches: tuple[MatchDecision, ...]
    :ivar rejected_matches: A tuple containing decisions that were deemed
        rejected matches during the evaluation.
    :type rejected_matches: tuple[MatchDecision, ...]
    :ivar review_reasons: A tuple of strings specifying the reasons for manual
        review as identified during the evaluation.
    :type review_reasons: tuple[str, ...]
    """
    function_id: str
    condition: ConditionLabel
    classifier_version: str
    accepted_matches: tuple[dict, ...]
    rejected_matches: tuple[dict, ...]
    review_reasons: tuple[str, ...]

    def to_dict(self) -> dict:
        """Return a JSON-serializable audit record without performing any I/O."""
        return {
            "condition": self.condition,
            "classifier_version": self.classifier_version,
            "accepted_matches": [asdict(match) for match in self.accepted_matches],
            "rejected_matches": [asdict(match) for match in self.rejected_matches],
            "review_reasons": list(self.review_reasons),
        }

"""
----------------------------------------------------------------------------------------------------------------
                                                FUNCTIONS SECTION
----------------------------------------------------------------------------------------------------------------
"""

def _rule(rule_id: str, pattern: str, sufficient: bool = True) -> ConditionRule:
    return ConditionRule(rule_id, re.compile(pattern, re.IGNORECASE), sufficient)


def _rules() -> tuple[ConditionRule, ...]:
    # Keep alternatives in explicit rules so adjacent string literals cannot silently
    # join two intended alternatives. IDs are stable audit identifiers, not labels.
    rules: tuple[ConditionRule, ...] = (
        _rule("fixer_upper_description", r"\bfixer[\s-]*upper\b"),
        _rule("handyman_description", r"\bhandy[\s-]*man\s+(?:special|home|house)\b"),
        _rule("needs_work", r"\b(?:needs?\s+(?:some\s+)?work|in\s+need\s+of\s+work)\b"),
        _rule("needs_rehab", r"\b(?:needs?|requires?)\s+(?:(?:total|complete|extensive)\s+)?rehab(?:ilitation)?\b"),
        _rule("requires_significant_repairs", r"\b(?:needs?|requires?)\s+(?:(?:significant|extensive|major|structural|substantial)\s+)?repairs?\b"),
        _rule("in_need_of_repair", r"\bin\s+need\s+of\s+(?:(?:significant|extensive|major)\s+)?repairs?\b"),
        _rule("requires_gut_renovation", r"\b(?:needs?|requires?)\s+(?:a\s+)?(?:full|total|complete)\s+gut(?:\s+renovation)?\b"),
        _rule("requires_overhaul", r"\b(?:needs?|requires?)\s+(?:a\s+)?complete\s+overhaul\b"),
        _rule("boarded_up_property", r"\b(?:house|home|property|windows?|doors?)\s+(?:(?:is|are|has\s+been|have\s+been)\s+)?boarded\s+up\b"),
        _rule("damage_requires_repair", r"\b(?:water\s+|fire\s+|structural\s+)?damage\s+(?:that\s+)?(?:needs?|requires?)\s+(?:immediate\s+)?(?:repairs?|remediation)\b"),
        _rule("mold_requires_remediation", r"\bmold\s+(?:that\s+)?(?:needs?|requires?)\s+(?:immediate\s+)?(?:removal|remediation|treatment)\b"),
        _rule(
            "major_tlc_required",
            r"\b(?:"
            r"(?:needs?|requires?|in\s+need\s+of)\s+(?:major|significant|extensive)\s+TLC"
            r"|(?:major|significant|extensive)\s+TLC\s+(?:(?:is\s+)?(?:not\s+)?)(?:needed|required)"
            r")\b",
        ),
        _rule(
            "mold_present",
            r"\b(?:(?:high|extensive|significant)\s+)?mold(?:\s+(?:issues?|growth))?"
            r"\s+(?:(?:is|are)\s+)?(?:not\s+)?(?:currently\s+)?present\b",
        ),
        _rule(
            "major_damage_present",
            r"\b(?:major|significant|extensive|severe)\s+(?:(?:storm|water|fire|structural)\s+)?damage\b",
        ),
        _rule(
            "major_repairs_needed",
            r"\b(?:major|significant|extensive|structural)\s+"
            r"(?:(?:roof|foundation|electrical|plumbing)\s+)?repairs?\s+"
            r"(?:(?:is|are)\s+)?(?:not\s+)?(?:needed|required)\b",
        ),
        # Ambiguous phrases are retained for review, but cannot establish condition.
        _rule("tlc_reference", r"\bTLC\b", False),
        _rule("damage_reference", r"\bdamag(?:e|ed)\b", False),
        _rule("mold_reference", r"\bmold\b", False),
        _rule("renovation_reference", r"\b(?:(?:full|total)\s+gut(?:\s+renovation)?|total\s+rehab|complete\s+overhaul)\b", False),
        _rule("cash_only_terms", r"\bcash(?:\s+offers?)?\s+only\b", False),
        _rule("rehab_financing", r"\b203\s*\(?k\)?", False),
        _rule("investor_language", r"\binvestor\s+special\b", False),
        _rule("access_warning", r"\b(?:proceed\s+with\s+caution|(?:at\s+)?your\s+own\s+risk)\b", False),
    )

    return rules


def _clause_bounds(text: str, start: int, end: int) -> tuple[int, int]:
    _BOUNDARY = re.compile(r"[,.!?;\n]+|\b(?:but|however|although|yet)\b", re.IGNORECASE)
    left, right = 0, len(text)

    for boundary in _BOUNDARY.finditer(text):
        if boundary.end() <= start:
            left = boundary.end()
        elif boundary.start() >= end:
            right = boundary.start()
            break
    return left, right


def _rejection_reason(text: str, match: re.Match[str], left: int, right: int) -> str | None:
    # Contrast boundaries keep a rejection in one clause from suppressing an
    # independent statement: "No water damage, but the roof needs major repairs."
    _NEGATION = re.compile(
        r"\b(?:no|not|never|without|neither|doesn['’]t|does\s+not|isn['’]t|is\s+not)\b"
        r"(?!\s+only\b)(?:\s+[\w'’-]+){0,7}\s*$", re.IGNORECASE
    )
    _PREVENTION = re.compile(
        r"\b(?:prevent|avoid|so\s+(?:that\s+)?[^.!?;]{0,40}(?:does|will|can)\s+not)\b",
        re.IGNORECASE,
    )
    _HYPOTHETICAL = re.compile(
        r"\b(?:if|whether|might|may|could|would|possible|potential|suspected)\b",
        re.IGNORECASE,
    )
    _PAST_NEED = re.compile(r"\b(?:used\s+to|previously|formerly|once)\b", re.IGNORECASE)

    # Bound checks to nearby words; do not negate an entire listing. Context in
    # the audit record is kept separately and retains the original spelling.
    before = " ".join(text[left:match.start()].split()[-8:])
    after = " ".join(text[match.end():right].split()[:8])
    matched = match.group()
    if _PREVENTION.search(before):
        return "Preventive instruction rather than an existing repair need"
    # Some specific rules deliberately capture negation inside the candidate so
    # the audit includes "major TLC is not required" or "mold is not present".
    negated_predicate = r"\bnot\s+(?:currently\s+)?(?:needed|required|present)\b"
    if (_NEGATION.search(before)
            or re.search(negated_predicate, matched, re.IGNORECASE)
            or re.match(r"(?:is\s+|are\s+)?" + negated_predicate, after, re.IGNORECASE)):
        return "Negated repair or defect statement"
    if _HYPOTHETICAL.search(before):
        return "Hypothetical or conditional repair statement"
    if (_PAST_NEED.search(before)
            or re.match(r"(?:previously|formerly|in\s+the\s+past)\b", after, re.IGNORECASE)):
        return "Historical rather than established current repair need"
    if re.match(r"(?:prevention|insurance|coverage|inspection|assessment)\b", after, re.IGNORECASE):
        return "Reference to prevention, coverage, or assessment rather than an established defect"
    if _is_resolved(text, match, left, right):
        return "Nearby completed-work statement tied to this defect or repair"
    return None


def _is_resolved(text: str, match: re.Match[str], left: int, right: int) -> bool:
    """Recognize local resolution statements, not unrelated completed amenities.

    For example, "mold present in the finished basement" still qualifies.
    Commas may introduce an immediate resolution (", now remediated"), but a
    new sentence/contrast is not used to cancel independent repair evidence.
    """
    before = " ".join(text[left:match.start()].split()[-8:])
    after = text[match.end():right].strip()
    continuation = text[right:]
    # A comma-separated resolution belongs to this candidate only when no
    # intervening new subject is introduced before the resolution verb.
    comma_resolution = re.match(
        r",\s*(?:now\s+)?(?:(?:has|have)\s+been\s+|(?:is|are|was|were)\s+)?"
        r"(?:fully\s+)?(?:remediated|repaired|resolved|fixed|completed)\b",
        continuation, re.IGNORECASE,
    )
    # Allow a location before the predicate: "mold present in basement has been
    # remediated". Do not accept intervening negation ("not yet remediated").
    resolution_after = re.match(
        r"(?:in\s+(?:the\s+)?(?:finished\s+)?(?:basement|attic|house|home)\s+)?"
        r"(?:(?:issues?|repairs?|remediation|renovations?)\s+)?"
        r"(?:(?:has|have|had)\s+been\s+|(?:is|are|was|were)\s+)?"
        r"(?:(?:now|already|fully|successfully)\s+)*"
        r"(?:remediated|repaired|resolved|fixed|completed|finished)\b",
        after, re.IGNORECASE,
    )
    # "Repaired water damage" / "completed full gut renovation". A completed
    # kitchen earlier in the clause must not suppress a separate roof defect.
    resolution_before = re.search(
        r"\b(?:remediated|repaired|resolved|fixed|completed)\s*"
        r"(?:(?:the|all|major|significant|extensive|severe|water|fire|storm|structural)\s+)*$",
        before, re.IGNORECASE,
    )
    if resolution_before:
        prefix = before[:resolution_before.start()]
        if re.search(r"\b(?:not|never|to\s+be)\s+(?:yet\s+)?$", prefix, re.IGNORECASE):
            resolution_before = None
    return bool(comma_resolution or resolution_after or resolution_before)


def classify_condition(primary_style: str | None, remarks: str | None) -> ConditionResult:
    """Classify without mutating inputs, logging, or updating related sale flags.

    Pass strings or None; dataframe callers must convert NaN/pandas.NA to None.
    An exact primary ``FixrUppr`` code (ignoring case/outer whitespace) suffices.
    All text candidates are evaluated, including repeated and rejected matches.
    Qualified repair evidence suffices even when another candidate is rejected.
    Ambiguous evidence alone returns Unknown with an explicit review reason.

    TLC alone remains unclassified, including "needs a little TLC". Explicit
    major/significant/extensive TLC needs qualify, as do affirmative mold
    presence and major damage statements that pass the context checks.
    Additional condition grades need separately reviewed
    definitions and rules, not an automatic mapping from Unknown.
    """
    for name, value in (("primary_style", primary_style), ("remarks", remarks)):
        if value is not None and not isinstance(value, str):
            raise TypeError(f"{name} must be a string or None")

    style, text = primary_style or "", remarks or ""
    accepted: list[dict] = []
    rejected: list[dict] = []
    review: list[str] = []

    if style.strip().casefold() == "fixruppr":
        start = len(style) - len(style.lstrip())
        accepted.append(MatchDecision(
            "primary_style_fixer_upper", "primary_style", style.strip(), style,
            start, start + len(style.strip()), "Explicit primary fixer-upper style",
        ).__dict__)

    candidates = sorted(
        ((match.start(), rule, match) for rule in _rules() for match in rule.pattern.finditer(text)),
        key=lambda item: (item[0], item[2].end(), item[1].rule_id),
    )
    for _, rule, match in candidates:
        left, right = _clause_bounds(text, match.start(), match.end())
        reason = _rejection_reason(text, match, left, right)
        if reason is None and not rule.sufficient:
            reason = "Insufficient evidence of current physical repair needs on its own"
        decision = MatchDecision(
            rule.rule_id, "remarks", match.group(),
            text[max(left, match.start() - 120):min(right, match.end() + 120)].strip(),
            match.start(), match.end(), reason or "Affirmative current repair evidence",
        ).__dict__
        (rejected if reason else accepted).append(decision)

    if not accepted and rejected:
        review.append("Candidates were rejected; review context before assigning a condition grade")
    if accepted and any(
        match['reason'].startswith(("Negated", "Historical", "Nearby completed"))
        for match in rejected
    ):
        review.append("Accepted evidence coexists with negated or historical evidence; inspect for conflicts")
    if not style.strip() and not text.strip():
        review.append("No primary style or listing remarks supplied")

    return ConditionResult(
        function_id="condition_result",
        condition="Fixer Upper" if accepted else "Unknown",
        classifier_version=CLASSIFIER_VERSION,
        accepted_matches=tuple(accepted),
        rejected_matches=tuple(rejected),
        review_reasons=tuple(review),
    )


if __name__ == "__main__":

    sample_inputs = [
        ("Ranch", "SOLD AS IS. NEEDS SUBSTANTIAL REPAIRS, VALUE IN SIZE AND LOCATION. CALL TENANTS JONES 789-4842 & SHERRATT 232-2947 FOR APPT. TO BE DELIVERED VACANT. AS A ONE FAMILY OCCUPANT ONLY CO OBTAINED BY BUYER.. CALL OFFICE"),
        ("Ranch", "The property requires significant repairs. The roof is in need of replacement."),
        ("Tudor", "Needs major TLC. Not safe to enter, contact the agent before viewing."),
        ("Colonial", "EXTRAORDINARY CHARM & APPEAL; GRAND WOODWORK, MOLDINGS & DETAIL, PLUMBING IN FOR BATH ON 3RD FLOOR. CLASSIC APPEAL.. None. LL, THEN PICK UP KEY LBR"),
        ("Cape Cod", "Recently renovated with modern amenities. Mold issues present from recent flood in the basement"),
        ("Victorian", "Beautifully restored with modern amenities. Five minutes from downtown"),
        ("Log Cabin", "HIGH CEILINGS.  ***THIS HOME NEEDS REHAB******** PROPERTY SALES PRICE APPROVED BY LENDER.  BUYER RESPONSIBLE FOR CO & FIRE INSPECTION*** CASH BUYERS ONLY HOME SOLD IN AS IS CONDITION. ****  APPROVED, APPROVED, APPROVED SHORT SALE AT LISTINGPRICE  **** APPROVED SHORT SALE *** CASH BUYERS ONLY WITH PROOF OF FUNDS AND MUST BE WILLING TO CLOSE BY 9/30/2016  *** SOLD STRICKLY AS IS, NO ACCESS *** SEE FROM OUTSIDE *** QUICK CLOSING....ANY OFFERS MUST COME WITH PROOF OF FUNDS.   **** BUILDERS WELCOME LOT SIZE 25 X 100.  BUYER RESPONSIBLE FOR ANY AND ALL CITY CERTIFICATES.    FINALLY BANK APPROVED & NOW     ********  BUYER  LOST QUALIFICATION ++++++++. SS APPROVED  $125,000 BUYER RESPONSIBLE FOR ANY AND ALL CITY INSPECTIONS. SOLD IN AS IS CONDITION CASH OFFERS ONLY W/PROOF OF FUNDS.*** NO ACCESS TO THE PROPERTY *** OWNER: MANUEL BONILLA (ONLY). *** OUTSIDE INSPECTION ONLY ***  DO NOT ATTEMPT TO GO INSIDE YOU WILL BE ***TRESPASSING*** EMAIL OFFERS TO:  JULIO.ROMAN@YAHOO.COM"),
        ("Multifamily", "Major renovations completed in 2022. Updated amenities include a new kitchen and bathroom."),
        ("Townhouse", "Crown molding and updated kitchen are present in the home. No renovations needed"),
        ("Bungalow", "Recently renovated with modern amenities. Five minutes from downtown"),
        ("Multifamily", "Updated amenities include a new kitchen and bathroom. Five minutes from downtown"),
        ("Fixer Upper", "The property requires significant repairs. The roof is in need of replacement."),
        ("Fixer Upper", "SHORT SALE, HANDYMAN SPECIAL, APPROVAL OF 2 LENDERS IS REQUIRED! PROPERTY IS BEING SOLD AS IS BUYER IS RESPONSIBLE TO OBTAIN C.O. AND ANY AND ALL GOVERNMENTAL PERMITS. 2 Bedrooms, 1 Bath. . PARTIAL REHAB IS REQUIRED. BATHROOM & KITCHEN NEED UPDATES. EXTERIOR WEAR & TEAR.. To show call LO....Showing  with the 48 hours notice. Please call  the Office 973-773-4000"),
        ("", "This home does not need any repairs. Completed renovations in 2023"),
        ("Cape Cod", "MAJOR REPAIRS NEEDED! THERE ARE NO WALLS AND FLOORS! CASH BUYERS OR 203K LOANS ONLY!"),
        ("Colonial", "Investor special awaiting anyone brave enough to do the job. It may take all of your money!"),
        ("", "Completed renovations in 2023. However, there's major storm damage from the recent Hurricane Sandy. Mold present"),
        ("Colonial", "Minor TLC needed. Just new paint job and a new window"),
        ("A-Frame", "Fixer Upper on a quite street in Morris County. Low Taxes only $1,544 per year!!!! Commuters location close to Rt 80, 46 and 206. Cash or rehab loan required to close. Survey with markers completed. Variance approved for addition. Price reflects condition of house. AS IS Sale, buyer is responsible for all repairs, inspections, permits, certs. & C/O... MOTIVATED SELLER...MAKE OFFER!. Owner is a licensed realtor and the listing agent of this property. All offers to be submitted on a written contract. Needs new septic to be paid for by the buyer.. Vacant, Go Direct, bring flashlight, no utilities on go during the day"),
        ("Townhouse", "House in need of repairs, 2 bedrooms, 1 full bath.. call direct 973 767 2850 to request and appointment. short sale subject to third party approval.. 24hr, Notice, Call direct. 973 767 2850 Short Sale to third Part Approval"),
        ("Bungalow", "HOUSE-1fixer upper 1198 SQ FT 2 BA: COTTAGE  2 BA-1488 SQ FT  2017 SEPTIC 4 BDR. New well Live in cottage and fix up house NEW SEPTIC SERVICES BOTH HOMES- TWO WELLS;  PROPERTY BORDERS STREAM-OLD POOL AS IS- FIXER UPPER- COTTAGE IS SIMPLE -LIVE IN IT WHILE FIXING UP OTHER HOME.. Short sale in progress    old Pool AS IS 2 houses MAIN HOUSE NEEDS RENOVATION Pretty spot for fishing.  2017 septic system,   See show instructions on media. SHOW INSTRUCTIONS LB ON BACK COTTAGE DOOR SOME NOTICE PLEASE; SHOW COTT FIRST- 2 KEYS"),
        ("Detached", "HUD OWNED CASE#352-430077 SOLD 'AS-IS'Evidence of mold-no remediation done;If needed-termite treatment/repairs are done at the buyers expense.May contain lead based paint.Sales commission up to 5%.. Go to www.nhmsi.com for additional information or Broker assistance, please call 1-866-382-4447.. HUD PADLOCK ON FRONT DOOR."),
        ("Mediterranian", "Mediteranean style home thats needs work but in great area.  House sold in AS IS condition. Buyerresponsible for CCO repairs if needed. Please leave card and lock doors. None")
    ]

    for idx, sample in enumerate(sample_inputs):
        final_result = classify_condition(primary_style=sample[0], remarks=sample[1])
        pprint(json.dumps(final_result.__dict__))
        print()

        # if idx == 10:
        #     break
