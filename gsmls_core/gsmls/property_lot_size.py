"""Pure, explainable parsing of GSMLS residential lot-size fields.

GSMLS supplies both ``ACRES`` and a free-text ``LOTSIZE`` value.  The acreage
field is frequently rounded, while the text may contain exact dimensions or an
explicit square-foot measurement.  This module parses both sources without
depending on pandas, Kafka, or database clients and records why a value was
selected.

``None`` means that no supported, positive lot area could be established.  A
caller that must persist a numeric sentinel may convert ``None`` to zero at its
boundary, while retaining :meth:`LotSizeResult.to_dict` for review.
"""

from __future__ import annotations

import json
import math
import pandas as pd
import re
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pprint import pprint
from typing import Literal



PARSER_VERSION = "1.0.0"
SQFT_PER_ACRE = Decimal("43560")
LotSizeSource = Literal["lotsize", "acres", "unknown"]

_NUMBER = r"(?:(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?|\.\d+)"
_DIMENSIONS = re.compile(
    rf"^\s*(?P<width>{_NUMBER})\s*[x×]\s*(?P<depth>{_NUMBER})(?P<suffix>.*)$",
    re.IGNORECASE,
)
_SQUARE_FEET = re.compile(
    rf"^\s*(?P<value>{_NUMBER})\s*"
    rf"(?P<unit>SF|SQFT|SQ\.?\s*FT|SQ\.?\s*FEET|SQUARE\s*FEET)\b"
    rf"(?P<suffix>.*)$",
    re.IGNORECASE,
)
_ACRES = re.compile(
    rf"^\s*(?P<value>{_NUMBER})\s*"
    rf"(?P<unit>ACRES?|AC\.?)\s*(?P<suffix>.*)$",
    re.IGNORECASE,
)
_BARE_NUMBER = re.compile(rf"^\s*(?P<value>{_NUMBER})\s*$")
_ADDITIONAL_DIMENSIONS = re.compile(
    rf"(?:^|\D){_NUMBER}\s*[x×]\s*{_NUMBER}(?:\D|$)",
    re.IGNORECASE,
)
_MULTI_LOT = re.compile(r"\b\d+(?:\.\d+)?\s*(?:LT|LOTS?)\b", re.IGNORECASE)
_DESCRIPTIVE = re.compile(
    r"\b(?:COMMON|CONDO|SITE|TOWNHOUSE|PERCENT|PCT)\b|%",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ParsedLotSize:
    """One candidate parsed from the free-text ``LOTSIZE`` field."""

    square_feet: float | None
    rule_id: str | None
    suffix: str
    review_reasons: tuple[str, ...]


@dataclass(frozen=True)
class LotSizeResult:
    """Selected lot area plus both source candidates and audit information."""

    function_id: str
    square_feet: float | None
    source: LotSizeSource
    rule_id: str | None
    lotsize_candidate_sqft: float | None
    acres_candidate_sqft: float | None
    raw_lotsize: str | None
    raw_acres: str | None
    parser_version: str
    review_reasons: tuple[str, ...]

    def to_dict(self) -> dict:
        """Return a JSON-serializable audit record without performing I/O."""

        return asdict(self)


def _decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None

    try:
        number = Decimal(str(value).strip().replace(",", ""))
    except (InvalidOperation, ValueError):
        return None

    if not number.is_finite() or number <= 0:
        return None
    return number


def _square_feet(value: Decimal) -> float:
    """Round to hundredths of a square foot for stable database values."""

    return float(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _suffix_reasons(suffix: str) -> list[str]:
    cleaned = suffix.strip(" .,-")
    if not cleaned:
        return []

    reasons = [f"Unconsumed LOTSIZE suffix: {cleaned}"]
    if _MULTI_LOT.search(cleaned):
        reasons.append("Multi-lot marker requires review before assuming one rectangular area")
    if _ADDITIONAL_DIMENSIONS.search(cleaned):
        reasons.append("Additional dimensions require review for an irregular or compound lot")
    return reasons


def parse_lotsize_text(lotsize: str | None) -> ParsedLotSize:
    """Parse a single GSMLS ``LOTSIZE`` string.

    Supported evidence, in order, is explicit square feet, width-by-depth
    dimensions, explicit acreage, and a bare numeric value.  Bare decimals below
    100 are interpreted as acres, matching the observed GSMLS convention.  Bare
    integers below 100 remain unresolved because they may be frontage, acreage,
    or an incomplete dimension.
    """

    if lotsize is not None and not isinstance(lotsize, str):
        return ParsedLotSize(
            None,
            None,
            "",
            ("LOTSIZE format was not recognized",),
        )

    text = (lotsize or "").strip()
    if not text or re.fullmatch(r"0+(?:\.0+)?", text):
        return ParsedLotSize(None, None, "", ("No positive LOTSIZE value supplied",))
    if _DESCRIPTIVE.search(text) and not re.match(r"^\s*[\d,.]", text):
        return ParsedLotSize(
            None,
            None,
            "",
            ("Descriptive or common-interest LOTSIZE does not establish a parcel area",),
        )

    match = _SQUARE_FEET.match(text)
    if match:
        value = _decimal(match.group("value"))
        suffix = match.group("suffix")
        reasons = _suffix_reasons(suffix)
        return ParsedLotSize(
            _square_feet(value) if value is not None else None,
            "explicit_square_feet",
            suffix.strip(),
            tuple(reasons),
        )

    match = _DIMENSIONS.match(text)
    if match:
        width = _decimal(match.group("width"))
        depth = _decimal(match.group("depth"))
        suffix = match.group("suffix")
        reasons = _suffix_reasons(suffix)
        if _MULTI_LOT.search(suffix) or _ADDITIONAL_DIMENSIONS.search(suffix):
            return ParsedLotSize(None, "compound_dimensions", suffix.strip(), tuple(reasons))
        value = width * depth if width is not None and depth is not None else None
        return ParsedLotSize(
            _square_feet(value) if value is not None else None,
            "width_by_depth",
            suffix.strip(),
            tuple(reasons),
        )

    match = _ACRES.match(text)
    if match:
        acres = _decimal(match.group("value"))
        suffix = match.group("suffix")
        reasons = _suffix_reasons(suffix)
        return ParsedLotSize(
            _square_feet(acres * SQFT_PER_ACRE) if acres is not None else None,
            "explicit_acres",
            suffix.strip(),
            tuple(reasons),
        )

    match = _BARE_NUMBER.match(text)
    if match:
        value = _decimal(match.group("value"))
        if value is None:
            return ParsedLotSize(None, None, "", ("No positive LOTSIZE value supplied",))
        if value < 100 and "." in match.group("value"):
            return ParsedLotSize(
                _square_feet(value * SQFT_PER_ACRE),
                "bare_decimal_acres",
                "",
                ("Bare decimal interpreted as acres using observed GSMLS convention",),
            )
        if value >= 100:
            return ParsedLotSize(
                _square_feet(value),
                "bare_square_feet",
                "",
                ("Bare value interpreted as square feet",),
            )
        return ParsedLotSize(
            None,
            "ambiguous_bare_number",
            "",
            ("Bare number below 100 has no reliable unit",),
        )

    return ParsedLotSize(
        None,
        None,
        "",
        ("LOTSIZE format was not recognized",),
    )


def calculate_property_lot_size(
    lotsize: str | None,
    acres: str | int | float | None,
) -> LotSizeResult:
    """Select square footage from ``LOTSIZE`` first, then ``ACRES``.

    Exact dimensions and explicit units in ``LOTSIZE`` take precedence over the
    rounded ``ACRES`` column.  When both sources exist, a difference greater than
    two percent is retained as a review reason.  This function performs no I/O
    and does not replace unresolved values with zero.
    """

    if acres is not None and not isinstance(acres, (str, int, float)):
        raise TypeError("acres must be a string, number, or None")
    if isinstance(acres, float) and not math.isfinite(acres):
        acres_value = None
    else:
        acres_value = _decimal(acres)

    parsed = parse_lotsize_text(lotsize)
    acres_sqft = (
        _square_feet(acres_value * SQFT_PER_ACRE)
        if acres_value is not None
        else None
    )
    review = list(parsed.review_reasons)

    if parsed.square_feet is not None and acres_sqft is not None:
        difference = abs(parsed.square_feet - acres_sqft)
        denominator = max(parsed.square_feet, acres_sqft)
        if denominator and difference / denominator > 0.02:
            review.append(
                "LOTSIZE and ACRES candidates differ by more than two percent; "
                "the more explicit LOTSIZE value was selected"
            )

    if parsed.square_feet is not None:
        square_feet = parsed.square_feet
        source: LotSizeSource = "lotsize"
        rule_id = parsed.rule_id
    elif acres_sqft is not None:
        square_feet = acres_sqft
        source = "acres"
        rule_id = "acres_column_fallback"
    else:
        square_feet = None
        source = "unknown"
        rule_id = None

    return LotSizeResult(
        function_id="lotsize_result",
        square_feet=square_feet,
        source=source,
        rule_id=rule_id,
        lotsize_candidate_sqft=parsed.square_feet,
        acres_candidate_sqft=acres_sqft,
        raw_lotsize=lotsize,
        raw_acres=None if acres is None else str(acres),
        parser_version=PARSER_VERSION,
        review_reasons=tuple(dict.fromkeys(review)),
    )


if __name__ == "__main__":

    sample_file = r'C:\Users\jibreel.q.hameed.AC\PycharmProjects\GSMLS-Analysis-Updated\sample_data_09102026.xls'
    data = pd.read_excel(sample_file)
    sample_data = data.sample(50)
    sample_data.reset_index(drop=True, inplace=True)

    for idx, row in sample_data.iterrows():
        result = calculate_property_lot_size(row['LOTSIZE'], row['ACRES'])
        pprint(json.dumps(result.__dict__))
        print()

        # if idx == 0:
        #     break

