"""Explainable classification of short-sale and bank-owned listing status.

The classifier is independent of pandas, Kafka, and database clients.  It does
not mutate related investment or distressed-sale fields.  ``None`` means that
the remarks do not contain enough accepted evidence to decide; it must not be
silently interpreted as ``False`` when reviewing historical rows.

Callers may persist :meth:`PropertySaleStatusResult.to_dict` with the record key
and exact input text for audit and replay.  The rules should be evaluated against
a reviewed sample of GSMLS listings before they are used for bulk updates.
"""

from __future__ import annotations

import re
import json
from dataclasses import asdict, dataclass
from typing import Literal
from pprint import pprint


CLASSIFIER_VERSION = "1.0.0"
StatusKind = Literal["short_sale", "bank_owned"]


@dataclass(frozen=True)
class StatusRule:
    """A stable, auditable expression of evidence for one status."""

    rule_id: str
    status: StatusKind
    pattern: re.Pattern[str]
    sufficient: bool = True


@dataclass(frozen=True)
class StatusMatch:
    """One accepted or rejected candidate found in the listing remarks."""

    rule_id: str
    status: StatusKind
    matched_text: str
    context: str
    start: int
    end: int
    reason: str


@dataclass(frozen=True)
class StatusDecision:
    """The decision and supporting evidence for a single status."""

    value: bool | None
    accepted_matches: tuple[dict, ...]
    rejected_matches: tuple[dict, ...]
    review_reasons: tuple[str, ...]


@dataclass(frozen=True)
class PropertySaleStatusResult:
    """Independent short-sale and bank-owned decisions for one listing."""

    function_id: str
    short_sale: dict
    bank_owned: dict
    classifier_version: str
    review_reasons: tuple[str, ...]

    def to_dict(self) -> dict:
        """Return a JSON-serializable audit record without performing I/O."""

        return asdict(self)


def _rule(
    rule_id: str,
    status: StatusKind,
    pattern: str,
    sufficient: bool = True,
) -> StatusRule:
    return StatusRule(
        rule_id=rule_id,
        status=status,
        pattern=re.compile(pattern, re.IGNORECASE),
        sufficient=sufficient,
    )


def _rules() -> tuple[StatusRule, ...]:
    # Each alternative is an explicit rule.  This prevents adjacent string
    # literals from joining alternatives, which occurred in the legacy regex.
    return (
        _rule("short_sale_explicit", "short_sale", r"\bshort[\s-]+sale\b"),
        _rule(
            "short_sale_third_party_approval",
            "short_sale",
            r"\bsubject\s+to\s+(?:the\s+)?(?:third[\s-]+party|lenders?)\s+approval\b",
        ),
        _rule(
            "short_sale_bank_approval",
            "short_sale",
            r"\bsubject\s+to\s+(?:the\s+)?banks?\s+approval\b",
        ),
        _rule(
            "short_sale_lender_approval_required",
            "short_sale",
            r"\b(?:approval\s+of\s+(?:the\s+)?(?:\d+\s+)?lenders?\s+is\s+required"
            r"|lenders?\s+approval\s+(?:is\s+)?required)\b",
        ),
        _rule("bank_owned_explicit", "bank_owned", r"\bbank[\s-]+owned\b"),
        _rule("reo_explicit", "bank_owned", r"\bREO(?:\s+(?:property|listing|asset|owned))?\b"),
        _rule("hud_owned", "bank_owned", r"\bHUD[\s-]+owned\b"),
        _rule("fannie_mae_owned", "bank_owned", r"\bFannie\s+Mae[\s-]+owned\b"),
        _rule("freddie_mac_owned", "bank_owned", r"\bFreddie\s+Mac[\s-]+owned\b"),
        # These legacy signals are retained for audit/review, but do not prove
        # that the current owner is a bank.
        _rule("estate_sale_reference", "bank_owned", r"\bestate\s+sale\b", False),
        _rule("corporate_owned_reference", "bank_owned", r"\bcorporate[\s-]+owned\b", False),
        _rule("foreclosure_reference", "bank_owned", r"\b(?:bank\s+)?foreclos(?:ure|ed)\b", False),
    )


_BOUNDARY = re.compile(r"[,.!?;\n]+|\b(?:but|however|although|yet)\b", re.IGNORECASE)
_NEGATION = re.compile(
    r"\b(?:no|not|never|without|isn['’]t|is\s+not|wasn['’]t|was\s+not)\b"
    r"(?!\s+only\b)(?:\s+[\w'’/-]+){0,7}\s*$",
    re.IGNORECASE,
)
_HYPOTHETICAL = re.compile(
    r"\b(?:if|whether|might|may|could|would|possible|potential|suspected)\b",
    re.IGNORECASE,
)
_HISTORICAL = re.compile(
    r"\b(?:formerly|previously|once|used\s+to\s+be|was|were)\b",
    re.IGNORECASE,
)


def _clause_bounds(text: str, start: int, end: int) -> tuple[int, int]:
    left, right = 0, len(text)
    for boundary in _BOUNDARY.finditer(text):
        if boundary.end() <= start:
            left = boundary.end()
        elif boundary.start() >= end:
            right = boundary.start()
            break
    return left, right


def _rejection_reason(
    text: str,
    match: re.Match[str],
    left: int,
    right: int,
) -> str | None:
    before = " ".join(text[left:match.start()].split()[-8:])
    after = " ".join(text[match.end():right].split()[:8])

    if _NEGATION.search(before):
        return "Explicitly negated status"
    if re.match(r"(?:is|was|are|were)\s+not\b", after, re.IGNORECASE):
        return "Explicitly negated status"
    if _HYPOTHETICAL.search(before):
        return "Hypothetical or conditional status reference"
    if _HISTORICAL.search(before):
        return "Historical rather than established current status"
    return None


def _decision(status: StatusKind, text: str) -> dict:
    accepted: list[dict] = []
    rejected: list[dict] = []
    review: list[str] = []

    candidates = sorted(
        (
            (match.start(), rule, match)
            for rule in _rules()
            if rule.status == status
            for match in rule.pattern.finditer(text)
        ),
        key=lambda item: (item[0], item[2].end(), item[1].rule_id),
    )
    for _, rule, match in candidates:
        left, right = _clause_bounds(text, match.start(), match.end())
        reason = _rejection_reason(text, match, left, right)
        if reason is None and not rule.sufficient:
            reason = "Ambiguous reference does not establish current ownership"

        status_match = StatusMatch(
            rule_id=rule.rule_id,
            status=status,
            matched_text=match.group(),
            context=text[max(left, match.start() - 120):min(right, match.end() + 120)].strip(),
            start=match.start(),
            end=match.end(),
            reason=reason or "Affirmative current status evidence",
        ).__dict__
        (rejected if reason else accepted).append(status_match)

    negated = any(match['reason'] == "Explicitly negated status" for match in rejected)
    if accepted and negated:
        review.append("Affirmative and negated evidence coexist; inspect the listing chronology")
    elif not accepted and rejected and not negated:
        review.append("Only ambiguous, historical, or conditional candidates were found")

    if accepted:
        value: bool | None = True
    elif negated:
        value = False
    else:
        value = None

    return StatusDecision(
        value=value,
        accepted_matches=tuple(accepted),
        rejected_matches=tuple(rejected),
        review_reasons=tuple(review),
    ).__dict__


def classify_property_sale_status(listing_remarks: str | None) -> PropertySaleStatusResult:
    """Classify short-sale and bank-owned status from listing remarks.

    ``True`` requires affirmative accepted evidence.  ``False`` requires an
    explicit negation and no affirmative evidence.  ``None`` represents absent
    or inconclusive evidence.  Both statuses are evaluated independently.
    """

    if listing_remarks is not None and not isinstance(listing_remarks, str):
        raise TypeError("listing_remarks must be a string or None")

    text = listing_remarks or ""
    short_sale = _decision("short_sale", text)
    bank_owned = _decision("bank_owned", text)
    review: list[str] = []

    if not text.strip():
        review.append("No listing remarks supplied")
    elif short_sale['value'] is True and bank_owned['value'] is False:
        review.append("Short sales have affirmative evidence; inspect listing chronology")
    elif short_sale['value'] is False and bank_owned['value'] is True:
        review.append("Bank owned have affirmative evidence; inspect listing chronology")
    elif short_sale['value'] is True and bank_owned['value'] is True:
        review.append("Both statuses have affirmative evidence; inspect listing chronology")

    return PropertySaleStatusResult(
        function_id="sale_status_result",
        short_sale=short_sale,
        bank_owned=bank_owned,
        classifier_version=CLASSIFIER_VERSION,
        review_reasons=tuple(review),
    )


if __name__ == "__main__":

    sample_inputs = [
        ("Huge Lot 80x94, Great Schools, Needs very little TLC. Huge Master Bedroom Suite. 2 Fireplaces.. MOTIVATED SELLER BRING OFFERS. SHORT SALE IS SUBJECT TO BANK APPROVAL. NEWER KITCHEN NEEDED, ETC.. SHORT NOTICE OKAY. CALL NELSON 201-757-7355"),
        ("INCREDIBLE BUY. Best priced unit in Great Gorge One bedroom plus loft. sold in as is condition. This not a short sale or foreclosure.  Owner would rather sell at lower number than renovate. make all. offers. newer a/c condenser. needs flooring and painting. Seller has lowered price due to repairs. vacant lockbox"),
        ("Just move into this 4 bedrm cape !  Feartures: Hardwd floors, large livingrm, formal dining room,kitchen and 2 bedrms on 1st. 2 bedrms on second, finished basement, 1 car attached garage. Buyers must be pre-approved by Absolute Home Mortgage. Short sale to be approved by bank.. Buyers must be pre-approved by Absolute Home Mortgage. Short sale to be approved by bank.. Make appt w/973-626-4805 Showings:Tues&Thurs 5-7,Sat&Sun 1-4 Only"),
        ("PRE FORECLOSURE SHORT SALE SUBJECT TO BANK APPROVAL QUIET RESIDENTIAL NEIGHBORHOOD TREE LINED STREET CLOSE TO SHOPS AND TRANSPORATION. None. None"),
        ("BANK OWNED AND SOLD AS IS.BUYER RESPONSIBLE FOR C/O AND ALL CERTS OR REPAIR.PROOF OF FUNDS.NEEDS A FEW REPAIRS.. BANK OWNED AND SOLD AS IS.BUYER TO GET CO AND ALL CERTS ETC.PROOF OF FUNDS REQ.. CALL LA CELL # 973-650-8463"),
        ("bank owned and sold as is,better hurry,it's in good shape.. $1500.00 selling bonus. call la cell 973-650-8463"),
        ("3 YR OLD TOWNHOUSE IN GOOD CONDTION. BANK OWNED 2 BDRM, 2.1 BATHS, 1 CAR GAR, DECK,  GREAT SCHOOLS, COMMUNITY POOL AND MORE A MUST SEE.. None. CALL LISTING OFFICE FOR ACCESS CODE."),
        ("NO MORE SHOWINGS PER SELLERS REQUEST!!! BANK WILL NOT REMEDIATE ANY MOLD ISSUES OR REMOVE ANY POTENTIAL TANKS! BANK OWNED!!! SOLD STRICTLY 'AS IS'! NO GUARANTEES EXPRESSED OR IMPLIED!. BANK WILL NOT REMEDIATE ANY MOLD ISSUES OR REMOVE ANY POTENTIAL TANKS! BANK OWNED!!! SOLD 'AS IS'.. BANK REQUESTS NO MORE SHOWINGS!!! PROPERTY GOING TO AUCTION!!!"),
        ("REO, not a short sale. DO NOT DISTURB OCCUPANTS. Property may currently be occupied. Cash only offers. Seller may not be able to deliver possession at closing. Eviction proceedings may have begun. Details and offers online at www.kazork.com.. MUST CALL LISTING OFFICE"),
        ("Fairway Knolls section.  Stately colonial.  Needs work.  Being sold as is  This is not a short sale.  Seller is contract owner.. Call Diane McVey at (201) 845-5493 for easy showing.. Call Diane McVey at (201) 845-4477 or (201) 845-5493 for easy showings."),
        ("Oversized end unit 2 car garage huge finished walk-out basement. Master suite with walk-in closet and bath with whirlpool tub.Newer hardwood floors newer stainless appliances,nothing to do.. not a short sale quick closing possible easy to show.. call Tony 917 769 8570 1 hour notice"),
        ("Subject to Bank Lender Approval Short Sale Custom Sprawling Ranch with tremendous possibilities; a diamond in the rough.  Some features include Pond W/WTRFLL, steel beam construction. Oversized EIK, skylights, new hardwood floors, 2 fireplaces, Great room and Master have vaulted ceilings. Full Basement; half of which is grade level w/ 10' ceiling, pool, central vac, 2 yr old HTNG/AC unit and so much more! Strickly as is. *** owner has real estate license*** present all offers***short sale***. Subject to Bank/Lender approval short sale call LA Joy for all appts. 201-965-8381. NJ AND GS LKBX. call LA Joy for all appts 201-965-8381"),
        ("Fabulous brick colonial w/ wood burning fpl in the living rm and hdwd flrs throughout. Large DR w/ french doors leading to 3 season slate flr porch.  All the bdrms are large. Updated kitchen cabinets in this large EIK. Third flr has the 4th bdrm and the PR with newer carpet. House is larger than it appears. An absolute must see!!!. Call Simone for alarm code at 201-921-4609. Short sale - Third party approval needed on any offers.. call Simone for alarm code at 201-921-4609 lkbx installed 12/17"),
        ("Short Sale approved for immediate sale at $265,000.. short sale approved immiduate paerwork approved ready for immediate sale @ $265,000.. Call listing agent Mike Berk 201-956-3077 GSMLS"),
        ("!!!!!!!!!!!      BACK ON MARKET.  3/4 BEDROOMS 3 FULL BATHS, HOME HAS BEEN REDONE IN MARCH OF 2007. BANK APPROVAL ON SHORT SALE IN PROCESS. !!!!!!!!!!!   SEND IN YOUR OFFERS WILL NOT LAST !!!!!!!!!!!!. None. PLS CC MICHELLE 973-508-2480 THEN SHOW THANK YOU."),
        ("Investor/Developer Special in the heart of Newark's highly sought-after Ironbound section. This 2-story Colonial offers incredible upside for renovation, expansion, or long-term hold potential. Situated on a narrow urban lot with strong neighborhood demand, the property features an enclosed front porch, private side access, and a classic layout ready for repositioning. Surrounded by ongoing redevelopment, restaurants, shopping, transportation, and major highways, this is an ideal opportunity for investors, builders, or owner-occupants looking to customize a home in one of Newark's most vibrant communities. Property requires updates and is being sold strictly as-is. Buyer responsible for all city inspections, permits, certifications, and municipal requirements. Cash or renovation financing preferred. Great opportunity to bring your vision to life in the Ironbound.Disclosures: Estate sale and short sale subject to court approval and 3rd party approval.. Go & Show. Hold Harmless Required prior to showing and is available through GSMLS docs. Please submit all offers to d.leonard@realtyofamerica.com. Any questions please contact Keyanna Leonard at 908-213-5429 or email k.leonard@realtyofamerica.com. Sight unseen offers not preferred.. Lockbox Code is 1986."),
        ("Unlock the potential of this 4-bedroom, 2-bath home located in the heart of Bergen Point Bayonne! Situated on a quiet residential street, this property offers generous room sizes ready for your vision & creativity. With the right updates & renovations, this home can truly shine.. There is a full basement with ample storage or finishing potential, a private backyard, and convenient access to local shopping, parks, schools, and public transportation. Commuters will have easy access to highways, Light Rail & NYC. Whether you're an investor, contractor, or buyer looking to create your dream home, this is a rare opportunity to add value in a thriving neighborhood. Bring your imagination & transform this diamond in the rough into something special! 1888 Studio being built right up the street! This is an Estate Sale and is as-is.. Please use Showing Time. This is an estate and the property is being sold ''as is''. Seller will make no repairs. Working order of appliances and wall A/C units is unknown.. Please text 732-687-3097 to show or use Showing Time."),
        ("Welcome to this charming Cape Cod home located in the desirable Rock Springs section of West Orange. The 1st floor offers a convenient BR, a spacious family room and afull bath in immaculate condition. Set on a spawling lot, the property provides a serene, wooded feel with plenty of space for outdoor enjoyment & entertainment. Commuters will appreciate the nearby jitney service to the South Orange train station. This custom cape is full of potential and ready for your personal touch. Estate Sale--being sold as is.. None. None"),
        ("On the 5th floor of the Paragon sits the well maintained, two-bedroom, two full bath condo unit, ready for immediate occupancy.  This unit offers an eat-in kitchen with ample cabinet space, which flows seamlessly into the combined living & dining rooms, perfect for comfortable living and entertaining. Sliders from the living room leads directly to a nice sized balcony with great views of the surrounding area. Not to be overlooked is the laundry room sporting a washer/dryer combo and conveniently located within the unit. You will appreciate the beautiful hardwood floors throughout and lots of natural light. There are two deeded parking spaces that goes with the unit (11 &16). The building itself is well-maintained and its location is ideal, with easy access to highways for commuting to NYC and within walking distance of public transportation. You'll also find yourself close to shopping, restaurants, banks, houses of worship, and entertainment. This is a pet friendly building, so bring your furry friends along to this move-in ready unit. Estate Sale, selling as-is . Water is included in the HOA dues.The Seller is requesting that all interested parties submit their BEST and FINAL OFFERS by October 17, 2025 at 5pm.. Vacant Unit. Combination lockbox is located on the railing to the left of the front door with the #1 on the front to identify the box.. Use Showing Time to schedule appointments. Turn off lights. Combination lockbox is located on the side railing, to the left of the front door, identified by the #1 on the front of the box."),
        ("An exceptional opportunity to transform this classic 3 bedroom, 1.5 bath colonial into the home of your dreams. Nestled on a nice size lot, this home offers a rare combination of space and potential, featuring a detached 2-car garage and driveway parking for up to 5 vehicles. A true find in the North section of Bloomfield! Inside you'll find quaint colonial charm, a solid layout, and room to reimagine every space to suit your style. Whether you're an investor, contractor, or buyer ready to customize, this property is a blank canvas brimming with opportunity. Ideally located just minutes from major highways and NJ Transit, commuting to NYC is quick and convenient. You'll also love the proximity to local parks, schools, shopping, and dining options. Bring your vision and your creativity, homes with this kind of potential don't last long.. This is an estate sale, home is being sold in strictly as-is condition, no repairs or credits. Garage door opener is located on the wall inside through the sliding door on the back porch. Please do not remove from wall.. Please use ShowingTime to schedule an appt. Code Lockbox, Vacant, Text/Call agent with any questions."),
        ("Estate sale being sold as-is with no known structural issues, offering a great opportunity to update and add value in the highly desirable Milnes section of Fair Lawn. This spacious split-level home features an entry foyer with a closet, a bright living room with vaulted ceilings and a bay window, a formal dining area, and a large eat-in kitchen. The upper level offers three generously sized bedrooms, including a primary suite with a private bath, plus a full hall bath. The lower level includes a family room, half bath, and direct access to the yard and attached two-car garage. The unfinished basement provides incredible potential for additional space for recreation, storage, and laundry. This home has over 1,800 sq ft, a well-designed layout, and strong bones, ready for your personalized updates. Pleasantly located near top-rated public schools, NYC transportation, shopping, parks, and houses of worship.. First Showing at Open House Sunday, May 3, 12-4 pm. This is an estate sale. Property sold strictly as is with no known issues. Buyer is responsible for twp inspection & CO.. First Showing at OH Sun. May 3 @ 12-4 PM. Text agent with questions 201.949.8229, ShowingTime, and Text Agent for Confirmed Appointment after OH."),
        ("Spacious 5-bedroom, 3-bath Colonial Cape located in the highly sought-after Gregory section of West Orange, featuring stunning New York City views. This expanded home offers an abundance of living space, with generously sized rooms, a full bath on every level, a finished basement, and a flexible second-floor layout ideal for work-from-home options.  The first floor features a welcoming foyer, bright living room, formal dining room, and large eat-in kitchen, along with two bedrooms and a mudroom providing access to the yard. The second floor includes three additional bedrooms, a den, and a large sitting room that can serve as a second living area or home office.  Situated on a 91' x 103' lot, the property offers outdoor living space with a slate patio and level lawn area. A two-car built-in garage with interior access and a brick paver driveway accommodating four cars adds both convenience and curb appeal.  Prime location for commuters with easy access to NYC trains and buses. Close to Rock Spring Golf Club, Turtle Back Zoo, South Mountain Reservation, the Dog Park, and the Fairy Trail, as well as schools, shopping, and dining.  Bring your vision and make this spacious Gregory-section home your own.. Estate Sale home is being sold as is. Showings Sat and Sun 1-4 Text Nancy for appt  908 403-7304. Seller may be present for showings")
        ]

    for idx, sample in enumerate(sample_inputs):
        final_result = classify_property_sale_status(sample)
        pprint(json.dumps(final_result.__dict__))
        print()

        # if idx == 0:
        #     break
