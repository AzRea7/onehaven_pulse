# OneStream Pulse Investor Market Decision Framework

## Purpose

This framework assigns a deterministic investor stance to a market using current OneStream Pulse market metrics, coverage diagnostics, confidence signals, and missing-data rules.

The stance is not a property-level buy recommendation. It is a market-selection signal.

## Product principle

The stance should help an investor decide where to spend research time.

It should not overstate conviction. A market with promising signals but material missing data should usually be Watchlist, not Attractive.

## Stances

### Attractive

The market has strong enough data coverage and a favorable mix of affordability, rent/price momentum, labor stability, and confidence.

Attractive is a high-conviction market-research priority. It still does not mean “buy property.”

A market should not be Attractive if:
- any core dimension is negative;
- two or more secondary dimensions are missing;
- core missing-data inputs are material;
- confidence is below the positive threshold.

### Watchlist

The market has promising signals but one or more issues prevent high conviction.

Examples:
- rent momentum and affordability are favorable, but price momentum is weak;
- permits are missing;
- inventory detail is incomplete;
- confidence is usable but not strong;
- missing score inputs are material.

Use Watchlist for markets that deserve monitoring or selective research.

### Mixed

The market has enough data to evaluate but contains contradictory signals without a clear positive or negative tilt.

Use Mixed when strategy fit matters.

### Avoid

The market has enough data to evaluate and shows unfavorable signals: weak affordability, poor labor stability, low confidence, weak rent/price dynamics, or multiple core negatives.

Avoid means deprioritize unless there is a special local thesis.

### Insufficient Data

The market lacks the minimum required data coverage for a responsible stance.

Use Insufficient Data to avoid overinterpreting incomplete data.

## Required inputs

Minimum required categories for a full stance:

- price
- rent
- affordability
- labor

Important but not always required:

- inventory
- permits

## Deterministic scoring dimensions

Each market receives dimension statuses:

- positive
- neutral
- negative
- missing

Dimensions:

- price_momentum
- rent_momentum
- affordability
- labor_stability
- inventory_pressure
- supply_pressure
- confidence
- coverage_quality

## Core dimensions

Core dimensions:

- price_momentum
- rent_momentum
- affordability
- labor_stability
- confidence
- coverage_quality

Secondary dimensions:

- inventory_pressure
- supply_pressure

## Missing data behavior

Missing price, rent, affordability, labor, confidence, or scoreable period can force Insufficient Data.

Missing inventory or permits should not automatically force Insufficient Data, but it should reduce conviction and appear as a risk.

Material missing score inputs should prevent Attractive.

## Rule summary

Insufficient Data:
- Missing required coverage: price, rent, affordability, or labor.
- Or confidence score is missing.
- Or latest scoreable period is missing.
- Or two or more core dimensions are missing.

Avoid:
- Confidence is below 0.60.
- Or three or more core dimensions are negative.
- Or affordability and labor stability are both negative.

Attractive:
- Required coverage is present.
- Confidence is positive.
- Coverage quality is positive or neutral.
- Rent momentum is positive.
- Affordability is positive or neutral.
- Labor stability is positive or neutral.
- Price momentum is positive or neutral.
- No core dimensions are negative.
- No core dimensions are missing.
- Material missing score inputs are not present.
- At most one secondary dimension is missing.

Watchlist:
- Required coverage is present.
- Confidence is usable.
- At least two core dimensions are positive.
- No Avoid rule applies.
- The market has some issue preventing Attractive, such as missing permits, missing inventory detail, neutral confidence, weak price momentum, or missing score inputs.

Mixed:
- Required coverage is present.
- No Insufficient Data or Avoid rule applies.
- The market does not satisfy Attractive or Watchlist.
- Signals are contradictory or strategy-dependent.

## AI usage

AI may summarize the deterministic stance. AI may not change the stance, driver list, risk list, confidence score, coverage flags, or rule trace.
