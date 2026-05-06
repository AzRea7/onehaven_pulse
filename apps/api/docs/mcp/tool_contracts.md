# MCP Tool Contracts v1

**Epic:** Epic 10 — AI/MCP Tool Layer

**Story:** Story 10.1 — Define MCP tool contracts

## Principles

- Tools are deterministic wrappers over existing API endpoints.
- Tools return structured JSON, not prose.
- Tools do not require direct database access.
- Tools do not depend on hidden prompt logic.
- AI explanation layers must cite fields returned by these tools.
- Missing data must be represented explicitly, not inferred silently.

## Tools

### `get_market_context`

Return structured market context, evidence, scoring, coverage, risks, data quality, and freshness for one canonical geography.

- Method: `GET`
- Endpoint: `/markets/{geo_id}/context`
- Example: `/markets/metro_19820/context`
- Permission scope: `read:market_context`
- Requires direct DB access: `none`
- Latency target: `500 ms`

#### Data freshness

- Source: API response source_freshness and latest_data_period
- Stale behavior: Return stale flags from source freshness. Do not hide stale data.

#### Failure behavior

- `invalid_geo_id`: Return structured not_found error from API.
- `partial_data`: Return context with coverage false categories and missing_score_inputs.
- `upstream_failure`: Return API error with request id when available.

#### Input schema

```json
{
  "additionalProperties": false,
  "properties": {
    "geo_id": {
      "description": "Canonical geography id, for example us or metro_19820.",
      "pattern": "^(us|state_[A-Z]{2}|metro_[0-9]{5}|county_[0-9]{5}|zcta_[0-9]{5})$",
      "type": "string"
    }
  },
  "required": [
    "geo_id"
  ],
  "type": "object"
}
```

#### Output schema

```json
{
  "properties": {
    "confidence_score": {
      "maximum": 1,
      "minimum": 0,
      "type": [
        "number",
        "null"
      ]
    },
    "coverage": {
      "properties": {
        "affordability": {
          "type": "boolean"
        },
        "inventory": {
          "type": "boolean"
        },
        "labor": {
          "type": "boolean"
        },
        "permits": {
          "type": "boolean"
        },
        "price": {
          "type": "boolean"
        },
        "rent": {
          "type": "boolean"
        }
      },
      "required": [
        "price",
        "rent",
        "inventory",
        "affordability",
        "labor",
        "permits"
      ],
      "type": "object"
    },
    "cycle_phase": {
      "type": [
        "string",
        "null"
      ]
    },
    "data_quality": {
      "type": "object"
    },
    "data_status": {
      "type": "string"
    },
    "evidence": {
      "type": "object"
    },
    "geo_id": {
      "type": "string"
    },
    "geo_type": {
      "type": "string"
    },
    "investor_signal": {
      "type": [
        "string",
        "null"
      ]
    },
    "latest_data_period": {
      "format": "date",
      "type": [
        "string",
        "null"
      ]
    },
    "latest_period": {
      "format": "date",
      "type": [
        "string",
        "null"
      ]
    },
    "market": {
      "type": "string"
    },
    "mcp": {
      "type": "object"
    },
    "risks": {
      "type": "array"
    },
    "score_breakdown": {
      "type": "object"
    },
    "source_freshness": {
      "type": "array"
    }
  },
  "required": [
    "geo_id",
    "market",
    "geo_type",
    "latest_period",
    "data_status",
    "cycle_phase",
    "investor_signal",
    "confidence_score",
    "evidence",
    "score_breakdown",
    "coverage",
    "risks",
    "data_quality"
  ],
  "type": "object"
}
```

### `get_market_timeseries`

Return ordered monthly time-series values for selected metrics for one canonical geography.

- Method: `GET`
- Endpoint: `/markets/{geo_id}/timeseries`
- Example: `/markets/metro_19820/timeseries?metrics=zhvi_yoy,zori_yoy,payment_to_income_ratio,unemployment_rate&start_date=2024-01-01`
- Permission scope: `read:market_timeseries`
- Requires direct DB access: `none`
- Latency target: `500 ms`

#### Data freshness

- Source: Returned period_month values and metric nulls.
- Stale behavior: Return available historical periods; do not fabricate missing months.

#### Failure behavior

- `invalid_metric`: API rejects unsupported metrics.
- `empty_range`: Return empty items with valid market identity.
- `too_large_range`: API should enforce bounded windows from performance guardrails.

#### Input schema

```json
{
  "additionalProperties": false,
  "properties": {
    "end_date": {
      "format": "date",
      "type": [
        "string",
        "null"
      ]
    },
    "geo_id": {
      "type": "string"
    },
    "metrics": {
      "items": {
        "enum": [
          "zhvi",
          "zhvi_yoy",
          "zori",
          "zori_yoy",
          "home_price_yoy",
          "rent_yoy",
          "median_sale_price",
          "active_listings",
          "median_days_on_market",
          "months_supply",
          "mortgage_rate_30y",
          "unemployment_rate",
          "price_to_income_ratio",
          "payment_to_income_ratio",
          "estimated_monthly_payment",
          "composite_cycle_score",
          "confidence_score"
        ],
        "type": "string"
      },
      "maxItems": 12,
      "minItems": 1,
      "type": "array"
    },
    "start_date": {
      "format": "date",
      "type": [
        "string",
        "null"
      ]
    }
  },
  "required": [
    "geo_id",
    "metrics"
  ],
  "type": "object"
}
```

#### Output schema

```json
{
  "properties": {
    "items": {
      "items": {
        "properties": {
          "missing_metrics": {
            "items": {
              "type": "string"
            },
            "type": "array"
          },
          "period_month": {
            "format": "date",
            "type": "string"
          },
          "values": {
            "type": "object"
          }
        },
        "required": [
          "period_month",
          "values"
        ],
        "type": "object"
      },
      "type": "array"
    },
    "market": {
      "type": "object"
    },
    "metrics": {
      "items": {
        "type": "string"
      },
      "type": "array"
    }
  },
  "required": [
    "market",
    "metrics",
    "items"
  ],
  "type": "object"
}
```

### `compare_markets`

Compare 2–5 canonical markets using deterministic latest metrics and time-series values.

- Method: `GET`
- Endpoint: `/compare/markets`
- Example: `/compare/markets?geo_ids=metro_19820,metro_16980&metrics=zhvi_yoy,zori_yoy,payment_to_income_ratio,unemployment_rate&start_date=2024-01-01`
- Permission scope: `read:market_compare`
- Requires direct DB access: `none`
- Latency target: `1000 ms`

#### Data freshness

- Source: Returned latest rows and time-series periods.
- Stale behavior: Expose nulls/missing metrics per market.

#### Failure behavior

- `too_few_markets`: Reject fewer than 2 geo_ids.
- `too_many_markets`: Reject more than 5 geo_ids.
- `invalid_geo_id`: Return structured invalid market information.

#### Input schema

```json
{
  "additionalProperties": false,
  "properties": {
    "end_date": {
      "format": "date",
      "type": [
        "string",
        "null"
      ]
    },
    "geo_ids": {
      "items": {
        "type": "string"
      },
      "maxItems": 5,
      "minItems": 2,
      "type": "array"
    },
    "metrics": {
      "items": {
        "type": "string"
      },
      "maxItems": 12,
      "minItems": 1,
      "type": "array"
    },
    "start_date": {
      "format": "date",
      "type": [
        "string",
        "null"
      ]
    }
  },
  "required": [
    "geo_ids",
    "metrics"
  ],
  "type": "object"
}
```

#### Output schema

```json
{
  "properties": {
    "latest": {
      "type": "array"
    },
    "markets": {
      "type": "array"
    },
    "timeseries": {
      "type": "array"
    }
  },
  "required": [
    "markets",
    "latest",
    "timeseries"
  ],
  "type": "object"
}
```

### `search_markets`

Search/list canonical markets by geography type, state, text query, pagination, and confidence filter when supported.

- Method: `GET`
- Endpoint: `/markets`
- Example: `/markets?query=Detroit&limit=10`
- Permission scope: `read:market_search`
- Requires direct DB access: `none`
- Latency target: `300 ms`

#### Data freshness

- Source: geo.dim_geo and latest app-facing metrics.
- Stale behavior: Search identity remains valid even if latest metrics are missing.

#### Failure behavior

- `empty_result`: Return total 0 and empty items.
- `invalid_filter`: Return validation error.

#### Input schema

```json
{
  "additionalProperties": false,
  "properties": {
    "geo_type": {
      "enum": [
        "national",
        "state",
        "metro",
        "county",
        "zcta",
        null
      ],
      "type": [
        "string",
        "null"
      ]
    },
    "limit": {
      "default": 20,
      "maximum": 100,
      "minimum": 1,
      "type": "integer"
    },
    "offset": {
      "default": 0,
      "minimum": 0,
      "type": "integer"
    },
    "query": {
      "type": [
        "string",
        "null"
      ]
    },
    "state": {
      "type": [
        "string",
        "null"
      ]
    }
  },
  "type": "object"
}
```

#### Output schema

```json
{
  "properties": {
    "items": {
      "type": "array"
    },
    "total": {
      "type": [
        "integer",
        "null"
      ]
    }
  },
  "required": [
    "items"
  ],
  "type": "object"
}
```

### `get_source_freshness`

Return source/dataset freshness, latest source period, load status, stale flags, and error messages.

- Method: `GET`
- Endpoint: `/admin/source-freshness`
- Fallback endpoint: `/audit/source-freshness`
- Example: `/admin/source-freshness`
- Permission scope: `read:source_freshness`
- Requires direct DB access: `none`
- Latency target: `500 ms`

#### Data freshness

- Source: analytics/source freshness audit table.
- Stale behavior: Return is_stale and stale_reason fields.

#### Failure behavior

- `no_freshness_rows`: Return empty items, not fabricated statuses.
- `failed_source`: Return last_status and error_message.

#### Input schema

```json
{
  "additionalProperties": false,
  "properties": {
    "dataset": {
      "type": [
        "string",
        "null"
      ]
    },
    "source": {
      "type": [
        "string",
        "null"
      ]
    }
  },
  "type": "object"
}
```

#### Output schema

```json
{
  "properties": {
    "items": {
      "items": {
        "properties": {
          "dataset": {
            "type": "string"
          },
          "error_message": {
            "type": [
              "string",
              "null"
            ]
          },
          "is_stale": {
            "type": [
              "boolean",
              "null"
            ]
          },
          "last_loaded_at": {
            "type": [
              "string",
              "null"
            ]
          },
          "last_status": {
            "type": [
              "string",
              "null"
            ]
          },
          "latest_source_period": {
            "type": [
              "string",
              "null"
            ]
          },
          "record_count": {
            "type": [
              "integer",
              "null"
            ]
          },
          "source": {
            "type": "string"
          },
          "stale_reason": {
            "type": [
              "string",
              "null"
            ]
          }
        },
        "required": [
          "source",
          "dataset"
        ],
        "type": "object"
      },
      "type": "array"
    }
  },
  "type": "object"
}
```

### `get_geo_coverage`

Return coverage diagnostics, available metrics, missing score inputs, and latest scoreable period for one geography.

- Method: `GET`
- Endpoint: `/markets/{geo_id}/coverage`
- Example: `/markets/metro_19820/coverage`
- Permission scope: `read:geo_coverage`
- Requires direct DB access: `none`
- Latency target: `500 ms`

#### Data freshness

- Source: Latest app-facing market metrics and coverage logic.
- Stale behavior: Return latest_data_period and latest_scoreable_period.

#### Failure behavior

- `invalid_geo_id`: Return structured not_found error from API.
- `missing_metrics`: Return coverage false categories and missing_score_inputs.

#### Input schema

```json
{
  "additionalProperties": false,
  "properties": {
    "geo_id": {
      "type": "string"
    }
  },
  "required": [
    "geo_id"
  ],
  "type": "object"
}
```

#### Output schema

```json
{
  "properties": {
    "available_metrics": {
      "items": {
        "type": "string"
      },
      "type": "array"
    },
    "coverage": {
      "type": "object"
    },
    "data_status": {
      "type": "string"
    },
    "geo_id": {
      "type": "string"
    },
    "latest_data_period": {
      "format": "date",
      "type": [
        "string",
        "null"
      ]
    },
    "latest_scoreable_period": {
      "format": "date",
      "type": [
        "string",
        "null"
      ]
    },
    "missing_score_inputs": {
      "items": {
        "type": "string"
      },
      "type": "array"
    }
  },
  "required": [
    "geo_id",
    "latest_data_period",
    "latest_scoreable_period",
    "coverage",
    "available_metrics",
    "missing_score_inputs",
    "data_status"
  ],
  "type": "object"
}
```
