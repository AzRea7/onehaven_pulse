const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || process.env.API_BASE_URL || "http://localhost:8000";

const checks = [
  {
    name: "health",
    url: "/health",
    validate: (json) => {
      assert(json.status, "health.status missing");
      assert(json.database, "health.database missing");
    },
  },
  {
    name: "market detail us",
    url: "/markets/us",
    validate: (json) => {
      assert(json.market?.geo_id === "us", "market.geo_id should be us");
      assert("confidence_score" in json, "confidence_score missing");
      assert(Array.isArray(json.source_freshness), "source_freshness should be array");
    },
  },
  {
    name: "market detail detroit",
    url: "/markets/metro_19820",
    validate: (json) => {
      assert(json.market?.geo_id === "metro_19820", "market.geo_id should be metro_19820");
      assert("score_breakdown" in json, "score_breakdown missing");
    },
  },
  {
    name: "market coverage detroit",
    url: "/markets/metro_19820/coverage",
    validate: (json) => {
      assert(json.geo_id === "metro_19820", "coverage geo_id mismatch");
      assert(typeof json.coverage?.price === "boolean", "coverage.price should be boolean");
      assert(Array.isArray(json.available_metrics), "available_metrics should be array");
    },
  },
  {
    name: "market context detroit",
    url: "/markets/metro_19820/context",
    validate: (json) => {
      assert(json.market, "context.market missing");
      assert(json.evidence, "context.evidence missing");
      assert(Array.isArray(json.risks), "context.risks should be array");
    },
  },
  {
    name: "timeseries us",
    url: "/markets/us/timeseries?metrics=home_price_yoy,rent_yoy,mortgage_rate_30y,unemployment_rate,composite_cycle_score&start_date=2024-01-01",
    validate: (json) => {
      assert(Array.isArray(json.items), "timeseries.items should be array");
      assert(json.items.length > 0, "timeseries.items empty");
      assert("period_month" in json.items[0], "timeseries item period_month missing");
      assert("values" in json.items[0], "timeseries item values missing");
    },
  },
  {
    name: "map markets",
    url: "/map/markets?geo_type=metro&metric=building_permits",
    validate: (json) => {
      assert(json.type === "FeatureCollection", "map type should be FeatureCollection");
      assert(Array.isArray(json.features), "map features should be array");
      assert(json.features.length > 0, "map features empty");
      assert(json.features[0].properties?.geo_id, "map feature geo_id missing");
    },
  },
  {
    name: "compare markets",
    url: "/compare/markets?geo_ids=us,metro_19820&metrics=home_price_yoy,rent_yoy,payment_to_income_ratio,unemployment_rate,composite_cycle_score&start_date=2024-01-01",
    validate: (json) => {
      assert(Array.isArray(json.markets), "compare.markets should be array");
      assert(Array.isArray(json.latest), "compare.latest should be array");
      assert(Array.isArray(json.timeseries), "compare.timeseries should be array");
      assert(json.timeseries.length > 0, "compare.timeseries empty");
      assert("markets" in json.timeseries[0], "compare timeseries item should have markets object");
    },
  },
  {
    name: "screener",
    url: "/markets/screener?geo_type=metro&min_confidence=0.5&limit=10",
    validate: (json) => {
      assert(Array.isArray(json.items), "screener.items should be array");
      assert(typeof json.total === "number", "screener.total should be number");
      if (json.items.length > 0) {
        assert(json.items[0].market?.geo_id, "screener first item market.geo_id missing");
        assert(typeof json.items[0].values === "object", "screener item values missing");
      }
    },
  },
  {
    name: "source freshness",
    url: "/audit/source-freshness",
    validate: (json) => {
      assert(Array.isArray(json.items), "source freshness items should be array");
      assert(json.items.length > 0, "source freshness empty");
      assert(json.items[0].source, "source freshness source missing");
      assert(json.items[0].dataset, "source freshness dataset missing");
    },
  },
];

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

async function main() {
  console.log("== OneHaven API contract validation ==");
  console.log(`API_BASE_URL=${API_BASE_URL}`);
  console.log("");

  for (const check of checks) {
    const url = `${API_BASE_URL}${check.url}`;
    process.stdout.write(`-- ${check.name}: `);

    const response = await fetch(url);

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`${check.name} failed HTTP ${response.status}: ${text}`);
    }

    const json = await response.json();
    check.validate(json);
    console.log("ok");
  }

  console.log("");
  console.log("API contract validation passed.");
}

main().catch((error) => {
  console.error("");
  console.error(error);
  process.exit(1);
});
