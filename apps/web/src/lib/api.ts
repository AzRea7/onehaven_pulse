
export type GeographyIdentity = {
  geo_id: string;
  geo_type: string;
  name: string;
  display_name: string;
  state_code?: string | null;
  state_name?: string | null;
  county_fips?: string | null;
  cbsa_code?: string | null;
  place_fips?: string | null;
  zcta?: string | null;
  country_code?: string | null;
  latitude?: number | null;
  longitude?: number | null;
  hierarchy_level?: number | null;
  canonical_slug?: string | null;
};

export type GeographyRelationshipItem = {
  parent: GeographyIdentity;
  child: GeographyIdentity;
  relationship_type: string;
  source: string;
  confidence_score: number | string;
  overlap_ratio?: number | string | null;
  is_active: boolean;
};

export type GeographyChildrenResponse = {
  geo_id: string;
  relationship_type: string;
  child_geo_type?: string | null;
  items: GeographyRelationshipItem[];
};

export type GeographyParentsResponse = {
  geo_id: string;
  relationship_type: string;
  parent_geo_type?: string | null;
  items: GeographyRelationshipItem[];
};

export type GeographyRelatedResponse = {
  geo_id: string;
  relationship_type: string;
  parent_geo_type?: string | null;
  child_geo_type?: string | null;
  parents: GeographyRelationshipItem[];
  children: GeographyRelationshipItem[];
};

import axios, { AxiosError } from "axios";
import { z } from "zod";

import { config } from "./config";

export const apiErrorPayloadSchema = z.object({
  error: z.object({
    code: z.string(),
    message: z.string(),
    details: z.unknown().nullable().optional(),
    request_id: z.string().nullable().optional(),
  }),
});

export type ApiErrorPayload = z.infer<typeof apiErrorPayloadSchema>;

export class ApiClientError extends Error {
  code: string;
  status?: number;
  requestId?: string | null;
  details?: unknown;

  constructor({
    message,
    code,
    status,
    requestId,
    details,
  }: {
    message: string;
    code: string;
    status?: number;
    requestId?: string | null;
    details?: unknown;
  }) {
    super(message);
    this.name = "ApiClientError";
    this.code = code;
    this.status = status;
    this.requestId = requestId;
    this.details = details;
  }
}

export const apiClient = axios.create({
  baseURL: config.apiUrl,
  timeout: 15000,
  headers: {
    "Content-Type": "application/json",
  },
});

apiClient.interceptors.response.use(
  (response) => response,
  (error: AxiosError) => {
    const status = error.response?.status;

    const parsed = apiErrorPayloadSchema.safeParse(error.response?.data);

    if (parsed.success) {
      throw new ApiClientError({
        message: parsed.data.error.message,
        code: parsed.data.error.code,
        status,
        requestId: parsed.data.error.request_id,
        details: parsed.data.error.details,
      });
    }

    throw new ApiClientError({
      message: error.message || "API request failed.",
      code: "api_request_failed",
      status,
    });
  },
);

export const healthResponseSchema = z.object({
  status: z.string(),
  service: z.string().optional(),
  version: z.string().optional(),
  environment: z.string().optional(),
  database: z.string().optional(),
});

export type HealthResponse = z.infer<typeof healthResponseSchema>;

export async function getHealth(): Promise<HealthResponse> {
  const response = await apiClient.get("/health");
  return healthResponseSchema.parse(response.data);
}

export const sourceFreshnessItemSchema = z.object({
  source: z.string(),
  dataset: z.string(),
  expected_frequency: z.string().optional(),
  freshness_threshold_days: z.number().optional(),
  latest_source_period: z.string().nullable().optional(),
  last_loaded_at: z.string().nullable().optional(),
  last_status: z.string().nullable().optional(),
  is_stale: z.boolean().nullable().optional(),
  stale_reason: z.string().nullable().optional(),
  record_count: z.number().nullable().optional(),
  error_message: z.string().nullable().optional(),
});

export const sourceFreshnessResponseSchema = z.object({
  items: z.array(sourceFreshnessItemSchema),
});

export type SourceFreshnessItem = z.infer<typeof sourceFreshnessItemSchema>;

export async function getSourceFreshness(): Promise<SourceFreshnessItem[]> {
  const response = await apiClient.get("/audit/source-freshness");
  return sourceFreshnessResponseSchema.parse(response.data).items;
}

export const marketIdentitySchema = z.object({
  geo_id: z.string(),
  geo_type: z.string(),
  name: z.string(),
  display_name: z.string().nullable().optional(),
  state_code: z.string().nullable().optional(),
  state_name: z.string().nullable().optional(),
  county_fips: z.string().nullable().optional(),
  cbsa_code: z.string().nullable().optional(),
  zcta: z.string().nullable().optional(),
  country_code: z.string().nullable().optional(),
  latitude: z.number().nullable().optional(),
  longitude: z.number().nullable().optional(),
});

export type MarketIdentity = z.infer<typeof marketIdentitySchema>;

export const metricValueSchema = z.object({
  metric: z.string().nullable().optional(),
  value: z.number().nullable().optional(),
});

export const inventoryConditionSchema = z.object({
  active_listings_yoy: z.number().nullable().optional(),
  months_supply: z.number().nullable().optional(),
  median_days_on_market: z.number().nullable().optional(),
  condition: z.string().nullable().optional(),
});

export const scoreBreakdownSchema = z.object({
  composite_cycle_score: z.number().nullable().optional(),
  price_momentum: z.number().nullable().optional(),
  rent_momentum: z.number().nullable().optional(),
  inventory_tightness: z.number().nullable().optional(),
  affordability: z.number().nullable().optional(),
  labor_market: z.number().nullable().optional(),
  data_completeness: z.number().nullable().optional(),
});

export const marketDetailResponseSchema = z.object({
  market: marketIdentitySchema,
  latest_period: z.string().nullable(),
  cycle_phase: z.string().nullable(),
  confidence_score: z.number().nullable(),
  investor_signal: z.string().nullable(),
  price_growth: metricValueSchema.nullable().optional(),
  rent_growth: metricValueSchema.nullable().optional(),
  inventory_condition: inventoryConditionSchema.nullable().optional(),
  score_breakdown: scoreBreakdownSchema.nullable().optional(),
  source_freshness: z.array(sourceFreshnessItemSchema).optional(),
  quality_flags: z.record(z.string(), z.unknown()).nullable().optional(),
  source_flags: z.record(z.string(), z.unknown()).nullable().optional(),
});

export type MarketDetailResponse = z.infer<typeof marketDetailResponseSchema>;

export async function getMarketDetail(
  geoId: string,
): Promise<MarketDetailResponse> {
  const response = await apiClient.get(`/markets/${geoId}`);
  return marketDetailResponseSchema.parse(response.data);
}

export const marketContextRiskSchema = z.object({
  code: z.string(),
  severity: z.string(),
  message: z.string(),
});

export const marketContextResponseSchema = z.object({
  mcp: z
    .object({
      tool_name: z.string(),
      resource_type: z.string(),
      resource_id: z.string(),
      schema_version: z.string(),
    })
    .optional(),
  market: z.string(),
  geo_id: z.string().optional(),
  latest_period: z.string().nullable(),
  latest_data_period: z.string().nullable().optional(),
  data_status: z.string().nullable().optional(),
  cycle_phase: z.string().nullable(),
  investor_signal: z.string().nullable(),
  confidence_score: z.number().nullable().optional(),
  evidence: z.object({
    price_growth_yoy: z.number().nullable().optional(),
    price_growth_metric: z.string().nullable().optional(),
    rent_growth_yoy: z.number().nullable().optional(),
    rent_growth_metric: z.string().nullable().optional(),
    inventory_trend: z.string().nullable().optional(),
    active_listings_yoy: z.number().nullable().optional(),
    months_supply: z.number().nullable().optional(),
    median_days_on_market: z.number().nullable().optional(),
    affordability: z.string().nullable().optional(),
    payment_to_income_ratio: z.number().nullable().optional(),
    price_to_income_ratio: z.number().nullable().optional(),
    unemployment_rate: z.number().nullable().optional(),
    building_permits: z.number().nullable().optional(),
    composite_cycle_score: z.number().nullable().optional(),
  }),
  coverage: z
    .object({
      price: z.boolean(),
      rent: z.boolean(),
      inventory: z.boolean(),
      affordability: z.boolean(),
      labor: z.boolean(),
      permits: z.boolean(),
    })
    .optional(),
  risks: z.array(marketContextRiskSchema),
  data_quality: z.record(z.string(), z.unknown()).optional(),
});

export type MarketContextResponse = z.infer<typeof marketContextResponseSchema>;

export async function getMarketContext(
  geoId: string,
): Promise<MarketContextResponse> {
  const response = await apiClient.get(`/markets/${geoId}/context`);
  return marketContextResponseSchema.parse(response.data);
}

export const marketCoverageResponseSchema = z.object({
  geo_id: z.string(),
  latest_data_period: z.string().nullable(),
  latest_scoreable_period: z.string().nullable(),
  coverage: z.object({
    price: z.boolean(),
    rent: z.boolean(),
    inventory: z.boolean(),
    affordability: z.boolean(),
    labor: z.boolean(),
    permits: z.boolean(),
  }),
  available_metrics: z.array(z.string()),
  missing_score_inputs: z.array(z.string()),
  data_status: z.string(),
});

export type MarketCoverageResponse = z.infer<typeof marketCoverageResponseSchema>;

export async function getMarketCoverage(
  geoId: string,
): Promise<MarketCoverageResponse> {
  const response = await apiClient.get(`/markets/${geoId}/coverage`);
  return marketCoverageResponseSchema.parse(response.data);
}


export const marketTimeSeriesItemSchema = z.object({
  period_month: z.string(),
  values: z.record(z.string(), z.number().nullable()),
  missing_metrics: z.array(z.string()),
});

export const marketTimeSeriesResponseSchema = z.object({
  market: marketIdentitySchema,
  metrics: z.array(z.string()),
  start_date: z.string().nullable().optional(),
  end_date: z.string().nullable().optional(),
  date_window_source: z.string().nullable().optional(),
  items: z.array(marketTimeSeriesItemSchema),
});

export type MarketTimeSeriesItem = z.infer<typeof marketTimeSeriesItemSchema>;
export type MarketTimeSeriesResponse = z.infer<
  typeof marketTimeSeriesResponseSchema
>;

export async function getMarketTimeSeries({
  geoId,
  metrics,
  startDate,
  endDate,
}: {
  geoId: string;
  metrics: string[];
  startDate?: string;
  endDate?: string;
}): Promise<MarketTimeSeriesResponse> {
  const params = new URLSearchParams({
    metrics: metrics.join(","),
  });

  if (startDate) {
    params.set("start_date", startDate);
  }

  if (endDate) {
    params.set("end_date", endDate);
  }

  const response = await apiClient.get(
    `/markets/${geoId}/timeseries?${params.toString()}`,
  );

  return marketTimeSeriesResponseSchema.parse(response.data);
}

export const marketListItemSchema = marketIdentitySchema;

export const marketListResponseSchema = z.object({
  items: z.array(marketListItemSchema),
  total: z.number(),
  limit: z.number(),
  offset: z.number(),
});

export type MarketListItem = z.infer<typeof marketListItemSchema>;
export type MarketListResponse = z.infer<typeof marketListResponseSchema>;

export async function searchMarkets({
  search,
  geoType,
  limit = 20,
  offset = 0,
}: {
  search?: string;
  geoType?: string;
  limit?: number;
  offset?: number;
}): Promise<MarketListResponse> {
  const params = new URLSearchParams({
    limit: String(limit),
    offset: String(offset),
  });

  if (search) {
    params.set("search", search);
  }

  if (geoType) {
    params.set("geo_type", geoType);
  }

  const response = await apiClient.get(`/markets?${params.toString()}`);
  return marketListResponseSchema.parse(response.data);
}

export const compareLatestItemSchema = z.object({
  geo_id: z.string(),
  latest_period: z.string().nullable(),
  latest_data_period: z.string().nullable().optional(),
  data_status: z.string().nullable().optional(),
  cycle_phase: z.string().nullable(),
  investor_signal: z.string().nullable(),
  confidence_score: z.number().nullable(),
  values: z.record(z.string(), z.number().nullable()),
  missing_metrics: z.array(z.string()),
});

export const compareTimeseriesItemSchema = z.object({
  period_month: z.string(),
  markets: z.record(
    z.string(),
    z.record(z.string(), z.number().nullable()),
  ),
  missing: z.record(z.string(), z.array(z.string())).optional(),
});

export const marketCompareResponseSchema = z.object({
  markets: z.array(marketIdentitySchema),
  metrics: z.array(z.string()),
  start_date: z.string().nullable().optional(),
  end_date: z.string().nullable().optional(),
  date_window_source: z.string().nullable().optional(),
  latest: z.array(compareLatestItemSchema),
  timeseries: z.array(compareTimeseriesItemSchema),
  invalid_geo_ids: z.array(z.string()).optional(),
});

export type CompareLatestItem = z.infer<typeof compareLatestItemSchema>;
export type CompareTimeseriesItem = z.infer<typeof compareTimeseriesItemSchema>;
export type MarketCompareResponse = z.infer<typeof marketCompareResponseSchema>;

export async function compareMarkets({
  geoIds,
  metrics,
  startDate,
  endDate,
}: {
  geoIds: string[];
  metrics: string[];
  startDate?: string;
  endDate?: string;
}): Promise<MarketCompareResponse> {
  const params = new URLSearchParams({
    geo_ids: geoIds.join(","),
    metrics: metrics.join(","),
  });

  if (startDate) {
    params.set("start_date", startDate);
  }

  if (endDate) {
    params.set("end_date", endDate);
  }

  const response = await apiClient.get(`/compare/markets?${params.toString()}`);
  return marketCompareResponseSchema.parse(response.data);
}


export const screenerMarketResultSchema = z.object({
  market: marketIdentitySchema,
  latest_period: z.string().nullable(),
  latest_data_period: z.string().nullable(),
  data_status: z.string().nullable(),
  cycle_phase: z.string().nullable(),
  investor_signal: z.string().nullable(),

  investor_stance: z.string().nullable().optional(),
  investor_stance_label: z.string().nullable().optional(),
  investor_stance_score: z.number().nullable().optional(),
  investor_signal_rule_version: z.string().nullable().optional(),
  material_missing_score_inputs: z.boolean().nullable().optional(),

  confidence_score: z.number().nullable(),
  values: z.record(z.string(), z.number().nullable()),
  missing_metrics: z.array(z.string()),
});

export const marketScreenerResponseSchema = z.object({
  items: z.array(screenerMarketResultSchema),
  total: z.number(),
  limit: z.number(),
  offset: z.number(),
  filters: z.record(z.string(), z.union([z.string(), z.number(), z.null()]).optional()),
});

export type ScreenerMarketResult = z.infer<typeof screenerMarketResultSchema>;
export type MarketScreenerResponse = z.infer<typeof marketScreenerResponseSchema>;

export async function screenMarkets({
  geoType = "metro",
  state,
  cyclePhase,
  investorSignal,
  minConfidence,
  minPriceGrowth,
  maxPriceGrowth,
  minRentGrowth,
  maxInventoryGrowth,
  maxPaymentToIncome,
  limit = 50,
  offset = 0,
}: {
  geoType?: string;
  state?: string;
  cyclePhase?: string;
  investorSignal?: string;
  minConfidence?: number;
  minPriceGrowth?: number;
  maxPriceGrowth?: number;
  minRentGrowth?: number;
  maxInventoryGrowth?: number;
  maxPaymentToIncome?: number;
  limit?: number;
  offset?: number;
}): Promise<MarketScreenerResponse> {
  const params = new URLSearchParams({
    limit: String(limit),
    offset: String(offset),
  });

  if (geoType) params.set("geo_type", geoType);
  if (state) params.set("state", state);
  if (cyclePhase) params.set("cycle_phase", cyclePhase);
  if (investorSignal) params.set("investor_signal", investorSignal);
  if (minConfidence !== undefined) params.set("min_confidence", String(minConfidence));
  if (minPriceGrowth !== undefined) params.set("min_price_growth", String(minPriceGrowth));
  if (maxPriceGrowth !== undefined) params.set("max_price_growth", String(maxPriceGrowth));
  if (minRentGrowth !== undefined) params.set("min_rent_growth", String(minRentGrowth));
  if (maxInventoryGrowth !== undefined) params.set("max_inventory_growth", String(maxInventoryGrowth));
  if (maxPaymentToIncome !== undefined) params.set("max_payment_to_income", String(maxPaymentToIncome));

  const response = await apiClient.get(`/markets/screener?${params.toString()}`);
  return marketScreenerResponseSchema.parse(response.data);
}


export const geoJsonGeometrySchema = z.object({
  type: z.string(),
  coordinates: z.unknown(),
});

export const marketMapFeatureSchema = z.object({
  type: z.literal("Feature"),
  geometry: geoJsonGeometrySchema.nullable(),
  properties: z.object({
    geo_id: z.string(),
    geo_type: z.string(),
    name: z.string(),
    display_name: z.string().nullable().optional(),
    state_code: z.string().nullable().optional(),
    state_name: z.string().nullable().optional(),
    county_fips: z.string().nullable().optional(),
    cbsa_code: z.string().nullable().optional(),
    zcta: z.string().nullable().optional(),
    country_code: z.string().nullable().optional(),
    period_month: z.string().nullable().optional(),
    metric: z.string(),
    value: z.number().nullable(),
    cycle_phase: z.string().nullable(),
    investor_signal: z.string().nullable(),
    data_status: z.string().nullable().optional(),
  }),
});

export const marketMapResponseSchema = z.object({
  type: z.literal("FeatureCollection"),
  features: z.array(marketMapFeatureSchema),
});

export type MarketMapFeature = z.infer<typeof marketMapFeatureSchema>;
export type MarketMapResponse = z.infer<typeof marketMapResponseSchema>;

export async function getMarketMap({
  geoType = "metro",
  metric = "composite_cycle_score",
  periodMonth,
  state,
}: {
  geoType?: string;
  metric?: string;
  periodMonth?: string;
  state?: string;
}): Promise<MarketMapResponse> {
  const params = new URLSearchParams({
    geo_type: geoType,
    metric,
  });

  if (periodMonth) {
    params.set("period_month", periodMonth);
  }

  if (state) {
    params.set("state", state.toUpperCase());
  }

  const response = await apiClient.get(`/map/markets?${params.toString()}`);
  return marketMapResponseSchema.parse(response.data);
}


export function getErrorMessage(error: unknown): string {
  if (error instanceof ApiClientError) {
    return error.message;
  }

  if (error instanceof Error) {
    return error.message;
  }

  return "Something went wrong.";
}

function buildQuery(params: Record<string, string | number | boolean | null | undefined>): string {
  const query = new URLSearchParams();

  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== "") {
      query.set(key, String(value));
    }
  }

  const serialized = query.toString();
  return serialized ? `?${serialized}` : "";
}

const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000";

async function apiFetch<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(`API request failed: ${response.status} ${response.statusText}`);
  }

  return response.json() as Promise<T>;
}

export async function getGeographyChildren(
  geoId: string,
  params: {
    relationship_type?: string;
    child_geo_type?: string | null;
  } = {},
): Promise<GeographyChildrenResponse> {
  const query = buildQuery({
    relationship_type: params.relationship_type ?? "contains",
    child_geo_type: params.child_geo_type,
  });

  return apiFetch<GeographyChildrenResponse>(`/geographies/${encodeURIComponent(geoId)}/children${query}`);
}

export async function getGeographyParents(
  geoId: string,
  params: {
    relationship_type?: string;
    parent_geo_type?: string | null;
  } = {},
): Promise<GeographyParentsResponse> {
  const query = buildQuery({
    relationship_type: params.relationship_type ?? "contains",
    parent_geo_type: params.parent_geo_type,
  });

  return apiFetch<GeographyParentsResponse>(`/geographies/${encodeURIComponent(geoId)}/parents${query}`);
}

export async function getGeographyRelated(
  geoId: string,
  params: {
    relationship_type?: string;
    parent_geo_type?: string | null;
    child_geo_type?: string | null;
  } = {},
): Promise<GeographyRelatedResponse> {
  const query = buildQuery({
    relationship_type: params.relationship_type ?? "contains",
    parent_geo_type: params.parent_geo_type,
    child_geo_type: params.child_geo_type,
  });

  return apiFetch<GeographyRelatedResponse>(`/geographies/${encodeURIComponent(geoId)}/related${query}`);
}

export type GeographySearchItem = {
  geography: GeographyIdentity;
  parent_count: number;
  child_count: number;
};

export type GeographySearchResponse = {
  q?: string | null;
  geo_type?: string | null;
  limit: number;
  items: GeographySearchItem[];
};

export async function searchGeographies(
  params: {
    q?: string | null;
    geo_type?: string | null;
    limit?: number;
  } = {},
): Promise<GeographySearchResponse> {
  const query = buildQuery({
    q: params.q,
    geo_type: params.geo_type,
    limit: params.limit ?? 20,
  });

  return apiFetch<GeographySearchResponse>(`/geographies/search${query}`);
}

// Story 12.3 — Investor Signal

export type InvestorStance =
  | "attractive"
  | "watchlist"
  | "mixed"
  | "avoid"
  | "insufficient_data";

export type InvestorDimensionStatus =
  | "positive"
  | "neutral"
  | "negative"
  | "missing";

export type InvestorSignalSeverity = "low" | "medium" | "high";

export type InvestorSignalEvidence = {
  metric_name: string;
  value: unknown;
  period: string | null;
  interpretation: string;
};

export type InvestorSignalDriver = {
  name: string;
  status: InvestorDimensionStatus;
  message: string;
  evidence: InvestorSignalEvidence[];
};

export type InvestorSignalRisk = {
  name: string;
  severity: InvestorSignalSeverity;
  message: string;
  evidence: InvestorSignalEvidence[];
};

export type InvestorMarketSignal = {
  geo_id: string;
  stance: InvestorStance;
  stance_label: string;
  stance_score: number | null;
  stance_reason: string;
  rule_version: string;
  confidence_score: number | null;
  latest_data_period: string | null;
  latest_scoreable_period: string | null;
  required_coverage_present: boolean;
  material_missing_score_inputs: boolean;
  coverage: Record<string, boolean>;
  available_metrics: string[];
  missing_score_inputs: string[];
  dimension_statuses: Record<string, InvestorDimensionStatus>;
  drivers: InvestorSignalDriver[];
  risks: InvestorSignalRisk[];
  rule_trace: string[];
  deterministic: boolean;
};

export async function getInvestorSignal(
  geoId: string,
): Promise<InvestorMarketSignal> {
  const response = await apiClient.get(
    `/markets/${encodeURIComponent(geoId)}/investor-signal`,
  );
  return response.data as InvestorMarketSignal;
}
