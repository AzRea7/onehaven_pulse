import axios from "axios";
import { z } from "zod";

import { config } from "./config";

export const apiClient = axios.create({
  baseURL: config.apiUrl,
  timeout: 15000,
  headers: {
    "Content-Type": "application/json",
  },
});

export const healthResponseSchema = z.object({
  status: z.string(),
  service: z.string().optional(),
});

export type HealthResponse = z.infer<typeof healthResponseSchema>;

export async function getHealth(): Promise<HealthResponse> {
  const response = await apiClient.get("/health");
  return healthResponseSchema.parse(response.data);
}
