import { z } from "zod";

const clientEnvSchema = z.object({
  NEXT_PUBLIC_API_URL: z.string().url(),
});

const parsedEnv = clientEnvSchema.safeParse({
  NEXT_PUBLIC_API_URL:
    process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000",
});

if (!parsedEnv.success) {
  throw new Error(
    `Invalid frontend environment variables: ${parsedEnv.error.message}`,
  );
}

export const config = {
  apiUrl: parsedEnv.data.NEXT_PUBLIC_API_URL,
};
