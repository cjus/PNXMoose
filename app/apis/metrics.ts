import { Api, MooseCache } from "@514labs/moose-lib";
import { tags } from "typia";

interface PlaybackSpanQueryParams {
  appName?: string;
  stage?: string;
  videoId?: string;
  sessionId?: string;
  startDate?: string; // YYYY-MM-DD
  endDate?: string; // YYYY-MM-DD
  limit?: number & tags.Type<"int32">;
}

interface PlaybackSpanRow {
  eventDate: string;
  appName: string;
  stage: string;
  videoId: string;
  sessionId: string;
  spans: number;
  avgDurationMs: number;
}

export const PlaybackSpanApi = new Api<
  PlaybackSpanQueryParams,
  PlaybackSpanRow[]
>(
  "metrics/playback-span",
  async (
    { appName, stage, videoId, sessionId, startDate, endDate, limit = 200 },
    { client, sql }
  ) => {
    const cache = await MooseCache.get();
    const cacheKey = `metrics:playback-span:${appName || "all"}:${
      stage || "all"
    }:${videoId || "all"}:${sessionId || "all"}:${startDate || ""}:${
      endDate || ""
    }:${limit}`;

    const cached = await cache.get<PlaybackSpanRow[]>(cacheKey);
    if (cached && Array.isArray(cached) && cached.length > 0) return cached;

    const conditions: string[] = [];
    if (appName) conditions.push(`appName = '${appName}'`);
    if (stage) conditions.push(`stage = '${stage}'`);
    if (videoId) conditions.push(`videoId = '${videoId}'`);
    if (sessionId) conditions.push(`sessionId = '${sessionId}'`);
    if (startDate) conditions.push(`toDate(timestamp) >= '${startDate}'`);
    if (endDate) conditions.push(`toDate(timestamp) <= '${endDate}'`);
    const query = sql`
      SELECT
        toString(toDate(timestamp)) as eventDate,
        appName,
        stage,
        videoId,
        sessionId,
        toInt32(count(*)) as spans,
        round(avg(durationMs), 2) as avgDurationMs
      FROM PlaybackSpanEvent
      ${conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : ""}
      GROUP BY toDate(timestamp), appName, stage, videoId, sessionId
      ORDER BY eventDate DESC, spans DESC
      LIMIT 30
    `;

    try {
      const data = await client.query.execute<PlaybackSpanRow>(query);
      const result: PlaybackSpanRow[] = await data.json();
      await cache.set(cacheKey, result, 600);
      return result;
    } catch (error) {
      console.error("Metrics API error:", error);
      // Return empty array if table doesn't exist or query fails
      return [];
    }
  }
);
