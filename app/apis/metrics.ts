import { Api, MooseCache } from "@514labs/moose-lib";
import { PlaybackSpanAggregatedMV } from "../views/metricPlaybackSpanAggregated";
import { tags } from "typia";

interface PlaybackSpanQueryParams {
  appName?: string;
  stage?: "production" | "staging" | "development";
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
    if (startDate) conditions.push(`eventDate >= '${startDate}'`);
    if (endDate) conditions.push(`eventDate <= '${endDate}'`);
    const whereClause =
      conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

    const query = sql`
      SELECT
        toString(eventDate) as eventDate,
        appName,
        stage,
        videoId,
        sessionId,
        spans,
        round(avgDurationMs, 2) as avgDurationMs
      FROM ${PlaybackSpanAggregatedMV.targetTable}
      ${whereClause}
      ORDER BY eventDate DESC, spans DESC
      LIMIT ${limit}
    `;

    const data = await client.query.execute<PlaybackSpanRow>(query);
    const result: PlaybackSpanRow[] = await data.json();
    await cache.set(cacheKey, result, 600);
    return result;
  }
);
